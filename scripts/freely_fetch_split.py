#!/usr/bin/env python3
import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

FREELY_API = "https://www.freely.co.uk/api/tv-guide"

# Local image placeholders (the workflow step creates these files)
PROG_PLACEHOLDER = "img/programmes/placeholder.svg"
CHAN_PLACEHOLDER = "img/channels/placeholder.svg"

_iso_dur_re = re.compile(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?")

# --- Helpers ---------------------------------------------------------------

def _iso_to_minutes(s: str):
    m = _iso_dur_re.fullmatch(s or "")
    if not m:
        return None
    h = int(m.group(1) or 0)
    mi = int(m.group(2) or 0)
    se = int(m.group(3) or 0)
    return h * 60 + mi + (se // 60)

def slugify(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-") or "channel"


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def read_config(config_path: Optional[Path]) -> Dict[str, Any]:
    if not config_path:
        return {}
    with open(config_path, "r", encoding="utf-8") as f:
        import yaml  # lightweight, only used when config provided
        return yaml.safe_load(f) or {}


# --- Compatibility layer (best‑effort mapping to older Freesat/Freeview shape)
# Target shape (per channel file):
#   {
#     "channel": {"id": "560", "name": "BBC One"},
#     "events": [
#        {"startTime": 1696165200, "duration": 60, "name": "Title",
#         "description": "...", "image": "/path/or/url.jpg"},
#        ...
#     ],
#     "compat": {
#        "freesat_card": [ {"event": [...] } ]
#     }
#   }
# The extra compat.freesat_card[0].event mirrors the old sensor’s expectations
# (value_json.0.event), while preserving a clean top‑level shape.

Event = Dict[str, Any]
Channel = Dict[str, Any]


def _pick(d: Dict[str, Any], keys: List[str], default=None):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default


def normalise_event(ev: Dict[str, Any]) -> Event:
    # pick fields with the desired precedence:
    # name := main_title (fallbacks to name/title/etc)
    name_primary = _pick(ev, ["main_title", "name", "title", "programme", "program", "programmeTitle", "show"])

    # description := secondary_title (fallback to synopsis/description fields)
    desc_secondary = _pick(ev, ["secondary_title", "subtitle", "episode_title"])
    desc_fallback  = _pick(ev, ["description", "synopsis", "shortSynopsis", "longSynopsis", "summary"]) or ""

    start = _pick(ev, ["startTime", "start", "start_time", "start_timestamp", "time", "begin"])
    dur   = _pick(ev, ["duration", "durationMinutes", "duration_minutes", "dur", "length", "runtime"])

    # Prefer any image-like fields, but scrub remote URLs (we fill local/placeholder later)
    image = _pick(ev, ["image", "imageUrl", "image_url", "imageURL", "poster", "thumbnail", "fallback_image_url"]) or ""

    # ---- duration normalisation ----
    if isinstance(dur, (int, float)) and dur > 600:  # seconds -> minutes heuristic
        dur = round(dur / 60)
    if isinstance(dur, str) and dur.startswith("PT"):  # ISO8601 e.g. PT1H15M
        m = _iso_to_minutes(dur)
        if m is not None:
            dur = m

    # derive from numeric start/end if duration missing
    end = _pick(ev, ["endTime", "end", "end_time", "stop", "finish"])
    if dur is None and isinstance(start, (int, float)) and isinstance(end, (int, float)):
        dur = int(round((end - start) / 60))

    # ---- enforce local-only image later ----
    if isinstance(image, str) and image.strip().lower().startswith(("http://", "https://")):
        image = ""  # will become img/programmes/<hash>.* or placeholder in the CI step

    # scrub remote image fields from _raw
    raw = dict(ev)
    for k in ("image", "image_url", "imageUrl", "imageURL", "fallback_image_url"):
        raw.pop(k, None)

    return {
        "startTime": start,
        "duration": dur,
        "name": (name_primary or "")[:500],                 # guard against weirdly long titles
        "description": (desc_secondary or desc_fallback)[:2000],
        "image": image,
        "_raw": raw,
    }


def extract_channels(payload):
    # Freely: { status, data: { programs: [ ... ] } }
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict):
            progs = data.get("programs")
            if isinstance(progs, list) and progs and isinstance(progs[0], dict):
                return progs
    # fallback to old heuristics if needed
    if isinstance(payload, dict):
        for key in ("channels", "results", "items"):
            val = payload.get(key)
            if isinstance(val, list) and val and isinstance(val[0], dict):
                return val
    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        return payload
    return []


def extract_channel_id_name(ch):
    cid = _pick(ch, ["id", "channelId", "serviceId", "service_id", "sid", "uid", "service"])
    name = _pick(ch, ["name", "channelName", "title", "serviceName"]) or "Unknown"
    if cid is None:
        cid = slugify(str(name))
    return str(cid), str(name)


def extract_channel_logo(ch):
    return _pick(ch, [
        "logo", "logo_url", "logoUrl",
        "channelLogo", "channel_logo",
        "service_logo", "serviceLogo",
        "image", "image_url"
    ])


def extract_events(ch: Channel) -> List[Event]:
    for key in ("events", "event", "schedule", "schedules", "programmes", "programs"):
        v = ch.get(key)
        if isinstance(v, list):
            return [normalise_event(e) for e in v if isinstance(e, dict)]
    for key in ch.keys():
        v = ch[key]
        if isinstance(v, dict):
            for k2 in ("events", "event", "schedule"):
                v2 = v.get(k2)
                if isinstance(v2, list):
                    return [normalise_event(e) for e in v2 if isinstance(e, dict)]
    return []

class FreelyFetchError(Exception):
    pass

def write_error_marker(out_dir: Path, start: int, message: str) -> None:
    ensure_dir(out_dir)
    raw_dir = out_dir / "raw"
    ensure_dir(raw_dir)
    (raw_dir / f"guide_{start}_ERROR.txt").write_text(message, encoding="utf-8")

def fetch_freely(nid: str, start: int, session: Optional[requests.Session] = None, retries: int = 4, backoff: float = 1.7) -> Any:
    s = session or requests.Session()
    s.headers.update({
        "User-Agent": "elyobelyob-freely-split/1.0 (+https://github.com/elyobelyob/freely_tv_guide)",
        "Accept": "application/json",
        "Referer": "https://www.freely.co.uk/tv-guide",
        "Accept-Language": "en-GB,en;q=0.9",
    })

    params = {"nid": nid, "start": start}
    last_err = None

    for attempt in range(1, retries + 1):
        try:
            resp = s.get(FREELY_API, params=params, timeout=(5, 20))
            # Retry on transient codes
            if resp.status_code in (429, 502, 503, 504):
                raise FreelyFetchError(f"HTTP {resp.status_code} from Freely")
            # Try to parse JSON regardless of content-type (some servers mislabel)
            try:
                return resp.json()
            except Exception as je:
                text = (resp.text or "").strip()
                snippet = text[:240].replace("\n", " ")
                raise FreelyFetchError(
                    f"JSON decode failed (status={resp.status_code}, len={len(text)}): {snippet or '<empty response>'}"
                ) from je

        except (requests.RequestException, FreelyFetchError) as e:
            last_err = e
            if attempt >= retries:
                break
            time.sleep(backoff ** attempt)

    raise FreelyFetchError(f"Failed after {retries} attempts: {last_err}")



def write_outputs(payload: Any, out_dir: Path, start: int) -> Dict[str, Any]:
    ensure_dir(out_dir)
    raw_dir = out_dir / "raw"
    chan_dir = out_dir / "channels"
    ensure_dir(raw_dir)
    ensure_dir(chan_dir)

    # Save raw API payload
    raw_path = raw_dir / f"guide_{start}.json"
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    channels = extract_channels(payload)
    index = {"start": start, "channels": []}

    for ch in channels:
        cid, name = extract_channel_id_name(ch)
        events = extract_events(ch)

        # Force local-only event images BEFORE writing files
        for e in events:
            img = (e.get("image") or "").strip()
            if isinstance(img, str) and img.startswith(("http://", "https://")):
                e["image"] = PROG_PLACEHOLDER
            if not e.get("image"):
                e["image"] = PROG_PLACEHOLDER

        # Channel + logo (use local placeholder if remote)
        logo_src = extract_channel_logo(ch)
        channel_obj = {"id": cid, "name": name}
        if logo_src:
            channel_obj["logo"] = CHAN_PLACEHOLDER if str(logo_src).startswith(("http://","https://")) else str(logo_src)

        out_obj = {
            "channel": channel_obj,                              # keep logo field
            "events": events,
            "compat": {"freesat_card": [{"event": events}]},     # legacy card compat
        }

        # Write per-channel JSON
        chan_path = chan_dir / f"{cid}.json"
        with open(chan_path, "w", encoding="utf-8") as f:
            json.dump(out_obj, f, ensure_ascii=False, indent=2)

        # Index entry (include logo if present)
        entry = {"id": cid, "name": name, "path": f"channels/{cid}.json"}
        if channel_obj.get("logo"):
            entry["logo"] = channel_obj["logo"]
        index["channels"].append(entry)

    # Write index
    with open(out_dir / "index.json", "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

    return index


def main():
    ap = argparse.ArgumentParser(description="Fetch Freely guide and split into per-channel files")
    ap.add_argument("--nid", default=os.getenv("FREELY_NID", "64865"),
                    help="Network id (nid) for the Freely API (default: 64865)")
    ap.add_argument("--start", type=int, default=int(os.getenv("FREELY_START", "0") or 0),
                    help="UNIX timestamp (UTC) for the day start")
    ap.add_argument("--out", default=os.getenv("OUTPUT_DIR", "docs"),
                    help="Output folder (default: docs)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Fetch but do not write outputs")

    args = ap.parse_args()

    if not args.start:
        ap.error("--start is required (UNIX timestamp, seconds)")

    try:
        payload = fetch_freely(args.nid, args.start)
    except FreelyFetchError as e:
        # Log, drop a marker, and exit 0 so later workflow steps can still run
        msg = (f"[freely_fetch_split] {e}\n"
               f"url={FREELY_API}?nid={args.nid}&start={args.start}\n")
        print(msg, file=sys.stderr)
        write_error_marker(Path(args.out), args.start, msg)
        sys.exit(0)

    if args.dry_run:
        chs = extract_channels(payload)
        print(f"[freely_fetch_split] dry-run: channels={len(chs)} (no files written)")
        return

    index = write_outputs(payload, Path(args.out), args.start)
    print(f"[freely_fetch_split] wrote {len(index.get('channels', []))} channels to {args.out}/channels")

if __name__ == "__main__":
    main()


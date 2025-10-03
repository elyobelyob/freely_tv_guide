#!/usr/bin/env python3
import argparse
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

FREELY_API = "https://www.freely.co.uk/api/tv-guide"
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


def normalise_event(ev):
    start = _pick(ev, ["startTime", "start", "start_time", "start_timestamp", "time", "begin"])
    dur = _pick(ev, ["duration", "durationMinutes", "duration_minutes", "dur", "length", "runtime"])
    title = _pick(ev, ["name", "title", "main_title", "programme", "program", "programmeTitle", "show"])
    sub = _pick(ev, ["secondary_title", "subtitle", "episode_title"])
    if sub:
        title = f"{title}: {sub}" if title else sub
    desc = _pick(ev, ["description", "synopsis", "shortSynopsis", "longSynopsis", "summary"]) or ""
    image = _pick(ev, ["image", "imageUrl", "image_url", "imageURL", "poster", "thumbnail",
                       "image_url", "fallback_image_url"]) or ""

    # seconds→minutes heuristic
    if isinstance(dur, (int, float)) and dur > 600:
        dur = round(dur / 60)
    # ISO8601 duration like PT1H15M
    if isinstance(dur, str) and dur.startswith("PT"):
        m = _iso_to_minutes(dur)
        if m is not None:
            dur = m

    end = _pick(ev, ["endTime", "end", "end_time", "stop", "finish"])
    if dur is None and isinstance(start, (int, float)) and isinstance(end, (int, float)):
        dur = int(round((end - start) / 60))

    return {
        "startTime": start,          # ISO string is fine; HA's as_datetime handles it
        "duration": dur,             # now an int (minutes)
        "name": title or "",
        "description": desc,
        "image": image,
        "_raw": ev,
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
    # Try common keys for a channel logo
    return _pick(ch, [
        "logo", "logo_url", "logoUrl",
        "channelLogo", "channel_logo",
        "service_logo", "serviceLogo",
        "image", "image_url"  # fallback if they only provide one
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


def fetch_freely(nid: str, start: int, session: Optional[requests.Session] = None) -> Any:
    s = session or requests.Session()
    resp = s.get(FREELY_API, params={"nid": nid, "start": start}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def write_outputs(payload: Any, out_dir: Path, start: int) -> Dict[str, Any]:
    ensure_dir(out_dir)
    raw_dir = out_dir / "raw"
    chan_dir = out_dir / "channels"
    ensure_dir(raw_dir)
    ensure_dir(chan_dir)

    raw_path = raw_dir / f"guide_{start}.json"
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    channels = extract_channels(payload)
    index = {"start": start, "channels": []}

    for ch in channels:
        cid, name = extract_channel_id_name(ch)
        events = extract_events(ch)
        
    logo = extract_channel_logo(ch)
    channel_obj = {"id": cid, "name": name}
    if logo:
        channel_obj["logo"] = logo
    
    out_obj = {
        "channel": channel_obj,
        "events": events,
        "compat": {"freesat_card": [{"event": events}]},
    }

        
        out_obj = {
            "channel": {"id": cid, "name": name},
            "events": events,
            "compat": {"freesat_card": [{"event": events}]},
        }
        chan_path = chan_dir / f"{cid}.json"
        with open(chan_path, "w", encoding="utf-8") as f:
            json.dump(out_obj, f, ensure_ascii=False, indent=2)
        index["channels"].append({"id": cid, "name": name, "path": f"channels/{cid}.json"})

    with open(out_dir / "index.json", "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

    return index


def main():
    ap = argparse.ArgumentParser(description="Fetch Freely guide and split into per‑channel files")
    ap.add_argument("--nid", default=os.getenv("FREELY_NID", "64865"), help="Network id (nid) for the Freely API")
    ap.add_argument("--start", type=int, default=int(os.getenv("FREELY_START", "0") or 0), help="UNIX timestamp (UTC) for the day start")
    ap.add_argument("--out", default=os.getenv("OUTPUT_DIR", "docs"), help="Output folder (default: docs)")
    ap.add_argument("--dry-run", action="store_true", help="Fetch but do not write outputs")

    args = ap.parse_args()

    if not args.start:
        raise SystemExit("--start is required (UNIX timestamp for the day start)")

    payload = fetch_freely(args.nid, args.start)
    if args.dry_run:
        print(json.dumps(payload)[:2000])
        return

    index = write_outputs(payload, Path(args.out), args.start)
    print(json.dumps(index, indent=2))


if __name__ == "__main__":
    main()

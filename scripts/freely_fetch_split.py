#!/usr/bin/env python3
return payload
return []




def extract_channel_id_name(ch: Channel) -> Tuple[str, str]:
cid = _pick(ch, ["id", "channelId", "serviceId", "sid", "uid", "service"])
name = _pick(ch, ["name", "channelName", "title", "serviceName"]) or "Unknown"
if cid is None:
# build a stable id from name
cid = slugify(str(name))
return str(cid), str(name)




def extract_events(ch: Channel) -> List[Event]:
for key in ("events", "event", "schedule", "schedules", "programmes", "programs"):
v = ch.get(key)
if isinstance(v, list):
return [normalise_event(e) for e in v if isinstance(e, dict)]
# Sometimes events nested deeper
for key in ch.keys():
v = ch[key]
if isinstance(v, dict):
for k2 in ("events", "event", "schedule"):
v2 = v.get(k2)
if isinstance(v2, list):
return [normalise_event(e) for e in v2 if isinstance(e, dict)]
return []




# --- Fetch & Write --------------------------------------------------------


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


# write raw
raw_path = raw_dir / f"guide_{start}.json"
with open(raw_path, "w", encoding="utf-8") as f:
json.dump(payload, f, ensure_ascii=False, indent=2)


channels = extract_channels(payload)
index = {"start": start, "channels": []}


for ch in channels:
cid, name = extract_channel_id_name(ch)
events = extract_events(ch)
out_obj = {
"channel": {"id": cid, "name": name},
"events": events,
# compatibility block
"compat": {"freesat_card": [{"event": events}]},
}
# write file
chan_path = chan_dir / f"{cid}.json"
with open(chan_path, "w", encoding="utf-8") as f:
json.dump(out_obj, f, ensure_ascii=False, indent=2)
index["channels"].append({"id": cid, "name": name, "path": f"channels/{cid}.json"})


# write index
with open(out_dir / "index.json", "w", encoding="utf-8") as f:
json.dump(index, f, ensure_ascii=False, indent=2)


return index




# --- CLI -----------------------------------------------------------------


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

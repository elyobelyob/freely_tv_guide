# Freely TV Guide Splitter


Fetch the Freely TV-Guide API and publish **per‑channel JSON** files you can point Home Assistant REST sensors at (with a compatibility block mimicking the old Freesat/Freeview card shape).


## Quick start


```bash
# 1) clone your new repo and enter it
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt


# Example: 2025‑10‑03 00:00:00 UTC
python scripts/freely_fetch_split.py --nid 64865 --start 1759449600 --out docs

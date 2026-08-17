"""
Backfill cross-border physical flows (ENTSO-E A11) into electricity_crossborder_flows.

Why this exists
---------------
`electricity_crossborder_flows` has never held a row, so every question of the
form "did neighbours help or compete during this event?" was unanswerable. The
transmission edge function only ever fetches the latest window, which cannot
recover history. This walks the border list back over the period the rest of
the platform covers.

Physical flows are reported DIRECTIONALLY: a border carries two independent
series, and ENTSO-E returns points for a direction only while flow is actually
going that way. An interval absent from the A11 response is a genuine zero for
that direction, not missing data â€” so downstream net-flow arithmetic must treat
absent as 0 (COALESCE), and this script deliberately does not fabricate zero
rows for tens of millions of intervals.

Usage
-----
    python python/electricity_crossborder_backfill_entsoe.py
    python python/electricity_crossborder_backfill_entsoe.py --start 2026-05-01
    python python/electricity_crossborder_backfill_entsoe.py --pairs PL>CZ,CZ>PL

Environment: ENTSOE_API_TOKEN, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import os
import sys
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

ENTSOE_API = "https://web-api.tp.entsoe.eu/api"

DOMAINS: dict[str, str] = {
    "AT": "10YAT-APG------L", "BE": "10YBE----------2", "BG": "10YCA-BULGARIA-R",
    "CH": "10YCH-SWISSGRIDZ", "CZ": "10YCZ-CEPS-----N", "DE": "10Y1001A1001A83F",
    "DK1": "10YDK-1--------W", "DK2": "10YDK-2--------M", "EE": "10Y1001A1001A39I",
    "ES": "10YES-REE------0", "FI": "10YFI-1--------U", "FR": "10YFR-RTE------C",
    "GB": "10YGB----------A", "GR": "10YGR-HTSO-----Y", "HR": "10YHR-HEP------M",
    "HU": "10YHU-MAVIR----U", "IT": "10YIT-GRTN-----B", "LT": "10YLT-1001A0008Q",
    "LV": "10YLV-1001A00074", "NL": "10YNL----------L", "NO2": "10YNO-2--------T",
    "PL": "10YPL-AREA-----S", "PT": "10YPT-REN------W", "RO": "10YRO-TEL------P",
    "SE1": "10Y1001A1001A44P", "SE3": "10Y1001A1001A46L", "SE4": "10Y1001A1001A47J",
    "SI": "10YSI-ELES-----O", "SK": "10YSK-SEPS-----K",
}

# Mirrors TRANSMISSION_PAIRS in app.js so the map and the data agree.
PAIRS: list[tuple[str, str]] = [
    ("FR","DE"),("DE","FR"),("FR","BE"),("BE","FR"),
    ("FR","ES"),("ES","FR"),("FR","IT"),("IT","FR"),
    ("FR","GB"),("GB","FR"),("DE","AT"),("AT","DE"),
    ("DE","NL"),("NL","DE"),("DE","PL"),("PL","DE"),
    ("DE","CZ"),("CZ","DE"),("DE","DK1"),("DK1","DE"),
    ("DE","DK2"),("DK2","DE"),("BE","NL"),("NL","BE"),
    ("BE","GB"),("GB","BE"),("NL","GB"),("GB","NL"),
    ("AT","IT"),("IT","AT"),("AT","HU"),("HU","AT"),
    ("AT","CZ"),("CZ","AT"),("AT","SI"),("SI","AT"),
    ("ES","PT"),("PT","ES"),("CZ","SK"),("SK","CZ"),
    ("SK","HU"),("HU","SK"),("HU","RO"),("RO","HU"),
    ("HU","HR"),("HR","HU"),("RO","BG"),("BG","RO"),
    ("BG","GR"),("GR","BG"),("IT","SI"),("SI","IT"),
    ("IT","GR"),("GR","IT"),("SI","HR"),("HR","SI"),
    ("PL","CZ"),("CZ","PL"),("PL","SK"),("SK","PL"),
    ("NO2","NL"),("NL","NO2"),("NO2","DK1"),("DK1","NO2"),
    ("SE3","DK1"),("DK1","SE3"),("SE4","DK2"),("DK2","SE4"),
    ("FI","SE1"),("SE1","FI"),("FI","EE"),("EE","FI"),
    ("EE","LV"),("LV","EE"),("LV","LT"),("LT","LV"),
    ("LT","PL"),("PL","LT"),
]

RES_MINUTES = {"PT15M": 15, "PT30M": 30, "PT60M": 60, "P1D": 1440}


def ymdhm(d: datetime) -> str:
    return d.astimezone(timezone.utc).strftime("%Y%m%d%H%M")


def fetch_a11(token: str, out_dom: str, in_dom: str,
              start: datetime, end: datetime) -> str | None:
    """Returns XML, or None when ENTSO-E reports no matching data."""
    params = {
        "securityToken": token,
        "documentType": "A11",
        "out_Domain": out_dom,
        "in_Domain": in_dom,
        "periodStart": ymdhm(start),
        "periodEnd": ymdhm(end),
    }
    r = requests.get(ENTSOE_API, params=params, timeout=120)
    if r.status_code == 429:
        raise RuntimeError("429 rate limited")
    if r.status_code == 400 and "No matching data" in r.text:
        return None
    if not r.ok:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
    return r.text


def parse_points(xml: str) -> list[tuple[datetime, float]]:
    """Flatten every TimeSeries/Period/Point into (utc timestamp, MW)."""
    out: list[tuple[datetime, float]] = []
    root = ET.fromstring(xml)
    for period in root.iter():
        if not period.tag.endswith("Period"):
            continue
        start_el = period.find("./{*}timeInterval/{*}start")
        res_el = period.find("./{*}resolution")
        if start_el is None or res_el is None or not start_el.text:
            continue
        step = RES_MINUTES.get((res_el.text or "").strip())
        if not step:
            continue
        p_start = datetime.fromisoformat(start_el.text.replace("Z", "+00:00")).astimezone(timezone.utc)

        for pt in period.findall("./{*}Point"):
            pos_el = pt.find("./{*}position")
            qty_el = pt.find("./{*}quantity")
            if pos_el is None or qty_el is None:
                continue
            try:
                pos = int(pos_el.text)
                qty = float(qty_el.text)
            except (TypeError, ValueError):
                continue
            out.append((p_start + timedelta(minutes=(pos - 1) * step), qty))
    return out


def upsert(url: str, key: str, rows: list[dict]) -> None:
    endpoint = f"{url.rstrip('/')}/rest/v1/electricity_crossborder_flows?on_conflict=source,from_zone,to_zone,ts"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    r = requests.post(endpoint, headers=headers, json=rows, timeout=120)
    if not r.ok:
        raise RuntimeError(f"upsert HTTP {r.status_code}: {r.text[:300]}")


def quarters(start: datetime, end: datetime):
    """Yield [chunk_start, chunk_end) quarterly windows."""
    cur = start
    while cur < end:
        y, q0 = cur.year, (cur.month - 1) // 3
        nm = q0 * 3 + 4
        nxt = (datetime(y + 1, 1, 1, tzinfo=timezone.utc) if nm > 12
               else datetime(y, nm, 1, tzinfo=timezone.utc))
        yield cur, min(nxt, end)
        cur = nxt


def main() -> int:
    load_dotenv()
    token = os.getenv("ENTSOE_API_TOKEN")
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not token or not url or not key:
        print("Missing ENTSOE_API_TOKEN / SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY", file=sys.stderr)
        return 1

    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2021-01-01")
    ap.add_argument("--end", default="")
    ap.add_argument("--pairs", default="", help="comma list like PL>CZ,CZ>PL")
    ap.add_argument("--delay", type=float, default=0.2)

    ap.add_argument("--workers", type=int, default=6, help="parallel border pairs")
    args = ap.parse_args()

    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end = (datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc) if args.end
           else datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0))

    pairs = PAIRS
    if args.pairs:
        want = {tuple(p.split(">")) for p in args.pairs.split(",") if ">" in p}
        pairs = [p for p in PAIRS if p in want]

    windows = list(quarters(start, end))
    counters = {"rows": 0, "failed": 0, "skipped": 0, "done": 0}
    lock = threading.Lock()

    def do_pair(idx_pair):
        i, (a, b) = idx_pair
        if a not in DOMAINS or b not in DOMAINS:
            print(f"[{i}/{len(pairs)}] {a}->{b}: no EIC mapping, skipping", file=sys.stderr)
            return
        pair_rows = local_failed = local_skipped = 0
        for c_start, c_end in windows:
            try:
                xml = fetch_a11(token, DOMAINS[a], DOMAINS[b], c_start, c_end)
            except Exception as e:
                msg = str(e)
                print(f"  {a}->{b} {c_start.date()}: {msg}", file=sys.stderr)
                local_failed += 1
                time.sleep(10 if "429" in msg else 2)
                continue

            if xml is None:
                local_skipped += 1
                time.sleep(args.delay)
                continue

            try:
                pts = parse_points(xml)
            except ET.ParseError as e:
                print(f"  {a}->{b} {c_start.date()}: parse error {e}", file=sys.stderr)
                local_failed += 1
                continue

            rows = [{
                "ts": ts.isoformat().replace("+00:00", "Z"),
                "from_zone": a,
                "to_zone": b,
                "mw": mw,
                "source": "entsoe",
            } for ts, mw in pts if mw is not None]

            for j in range(0, len(rows), 500):
                try:
                    upsert(url, key, rows[j:j + 500])
                except Exception as e:
                    print(f"  {a}->{b} {c_start.date()}: {e}", file=sys.stderr)
                    local_failed += 1
                    break
            pair_rows += len(rows)
            time.sleep(args.delay)

        with lock:
            counters["rows"] += pair_rows
            counters["failed"] += local_failed
            counters["skipped"] += local_skipped
            counters["done"] += 1
            print(f"[{counters['done']}/{len(pairs)}] {a}->{b}: {pair_rows} points  "
                  f"(running total {counters['rows']})", flush=True)

    # Border pairs are independent, so they parallelise cleanly. Serially this
    # was ~2.2 min/pair => ~3 hours; ENTSO-E allows 400 requests/minute and a
    # few workers stay far below that.
    if args.workers > 1:
        with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
            list(ex.map(do_pair, enumerate(pairs, 1)))
    else:
        for item in enumerate(pairs, 1):
            do_pair(item)

    print(f"\nDone. rows={counters['rows']} empty_windows={counters['skipped']} "
          f"failures={counters['failed']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

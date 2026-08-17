"""
Population-weighted daily temperature + heatwave days for EU27 + UK/NO/CH.

Why this exists
---------------
The platform holds no weather data, so nothing could be said about how the
power and gas systems respond to heat. This builds the missing half: one row
per country per day carrying the population-weighted daily maximum
temperature, the local heatwave threshold, and heatwave event flags â€” shaped
to join straight onto `electricity_load_snapshots`, `gas_demand_daily` etc.

Method
------
Temperature: ERA5 reanalysis via the Open-Meteo archive API (free, no key).
Each country is represented by its largest metropolitan areas, weighted by
population, because electricity demand follows people rather than land area.
A national mean over sparsely populated territory would understate the heat
that the demand actually experiences.

Heatwave definition: the EEA / Copernicus convention (CTX90pct + duration),
also used in the Heat Wave Duration Index â€”

    a heatwave is >= MIN_RUN consecutive days on which the population-weighted
    daily maximum temperature exceeds the 90th percentile of daily maxima for
    that calendar day, computed over a 1991-2020 baseline using a +/- 7 day
    window around the calendar day.

The percentile baseline is deliberately relative, not absolute: 32 C is an
ordinary summer day in Seville and a serious heat event in Helsinki, and the
energy-system response is to the local anomaly, not the absolute number.

Usage
-----
    python python/weather_heatwave_days.py                 # build CSV
    python python/weather_heatwave_days.py --upload        # ...and upsert

Environment: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY (for --upload only).
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import mean

ARCHIVE_API = "https://archive-api.open-meteo.com/v1/archive"

# Baseline climatology. The p90 threshold has to come from a reference period —
# with only the analysis year the threshold would just be "the warmest 10% of
# that year", which is circular and not comparable across years.
#
# 1991-2020 is the WMO standard normal, but the +/- 7 day window means each
# calendar day's percentile pool is years x 15 days, so a shorter baseline is
# still statistically fine: 12 years gives ~180 samples per calendar day, ample
# for a 90th percentile, at a third of the download.
#
# The trade-off is real and worth stating: a recent baseline embeds recent
# warming, so thresholds sit higher and heatwave counts come out LOWER than
# against a 1991-2020 normal. Fine for "how does the system respond to heat",
# misleading for "is heat becoming more frequent" — use --baseline-start 1991
# for the latter.
DEFAULT_BASELINE_START, DEFAULT_BASELINE_END = 2014, 2025
BASELINE_START, BASELINE_END = DEFAULT_BASELINE_START, DEFAULT_BASELINE_END
WINDOW_DAYS = 7          # +/- days around the calendar day for the percentile
PERCENTILE = 0.90
MIN_RUN = 3              # consecutive hot days required to call it a heatwave

# Absolute floor, applied ALONGSIDE the percentile: any day at or above this
# counts as hot regardless of what the local percentile says.
#
# The percentile alone is purely relative, so in the hottest countries the bar
# climbs past the point where heat stops being an energy-system problem —
# Cyprus's mid-July p90 is 38.0 C and Spain's 36.0 C, meaning a 31 C day there
# registers as unremarkable. The floor only ever ADDS days, and only in
# countries whose threshold sits above it; where the p90 is already below 30 C
# (Poland 29.5, Finland 25.4) nothing changes.
ABSOLUTE_HOT_C = 30.0
# CTX90pct is a warm-SPELL index, not a heatwave index. Run year-round it flags
# any relative warm anomaly: a first pass gave Poland 26 "heatwave" days in
# February and an event peaking at 15.1 C. Those are real anomalies but the
# opposite of a heat event for the energy system — they cut heating demand, and
# they dragged the measured demand uplift negative. Heatwave events are
# therefore confined to the warm season, the convention Copernicus and the EEA
# use when the index is applied to heat impacts.
WARM_SEASON = (5, 9)     # May..September inclusive
SERIES_START = "2021-01-01"   # first day we keep (energy data starts here)
INTER_COUNTRY_SLEEP_S = 15    # Open-Meteo bills by response size; stay under the minutely cap

OUT_DIR = Path(__file__).parent / "data"
CACHE_DIR = Path(__file__).parent / "data" / "weather_cache"
OUT_CSV = OUT_DIR / "heatwave_country_daily.csv"

# Largest metro areas per country with rough populations (millions) used as
# weights. Three to four points capture most of the demand-relevant population
# without turning this into a gridded-reanalysis job.
CITIES: dict[str, list[tuple[str, float, float, float]]] = {
    "AT": [("Vienna", 48.21, 16.37, 2.0), ("Graz", 47.07, 15.44, 0.35), ("Linz", 48.31, 14.29, 0.28)],
    "BE": [("Brussels", 50.85, 4.35, 2.1), ("Antwerp", 51.22, 4.40, 1.2), ("Ghent", 51.05, 3.72, 0.56)],
    "BG": [("Sofia", 42.70, 23.32, 1.4), ("Plovdiv", 42.14, 24.75, 0.35), ("Varna", 43.20, 27.91, 0.34)],
    "CH": [("Zurich", 47.38, 8.54, 1.4), ("Geneva", 46.20, 6.14, 0.6), ("Basel", 47.56, 7.59, 0.55)],
    "CY": [("Nicosia", 35.17, 33.36, 0.33), ("Limassol", 34.71, 33.02, 0.24)],
    "CZ": [("Prague", 50.08, 14.44, 1.35), ("Brno", 49.20, 16.61, 0.38), ("Ostrava", 49.82, 18.26, 0.28)],
    "DE": [("Berlin", 52.52, 13.40, 3.7), ("Hamburg", 53.55, 9.99, 1.9), ("Munich", 48.14, 11.58, 1.5),
           ("Cologne", 50.94, 6.96, 1.1), ("Frankfurt", 50.11, 8.68, 0.76)],
    "DK": [("Copenhagen", 55.68, 12.57, 1.3), ("Aarhus", 56.16, 10.20, 0.29), ("Odense", 55.40, 10.39, 0.18)],
    "EE": [("Tallinn", 59.44, 24.75, 0.45), ("Tartu", 58.38, 26.72, 0.09)],
    "ES": [("Madrid", 40.42, -3.70, 3.3), ("Barcelona", 41.39, 2.17, 1.6), ("Valencia", 39.47, -0.38, 0.79),
           ("Seville", 37.39, -5.98, 0.68), ("Zaragoza", 41.65, -0.89, 0.68)],
    "FI": [("Helsinki", 60.17, 24.94, 0.66), ("Tampere", 61.50, 23.79, 0.24), ("Turku", 60.45, 22.27, 0.19)],
    "FR": [("Paris", 48.86, 2.35, 2.1), ("Marseille", 43.30, 5.37, 0.87), ("Lyon", 45.76, 4.84, 0.52),
           ("Toulouse", 43.60, 1.44, 0.50), ("Lille", 50.63, 3.06, 0.23)],
    "GB": [("London", 51.51, -0.13, 9.0), ("Birmingham", 52.49, -1.89, 1.1), ("Manchester", 53.48, -2.24, 0.55),
           ("Glasgow", 55.86, -4.25, 0.63)],
    "GR": [("Athens", 37.98, 23.73, 3.2), ("Thessaloniki", 40.64, 22.94, 0.81), ("Patras", 38.25, 21.73, 0.21)],
    "HR": [("Zagreb", 45.81, 15.98, 0.77), ("Split", 43.51, 16.44, 0.18), ("Rijeka", 45.33, 14.44, 0.13)],
    "HU": [("Budapest", 47.50, 19.04, 1.75), ("Debrecen", 47.53, 21.63, 0.20), ("Szeged", 46.25, 20.15, 0.16)],
    "IE": [("Dublin", 53.35, -6.26, 1.4), ("Cork", 51.90, -8.47, 0.22), ("Galway", 53.27, -9.06, 0.08)],
    "IT": [("Rome", 41.90, 12.50, 2.8), ("Milan", 45.46, 9.19, 1.4), ("Naples", 40.85, 14.27, 0.91),
           ("Turin", 45.07, 7.69, 0.85), ("Palermo", 38.12, 13.36, 0.63)],
    "LT": [("Vilnius", 54.69, 25.28, 0.59), ("Kaunas", 54.90, 23.90, 0.30)],
    "LU": [("Luxembourg", 49.61, 6.13, 0.13)],
    "LV": [("Riga", 56.95, 24.11, 0.61), ("Daugavpils", 55.87, 26.52, 0.08)],
    "MT": [("Valletta", 35.90, 14.51, 0.21)],
    "NL": [("Amsterdam", 52.37, 4.90, 0.92), ("Rotterdam", 51.92, 4.48, 0.65), ("The Hague", 52.08, 4.31, 0.55),
           ("Utrecht", 52.09, 5.12, 0.36)],
    "NO": [("Oslo", 59.91, 10.75, 0.71), ("Bergen", 60.39, 5.32, 0.29), ("Trondheim", 63.43, 10.39, 0.21)],
    "PL": [("Warsaw", 52.23, 21.01, 1.8), ("Krakow", 50.06, 19.94, 0.78), ("Lodz", 51.76, 19.46, 0.67),
           ("Wroclaw", 51.11, 17.04, 0.64), ("Poznan", 52.41, 16.93, 0.53)],
    "PT": [("Lisbon", 38.72, -9.13, 0.55), ("Porto", 41.15, -8.61, 0.23), ("Braga", 41.55, -8.43, 0.14)],
    "RO": [("Bucharest", 44.43, 26.10, 1.8), ("Cluj-Napoca", 46.77, 23.60, 0.32), ("Timisoara", 45.75, 21.23, 0.32)],
    "SE": [("Stockholm", 59.33, 18.07, 1.0), ("Gothenburg", 57.71, 11.97, 0.58), ("Malmo", 55.60, 13.00, 0.35)],
    "SI": [("Ljubljana", 46.06, 14.51, 0.29), ("Maribor", 46.55, 15.65, 0.11)],
    "SK": [("Bratislava", 48.15, 17.11, 0.44), ("Kosice", 48.72, 21.26, 0.24)],
}


def slice_blocks(blocks: list[dict], start: str, end: str) -> list[dict]:
    """Trim each location block's daily arrays to [start, end]."""
    out = []
    for b in blocks:
        daily = b.get("daily", {})
        times = daily.get("time", [])
        keep = [i for i, t in enumerate(times) if start <= t <= end]
        nb = dict(b)
        nb["daily"] = {
            k: ([v[i] for i in keep] if isinstance(v, list) else v)
            for k, v in daily.items()
        }
        out.append(nb)
    return out


def fetch_country(cc: str, start: str, end: str, retries: int = 5) -> list[dict]:
    """One archive call per country; Open-Meteo accepts comma-separated points.

    Responses are cached on disk. A country-decade of daily ERA5 across five
    points is a heavy request and Open-Meteo bills by response size, so a
    re-run (new end date, tweaked threshold) must not re-pull the 1991-2020
    baseline that will never change.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache = CACHE_DIR / f"{cc}_{start}_{end}.json"
    if cache.exists():
        print(f"  {cc}: cache hit", flush=True)
        return json.loads(cache.read_text(encoding="utf-8"))

    # A cached pull covering a WIDER window answers a narrower request for
    # free. Shortening the baseline must not throw away data already paid for.
    for other in sorted(CACHE_DIR.glob(f"{cc}_*.json")):
        try:
            _, c_start, c_end = other.stem.split("_", 2)
        except ValueError:
            continue
        if c_start <= start and c_end >= end:
            blocks = json.loads(other.read_text(encoding="utf-8"))
            sliced = slice_blocks(blocks, start, end)
            print(f"  {cc}: reusing wider cache {c_start}..{c_end}", flush=True)
            cache.write_text(json.dumps(sliced), encoding="utf-8")
            return sliced

    pts = CITIES[cc]
    params = {
        "latitude": ",".join(f"{p[1]}" for p in pts),
        "longitude": ",".join(f"{p[2]}" for p in pts),
        "start_date": start,
        "end_date": end,
        "daily": "temperature_2m_max,temperature_2m_mean",
        "timezone": "UTC",
    }
    url = f"{ARCHIVE_API}?{urllib.parse.urlencode(params)}"
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=300) as r:
                body = json.load(r)
            # A single point returns a dict; several return a list.
            blocks = body if isinstance(body, list) else [body]
            cache.write_text(json.dumps(blocks), encoding="utf-8")
            return blocks
        except urllib.error.HTTPError as e:
            detail = e.read().decode()[:200]
            if e.code == 429 and attempt < retries - 1:
                # The limit is per-minute, so back off past the window edge.
                wait = 65 * (attempt + 1)
                print(f"  {cc}: rate limited, waiting {wait}s", flush=True)
                time.sleep(wait)
                continue
            raise RuntimeError(f"{cc}: HTTP {e.code} {detail}")
        except Exception:
            if attempt < retries - 1:
                time.sleep(10)
                continue
            raise
    return []


def population_weighted(cc: str, blocks: list[dict]) -> dict[str, dict[str, float]]:
    """Collapse the per-city series into one population-weighted series."""
    pts = CITIES[cc]
    weights = [p[3] for p in pts]
    total_w = sum(weights)

    acc: dict[str, dict[str, float]] = {}
    times = blocks[0]["daily"]["time"]
    for i, day in enumerate(times):
        tmax_num = tmean_num = w_max = w_mean = 0.0
        for b, w in zip(blocks, weights):
            tx = b["daily"]["temperature_2m_max"][i]
            tm = b["daily"]["temperature_2m_mean"][i]
            if tx is not None:
                tmax_num += tx * w
                w_max += w
            if tm is not None:
                tmean_num += tm * w
                w_mean += w
        if w_max == 0:
            continue
        acc[day] = {
            "tmax": tmax_num / w_max,
            "tmean": tmean_num / w_mean if w_mean else None,
            "coverage": w_max / total_w,
        }
    return acc


def doy_key(d: date) -> int:
    """Day of year with Feb 29 folded onto Feb 28 so baselines line up."""
    if d.month == 2 and d.day == 29:
        return date(2001, 2, 28).timetuple().tm_yday
    return date(2001, d.month, d.day).timetuple().tm_yday


def percentile(values: list[float], q: float) -> float:
    """Linear-interpolated percentile (matches numpy's default)."""
    if not values:
        raise ValueError("empty")
    s = sorted(values)
    if len(s) == 1:
        return s[0]
    pos = q * (len(s) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(s) - 1)
    frac = pos - lo
    return s[lo] * (1 - frac) + s[hi] * frac


def build_thresholds(series: dict[str, dict]) -> dict[int, float]:
    """90th-percentile Tmax per calendar day over the baseline, +/- 7 days."""
    by_doy: dict[int, list[float]] = defaultdict(list)
    for day, vals in series.items():
        d = date.fromisoformat(day)
        if BASELINE_START <= d.year <= BASELINE_END:
            by_doy[doy_key(d)].append(vals["tmax"])

    thresholds: dict[int, float] = {}
    for doy in range(1, 366):
        pool: list[float] = []
        for off in range(-WINDOW_DAYS, WINDOW_DAYS + 1):
            k = ((doy - 1 + off) % 365) + 1
            pool.extend(by_doy.get(k, []))
        if pool:
            thresholds[doy] = percentile(pool, PERCENTILE)
    return thresholds


def flag_heatwaves(rows: list[dict]) -> None:
    """Mark runs of >= MIN_RUN consecutive hot days as heatwave events."""
    rows.sort(key=lambda r: r["date"])
    run_start = None
    run: list[dict] = []

    def close(run: list[dict]) -> None:
        if len(run) < MIN_RUN:
            return
        event = f"{run[0]['country_code']}-{run[0]['date']}"
        for i, r in enumerate(run, start=1):
            r["heatwave_id"] = event
            r["heatwave_day"] = i
            r["heatwave_length"] = len(run)

    prev_date = None
    for r in rows:
        d = date.fromisoformat(r["date"])
        in_season = WARM_SEASON[0] <= d.month <= WARM_SEASON[1]
        # `is_hot_day` stays the raw all-year percentile exceedance so the
        # underlying index remains inspectable; only event grouping is gated.
        qualifies = r["is_hot_day"] and in_season
        contiguous = prev_date is not None and (d - prev_date).days == 1
        if qualifies and (contiguous or not run):
            run.append(r)
        elif qualifies:
            close(run)
            run = [r]
        else:
            close(run)
            run = []
        prev_date = d
    close(run)


def main() -> int:
    global BASELINE_START, BASELINE_END

    ap = argparse.ArgumentParser()
    ap.add_argument("--upload", action="store_true", help="upsert into Supabase")
    ap.add_argument("--countries", default="", help="comma list; default all")
    ap.add_argument("--end", default="", help="last day (default: yesterday)")
    ap.add_argument("--baseline-start", type=int, default=DEFAULT_BASELINE_START,
                    help=f"first baseline year (default {DEFAULT_BASELINE_START}; "
                         "use 1991 for the WMO standard normal)")
    ap.add_argument("--baseline-end", type=int, default=DEFAULT_BASELINE_END,
                    help=f"last baseline year (default {DEFAULT_BASELINE_END})")
    args = ap.parse_args()

    BASELINE_START, BASELINE_END = args.baseline_start, args.baseline_end
    end = args.end or (date.today() - timedelta(days=1)).isoformat()
    start = f"{BASELINE_START}-01-01"
    years = BASELINE_END - BASELINE_START + 1
    print(f"Baseline {BASELINE_START}-{BASELINE_END} ({years} years, "
          f"~{years * (2 * WINDOW_DAYS + 1)} samples per calendar day)", flush=True)
    wanted = ([c.strip().upper() for c in args.countries.split(",") if c.strip()]
              if args.countries else sorted(CITIES))

    OUT_DIR.mkdir(exist_ok=True)
    all_rows: list[dict] = []

    for n, cc in enumerate(wanted, 1):
        if cc not in CITIES:
            print(f"  skip {cc}: no city mapping", file=sys.stderr)
            continue
        print(f"[{n}/{len(wanted)}] {cc}: fetching {len(CITIES[cc])} points "
              f"{start} -> {end} ...", flush=True)
        try:
            blocks = fetch_country(cc, start, end)
        except Exception as e:
            print(f"  ERROR {cc}: {e}", file=sys.stderr)
            continue

        series = population_weighted(cc, blocks)
        thresholds = build_thresholds(series)
        if not thresholds:
            print(f"  ERROR {cc}: no baseline data", file=sys.stderr)
            continue

        rows: list[dict] = []
        for day, vals in series.items():
            if day < SERIES_START:
                continue
            d = date.fromisoformat(day)
            thr = thresholds.get(doy_key(d))
            if thr is None:
                continue
            tmax = round(vals["tmax"], 2)
            rows.append({
                "country_code": cc,
                "date": day,
                "tmax_c": tmax,
                "tmean_c": round(vals["tmean"], 2) if vals["tmean"] is not None else None,
                "threshold_p90_c": round(thr, 2),
                "anomaly_c": round(tmax - thr, 2),
                # Percentile OR the absolute floor.
                "is_hot_day": tmax > thr or tmax >= ABSOLUTE_HOT_C,
                "heatwave_id": None,
                "heatwave_day": None,
                "heatwave_length": None,
            })

        flag_heatwaves(rows)
        hw_days = sum(1 for r in rows if r["heatwave_id"])
        events = len({r["heatwave_id"] for r in rows if r["heatwave_id"]})
        print(f"  {cc}: {len(rows)} days, {hw_days} heatwave days across {events} events "
              f"(baseline p90 in Jul = {thresholds[196]:.1f} C)", flush=True)
        all_rows.extend(rows)
        time.sleep(INTER_COUNTRY_SLEEP_S)

    if not all_rows:
        print("No rows built.", file=sys.stderr)
        return 1

    cols = ["country_code", "date", "tmax_c", "tmean_c", "threshold_p90_c",
            "anomaly_c", "is_hot_day", "heatwave_id", "heatwave_day", "heatwave_length"]
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(all_rows)
    print(f"\nWrote {len(all_rows)} rows -> {OUT_CSV}")

    if args.upload:
        upload(all_rows)
    return 0


def upload(rows: list[dict]) -> None:
    # Every other script in python/ loads .env; without this the credentials are
    # absent, --upload silently no-ops and a completed 30-country build looks
    # like it succeeded while writing nothing to the database.
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        print("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY; skipping upload", file=sys.stderr)
        return
    endpoint = f"{url.rstrip('/')}/rest/v1/weather_country_daily?on_conflict=country_code,date"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    batch = 1000
    for i in range(0, len(rows), batch):
        chunk = rows[i:i + batch]
        req = urllib.request.Request(endpoint, data=json.dumps(chunk).encode(),
                                     headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                r.read()
            print(f"  upserted {i + len(chunk)}/{len(rows)}", flush=True)
        except urllib.error.HTTPError as e:
            print(f"  upsert failed at {i}: HTTP {e.code} {e.read().decode()[:300]}", file=sys.stderr)
            return
    print("Upload complete.")


if __name__ == "__main__":
    sys.exit(main())

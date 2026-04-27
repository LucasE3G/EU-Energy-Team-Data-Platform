import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from dotenv import load_dotenv


DOMAINS: Dict[str, str] = {
    "AT": "10YAT-APG------L",
    "BE": "10YBE----------2",
    "BG": "10YCA-BULGARIA-R",
    "HR": "10YHR-HEP------M",
    "CY": "10YCY-1001A0003J",
    "CZ": "10YCZ-CEPS-----N",
    "DK1": "10YDK-1--------W",
    "DK2": "10YDK-2--------M",
    "EE": "10Y1001A1001A39I",
    "FI": "10YFI-1--------U",
    "FR": "10YFR-RTE------C",
    # Prices use different EICs than generation/load for some zones.
    "DE": "10Y1001A1001A82H",
    "GR": "10YGR-HTSO-----Y",
    "HU": "10YHU-MAVIR----U",
    "IE": "10YIE-1001A00010",
    # Italy is split into multiple price areas; we combine them below.
    "IT": "10Y1001A1001A73I",
    "LV": "10YLV-1001A00074",
    "LT": "10YLT-1001A0008Q",
    "MT": "10YMT-1001A0003F",
    "NL": "10YNL----------L",
    "NO1": "10YNO-1--------2",
    "NO2": "10YNO-2--------T",
    "NO3": "10YNO-3--------J",
    "NO4": "10YNO-4--------9",
    "NO5": "10Y1001A1001A48H",
    "PL": "10YPL-AREA-----S",
    "PT": "10YPT-REN------W",
    "RO": "10YRO-TEL------P",
    "SK": "10YSK-SEPS-----K",
    "SI": "10YSI-ELES-----O",
    "ES": "10YES-REE------0",
    "SE1": "10Y1001A1001A44P",
    "SE2": "10Y1001A1001A45N",
    "SE3": "10Y1001A1001A46L",
    "SE4": "10Y1001A1001A47J",
    "CH": "10YCH-SWISSGRIDZ",
    "GB": "10YGB----------A",
}

# Some countries publish day-ahead prices per price area. For those, we
# combine multiple domains by timestamp (simple average).
PRICE_DOMAIN_OVERRIDES: Dict[str, List[str]] = {
    "IT": ["10Y1001A1001A73I", "10Y1001A1001A74G"],
}


def ymdhm(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y%m%d%H%M")


def entsoe_get_prices(token: str, domain: str, start_utc: datetime, end_utc: datetime, session: requests.Session) -> str:
    params = {
        "securityToken": token,
        "documentType": "A44",
        "processType": "A01",
        "in_Domain": domain,
        "out_Domain": domain,
        "periodStart": ymdhm(start_utc),
        "periodEnd": ymdhm(end_utc),
    }
    r = session.get("https://web-api.tp.entsoe.eu/api", params=params, timeout=60)
    r.raise_for_status()
    return r.text


_TS_RE = re.compile(r"<timeInterval>\s*<start>([^<]+)</start>", re.M)
_RES_RE = re.compile(r"<resolution>([^<]+)</resolution>", re.M)
_POINT_RE = re.compile(r"<Point>[\s\S]*?</Point>", re.M)
_POS_RE = re.compile(r"<position>([^<]+)</position>")
_PRICE_RE = re.compile(r"<price\.amount>([^<]+)</price\.amount>")


def parse_a44(xml: str) -> List[Tuple[str, float]]:
    # Minimal parsing: use Period start + resolution + point positions.
    out: List[Tuple[str, float]] = []
    for ts_block in re.findall(r"<TimeSeries[\s\S]*?</TimeSeries>", xml):
        start = _TS_RE.search(ts_block)
        res = _RES_RE.search(ts_block)
        if not start or not res:
            continue
        start_ms = int(datetime.fromisoformat(start.group(1).replace("Z", "+00:00")).timestamp() * 1000)
        step = 60
        if res.group(1) == "PT15M":
            step = 15
        elif res.group(1) == "PT30M":
            step = 30
        elif res.group(1) == "PT60M":
            step = 60

        for p in _POINT_RE.findall(ts_block):
            mpos = _POS_RE.search(p)
            mpr = _PRICE_RE.search(p)
            if not mpos or not mpr:
                continue
            try:
                pos = int(mpos.group(1))
                price = float(mpr.group(1))
            except Exception:
                continue
            ts = datetime.fromtimestamp((start_ms + (pos - 1) * step * 60 * 1000) / 1000, tz=timezone.utc).isoformat()
            out.append((ts, price))
    return out


def supabase_upsert_prices(
    session: requests.Session,
    supabase_url: str,
    service_role: str,
    zone: str,
    domain: str,
    points: List[Tuple[str, float]],
) -> None:
    url = f"{supabase_url.rstrip('/')}/rest/v1/electricity_day_ahead_prices?on_conflict=source,zone_id,ts"
    headers = {
        "Authorization": f"Bearer {service_role}",
        "apikey": service_role,
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

    # Dedupe timestamps within a single ENTSO-E response/window. If a payload
    # contains the same (source, zone_id, ts) twice, Postgres rejects the upsert:
    # "ON CONFLICT DO UPDATE command cannot affect row a second time".
    by_ts: Dict[str, float] = {}
    for ts, price in points:
        by_ts[ts] = price

    rows = [
        {
            "zone_id": zone,
            "ts": ts,
            "price_eur_per_mwh": by_ts[ts],
            "currency": "EUR",
            "source": "entsoe",
            "raw": {"domain": domain},
        }
        for ts in sorted(by_ts.keys())
    ]

    chunk = 500
    for i in range(0, len(rows), chunk):
        r = session.post(url, headers=headers, json=rows[i : i + chunk], timeout=60)
        if not r.ok:
            raise RuntimeError(f"Supabase upsert failed HTTP {r.status_code}: {r.text[:500]}")


def main() -> int:
    load_dotenv()
    token = os.getenv("ENTSOE_API_TOKEN")
    supabase_url = os.getenv("SUPABASE_URL")
    service_role = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not token or not supabase_url or not service_role:
        raise RuntimeError("Missing ENTSOE_API_TOKEN / SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY")

    years = int(os.getenv("ENTSOE_PRICE_BACKFILL_YEARS", "5"))
    chunk_days = int(os.getenv("ENTSOE_PRICE_CHUNK_DAYS", "14"))
    sleep_s = float(os.getenv("ENTSOE_PRICE_SLEEP_SECONDS", "0.15"))
    zones_env = (os.getenv("ENTSOE_PRICE_ZONES") or "").strip()
    zones = [z.strip().upper() for z in zones_env.split(",") if z.strip()] if zones_env else list(DOMAINS.keys())

    end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(days=365 * years)

    s = requests.Session()
    print(f"=== electricity_price_backfill_entsoe: zones={len(zones)} years={years} chunk_days={chunk_days} ===", flush=True)
    print(f"window: {start.isoformat()} -> {end.isoformat()}", flush=True)

    total_points = 0
    for zone in zones:
        domain = DOMAINS.get(zone)
        if not domain:
            continue
        domains = PRICE_DOMAIN_OVERRIDES.get(zone, [domain])
        print(f"\n--- {zone}: {','.join(domains)} ---", flush=True)
        t0 = start
        while t0 < end:
            t1 = min(t0 + timedelta(days=chunk_days), end)
            try:
                # Merge multiple price areas if configured.
                sum_by_ts: Dict[str, float] = {}
                n_by_ts: Dict[str, int] = {}
                for dom in domains:
                    xml = entsoe_get_prices(token, dom, t0, t1, s)
                    pts = parse_a44(xml)
                    if not pts:
                        continue
                    # Dedupe within each doc
                    by_ts: Dict[str, float] = {}
                    for ts, price in pts:
                        by_ts[ts] = price
                    for ts, price in by_ts.items():
                        sum_by_ts[ts] = sum_by_ts.get(ts, 0.0) + float(price)
                        n_by_ts[ts] = n_by_ts.get(ts, 0) + 1

                points = [(ts, sum_by_ts[ts] / n_by_ts[ts]) for ts in sorted(sum_by_ts.keys())]
                if points:
                    # Use the primary domain for raw tagging; actual domains are in the price average.
                    supabase_upsert_prices(s, supabase_url, service_role, zone, domains[0], points)
                    total_points += len(points)
                print(f"{zone} {t0.date()} -> {t1.date()} points={len(points)}", flush=True)
            except Exception as e:
                print(f"{zone} {t0.date()} -> {t1.date()} ERROR: {e}", flush=True)
            time.sleep(sleep_s)
            t0 = t1

    print(f"\nDone. total_points_upserted={total_points}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


import os
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional

import requests
from dotenv import load_dotenv

ENTSOE_API = "https://web-api.tp.entsoe.eu/api"

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
    "DE": "10Y1001A1001A83F",
    "GR": "10YGR-HTSO-----Y",
    "HU": "10YHU-MAVIR----U",
    "IE": "10YIE-1001A00010",
    "IT": "10YIT-GRTN-----B",
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

# ENTSO-E psrType codes used in A75 responses.
PSR_LABELS: Dict[str, str] = {
    "B01": "Biomass",
    "B02": "Fossil Brown coal/Lignite",
    "B03": "Fossil Coal-derived gas",
    "B04": "Fossil Gas",
    "B05": "Fossil Hard coal",
    "B06": "Fossil Oil",
    "B07": "Fossil Oil shale",
    "B08": "Fossil Peat",
    "B09": "Geothermal",
    "B10": "Hydro Pumped Storage",
    "B11": "Hydro Run-of-river",
    "B12": "Hydro Water Reservoir",
    "B13": "Marine",
    "B14": "Nuclear",
    "B15": "Other renewable",
    "B16": "Solar",
    "B17": "Waste",
    "B18": "Wind Offshore",
    "B19": "Wind Onshore",
    "B20": "Other",
}


def ymdhm(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y%m%d%H%M")


@dataclass
class GenPoint:
    ts: datetime
    psr_type: str
    mw: float


def fetch_a75_xml(token: str, domain: str, start: datetime, end: datetime) -> str:
    """Fetch A75 (Actual Generation Per Production Type) from ENTSO-E."""
    params = {
        "securityToken": token,
        "documentType": "A75",
        "processType": "A16",
        "in_Domain": domain,
        "periodStart": ymdhm(start),
        "periodEnd": ymdhm(end),
    }
    r = requests.get(ENTSOE_API, params=params, timeout=60)
    text = r.text
    if r.status_code == 429:
        raise RuntimeError(f"429 Too Many Requests | Retry-After={r.headers.get('Retry-After')}")
    if not r.ok:
        raise RuntimeError(f"HTTP {r.status_code}: {text[:300]}")
    return text


def parse_generation_by_type(xml: str) -> List[GenPoint]:
    """
    Parse A75 XML into per-timestamp, per-psrType MW values.
    Multiple TimeSeries can cover the same (ts, psrType); keep the maximum
    to avoid double-counting duplicates across business types / units.
    """
    root = ET.fromstring(xml)
    # key: (ts, psr_type) → mw
    buckets: Dict[tuple, float] = {}

    for ts_el in root.findall(".//{*}TimeSeries"):
        psr_el = ts_el.find(".//{*}MktPSRType/{*}psrType")
        if psr_el is None or not psr_el.text:
            continue
        psr_type = psr_el.text.strip()

        period = ts_el.find(".//{*}Period")
        if period is None:
            continue

        start_el = period.find(".//{*}timeInterval/{*}start")
        res_el   = period.find(".//{*}resolution")
        if start_el is None or not start_el.text or res_el is None or not res_el.text:
            continue

        try:
            period_start = datetime.fromisoformat(
                start_el.text.replace("Z", "+00:00")
            ).astimezone(timezone.utc)
        except Exception:
            continue

        resolution = res_el.text.strip()
        step_minutes = 15 if resolution == "PT15M" else 30 if resolution == "PT30M" else 60

        for point_el in period.findall(".//{*}Point"):
            pos_el = point_el.find(".//{*}position")
            qty_el = point_el.find(".//{*}quantity")
            if pos_el is None or qty_el is None or not pos_el.text or not qty_el.text:
                continue
            try:
                pos = int(pos_el.text)
                qty = float(qty_el.text)
            except Exception:
                continue

            ts = (period_start + timedelta(minutes=(pos - 1) * step_minutes)).replace(
                second=0, microsecond=0, tzinfo=timezone.utc
            )
            key = (ts, psr_type)
            prev = buckets.get(key)
            buckets[key] = qty if prev is None else max(prev, qty)

    points = [GenPoint(ts=k[0], psr_type=k[1], mw=v) for k, v in buckets.items()]
    points.sort(key=lambda p: (p.ts, p.psr_type))
    return points


def chunked(it: List[dict], size: int) -> Iterable[List[dict]]:
    for i in range(0, len(it), size):
        yield it[i : i + size]


def supabase_upsert_rows(
    supabase_url: str,
    service_role_key: str,
    rows: List[dict],
    on_conflict: str = "source,zone_id,ts,psr_type",
) -> None:
    if not rows:
        return
    url = f"{supabase_url.rstrip('/')}/rest/v1/electricity_generation_snapshots?on_conflict={on_conflict}"
    headers = {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    r = requests.post(url, headers=headers, json=rows, timeout=60)
    if not r.ok:
        raise RuntimeError(f"Supabase upsert failed HTTP {r.status_code}: {r.text[:300]}")


def main() -> None:
    load_dotenv()
    entsoe_token = os.getenv("ENTSOE_API_TOKEN")
    supabase_url = os.getenv("SUPABASE_URL")
    service_role  = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not entsoe_token:
        print("Missing ENTSOE_API_TOKEN in .env", file=sys.stderr)
        sys.exit(1)
    if not supabase_url or not service_role:
        print("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY in .env", file=sys.stderr)
        sys.exit(1)

    years      = int(os.getenv("ENTSOE_GEN_BACKFILL_YEARS", "5"))
    chunk_days = int(os.getenv("ENTSOE_GEN_CHUNK_DAYS", "7"))
    delay_s    = float(os.getenv("ENTSOE_GEN_DELAY_SECONDS", "0.25"))
    max_req    = int(os.getenv("ENTSOE_GEN_MAX_REQUESTS", "1000000"))
    batch_size = int(os.getenv("ENTSOE_GEN_UPSERT_BATCH", "500"))

    zones_env = os.getenv("ENTSOE_ZONES")
    zones = [z.strip() for z in zones_env.split(",")] if zones_env else list(DOMAINS.keys())

    now          = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    target_start = now - timedelta(days=years * 365)

    req = 0
    for zone in zones:
        domain = DOMAINS.get(zone)
        if not domain:
            continue
        print(f"\n== GENERATION {zone} ({domain}) ==")
        end = now

        while end > target_start and req < max_req:
            start = max(end - timedelta(days=chunk_days), target_start)

            try:
                xml    = fetch_a75_xml(entsoe_token, domain, start, end)
                points = parse_generation_by_type(xml)
            except Exception as e:
                msg = str(e)
                print(f"  Error {zone} {start.date()}→{end.date()}: {msg}", file=sys.stderr)
                time.sleep(10 if "429" in msg else 2)
                continue

            req += 1
            if delay_s > 0:
                time.sleep(delay_s)

            rows: List[dict] = [
                {
                    "zone_id":  zone,
                    "ts":       p.ts.isoformat().replace("+00:00", "Z"),
                    "psr_type": p.psr_type,
                    "mw":       p.mw,
                    "source":   "entsoe",
                }
                for p in points
                if p.mw >= 0  # keep 0 (valid for types with no output at that moment)
            ]

            for chunk in chunked(rows, batch_size):
                supabase_upsert_rows(supabase_url, service_role, chunk)

            print(f"  OK req={req} points={len(rows)} {start.date()}→{end.date()}")
            end = start

    print("\nDone.")


if __name__ == "__main__":
    main()

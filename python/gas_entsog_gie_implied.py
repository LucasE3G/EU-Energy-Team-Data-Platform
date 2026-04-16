import json
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


ENTSOG_BASE = "https://transparency.entsog.eu/api/v1"
GIE_BASE = "https://agsi.gie.eu/api"


# EU27 (ISO2)
EU27 = [
    "AT",
    "BE",
    "BG",
    "HR",
    "CY",
    "CZ",
    "DK",
    "EE",
    "FI",
    "FR",
    "DE",
    "GR",
    "HU",
    "IE",
    "IT",
    "LV",
    "LT",
    "LU",
    "MT",
    "NL",
    "PL",
    "PT",
    "RO",
    "SK",
    "SI",
    "ES",
    "SE",
]


def _iso(d: date) -> str:
    return d.isoformat()


def _chunked(seq: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def make_retrying_session() -> requests.Session:
    """
    Long backfills will occasionally see RemoteDisconnected / transient 5xx.
    This session retries safely on idempotent GETs.
    """
    s = requests.Session()
    retry = Retry(
        total=8,
        connect=8,
        read=8,
        status=8,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _to_mwh(value: float, unit: str) -> float:
    """
    ENTSOG often returns kWh/d for daily series.
    AGSI returns various (commonly GWh, TWh) depending on field.
    We normalize all energy to MWh.
    """
    u = (unit or "").strip()
    if u in ("kWh/d", "kWh"):
        return float(value) / 1_000.0
    if u in ("MWh/d", "MWh"):
        return float(value)
    if u in ("GWh/d", "GWh"):
        return float(value) * 1_000.0
    if u in ("TWh/d", "TWh"):
        return float(value) * 1_000_000.0
    # Unknown: assume already MWh (keeps pipeline running; flagged via raw)
    return float(value)


@dataclass(frozen=True)
class DirectionId:
    tso_item_identifier: str
    direction: str  # 'entry' or 'exit'
    other_country: Optional[str]


def fetch_interconnection_directions_for_country(country: str, session: Optional[requests.Session] = None) -> List[DirectionId]:
    """
    Build a list of tsoItemIdentifier+directionKey pairs that represent physical flow
    crossing the country border (imports/exports) using ENTSOG /interconnections.
    """
    s = session or make_retrying_session()
    directions: List[DirectionId] = []

    # 1) country as 'toCountryKey' (imports on this side usually directionKey='entry')
    for params in (
        {"toCountryKey": country, "limit": -1},
        {"fromCountryKey": country, "limit": -1},
    ):
        r = s.get(f"{ENTSOG_BASE}/interconnections", params=params, timeout=60)
        if r.status_code == 404:
            # Some countries (e.g., CY) have no ENTSOG interconnections exposed.
            # Treat as "no data" instead of failing the whole backfill.
            continue
        if not r.ok:
            raise RuntimeError(f"ENTSOG interconnections HTTP {r.status_code}: {r.text[:200]}")
        j = r.json()
        for it in j.get("interconnections", []) or []:
            from_c = it.get("fromCountryKey")
            to_c = it.get("toCountryKey")
            if not from_c or not to_c:
                continue
            if from_c == to_c:
                continue  # internal points (distribution, storage)

            # On each side of an interconnection, ENTSOG exposes a TSO item identifier + direction.
            if to_c == country and it.get("toHasData") in ("1", 1, True, "true"):
                tid = it.get("toTsoItemIdentifier")
                dirk = it.get("toDirectionKey")
                if tid and dirk in ("entry", "exit"):
                    directions.append(DirectionId(tso_item_identifier=tid, direction=dirk, other_country=from_c))

            if from_c == country and it.get("fromHasData") in ("1", 1, True, "true"):
                tid = it.get("fromTsoItemIdentifier")
                dirk = it.get("fromDirectionKey")
                if tid and dirk in ("entry", "exit"):
                    directions.append(DirectionId(tso_item_identifier=tid, direction=dirk, other_country=to_c))

    # De-duplicate
    seen = set()
    out: List[DirectionId] = []
    for d in directions:
        k = (d.tso_item_identifier, d.direction)
        if k in seen:
            continue
        seen.add(k)
        out.append(d)
    return out


def fetch_entsog_physical_flow_daily(
    tso_item_identifiers: List[str],
    start: date,
    end: date,
    session: Optional[requests.Session] = None,
) -> List[dict]:
    """
    Fetch daily physical flow for a list of tsoItemIdentifier values.
    Returns raw rows (one per series-day) from ENTSOG operationaldatas.
    """
    if not tso_item_identifiers:
        return []
    s = session or make_retrying_session()

    rows: List[dict] = []
    for batch in _chunked(tso_item_identifiers, 40):
        params = {
            "indicator": "Physical Flow",
            "periodType": "day",
            "from": _iso(start),
            "to": _iso(end),
            "tsoItemIdentifier": ",".join(batch),
            "limit": -1,
            "includeExemptions": 0,
        }
        r = s.get(f"{ENTSOG_BASE}/operationaldatas", params=params, timeout=60)
        if r.status_code == 404:
            # No result found for this filter/batch; treat as empty.
            continue
        if not r.ok:
            raise RuntimeError(f"ENTSOG operationaldatas HTTP {r.status_code}: {r.text[:200]}")
        j = r.json()
        rows.extend(j.get("operationaldatas", []) or [])
        time.sleep(0.05)
    return rows


def compute_net_imports_mwh_by_day(
    directions: List[DirectionId],
    flow_rows: List[dict],
) -> Dict[date, float]:
    """
    Sum entry flows as imports and exit flows as exports, then net = imports - exports.

    IMPORTANT:
    ENTSOG `tsoItemIdentifier` values can appear for both directions (entry/exit).
    We therefore use the `directionKey` present on each operational data row, rather than
    assigning a single direction per identifier.
    """
    tids = {d.tso_item_identifier for d in directions}

    imports: Dict[date, float] = {}
    exports: Dict[date, float] = {}

    for r in flow_rows:
        tid = r.get("tsoItemIdentifier")
        if not tid or tid not in tids:
            continue

        period_from = r.get("periodFrom")
        if not period_from:
            continue
        # Parse ISO with offset (ENTSOG returns +01/+02). We keep gas day as local date.
        try:
            dt = datetime.fromisoformat(period_from)
        except Exception:
            continue
        d = dt.date()

        val = r.get("value")
        if val is None:
            continue
        try:
            v = float(val)
        except Exception:
            continue

        mwh = _to_mwh(v, r.get("unit") or "kWh/d")
        row_dir = r.get("directionKey")
        if row_dir == "entry":
            imports[d] = imports.get(d, 0.0) + mwh
        elif row_dir == "exit":
            exports[d] = exports.get(d, 0.0) + mwh
        else:
            # Unexpected / missing direction; ignore
            continue

    out: Dict[date, float] = {}
    for d in set(imports.keys()) | set(exports.keys()):
        out[d] = imports.get(d, 0.0) - exports.get(d, 0.0)
    return out


def fetch_gie_storage_daily(country: str, start: date, end: date, apikey: str, session: Optional[requests.Session] = None) -> List[dict]:
    """
    Fetch AGSI storage daily series for a country using x-key header.
    """
    s = session or make_retrying_session()
    rows: List[dict] = []

    page = 1
    size = 300
    while True:
        params = {
            "page": page,
            "size": size,
            "country": country,
            "from": _iso(start),
            "to": _iso(end),
        }
        r = s.get(GIE_BASE, params=params, headers={"x-key": apikey}, timeout=60)
        if not r.ok:
            raise RuntimeError(f"GIE HTTP {r.status_code}: {r.text[:200]}")
        j = r.json()
        data = j.get("data", []) or []
        rows.extend(data)
        last_page = int(j.get("last_page") or 0)
        if page >= last_page or last_page == 0:
            break
        page += 1
        time.sleep(0.05)

    return rows


def compute_net_withdrawal_mwh_by_day(gie_rows: List[dict]) -> Dict[date, float]:
    """
    Prefer 'netWithdrawal' if present; otherwise compute withdrawal - injection.

    For implied demand, we treat storage as a *draw* signal, i.e. we clamp negatives to 0.
    Injections should not reduce consumption.
    """
    out: Dict[date, float] = {}
    for r in gie_rows:
        gas_day = r.get("gasDayStart") or r.get("gasDay")
        if not gas_day:
            continue
        try:
            d = date.fromisoformat(str(gas_day)[:10])
        except Exception:
            continue

        unit = r.get("unit") or r.get("gasInStorageUnit") or "GWh"

        # Try official field names used by AGSI
        net = r.get("netWithdrawal")
        if net is None:
            w = r.get("withdrawal")
            inj = r.get("injection")
            if w is None and inj is None:
                continue
            try:
                net = float(w or 0.0) - float(inj or 0.0)
            except Exception:
                continue
        try:
            net = float(net)
        except Exception:
            continue

        # netWithdrawal is typically in GWh; normalize via unit (best-effort)
        mwh = _to_mwh(net, unit)
        if mwh < 0:
            mwh = 0.0
        out[d] = out.get(d, 0.0) + mwh
    return out


def load_sector_shares(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def split_sectors(total_mwh: float, power_mwh: float, household_share: float, industry_share: float) -> Tuple[float, float, float, float]:
    power = max(0.0, min(float(power_mwh), float(total_mwh)))
    nonpower = max(0.0, float(total_mwh) - power)
    hh = nonpower * float(household_share)
    ind = nonpower * float(industry_share)
    # Residual (from rounding) is pushed into industry
    residual = nonpower - (hh + ind)
    ind += residual
    return float(total_mwh), power, hh, ind


def compute_country_series(
    country: str,
    start: date,
    end: date,
    gie_apikey: str,
    shares_cfg: dict,
    session: Optional[requests.Session] = None,
) -> List[dict]:
    """
    Compute daily implied demand for a country for [start,end] inclusive.
    Total = netImports + storageNetWithdrawal.
    Sector split: fixed shares for household/industry; power is estimated as a fixed share of total (v1).
    """
    s = session or requests.Session()

    directions = fetch_interconnection_directions_for_country(country, session=s)
    tids = [d.tso_item_identifier for d in directions]
    flows = fetch_entsog_physical_flow_daily(tids, start, end, session=s)
    net_imports = compute_net_imports_mwh_by_day(directions, flows)

    gie_rows = fetch_gie_storage_daily(country, start, end, apikey=gie_apikey, session=s)
    net_withdrawal = compute_net_withdrawal_mwh_by_day(gie_rows)

    default = shares_cfg.get("_default", {})
    csh = shares_cfg.get(country, {})
    power_share = float(csh.get("power", default.get("power", 0.2)))
    hh_share = float(csh.get("household", default.get("household", 0.4)))
    ind_share = float(csh.get("industry", default.get("industry", 0.4)))

    # Normalize hh/ind to sum to 1 for non-power part
    ssum = hh_share + ind_share
    if ssum <= 0:
        hh_share, ind_share = 0.5, 0.5
    else:
        hh_share, ind_share = hh_share / ssum, ind_share / ssum

    out: List[dict] = []
    days = (end - start).days
    for i in range(days + 1):
        d = start + timedelta(days=i)
        total = (net_imports.get(d, 0.0) + net_withdrawal.get(d, 0.0))
        if total < 0:
            total = 0.0

        power = total * power_share
        total, power, hh, ind = split_sectors(total, power, hh_share, ind_share)

        out.append(
            {
                "country_code": country,
                "gas_day": d.isoformat(),
                "total_mwh": total,
                "power_mwh": power,
                "household_mwh": hh,
                "industry_mwh": ind,
                "source_total": "entsog+gie",
                "source_power": "estimated",
                "source_split": "fixed_shares",
                "method_version": "v1",
                "quality_flag": "observed_total_estimated_sectors",
                "raw": {
                    "net_imports_mwh": net_imports.get(d, 0.0),
                    "net_withdrawal_mwh": net_withdrawal.get(d, 0.0),
                    "power_share": power_share,
                    "hh_share": hh_share,
                    "ind_share": ind_share,
                    "tso_item_identifiers": len(tids),
                },
            }
        )
    return out


def supabase_upsert_gas_rows(supabase_url: str, service_role_key: str, rows: List[dict]) -> None:
    if not rows:
        return
    url = f"{supabase_url.rstrip('/')}/rest/v1/gas_demand_daily?on_conflict=method_version,country_code,gas_day"
    headers = {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }

    # Chunk + retry: Supabase/Cloudflare may transiently 5xx on large writes.
    batch_size = int(os.getenv("SUPABASE_UPSERT_BATCH", "200"))
    max_retries = int(os.getenv("SUPABASE_UPSERT_RETRIES", "8"))
    base_sleep = float(os.getenv("SUPABASE_UPSERT_BACKOFF_SECONDS", "1.0"))

    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        attempt = 0
        while True:
            attempt += 1
            try:
                r = requests.post(url, headers=headers, json=batch, timeout=60)
            except requests.exceptions.RequestException as e:
                if attempt >= max_retries:
                    raise RuntimeError(f"Supabase upsert failed after {attempt} attempts: {e}") from e
                time.sleep(base_sleep * (2 ** (attempt - 1)))
                continue

            if r.ok:
                break

            # Retry on transient errors / rate limits
            if r.status_code in (408, 425, 429, 500, 502, 503, 504):
                if attempt >= max_retries:
                    raise RuntimeError(f"Supabase upsert failed HTTP {r.status_code}: {r.text[:300]}")
                # Respect Retry-After if provided
                ra = r.headers.get("Retry-After")
                if ra:
                    try:
                        sleep_s = float(ra)
                    except Exception:
                        sleep_s = base_sleep * (2 ** (attempt - 1))
                else:
                    sleep_s = base_sleep * (2 ** (attempt - 1))
                time.sleep(sleep_s)
                continue

            raise RuntimeError(f"Supabase upsert failed HTTP {r.status_code}: {r.text[:300]}")


def main():
    load_dotenv()
    supabase_url = os.getenv("SUPABASE_URL")
    service_role = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    gie_key = os.getenv("GIE_API_KEY")
    if not supabase_url or not service_role:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY in .env")
    if not gie_key:
        raise RuntimeError("Missing GIE_API_KEY in .env")

    shares_path = os.getenv("GAS_SECTOR_SHARES", "python/gas_sector_shares_eu.json")
    shares_cfg = load_sector_shares(shares_path)

    days = int(os.getenv("GAS_INGEST_DAYS", "7"))
    end = date.today()
    start = end - timedelta(days=days)

    s = requests.Session()
    for c in EU27:
        print(f"== {c} {start} -> {end} ==")
        rows = compute_country_series(c, start, end, gie_apikey=gie_key, shares_cfg=shares_cfg, session=s)
        supabase_upsert_gas_rows(supabase_url, service_role, rows)
        time.sleep(0.1)

    print("Done.")


if __name__ == "__main__":
    main()


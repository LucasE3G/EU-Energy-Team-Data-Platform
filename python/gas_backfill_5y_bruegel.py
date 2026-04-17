import calendar
import os
import statistics
import time
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv


def _allocate_remainder_capped(
    remainder: float,
    fallback_day_keys: List[str],
    implied_day_totals: List[float],
    days_in_month: int,
) -> Tuple[Dict[str, float], float]:
    """Distribute remainder uniformly across fallback days, capped to avoid spikes.

    Cap per day = max(2.5 * median(implied_day_totals), 1.2 * monthly_average).
    If no implied days, cap = max(2.0 * monthly_average, remainder / F).
    Any amount that cannot fit under the cap is returned as ``unallocated_mwh``.
    """
    F = len(fallback_day_keys)
    if F == 0:
        return {}, remainder
    if remainder <= 0:
        return {k: 0.0 for k in fallback_day_keys}, 0.0

    monthly_total_est = remainder + sum(implied_day_totals)
    monthly_avg = monthly_total_est / max(days_in_month, 1)
    positive = [t for t in implied_day_totals if t > 0]
    if positive:
        med = statistics.median(positive)
        cap = max(2.5 * med, 1.2 * monthly_avg)
    else:
        cap = max(2.0 * monthly_avg, remainder / F)

    alloc: Dict[str, float] = {k: remainder / F for k in fallback_day_keys}
    for _ in range(10):
        overflow = 0.0
        for k in fallback_day_keys:
            if alloc[k] > cap:
                overflow += alloc[k] - cap
                alloc[k] = cap
        if overflow <= 1e-6:
            return alloc, 0.0
        uncapped = [k for k in fallback_day_keys if alloc[k] < cap - 1e-9]
        if not uncapped:
            return alloc, overflow
        share = overflow / len(uncapped)
        for k in uncapped:
            alloc[k] += share
    # Trim any residual above cap and report as unallocated
    final_unalloc = 0.0
    for k in fallback_day_keys:
        if alloc[k] > cap:
            final_unalloc += alloc[k] - cap
            alloc[k] = cap
    return alloc, final_unalloc

try:
    # When run as a module or when repo root is on PYTHONPATH.
    from python.gas_entsog_gie_implied import (
        EU27,
        compute_net_imports_mwh_by_day,
        compute_net_withdrawal_mwh_by_day,
        fetch_entsog_physical_flow_daily,
        fetch_gie_storage_daily,
        fetch_interconnection_directions_for_country,
        supabase_upsert_gas_rows,
    )
except ModuleNotFoundError:
    # When run as `python python/gas_backfill_5y_bruegel.py`.
    from gas_entsog_gie_implied import (  # type: ignore
        EU27,
        compute_net_imports_mwh_by_day,
        compute_net_withdrawal_mwh_by_day,
        fetch_entsog_physical_flow_daily,
        fetch_gie_storage_daily,
        fetch_interconnection_directions_for_country,
        supabase_upsert_gas_rows,
    )


ENTSOE_API = "https://web-api.tp.entsoe.eu/api"
EUROSTAT_API = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"

# A pragmatic 1-zone-per-country mapping (EU-only).
# Note: DK/SE/IT can be multi-zone in reality; refine later if you want higher fidelity.
ENTSOE_DOMAIN: Dict[str, str] = {
    "AT": "10YAT-APG------L",
    "BE": "10YBE----------2",
    "BG": "10YCA-BULGARIA-R",
    "HR": "10YHR-HEP------M",
    "CY": "10YCY-1001A0003J",
    "CZ": "10YCZ-CEPS-----N",
    "DK": "10YDK-1--------W",  # DK1
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
    "LU": "10YLU-CEGEDEL-NQ",
    "MT": "10YMT-1001A0003F",
    "NL": "10YNL----------L",
    "PL": "10YPL-AREA-----S",
    "PT": "10YPT-REN------W",
    "RO": "10YRO-TEL------P",
    "SK": "10YSK-SEPS-----K",
    "SI": "10YSI-ELES-----O",
    "ES": "10YES-REE------0",
    "SE": "10YSE-1--------K",
}


def ymdhm(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y%m%d%H%M")


def days_in_month(d: date) -> int:
    next_month = (d.replace(day=28) + timedelta(days=4)).replace(day=1)
    last = next_month - timedelta(days=1)
    return last.day


def tj_to_mwh(tj: float) -> float:
    # 1 TJ = 277.777... MWh
    return tj * (1_000_000.0 / 3600.0)


def fetch_eurostat_monthly_ic_obs_tj_by_month(country: str, session: requests.Session) -> Dict[str, float]:
    """
    Eurostat monthly gas demand backbone.
    Dataset: nrg_cb_gasm, balance item IC_OBS, fuel G3000, unit TJ_GCV.
    Returns: { 'YYYY-MM': TJ_GCV }
    """
    params = {
        "freq": "M",
        "nrg_bal": "IC_OBS",
        "siec": "G3000",
        "unit": "TJ_GCV",
        "geo": country,
    }
    r = session.get(f"{EUROSTAT_API}/nrg_cb_gasm", params=params, timeout=60)
    if not r.ok:
        return {}
    j = r.json()
    values = j.get("value") or {}
    if not values:
        return {}

    time_index = (((j.get("dimension") or {}).get("time") or {}).get("category") or {}).get("index") or {}
    by_pos = {int(pos): str(code) for code, pos in time_index.items()}

    out: Dict[str, float] = {}
    for k, v in values.items():
        try:
            pos = int(k)
            ym = by_pos.get(pos)
            if not ym:
                continue
            out[ym] = float(v)
        except Exception:
            continue
    return out


def fetch_entsoe_a75_xml(token: str, domain: str, start: datetime, end: datetime, session: requests.Session) -> str:
    params = {
        "securityToken": token,
        "documentType": "A75",  # generation per type
        "processType": "A16",  # realised
        "in_Domain": domain,
        "periodStart": ymdhm(start),
        "periodEnd": ymdhm(end),
    }
    # Use shared retrying session; keep a slightly longer read timeout for large XML.
    r = session.get(ENTSOE_API, params=params, timeout=(20, 90))
    text = r.text
    if r.status_code == 429:
        raise RuntimeError(f"ENTSOE 429 Too Many Requests | Retry-After={r.headers.get('Retry-After')}")
    if not r.ok:
        raise RuntimeError(f"ENTSOE HTTP {r.status_code}: {text[:300]}")
    return text


def parse_entsoe_gas_generation_mwh_by_day(xml: str) -> Dict[date, float]:
    """
    Parse ENTSO-E A75 realised generation per type, keep only PSR type B04 (Fossil Gas).
    Returns daily electricity generation in MWh_electric.

    ENTSO-E quantities are in MW (average over interval); convert to MWh by multiplying by interval hours.
    """
    import xml.etree.ElementTree as ET

    root = ET.fromstring(xml)
    out: Dict[date, float] = {}

    for ts_el in root.findall(".//{*}TimeSeries"):
        psr_el = ts_el.find(".//{*}MktPSRType/{*}psrType")
        psr = psr_el.text.strip() if psr_el is not None and psr_el.text else None
        if psr != "B04":
            continue

        period = ts_el.find(".//{*}Period")
        if period is None:
            continue

        start_el = period.find(".//{*}timeInterval/{*}start")
        res_el = period.find(".//{*}resolution")
        if start_el is None or not start_el.text or res_el is None or not res_el.text:
            continue

        try:
            period_start = datetime.fromisoformat(start_el.text.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            continue

        resolution = res_el.text.strip()
        if resolution == "PT15M":
            step_minutes = 15
        elif resolution == "PT30M":
            step_minutes = 30
        else:
            step_minutes = 60
        step_hours = step_minutes / 60.0

        for point_el in period.findall(".//{*}Point"):
            pos_el = point_el.find(".//{*}position")
            qty_el = point_el.find(".//{*}quantity")
            if pos_el is None or qty_el is None or not pos_el.text or not qty_el.text:
                continue
            try:
                pos = int(pos_el.text)
                mw = float(qty_el.text)
            except Exception:
                continue
            ts = period_start + timedelta(minutes=(pos - 1) * step_minutes)
            d = ts.date()
            out[d] = out.get(d, 0.0) + mw * step_hours

    return out


def fetch_eurostat_household_industry_shares(country: str, year: int, session: requests.Session) -> Optional[Dict[str, float]]:
    """
    Bruegel-style fallback when grid split isn't available EU-wide:
    use Eurostat annual energy balances (nrg_bal_c) to get *official* household vs industry
    final consumption shares for natural gas.

    Household proxy: FC_OTH_HH_E
    Industry: FC_IND_E
    """
    def fetch_one(code: str) -> Optional[float]:
        # Eurostat API doesn't reliably accept comma-separated multi-values for nrg_bal,
        # so we query each balance item separately.
        params = {
            "freq": "A",
            "nrg_bal": code,
            "siec": "G3000",
            "unit": "TJ",
            "geo": country,
            "time": str(year),
        }
        r = session.get(f"{EUROSTAT_API}/nrg_bal_c", params=params, timeout=60)
        if not r.ok:
            return None
        j = r.json()
        values = j.get("value") or {}
        if not values:
            return None
        # With all dimensions fully filtered, the sole value is at flat index 0.
        v = values.get("0")
        if v is None:
            # Sometimes the key is numeric 0
            v = values.get(0)
        if v is None:
            return None
        try:
            return float(v)
        except Exception:
            return None

    ind = fetch_one("FC_IND_E")
    hh = fetch_one("FC_OTH_HH_E")
    if ind is None or hh is None:
        return None

    denom = ind + hh
    if denom <= 0:
        return None
    return {"household": hh / denom, "industry": ind / denom}


def fetch_best_available_shares(country: str, target_year: int, session: requests.Session, max_lookback_years: int = 6) -> Tuple[Optional[Dict[str, float]], Optional[int]]:
    """
    Try target_year, then target_year-1, ... for max_lookback_years.
    Returns (shares, year_used).
    """
    for y in range(target_year, target_year - max_lookback_years - 1, -1):
        sh = fetch_eurostat_household_industry_shares(country, y, session=session)
        if sh and sh.get("household") is not None and sh.get("industry") is not None:
            return sh, y
    return None, None


def compute_country_backfill(
    country: str,
    start_day: date,
    end_day: date,
    gie_apikey: str,
    entsoe_token: str,
    efficiency: float = 0.50,
    session: Optional[requests.Session] = None,
) -> List[dict]:
    """
    Bruegel-aligned sector backfill:
    - total = ENTSOG net imports + GIE net withdrawals (MWh/day)
    - power = ENTSO-E gas generation (B04) converted to gas burn via efficiency (default 50%)
    - household/industry split = fixed shares of non-power for now (can be replaced with distribution/transmission proxies).
    """
    s = session or requests.Session()

    euro_ic_obs_tj_by_month = fetch_eurostat_monthly_ic_obs_tj_by_month(country, session=s)

    # Total (implied) from ENTSOG+GIE
    directions = fetch_interconnection_directions_for_country(country, session=s)
    tids = [d.tso_item_identifier for d in directions]
    flows = fetch_entsog_physical_flow_daily(tids, start_day, end_day, session=s)
    net_imports = compute_net_imports_mwh_by_day(directions, flows)

    gie_rows = fetch_gie_storage_daily(country, start_day, end_day, apikey=gie_apikey, session=s)
    net_withdrawal = compute_net_withdrawal_mwh_by_day(gie_rows)

    # Power from ENTSO-E A75 (gas generation) -> gas burn
    domain = ENTSOE_DOMAIN.get(country)
    gas_power_mwh_by_day: Dict[date, float] = {}
    if domain:
        # Fetch in 14-day chunks to avoid timeouts / large payloads
        chunk = timedelta(days=14)
        cur = start_day
        while cur <= end_day:
            cur_end = min(end_day, cur + chunk)
            start_dt = datetime(cur.year, cur.month, cur.day, 0, 0, tzinfo=timezone.utc)
            end_dt = datetime(cur_end.year, cur_end.month, cur_end.day, 23, 0, tzinfo=timezone.utc)
            xml = fetch_entsoe_a75_xml(entsoe_token, domain, start_dt, end_dt, session=s)
            elec_mwh = parse_entsoe_gas_generation_mwh_by_day(xml)
            for d, v in elec_mwh.items():
                gas_power_mwh_by_day[d] = gas_power_mwh_by_day.get(d, 0.0) + (v / max(efficiency, 1e-6))
            time.sleep(0.25)
            cur = cur_end + timedelta(days=1)

    # Non-power split shares: Eurostat annual official household vs industry shares (Bruegel-style fallback).
    # If unavailable, fallback to env shares.
    shares_cache: Dict[int, Tuple[Optional[Dict[str, float]], Optional[int]]] = {}

    # First pass: collect raw inputs per day (we apply a month-level budget in a second pass)
    day_rows: List[dict] = []
    days = (end_day - start_day).days
    for i in range(days + 1):
        d = start_day + timedelta(days=i)
        net_imp = net_imports.get(d, 0.0)
        net_wd = net_withdrawal.get(d, 0.0)
        total_unclamped = net_imp + net_wd
        implied_total = total_unclamped
        if implied_total < 0:
            implied_total = 0.0

        raw_power = gas_power_mwh_by_day.get(d, 0.0)

        ym = d.strftime("%Y-%m")
        euro_tj = euro_ic_obs_tj_by_month.get(ym)
        euro_month_mwh = tj_to_mwh(euro_tj) if euro_tj is not None else None
        euro_day_mwh = (euro_month_mwh / days_in_month(d)) if euro_month_mwh is not None else None

        implied_ok = bool(implied_total > 0 and (raw_power <= 0 or implied_total >= raw_power))

        y = d.year
        cached = shares_cache.get(y)
        if cached is None:
            sh, year_used = fetch_best_available_shares(country, y, session=s)
            shares_cache[y] = (sh, year_used)
        else:
            sh, year_used = cached

        if sh and sh.get("household") is not None and sh.get("industry") is not None:
            hh_share = float(sh["household"])
            ind_share = float(sh["industry"])
            if year_used == y:
                source_split = "eurostat_exact_year_nrg_bal_c"
            else:
                source_split = f"eurostat_previous_year_nrg_bal_c:{year_used}"
        else:
            hh_share = float(os.getenv("GAS_HOUSEHOLD_SHARE", "0.5"))
            ind_share = float(os.getenv("GAS_INDUSTRY_SHARE", "0.5"))
            ssum = hh_share + ind_share
            if ssum <= 0:
                hh_share, ind_share = 0.5, 0.5
            else:
                hh_share, ind_share = hh_share / ssum, ind_share / ssum
            source_split = "fallback_env_shares_nonpower"

        day_rows.append(
            {
                "country_code": country,
                "gas_day": d.isoformat(),
                "ym": ym,
                "net_imports_mwh": net_imp,
                "net_withdrawal_mwh": net_wd,
                "total_unclamped_mwh": total_unclamped,
                "implied_total_mwh": implied_total,
                "implied_ok": implied_ok,
                "eurostat_ic_obs_tj_gcv": euro_tj,
                "eurostat_ic_obs_month_mwh": euro_month_mwh,
                "eurostat_ic_obs_day_mwh": euro_day_mwh,
                "raw_power_mwh": raw_power,
                "hh_share_nonpower": hh_share,
                "ind_share_nonpower": ind_share,
                "source_split": source_split,
            }
        )

    # Second pass: implied daily values are NEVER rewritten. For each month with a
    # Eurostat monthly target, only fallback days are adjusted, and they share only
    # the remaining headroom: max(0, eurostat_month - sum(implied_days_in_month)).
    # If there is no Eurostat target, fallback days stay at their implied_total (0
    # by construction). If there are no fallback days, nothing in the month is
    # adjusted.
    by_month: Dict[str, List[dict]] = {}
    for r in day_rows:
        by_month.setdefault(r["ym"], []).append(r)

    def base_raw(r: dict, month_target: Optional[float]) -> dict:
        return {
            "net_imports_mwh": r["net_imports_mwh"],
            "net_withdrawal_mwh": r["net_withdrawal_mwh"],
            "total_unclamped_mwh": r["total_unclamped_mwh"],
            "implied_total_mwh": r["implied_total_mwh"],
            "eurostat_ic_obs_month": r["ym"],
            "eurostat_ic_obs_tj_gcv": r["eurostat_ic_obs_tj_gcv"],
            "eurostat_ic_obs_month_mwh": month_target,
            "eurostat_ic_obs_day_mwh": r["eurostat_ic_obs_day_mwh"],
            "entsoe_domain": domain,
            "efficiency": efficiency,
            "raw_power_mwh": float(r["raw_power_mwh"]),
            "hh_share_nonpower": float(r["hh_share_nonpower"]),
            "ind_share_nonpower": float(r["ind_share_nonpower"]),
            "tso_item_identifiers": len(tids),
        }

    def build_row(
        r: dict,
        total: Optional[float],
        selector: str,
        source_total: str,
        budget_mode: str,
        month_target: Optional[float],
        implied_sum: Optional[float],
        remainder: Optional[float],
        day_is_fallback: bool,
        month_is_complete: Optional[bool] = None,
        month_unallocated_mwh: Optional[float] = None,
        calibration_factor: Optional[float] = None,
    ) -> dict:
        raw_power = float(r["raw_power_mwh"])
        if total is None:
            power = None
            hh = None
            ind = None
            quality_flag = "no_data_gap"
        else:
            power = min(raw_power, total)
            if calibration_factor is not None:
                quality_flag = "calibrated_to_eurostat_monthly"
            elif raw_power > total:
                quality_flag = "power_capped_to_total"
            elif day_is_fallback:
                quality_flag = "eurostat_fallback_allocated"
            else:
                quality_flag = "observed_total_entsoe_power"
            nonpower = max(0.0, total - power)
            hh = nonpower * float(r["hh_share_nonpower"])
            ind = nonpower * float(r["ind_share_nonpower"])
            ind += nonpower - (hh + ind)
        raw = base_raw(r, month_target)
        raw.update(
            {
                "total_selector": selector,
                "total_budget_mode": budget_mode,
                "month_target_mwh": month_target,
                "month_implied_sum_mwh": implied_sum,
                "month_remainder_mwh": remainder,
                "month_unallocated_mwh": month_unallocated_mwh,
                "month_is_complete": month_is_complete,
                "month_calibration_factor": calibration_factor,
                "month_scale": calibration_factor,
                "day_is_fallback": day_is_fallback,
            }
        )
        return {
            "country_code": country,
            "gas_day": r["gas_day"],
            "total_mwh": total,
            "power_mwh": power,
            "household_mwh": hh,
            "industry_mwh": ind,
            "source_total": source_total,
            "source_power": "entsoe_a75_b04",
            "source_split": r["source_split"],
            "method_version": "v2_bruegel_power_entsoe",
            "quality_flag": quality_flag,
            "raw": raw,
        }

    calibrate_set = {
        c.strip().upper()
        for c in (os.getenv("GAS_CALIBRATE_COUNTRIES", "DE") or "").split(",")
        if c.strip()
    }
    calibrate_this = country.upper() in calibrate_set
    calib_min_implied_days = int(os.getenv("GAS_CALIBRATE_MIN_IMPLIED_DAYS", "7"))
    calib_lo = float(os.getenv("GAS_CALIBRATE_FACTOR_MIN", "0.2"))
    calib_hi = float(os.getenv("GAS_CALIBRATE_FACTOR_MAX", "5.0"))

    out: List[dict] = []
    for ym, rows in by_month.items():
        rows.sort(key=lambda x: x["gas_day"])
        month_target_raw = rows[0].get("eurostat_ic_obs_month_mwh")
        month_target = float(month_target_raw) if month_target_raw is not None else None

        y_i, m_i = int(ym[:4]), int(ym[5:7])
        expected_days = calendar.monthrange(y_i, m_i)[1]
        month_is_complete = len({r["gas_day"] for r in rows}) == expected_days

        implied_days = [r for r in rows if r["implied_ok"]]
        fallback_days = [r for r in rows if not r["implied_ok"]]
        implied_sum = sum(float(r["implied_total_mwh"]) for r in implied_days)

        # Branch A: per-month calibration (scale implied to match Eurostat monthly).
        # Only for countries in GAS_CALIBRATE_COUNTRIES, on complete months with
        # an Eurostat target, with enough implied days to derive a stable factor.
        calib_factor: Optional[float] = None
        if (
            calibrate_this
            and month_is_complete
            and month_target is not None
            and implied_sum > 0
            and len(implied_days) >= calib_min_implied_days
        ):
            raw_factor = month_target / implied_sum
            if calib_lo <= raw_factor <= calib_hi:
                calib_factor = raw_factor

        if calib_factor is not None:
            for r in rows:
                is_implied = r["implied_ok"]
                if is_implied:
                    out.append(
                        build_row(
                            r,
                            total=float(r["implied_total_mwh"]) * calib_factor,
                            selector="implied_calibrated_to_eurostat_monthly",
                            source_total="entsog_gie_implied_daily_calibrated",
                            budget_mode="per_month_calibration_to_eurostat",
                            month_target=month_target,
                            implied_sum=implied_sum,
                            remainder=0.0,
                            day_is_fallback=False,
                            month_is_complete=True,
                            month_unallocated_mwh=0.0,
                            calibration_factor=calib_factor,
                        )
                    )
                else:
                    out.append(
                        build_row(
                            r,
                            total=None,
                            selector="fallback_absorbed_by_calibration",
                            source_total="none_absorbed_by_calibration",
                            budget_mode="per_month_calibration_to_eurostat",
                            month_target=month_target,
                            implied_sum=implied_sum,
                            remainder=0.0,
                            day_is_fallback=True,
                            month_is_complete=True,
                            month_unallocated_mwh=0.0,
                            calibration_factor=calib_factor,
                        )
                    )
            continue

        # Branch B: no Eurostat target, or no fallback days, or month is incomplete.
        # Implied days keep their observed values; fallback days become NULL (gap).
        if month_target is None or not fallback_days or not month_is_complete:
            if month_target is None:
                budget_mode_noop = "no_eurostat_month_implied_only"
            elif not month_is_complete:
                budget_mode_noop = "month_incomplete_eurostat_budget_skipped"
            else:
                budget_mode_noop = "implied_untouched_no_fallback_allocation"

            for r in rows:
                is_implied = r["implied_ok"]
                if is_implied:
                    out.append(
                        build_row(
                            r,
                            total=float(r["implied_total_mwh"]),
                            selector="implied_observed",
                            source_total="entsog_gie_implied_daily",
                            budget_mode=budget_mode_noop,
                            month_target=month_target,
                            implied_sum=implied_sum if month_target is not None else None,
                            remainder=None,
                            day_is_fallback=False,
                            month_is_complete=month_is_complete,
                            month_unallocated_mwh=None,
                        )
                    )
                    continue
                if month_target is None:
                    selector = "no_fallback_filler_no_eurostat"
                    source_total = "none_no_eurostat_month"
                elif not month_is_complete:
                    selector = "fallback_no_data_month_incomplete"
                    source_total = "none_month_incomplete"
                else:
                    selector = "fallback_no_data"
                    source_total = "none_no_fallback_days"
                out.append(
                    build_row(
                        r,
                        total=None,
                        selector=selector,
                        source_total=source_total,
                        budget_mode=budget_mode_noop,
                        month_target=month_target,
                        implied_sum=implied_sum if month_target is not None else None,
                        remainder=None,
                        day_is_fallback=True,
                        month_is_complete=month_is_complete,
                        month_unallocated_mwh=None,
                    )
                )
            continue

        # Branch C: complete month with Eurostat target and fallback days: capped allocation.
        remainder = max(0.0, month_target - implied_sum)
        fallback_keys = [r["gas_day"] for r in fallback_days]
        implied_totals = [float(r["implied_total_mwh"]) for r in implied_days]
        alloc_by_day, unallocated = _allocate_remainder_capped(
            remainder=remainder,
            fallback_day_keys=fallback_keys,
            implied_day_totals=implied_totals,
            days_in_month=expected_days,
        )

        for r in rows:
            is_implied = r["implied_ok"]
            if is_implied:
                out.append(
                    build_row(
                        r,
                        total=float(r["implied_total_mwh"]),
                        selector="implied_observed",
                        source_total="entsog_gie_implied_daily",
                        budget_mode="implied_untouched_eurostat_fills_remainder_capped",
                        month_target=month_target,
                        implied_sum=implied_sum,
                        remainder=remainder,
                        day_is_fallback=False,
                        month_is_complete=True,
                        month_unallocated_mwh=unallocated,
                    )
                )
            else:
                out.append(
                    build_row(
                        r,
                        total=float(alloc_by_day.get(r["gas_day"], 0.0)),
                        selector="budget_eurostat_allocated_remainder_capped",
                        source_total="eurostat_nrg_cb_gasm_ic_obs_monthly_budgeted",
                        budget_mode="implied_untouched_eurostat_fills_remainder_capped",
                        month_target=month_target,
                        implied_sum=implied_sum,
                        remainder=remainder,
                        day_is_fallback=True,
                        month_is_complete=True,
                        month_unallocated_mwh=unallocated,
                    )
                )

    return out


def main():
    load_dotenv()
    supabase_url = os.getenv("SUPABASE_URL")
    service_role = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    gie_key = os.getenv("GIE_API_KEY")
    entsoe_token = os.getenv("ENTSOE_API_TOKEN")
    if not supabase_url or not service_role:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY in .env")
    if not gie_key:
        raise RuntimeError("Missing GIE_API_KEY in .env")
    if not entsoe_token:
        raise RuntimeError("Missing ENTSOE_API_TOKEN in .env")

    years = int(os.getenv("GAS_BACKFILL_YEARS", "5"))
    chunk_days = int(os.getenv("GAS_BACKFILL_CHUNK_DAYS", "14"))
    delay_s = float(os.getenv("GAS_BACKFILL_DELAY_SECONDS", "0.2"))
    method_version = os.getenv("GAS_METHOD_VERSION", "v2_bruegel_power_entsoe")
    start_country = (os.getenv("GAS_START_COUNTRY") or "").strip().upper() or None
    resume_from_db = os.getenv("GAS_RESUME_FROM_DB", "0").strip() in ("1", "true", "TRUE", "yes", "YES")
    force_recompute = os.getenv("GAS_FORCE_RECOMPUTE", "0").strip() in ("1", "true", "TRUE", "yes", "YES")

    end = date.today()
    start = end - timedelta(days=years * 365)

    # Optional: limit countries for testing: "FR,DE,ES"
    countries_env = os.getenv("GAS_COUNTRIES")
    countries = [c.strip().upper() for c in countries_env.split(",")] if countries_env else list(EU27)
    if start_country and start_country in countries:
        countries = countries[countries.index(start_country) :]
    if start_country and start_country not in countries:
        print(f"NOTE: GAS_START_COUNTRY={start_country} not in GAS_COUNTRIES list; ignoring start_country.")

    print(
        f"Backfill settings: method_version={method_version} resume_from_db={resume_from_db} force_recompute={force_recompute} start_country={start_country or ''}"
    )
    if countries:
        print(f"Countries to process (first 10): {', '.join(countries[:10])}")

    # Use the retrying session from the shared module (handles RemoteDisconnected etc.)
    try:
        from python.gas_entsog_gie_implied import make_retrying_session  # type: ignore
    except ModuleNotFoundError:
        from gas_entsog_gie_implied import make_retrying_session  # type: ignore

    s = make_retrying_session()

    def fetch_max_existing_day(country_code: str) -> Optional[date]:
        if not resume_from_db or force_recompute:
            return None
        url = (
            f"{supabase_url.rstrip('/')}/rest/v1/gas_demand_daily"
            f"?select=gas_day&country_code=eq.{country_code}&method_version=eq.{method_version}"
            f"&order=gas_day.desc&limit=1"
        )
        headers = {
            "apikey": service_role,
            "Authorization": f"Bearer {service_role}",
            "Content-Type": "application/json",
        }
        try:
            r = s.get(url, headers=headers, timeout=60)
            if not r.ok:
                return None
            rows = r.json()
            if not rows:
                return None
            return date.fromisoformat(str(rows[0]["gas_day"])[:10])
        except Exception:
            return None

    for c in countries:
        print(f"\n== Backfill {c} {start} -> {end} ==")
        cur = start
        last = fetch_max_existing_day(c)
        if last and last >= cur:
            cur = last + timedelta(days=1)
            if cur > end:
                print(f"  already complete through {last}; skipping")
                continue
        while cur <= end:
            cur_end = min(end, cur + timedelta(days=chunk_days))
            try:
                rows = compute_country_backfill(
                    c, cur, cur_end, gie_apikey=gie_key, entsoe_token=entsoe_token, session=s
                )
                # Ensure method_version matches runner setting
                for rr in rows:
                    rr["method_version"] = method_version
                supabase_upsert_gas_rows(supabase_url, service_role, rows)
                print(f"  upserted {len(rows)} rows for {cur} -> {cur_end}")
                cur = cur_end + timedelta(days=1)
            except requests.exceptions.RequestException as e:
                # Transient network issue: backoff and retry same chunk.
                print(f"  network error for {c} {cur}->{cur_end}: {e}. Retrying in 10s...")
                time.sleep(10)
            if delay_s > 0:
                time.sleep(delay_s)

    print("\nDone.")


if __name__ == "__main__":
    main()


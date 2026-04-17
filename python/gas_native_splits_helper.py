"""
Shared helper: enrich native daily rows (total-only) with sector splits.

Used by AT (AGGM), DK (Energinet), ES (Enagás) native extractors whose source
feeds don't include a power / household / industry breakdown. We recover the
split with Bruegel's method:

  power_mwh     = ENTSO-E gas-fired electricity generation (PSR=B04)
                  divided by assumed thermal efficiency (default 0.5).
  remainder     = total - power   (clamped >= 0)
  household_mwh = remainder * Eurostat HH share (FC_OTH_HH_E)
  industry_mwh  = remainder * Eurostat industry share (FC_IND_E)

Eurostat shares are fetched per year (falling back up to 6 years if the most
recent is not yet published) and cached in-process.

This helper is intentionally idempotent: it fills missing splits for rows that
already have a total, but never overwrites a total that the native source
provides.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import requests

from gas_backfill_5y_bruegel import (
    ENTSOE_DOMAIN,
    fetch_best_available_shares,
    fetch_entsoe_a75_xml,
    parse_entsoe_gas_generation_mwh_by_day,
)


def fetch_gas_for_power_by_day(
    country: str,
    start: date,
    end: date,
    entsoe_token: str,
    efficiency: float,
    session: requests.Session,
    log_prefix: str = "  ",
) -> Dict[date, float]:
    """
    Return {gas_day: gas_burn_mwh} for the window [start, end] for ``country``,
    by fetching ENTSO-E A75 (PSR=B04) in monthly chunks (same strategy the DE
    native extractor uses — yearly chunks trip the 90s API timeout).

    Returns an empty dict if the ENTSO-E domain is unknown or the token is missing.
    """
    domain = ENTSOE_DOMAIN.get(country)
    if not domain or not entsoe_token:
        return {}
    out: Dict[date, float] = {}
    cursor = date(start.year, start.month, 1)
    end_anchor = date(end.year, end.month, 1)
    while cursor <= end_anchor:
        y, m = cursor.year, cursor.month
        nxt = date(y + (1 if m == 12 else 0), 1 if m == 12 else m + 1, 1)
        st = datetime(y, m, 1, tzinfo=timezone.utc)
        en = datetime(nxt.year, nxt.month, nxt.day, tzinfo=timezone.utc)
        for attempt in range(1, 4):
            try:
                xml = fetch_entsoe_a75_xml(entsoe_token, domain, st, en, session)
                daily = parse_entsoe_gas_generation_mwh_by_day(xml)
                for d, mwh_elec in daily.items():
                    if start <= d <= end:
                        out[d] = mwh_elec / max(efficiency, 1e-6)
                print(f"{log_prefix}{country} ENTSO-E {y}-{m:02d}: {len(daily)} days", flush=True)
                break
            except Exception as e:
                if attempt == 3:
                    print(f"{log_prefix}{country} ENTSO-E {y}-{m:02d}: failed 3x: {e}", flush=True)
        cursor = nxt
    return out


def resolve_shares_for_years(
    country: str,
    years: List[int],
    session: requests.Session,
    log_prefix: str = "  ",
) -> Dict[int, Tuple[float, float]]:
    """
    Return {year: (hh_share, ind_share)}. Uses ``fetch_best_available_shares``
    which falls back to earlier years if the most recent Eurostat annual entry
    isn't yet published.

    Shares always sum to 1 (normalized); if Eurostat returns nothing for any
    year, we fall back to 50/50 so downstream splits stay well-defined.
    """
    out: Dict[int, Tuple[float, float]] = {}
    for y in years:
        sh, _year_used = fetch_best_available_shares(country, y, session=session)
        if sh and sh.get("household") is not None and sh.get("industry") is not None:
            hh = float(sh["household"])
            ind = float(sh["industry"])
            s = hh + ind
            if s <= 0:
                hh, ind = 0.5, 0.5
            else:
                hh, ind = hh / s, ind / s
            out[y] = (hh, ind)
            print(f"{log_prefix}{country} Eurostat shares {y}: HH={hh:.3f} Ind={ind:.3f}", flush=True)
        else:
            out[y] = (0.5, 0.5)
            print(f"{log_prefix}{country} Eurostat shares {y}: none -> 50/50 fallback", flush=True)
    return out


def enrich_row_with_split(
    row: dict,
    gas_for_power: Dict[date, float],
    shares_by_year: Dict[int, Tuple[float, float]],
) -> dict:
    """
    Return the row with power/household/industry fields populated (mutates-and-
    returns a shallow-copied dict). The row must already have total_mwh and
    gas_day. Any previously non-null split values are preserved (we never
    overwrite a real native split).
    """
    r = dict(row)
    if r.get("total_mwh") is None:
        return r
    try:
        d = datetime.fromisoformat(str(r["gas_day"])).date()
    except Exception:
        return r

    total = float(r["total_mwh"])
    power = r.get("power_mwh")
    if power is None:
        p = gas_for_power.get(d)
        if p is not None:
            power = max(0.0, min(float(p), total))
            r["power_mwh"] = power
            r["source_power"] = "entsoe_a75_b04"

    # Use share of the row's year; fallback to any available share if missing.
    year_shares = shares_by_year.get(d.year)
    if year_shares is None and shares_by_year:
        year_shares = next(iter(shares_by_year.values()))
    if year_shares is None:
        year_shares = (0.5, 0.5)
    hh_share, ind_share = year_shares

    if r.get("household_mwh") is None or r.get("industry_mwh") is None:
        remainder = max(0.0, total - float(r.get("power_mwh") or 0.0))
        if r.get("household_mwh") is None:
            r["household_mwh"] = remainder * hh_share
        if r.get("industry_mwh") is None:
            r["industry_mwh"] = remainder * ind_share
        raw = dict(r.get("raw") or {})
        raw.setdefault("hh_share_nonpower", hh_share)
        raw.setdefault("ind_share_nonpower", ind_share)
        raw.setdefault("split_source", "eurostat_annual_shares + entsoe_a75_power")
        r["raw"] = raw
        # Flag the split as derived so we can tell it apart from native splits.
        if r.get("source_split") in (None, "", r.get("source_total")):
            r["source_split"] = "entsoe_eurostat_derived"
    return r


def enrich_rows_with_split(
    rows: List[dict],
    country: str,
    entsoe_token: Optional[str],
    efficiency: float,
    session: requests.Session,
    log_prefix: str = "  ",
) -> List[dict]:
    """
    Convenience wrapper: take a list of native rows for one country and enrich
    them in-place by computing the ENTSO-E power column and Eurostat-weighted
    HH/industry split for any row that still has NULLs.

    Returns a new list.
    """
    if not rows:
        return rows
    days = []
    years: set[int] = set()
    for r in rows:
        try:
            d = datetime.fromisoformat(str(r["gas_day"])).date()
            days.append(d)
            years.add(d.year)
        except Exception:
            continue
    if not days:
        return rows
    start, end = min(days), max(days)

    gas_for_power: Dict[date, float] = {}
    if entsoe_token:
        gas_for_power = fetch_gas_for_power_by_day(
            country, start, end, entsoe_token, efficiency, session, log_prefix=log_prefix
        )
    else:
        print(f"{log_prefix}{country}: no ENTSOE_API_TOKEN, power stays NULL", flush=True)

    shares_by_year = resolve_shares_for_years(country, sorted(years), session, log_prefix=log_prefix)
    return [enrich_row_with_split(r, gas_for_power, shares_by_year) for r in rows]

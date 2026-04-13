import csv
import os
import sys
import time
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

try:
    import requests
except ImportError:
    print("Missing dependency: requests. Install with: pip install requests", file=sys.stderr)
    raise


RTE_TOKEN_URL = "https://digital.iservices.rte-france.com/token/oauth/"
RTE_MIX_URL = "https://digital.iservices.rte-france.com/open_api/actual_generation/v1/generation_mix_15min_time_scale"


def iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    # RTE expects: YYYY-MM-DDThh:mm:ss+02:00 (any timezone is allowed).
    # Use explicit offset and seconds only (no 'Z', no microseconds).
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")


def get_access_token(client_id: str, client_secret: str) -> str:
    basic = requests.auth._basic_auth_str(client_id, client_secret)
    r = requests.post(
        RTE_TOKEN_URL,
        headers={
            "Authorization": basic,
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data="",
        timeout=30,
    )
    r.raise_for_status()
    j = r.json()
    tok = j.get("access_token")
    if not tok:
        raise RuntimeError("RTE token response missing access_token")
    return tok


def compute_renewable_percent_for_timestamp(mix_rows, target_ts_iso: str):
    excluded_types = {"EXCHANGE", "PUMPING"}

    renewable = 0.0
    non_renewable = 0.0

    for series in mix_rows:
        ptype = series.get("production_type")
        psub = series.get("production_subtype")
        if not ptype or ptype in excluded_types:
            continue

        key = f"{ptype}:{psub}" if psub and psub != "TOTAL" else ptype

        # find matching point
        val = None
        for v in series.get("values") or []:
            if v.get("start_date") == target_ts_iso and isinstance(v.get("value"), (int, float)):
                val = float(v["value"])
                break
        if val is None:
            continue

        if key.startswith("HYDRO:HYDRO_PUMPED_STORAGE"):
            # storage, exclude
            continue

        if key == "SOLAR" or key.startswith("WIND") or key.startswith("HYDRO") or key.startswith("BIOENERGY"):
            renewable += val
        elif key == "NUCLEAR" or key.startswith("FOSSIL_"):
            non_renewable += val
        else:
            non_renewable += val  # conservative default

    total = renewable + non_renewable
    if total <= 0:
        return None, renewable, non_renewable, total
    return (renewable / total) * 100.0, renewable, non_renewable, total


def extract_points(mix_json):
    rows = mix_json.get("generation_mix_15min_time_scale") or []
    # collect all timestamps present
    ts_set = set()
    for series in rows:
        for v in series.get("values") or []:
            sd = v.get("start_date")
            if isinstance(sd, str):
                ts_set.add(sd)

    ts_list = sorted(ts_set)
    out = []
    for ts in ts_list:
        pct, ren, non, total = compute_renewable_percent_for_timestamp(rows, ts)
        if pct is None:
            continue
        out.append(
            {
                "ts": ts,
                "renewable_percent": pct,
                "renewable_mw": ren,
                "non_renewable_mw": non,
                "total_mw": total,
            }
        )
    return out


def main():
    load_dotenv()
    client_id = os.getenv("RTE_CLIENT_ID")
    client_secret = os.getenv("RTE_CLIENT_SECRET")
    if not client_id or not client_secret:
        print("Missing RTE_CLIENT_ID / RTE_CLIENT_SECRET in .env", file=sys.stderr)
        sys.exit(1)

    out_path = os.getenv("RTE_FRANCE_CSV", "rte_france_mix_15min.csv")
    years = int(os.getenv("RTE_BACKFILL_YEARS", "5"))
    chunk_days = int(os.getenv("RTE_CHUNK_DAYS", "14"))
    if chunk_days > 14:
        print("RTE_CHUNK_DAYS capped to 14 (RTE recommendation).")
        chunk_days = 14

    # aggressive mode knobs
    sleep_s = float(os.getenv("RTE_SLEEP_SECONDS", "0"))
    max_requests = int(os.getenv("RTE_MAX_REQUESTS", "1000000"))

    now = datetime.now(timezone.utc)
    target_start = now - timedelta(days=years * 365)
    end = now

    # Prepare CSV (append mode, write header if new)
    file_exists = os.path.exists(out_path)
    f = open(out_path, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(
        f,
        fieldnames=["ts", "renewable_percent", "renewable_mw", "non_renewable_mw", "total_mw"],
    )
    if not file_exists:
        writer.writeheader()

    # Note: we intentionally avoid “monotonic timestamp” dedupe here because the backfill
    # walks backwards in time (timestamps decrease between chunks). Any duplicates at
    # chunk boundaries can be de-duplicated later by (ts) if needed.

    token = None
    token_issued_at = 0.0

    def ensure_token():
        nonlocal token, token_issued_at
        # token valid ~2h; refresh every 90 minutes
        if not token or (time.time() - token_issued_at) > 90 * 60:
            token = get_access_token(client_id, client_secret)
            token_issued_at = time.time()
        return token

    req_count = 0
    backoff = 1.0
    current_chunk_days = chunk_days

    try:
        while end > target_start and req_count < max_requests:
            start = end - timedelta(days=current_chunk_days)
            if start < target_start:
                start = target_start

            ensure_token()

            params = {"start_date": iso(start), "end_date": iso(end)}
            headers = {"Authorization": f"Bearer {token}"}

            try:
                r = requests.get(RTE_MIX_URL, headers=headers, params=params, timeout=60)
            except Exception as e:
                print(f"Request failed ({e}); sleeping {backoff:.1f}s", file=sys.stderr)
                time.sleep(backoff)
                backoff = min(backoff * 2, 300)
                continue

            req_count += 1

            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                wait = float(retry_after) if retry_after and retry_after.isdigit() else backoff
                print(f"429 Too Many Requests. Retry-After={retry_after}. Sleeping {wait:.1f}s", file=sys.stderr)
                time.sleep(wait)
                backoff = min(max(backoff * 2, 1.0), 600)
                continue

            if r.status_code == 401:
                # token likely expired; refresh and retry once
                print("401 Unauthorized. Refreshing token and retrying once...", file=sys.stderr)
                token = None
                ensure_token()
                continue

            if not r.ok:
                # Try to parse functional errors to adapt chunk size.
                err_code = None
                try:
                    err_json = r.json()
                    err_code = err_json.get("error")
                except Exception:
                    err_json = None

                if r.status_code == 400 and err_code == "ACTUALGEN_MIX15_F03":
                    # Period too long: reduce chunk size and retry same end boundary.
                    if current_chunk_days > 1:
                        current_chunk_days = max(1, current_chunk_days // 2)
                        print(
                            f"400 {err_code} (period too long). Reducing chunk to {current_chunk_days} day(s) and retrying…",
                            file=sys.stderr,
                        )
                        continue

                if r.status_code == 400 and err_code == "ACTUALGEN_MIX15_F06":
                    print(
                        f"400 {err_code} (date format). start_date={params['start_date']} end_date={params['end_date']}",
                        file=sys.stderr,
                    )

                # Show a prepared URL for debugging (no secrets).
                try:
                    prep = requests.Request("GET", RTE_MIX_URL, params=params).prepare()
                    debug_url = prep.url
                except Exception:
                    debug_url = None

                prefix = f"HTTP {r.status_code}"
                if err_code:
                    prefix += f" {err_code}"
                if debug_url:
                    prefix += f" | {debug_url}"
                msg = (r.text or "")[:300]
                print(f"{prefix}: {msg}", file=sys.stderr)
                time.sleep(backoff)
                backoff = min(backoff * 2, 300)
                continue

            backoff = 1.0
            # If we had reduced chunk size earlier, try to creep back up to the configured maximum after a success.
            if current_chunk_days < chunk_days:
                current_chunk_days = min(chunk_days, current_chunk_days * 2)
            mix_json = r.json()
            points = extract_points(mix_json)
            if not points:
                print(f"No points for range {iso(start)} -> {iso(end)}", file=sys.stderr)
            else:
                for row in points:
                    writer.writerow(row)
                f.flush()

            print(
                f"OK {req_count} | wrote {len(points)} points | chunk={current_chunk_days}d | range {iso(start)} -> {iso(end)} | next end={iso(start)}"
            )

            end = start
            if sleep_s > 0:
                time.sleep(sleep_s)

    finally:
        f.close()


if __name__ == "__main__":
    main()


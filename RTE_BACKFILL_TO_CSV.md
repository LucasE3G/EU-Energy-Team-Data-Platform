## RTE France backfill → CSV (push until 429)

This script pulls France `generation_mix_15min_time_scale` in **14-day chunks** (RTE recommendation) and computes renewable share for each 15‑minute timestamp, writing to a local CSV.

### 1) Install Python deps

```bash
pip install -r requirements.txt
```

### 2) Ensure `.env` has RTE credentials
- `RTE_CLIENT_ID=...`
- `RTE_CLIENT_SECRET=...`

### 3) Run

```bash
python backfill_rte_france_to_csv.py
```

Output: `rte_france_mix_15min.csv` in the project folder.

### Useful knobs (environment variables)
- `RTE_BACKFILL_YEARS` (default `5`)
- `RTE_CHUNK_DAYS` (default `14`, max `14`)
- `RTE_SLEEP_SECONDS` (default `0`) — set to `0.5` or `1` if you start hitting 429s
- `RTE_MAX_REQUESTS` (default very large) — safety cap
- `RTE_FRANCE_CSV` (default `rte_france_mix_15min.csv`)

### What happens on rate limits
- On HTTP **429**, it respects `Retry-After` (if present) and uses exponential backoff.
- On HTTP **401**, it refreshes the OAuth token and continues.


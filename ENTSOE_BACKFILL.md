## ENTSO‑E backfill → Supabase (5 years, all zones)

This script pulls ENTSO‑E **A75 (Generation per type, realised)** for each zone in the EU+EEA+UK set and stores computed renewable share into `energy_mix_snapshots` with `source='entsoe'`.

### 1) Install deps

```bash
pip install -r requirements.txt
```

### 2) Ensure `.env` contains
- `ENTSOE_API_TOKEN=...`
- `SUPABASE_URL=...`
- `SUPABASE_SERVICE_ROLE_KEY=...`

### 3) Run

```bash
python entsoe_backfill_to_supabase.py
```

### Tuning knobs
- `ENTSOE_BACKFILL_YEARS` (default `5`)
- `ENTSOE_CHUNK_DAYS` (default `7`) — smaller chunks reduce XML size; increase cautiously
- `ENTSOE_DELAY_SECONDS` (default `0.2`) — throttling; 0.2s ≈ 5 req/sec avg
- `ENTSOE_ZONES` (optional) — test subset, e.g. `FR,DE,ES`
- `ENTSOE_UPSERT_BATCH` (default `500`)

### Note on volume
5 years at 15‑minute resolution is large. Depending on the zone’s resolution, you’ll store roughly:
- Hourly: ~43,800 points/zone/5y
- 15‑minute: ~175,200 points/zone/5y

### Why this script doesn't use the Python `supabase` library
Some Windows Python environments hit dependency conflicts (e.g. `proxy` argument errors). This script upserts via Supabase **REST** using `requests`, which is more robust.


## Energy Meter (France / RTE) via Supabase Edge Function

### 1) Create the table
- In Supabase SQL editor, run `energy_meter_schema.sql`.

### 2) Create the Edge Function
- In Supabase dashboard: **Edge Functions** → **New function**
- Name: `rte_ingest_france`
- Paste the code from `supabase/functions/rte_ingest_france/index.ts`
- Deploy

### 2b) (Optional) Create the Backfill Edge Function (history)
- In Supabase dashboard: **Edge Functions** → **New function**
- Name: `rte_backfill_france`
- Paste the code from `supabase/functions/rte_backfill_france/index.ts`
- Deploy

### 3) Set secrets (Edge Function environment variables)
In Supabase: **Project Settings** → **Edge Functions** → **Secrets**, add:
- `SUPABASE_URL` = your project URL
- `SUPABASE_SERVICE_ROLE_KEY` = your service role key
- `RTE_CLIENT_ID`
- `RTE_CLIENT_SECRET`
- `INGEST_TOKEN` = (optional) shared secret to protect manual triggering

### 4) Run it once (manual)
If you set `INGEST_TOKEN`, call with:

```bash
curl -X POST "https://<your-project-ref>.supabase.co/functions/v1/rte_ingest_france" \
  -H "Authorization: Bearer <INGEST_TOKEN>"
```

If you did **not** set `INGEST_TOKEN`, you can call it without auth, but that means anyone can trigger ingestion.

### 4b) Backfill history (manual)
The backfill function ingests the last N days (default 7, max 14) of 15-min mix points:

```bash
curl -X POST "https://<your-project-ref>.supabase.co/functions/v1/rte_backfill_france" \
  -H "Content-Type: application/json" \
  -d "{\"days\": 14}"
```

Repeat this over time (or schedule it) until you have the full year.

### 5) Schedule it (recommended)
Use Supabase’s Edge Function scheduling (Cron) to run every 15 minutes (or hourly).
Suggested schedule: `*/15 * * * *`

### 6) Frontend
The `Energy Meter` page now reads directly from the Supabase table `energy_mix_snapshots`, so it works even when served by `python -m http.server`.


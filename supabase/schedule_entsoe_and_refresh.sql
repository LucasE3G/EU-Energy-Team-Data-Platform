-- Schedules for ENTSO‑E ingest + EU aggregate materialized views
-- Run in the Supabase SQL Editor (once).
--
-- What this does:
-- - Calls the `entsoe_ingest_eu_latest` Edge Function every 15 minutes
-- - Refreshes the EU 15m materialized view every 15 minutes
-- - Refreshes the daily/weekly EU materialized views once per day
--
-- Prereqs:
-- - `supabase/functions/entsoe_ingest_eu_latest/config.toml` sets `verify_jwt = false`
-- - The objects in `energy_meter_perf.sql` have already been created
--
-- IMPORTANT:
-- Use the deployed function URL. Two common forms work:
-- - https://<PROJECT_REF>.functions.supabase.co/<function_name>
-- - https://<PROJECT_REF>.supabase.co/functions/v1/<function_name>

create extension if not exists pg_cron;
create extension if not exists pg_net;

-- Helper: base URL for Edge Functions
do $$
declare
  fn_url text := 'https://rvxukmupuzxbrwicowyn.supabase.co/functions/v1/entsoe_ingest_eu_latest';
begin
  -- ENTSO‑E ingest (every 15 minutes)
  perform cron.schedule(
    'entsoe_ingest_eu_latest__15m',
    '*/15 * * * *',
    format($job$
      select
        net.http_post(
          url := %L,
          headers := jsonb_build_object('content-type','application/json'),
          body := '{}'::jsonb
        );
    $job$, fn_url)
  );

  -- Refresh high-granularity EU aggregate (for day/week/month charts)
  perform cron.schedule(
    'refresh_energy_eu_15m_mv__15m',
    '*/15 * * * *',
    $job$
      refresh materialized view public.energy_eu_15m_mv;
    $job$
  );

  -- Refresh daily + weekly rollups once per day (02:20 UTC)
  perform cron.schedule(
    'refresh_energy_eu_daily_weekly_mv__daily',
    '20 2 * * *',
    $job$
      refresh materialized view public.energy_eu_daily_mv;
      refresh materialized view public.energy_eu_weekly_mv;
    $job$
  );
end $$;

-- Inspect jobs:
-- select * from cron.job order by jobname;
--
-- Unschedule (if you need to re-run with different timings):
-- select cron.unschedule(jobid) from cron.job where jobname in (
--   'entsoe_ingest_eu_latest__15m',
--   'refresh_energy_eu_15m_mv__15m',
--   'refresh_energy_eu_daily_weekly_mv__daily'
-- );


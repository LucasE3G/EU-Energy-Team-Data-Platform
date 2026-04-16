-- Schedules for EU gas implied demand ingest (ENTSOG + GIE)
-- Run in the Supabase SQL Editor (once), after deploying the edge function.
--
-- What this does:
-- - Calls the `gas_ingest_eu_latest` Edge Function every day (recommended hourly/daily; start daily)
--
-- Prereqs:
-- - `supabase/functions/gas_ingest_eu_latest/config.toml` sets `verify_jwt = false`
-- - Edge function has secrets set: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, GIE_API_KEY
-- - `gas_demand_schema.sql` has been run to create `public.gas_demand_daily`
--
-- IMPORTANT:
-- Use the deployed function URL:
-- - https://<PROJECT_REF>.supabase.co/functions/v1/gas_ingest_eu_latest

create extension if not exists pg_cron;
create extension if not exists pg_net;

do $$
declare
  fn_url text := 'https://rvxukmupuzxbrwicowyn.supabase.co/functions/v1/gas_ingest_eu_latest';
begin
  -- Gas ingest once per day (03:10 UTC) for the last 10 days (covers revisions)
  perform cron.schedule(
    'gas_ingest_eu_latest__daily',
    '10 3 * * *',
    format($job$
      select
        net.http_post(
          url := %L,
          headers := jsonb_build_object('content-type','application/json'),
          body := jsonb_build_object('days', 10)
        );
    $job$, fn_url)
  );
end $$;

-- Inspect jobs:
-- select * from cron.job order by jobname;
--
-- Unschedule:
-- select cron.unschedule(jobid) from cron.job where jobname in (
--   'gas_ingest_eu_latest__daily'
-- );


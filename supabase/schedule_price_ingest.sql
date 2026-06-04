-- Schedules for ENTSO-E day-ahead electricity price ingest + MV refresh.
-- Run once in the Supabase SQL Editor after deploying entsoe_ingest_price_eu_latest.
--
-- Two separate jobs:
--   1. Price ingest: fetches all zones daily at 13:30 UTC (after ENTSO-E publishes
--      next-day prices at ~12:00-13:00 CET). Runs again at 07:30 in case the 13:30
--      run missed yesterday's late-publishing zones.
--   2. MV refresh: runs every 2 hours independently of the ingest, so a timeout or
--      partial ingest run does not leave the EU price materialized views stale.
--
-- Prereqs:
--   - `electricity_price_perf.sql` and `electricity_price_refresh_fn.sql` applied
--   - `entsoe_ingest_price_eu_latest` deployed with `verify_jwt = false`
--   - Secrets ENTSOE_API_TOKEN, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY set

create extension if not exists pg_cron;
create extension if not exists pg_net;

do $$
declare
  fn_url text := 'https://rvxukmupuzxbrwicowyn.supabase.co/functions/v1/entsoe_ingest_price_eu_latest';
begin
  -- Ingest at 13:30 UTC (after next-day prices are typically published by ENTSO-E)
  perform cron.schedule(
    'entsoe_ingest_price_eu_latest__1330',
    '30 13 * * *',
    format($job$
      select net.http_post(
        url := %L,
        headers := jsonb_build_object('content-type', 'application/json'),
        body := jsonb_build_object('concurrency', 6, 'delay_ms', 200)
      );
    $job$, fn_url)
  );

  -- Second run at 07:30 UTC to catch any zones that publish late or revise overnight
  perform cron.schedule(
    'entsoe_ingest_price_eu_latest__0730',
    '30 7 * * *',
    format($job$
      select net.http_post(
        url := %L,
        headers := jsonb_build_object('content-type', 'application/json'),
        body := jsonb_build_object('concurrency', 6, 'delay_ms', 200)
      );
    $job$, fn_url)
  );

  -- Refresh EU price MVs every 2 hours independently of the ingest.
  -- This ensures the MVs are never more than 2 hours stale even if the
  -- edge function times out before its own refresh_electricity_price_mvs() call.
  perform cron.schedule(
    'refresh_electricity_price_mvs__2h',
    '15 */2 * * *',
    $job$
      select public.refresh_electricity_price_mvs();
    $job$
  );
end $$;

-- Inspect: select * from cron.job order by jobname;
--
-- Unschedule:
-- select cron.unschedule(jobid) from cron.job where jobname in (
--   'entsoe_ingest_price_eu_latest__1330',
--   'entsoe_ingest_price_eu_latest__0730',
--   'refresh_electricity_price_mvs__2h'
-- );

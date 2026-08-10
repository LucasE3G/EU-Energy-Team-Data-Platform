-- Schedules for ENTSO-E electricity load ingest + EU load MV refresh.
-- Run once in the Supabase SQL Editor after deploying entsoe_ingest_load_eu_latest.
--
-- Why this file exists:
--   The three public.electricity_eu_load_*_mv views were refreshed by nothing.
--   `refresh_electricity_load_mvs()` only covered the *_mwh rollups, and no
--   pg_cron job targeted them either, so the EU demand chart — which reads
--   electricity_eu_load_15m_mv — was serving months-old data even while the
--   underlying snapshots were current. Apply `electricity_load_refresh_fn.sql`
--   (which now includes them) before running this.
--
-- Two separate jobs, mirroring supabase/schedule_price_ingest.sql:
--   1. Load ingest, hourly. The `Electricity ENTSO-E latest` GitHub Action also
--      calls this function hourly; the upsert is idempotent on
--      (source, zone_id, ts), so the duplication is harmless and means a
--      failure of either scheduler alone no longer stops the feed.
--   2. MV refresh, hourly and independent of the ingest, so a timed-out or
--      partial ingest run cannot leave the EU load views stale.
--
-- Prereqs:
--   - `electricity_load_perf.sql`, `electricity_load_mwh.sql` and
--     `electricity_load_refresh_fn.sql` applied
--   - `entsoe_ingest_load_eu_latest` deployed with `verify_jwt = false`
--   - Secrets ENTSOE_API_TOKEN, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY set

create extension if not exists pg_cron;
create extension if not exists pg_net;

do $$
declare
  fn_url text := 'https://rvxukmupuzxbrwicowyn.supabase.co/functions/v1/entsoe_ingest_load_eu_latest';
begin
  -- Ingest at :42 past the hour, offset from the GitHub Action's :12 so the
  -- two schedulers cover different parts of the hour.
  perform cron.schedule(
    'entsoe_ingest_load_eu_latest__hourly',
    '42 * * * *',
    format($job$
      select net.http_post(
        url := %L,
        headers := jsonb_build_object('content-type', 'application/json'),
        body := jsonb_build_object('delay_ms', 200)
      );
    $job$, fn_url)
  );

  -- Refresh the EU load MVs every hour, independently of the ingest.
  perform cron.schedule(
    'refresh_electricity_load_mvs__hourly',
    '25 * * * *',
    $job$
      select public.refresh_electricity_load_mvs();
    $job$
  );
end $$;

-- Inspect: select * from cron.job order by jobname;
--
-- Check recent outcomes (this is how a silent ingest failure shows up):
--   select j.jobname, r.status, r.return_message, r.start_time
--   from cron.job_run_details r
--   join cron.job j using (jobid)
--   where j.jobname like '%load%'
--   order by r.start_time desc
--   limit 20;
--
-- Unschedule:
-- select cron.unschedule(jobid) from cron.job where jobname in (
--   'entsoe_ingest_load_eu_latest__hourly',
--   'refresh_electricity_load_mvs__hourly'
-- );

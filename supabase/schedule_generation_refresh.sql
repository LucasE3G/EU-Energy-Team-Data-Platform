-- Automatic refresh schedules for electricity generation materialized views.
-- Run once in the Supabase SQL Editor AFTER running electricity_generation_mwh.sql.
--
-- Prereqs:
--   - pg_cron and pg_net extensions enabled (see schedule_entsoe_and_refresh.sql)
--   - All 5 generation MVs created by electricity_generation_mwh.sql
--   - entsoe_ingest_eu_latest already writes to electricity_generation_snapshots every 15m
--
-- Dependency order enforced:
--   daily_mwh → weekly_mwh → eu_daily_mwh → eu_weekly_mwh
--
-- Note: electricity_eu_generation_15m_mv uses hourly bins (avg per zone → sum across zones)
-- despite the "15m" name (kept for query compatibility). Refreshed hourly at :05.

do $$
begin
  -- Refresh EU hourly generation aggregate once per hour (data granularity is 1h)
  perform cron.schedule(
    'refresh_electricity_eu_generation_15m_mv__15m',
    '5 * * * *',
    $job$
      refresh materialized view concurrently public.electricity_eu_generation_15m_mv;
    $job$
  );

  -- Refresh daily + weekly rollups once per day at 02:30 UTC
  -- (10 min after the existing energy_eu daily refresh at 02:20)
  perform cron.schedule(
    'refresh_electricity_generation_daily_weekly_mv__daily',
    '30 2 * * *',
    $job$
      refresh materialized view concurrently public.electricity_generation_daily_mwh;
      refresh materialized view concurrently public.electricity_generation_weekly_mwh;
      refresh materialized view concurrently public.electricity_eu_generation_daily_mwh;
      refresh materialized view concurrently public.electricity_eu_generation_weekly_mwh;
    $job$
  );
end $$;

-- Inspect jobs:
-- select * from cron.job order by jobname;
--
-- Unschedule (if you need to re-run with different timings):
-- select cron.unschedule(jobid) from cron.job where jobname in (
--   'refresh_electricity_eu_generation_15m_mv__15m',
--   'refresh_electricity_generation_daily_weekly_mv__daily'
-- );

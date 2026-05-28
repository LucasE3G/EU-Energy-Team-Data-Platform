-- Schedule for ENTSO-E cross-border flow ingest (electricity transmission)
-- Run once in the Supabase SQL Editor after deploying entsoe_ingest_transmission_eu_latest.
--
-- What this does:
-- - Calls the edge function every hour to keep the latest flows fresh
-- - The function uses its built-in DEFAULT_PAIRS list (82 EU border pairs)
--
-- Prereqs:
-- - `electricity_transmission_schema.sql` and `electricity_transmission_perf.sql` run
-- - `entsoe_ingest_transmission_eu_latest` deployed with `verify_jwt = false`
-- - Secret ENTSOE_API_TOKEN set in Supabase dashboard → Edge Functions → Secrets

create extension if not exists pg_cron;
create extension if not exists pg_net;

do $$
declare
  fn_url text := 'https://rvxukmupuzxbrwicowyn.supabase.co/functions/v1/entsoe_ingest_transmission_eu_latest';
begin
  perform cron.schedule(
    'entsoe_ingest_transmission__hourly',
    '5 * * * *',
    format($job$
      select
        net.http_post(
          url := %L,
          headers := jsonb_build_object('content-type', 'application/json'),
          body := '{"concurrency":6,"delay_ms":80}'::jsonb
        );
    $job$, fn_url)
  );
end $$;

-- Inspect: select * from cron.job order by jobname;
--
-- Unschedule:
-- select cron.unschedule(jobid) from cron.job where jobname = 'entsoe_ingest_transmission__hourly';

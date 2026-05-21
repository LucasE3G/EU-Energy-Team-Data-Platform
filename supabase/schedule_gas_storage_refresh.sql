-- Schedule for EU gas storage daily ingest (GIE AGSI+).
-- Run once in Supabase SQL Editor after deploying gas_ingest_storage_eu.
-- Data is published by GIE around 19:30 CET; 20:00 UTC (21:00 CET) is safe.

create extension if not exists pg_cron;
create extension if not exists pg_net;

do $$
declare
  fn_url text := 'https://rvxukmupuzxbrwicowyn.supabase.co/functions/v1/gas_ingest_storage_eu';
begin
  perform cron.schedule(
    'gas_ingest_storage_eu__daily',
    '0 20 * * *',
    format($job$
      select
        net.http_post(
          url := %L,
          headers := jsonb_build_object('content-type', 'application/json'),
          body := '{}'::jsonb
        );
    $job$, fn_url)
  );
end $$;

-- Inspect: select * from cron.job order by jobname;
--
-- Unschedule:
-- select cron.unschedule(jobid) from cron.job where jobname = 'gas_ingest_storage_eu__daily';

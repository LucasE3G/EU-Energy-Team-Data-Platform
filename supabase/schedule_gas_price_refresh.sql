-- Schedule for TTF gas price daily ingest.
-- Run once in Supabase SQL Editor after deploying gas_ingest_ttf_price.
--
-- Fetches TTF=F closing price from Yahoo Finance every day at 18:30 UTC
-- (after European gas markets close ~17:30 CET / 16:30 UTC).

create extension if not exists pg_cron;
create extension if not exists pg_net;

do $$
declare
  fn_url text := 'https://rvxukmupuzxbrwicowyn.supabase.co/functions/v1/gas_ingest_ttf_price';
begin
  perform cron.schedule(
    'gas_ingest_ttf_price__daily',
    '30 18 * * *',
    format($job$
      select
        net.http_post(
          url := %L,
          headers := jsonb_build_object('content-type', 'application/json'),
          body := '{"range":"5d"}'::jsonb
        );
    $job$, fn_url)
  );
end $$;

-- Inspect: select * from cron.job order by jobname;
--
-- Unschedule:
-- select cron.unschedule(jobid) from cron.job where jobname = 'gas_ingest_ttf_price__daily';

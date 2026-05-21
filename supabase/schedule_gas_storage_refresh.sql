-- pg_cron schedule for EU gas storage refresh (GIE AGSI+)
-- Run in Supabase SQL editor (cron.schedule requires pg_cron extension enabled).
-- Data is published by GIE around 19:30 CET, so 20:00 UTC (21:00 CET) is safe.

select cron.schedule(
  'gas_ingest_storage_eu__daily',
  '0 20 * * *',  -- daily at 20:00 UTC
  $$
  select net.http_post(
    url    => (select decrypted_secret from vault.decrypted_secrets where name = 'SUPABASE_URL') || '/functions/v1/gas_ingest_storage_eu',
    body   => '{"from": null, "to": null}'::jsonb,
    headers => jsonb_build_object(
      'Content-Type', 'application/json',
      'Authorization', 'Bearer ' || (select decrypted_secret from vault.decrypted_secrets where name = 'SUPABASE_SERVICE_ROLE_KEY')
    )
  );
  $$
);

-- Postgres helper function to refresh all electricity load materialized views.
-- Run once in Supabase SQL editor, then the edge function calls it automatically.
--
-- Order matters: daily_mwh must refresh before weekly_mwh and eu views that
-- depend on it, and electricity_eu_load_15m_mv must refresh before the daily /
-- weekly EU views that are built on top of it.
--
-- The three electricity_eu_load_*_mv views were previously refreshed by nothing
-- at all — not by this function and not by any pg_cron job — which left the EU
-- demand chart reading months-old data. They are included here now, and
-- `supabase/schedule_load_ingest.sql` also refreshes them on an independent
-- schedule so a failed ingest cannot leave them stale.

-- REFRESH ... CONCURRENTLY requires a unique index on each view. The MWh views
-- already have one; the EU load MVs are unique on ts by construction.
create unique index if not exists electricity_eu_load_15m_mv_ts_key
  on public.electricity_eu_load_15m_mv (ts);
create unique index if not exists electricity_eu_load_daily_mv_ts_key
  on public.electricity_eu_load_daily_mv (ts);
create unique index if not exists electricity_eu_load_weekly_mv_ts_key
  on public.electricity_eu_load_weekly_mv (ts);

create or replace function public.refresh_electricity_load_mvs()
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  -- Per-zone and EU consumption (MWh) rollups
  refresh materialized view concurrently public.electricity_load_daily_mwh;
  refresh materialized view concurrently public.electricity_load_weekly_mwh;
  refresh materialized view concurrently public.electricity_eu_load_daily_mwh;
  refresh materialized view concurrently public.electricity_eu_load_weekly_mwh;

  -- EU aggregate load (MW) — 15m first, then the views derived from it
  refresh materialized view concurrently public.electricity_eu_load_15m_mv;
  refresh materialized view concurrently public.electricity_eu_load_daily_mv;
  refresh materialized view concurrently public.electricity_eu_load_weekly_mv;
end;
$$;

-- Allow the service role (used by the edge function) to call it.
grant execute on function public.refresh_electricity_load_mvs() to service_role;

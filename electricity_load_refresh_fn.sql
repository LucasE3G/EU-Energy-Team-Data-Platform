-- Postgres helper function to refresh all electricity load MWh materialized views.
-- Run once in Supabase SQL editor, then the edge function calls it automatically.
--
-- Order matters: daily_mwh must refresh before weekly_mwh and eu views that depend on it.

create or replace function public.refresh_electricity_load_mvs()
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  refresh materialized view concurrently public.electricity_load_daily_mwh;
  refresh materialized view concurrently public.electricity_load_weekly_mwh;
  refresh materialized view concurrently public.electricity_eu_load_daily_mwh;
  refresh materialized view concurrently public.electricity_eu_load_weekly_mwh;
end;
$$;

-- Allow the service role (used by the edge function) to call it.
grant execute on function public.refresh_electricity_load_mvs() to service_role;

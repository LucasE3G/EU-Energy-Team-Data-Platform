-- Helper RPC to refresh price MVs after ingestion.
-- Called from Edge Functions.

create or replace function public.refresh_electricity_price_mvs()
returns void
language plpgsql
security definer
as $$
begin
  refresh materialized view concurrently public.electricity_eu_price_hourly_mv;
  refresh materialized view concurrently public.electricity_eu_price_daily_mv;
  refresh materialized view concurrently public.electricity_eu_price_weekly_mv;
end;
$$;

grant execute on function public.refresh_electricity_price_mvs() to anon;
grant execute on function public.refresh_electricity_price_mvs() to authenticated;


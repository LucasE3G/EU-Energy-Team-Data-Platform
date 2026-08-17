-- One function that refreshes the whole heatwave chain in dependency order.
--
-- The refresh functions had drifted from the schema: mv_crossborder_country_daily
-- and mv_heatwave_coverage_delta existed but were in none of them, so the cron
-- would have quietly served stale flow and coverage numbers while everything
-- else moved. Keeping the order in one place is what stops that recurring.

set statement_timeout = '1800s';

create or replace function public.refresh_heatwave_all()
returns void language plpgsql security definer set search_path = public as $$
begin
  -- 1. Warm-season daily rollups, straight off the raw snapshot tables.
  refresh materialized view concurrently public.mv_load_daily_warm;
  refresh materialized view concurrently public.mv_price_daily_warm;
  refresh materialized view concurrently public.mv_renewable_daily_warm;
  refresh materialized view concurrently public.mv_generation_daily_warm;

  -- 2. Cross-border: raw pairs, then the per-country net with its flow-weighted
  --    completeness flag.
  refresh materialized view public.mv_crossborder_net_daily;
  refresh materialized view concurrently public.mv_crossborder_country_daily;

  -- 3. The balance and its long form.
  refresh materialized view concurrently public.mv_heatwave_daily_balance;
  refresh materialized view concurrently public.mv_heatwave_component_daily;

  -- 4. Paired deltas that the page reads directly.
  refresh materialized view concurrently public.mv_heatwave_component_delta;
  refresh materialized view concurrently public.mv_heatwave_coverage_delta;
end;
$$;

grant execute on function public.refresh_heatwave_all() to service_role;

-- The older entry points stay, so anything already calling them keeps working,
-- but they now delegate rather than refreshing a stale subset.
create or replace function public.refresh_heatwave_mvs()
returns void language plpgsql security definer set search_path = public as $$
begin perform public.refresh_heatwave_all(); end;
$$;

create or replace function public.refresh_heatwave_balance_mvs()
returns void language plpgsql security definer set search_path = public as $$
begin perform public.refresh_heatwave_all(); end;
$$;

create or replace function public.refresh_heatwave_delta_mv()
returns void language plpgsql security definer set search_path = public as $$
begin perform public.refresh_heatwave_all(); end;
$$;

grant execute on function public.refresh_heatwave_mvs()         to service_role;
grant execute on function public.refresh_heatwave_balance_mvs() to service_role;
grant execute on function public.refresh_heatwave_delta_mv()    to service_role;

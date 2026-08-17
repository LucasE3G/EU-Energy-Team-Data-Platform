-- Materialize the shared daily balance.
--
-- v_heatwave_daily_balance joins load, generation and cross-border flows per
-- country-day, and THREE views recompute it: trade_position, demand_sources and
-- gap_coverage. Individually each returned in under half a second except
-- gap_coverage at 2.2 s — but the page issues fourteen queries in parallel, so
-- they contend and the slowest exceeded the anon role's statement timeout.
-- Hence "canceling statement due to statement timeout" on load.
--
-- The daily grain is small (about 30 countries x 900 warm-season days), so
-- materializing it once turns three expensive views into cheap ones.

set statement_timeout = '900s';

drop materialized view if exists public.mv_heatwave_daily_balance cascade;
create materialized view public.mv_heatwave_daily_balance as
with imp as (
  select to_country as country_code, date, sum(net_export_gwh) as net_import_gwh
  from public.mv_crossborder_net_daily
  group by 1, 2
),
gen as (
  select country_code, date,
         sum(avg_mw) filter (where fuel = 'gas')   * 24.0 / 1000.0 as gas_gwh,
         sum(avg_mw) filter (where fuel = 'solar') * 24.0 / 1000.0 as solar_gwh,
         sum(avg_mw) filter (where fuel = 'wind')  * 24.0 / 1000.0 as wind_gwh,
         sum(avg_mw) filter (where fuel not in ('gas','solar','wind'))
                                                   * 24.0 / 1000.0 as other_gwh
  from public.mv_generation_daily_warm
  group by 1, 2
)
select
  w.country_code,
  w.date,
  extract(month from w.date)::int  as month,
  (w.heatwave_id is not null)      as in_heatwave,
  l.avg_load_mw * 24.0 / 1000.0    as demand_gwh,
  g.gas_gwh, g.solar_gwh, g.wind_gwh, g.other_gwh,
  i.net_import_gwh
from public.weather_country_daily w
join public.mv_load_daily_warm l on l.country_code = w.country_code and l.date = w.date
join gen g on g.country_code = w.country_code and g.date = w.date
join imp i on i.country_code = w.country_code and i.date = w.date
where l.samples >= 20;

create unique index if not exists mv_heatwave_daily_balance_key
  on public.mv_heatwave_daily_balance (country_code, date);

-- Keep the view name so nothing downstream has to change.
create or replace view public.v_heatwave_daily_balance as
select * from public.mv_heatwave_daily_balance;

-- Per-component long form, also materialized: gap_coverage was the slow one.
drop materialized view if exists public.mv_heatwave_component_daily cascade;
create materialized view public.mv_heatwave_component_daily as
select g.country_code, g.date, g.fuel as component, g.avg_mw * 24.0 / 1000.0 as gwh
from public.mv_generation_daily_warm g
union all
select b.country_code, b.date, 'imports', b.net_import_gwh
from public.mv_heatwave_daily_balance b;

create unique index if not exists mv_heatwave_component_daily_key
  on public.mv_heatwave_component_daily (country_code, date, component);

create or replace function public.refresh_heatwave_balance_mvs()
returns void language plpgsql security definer set search_path = public as $$
begin
  refresh materialized view concurrently public.mv_heatwave_daily_balance;
  refresh materialized view concurrently public.mv_heatwave_component_daily;
end;
$$;
grant execute on function public.refresh_heatwave_balance_mvs() to service_role;

grant select on public.mv_heatwave_daily_balance    to anon;
grant select on public.mv_heatwave_component_daily  to anon;
grant select on public.v_heatwave_daily_balance     to anon;

-- Interconnection during heatwaves: did trade help, who supplied it, and does
-- it move with gas?
--
-- Uses the MEASURED cross-border flows (mv_crossborder_net_daily), not the
-- residual the national balance falls back on.
--
-- Everything below is built from ONE matched day set. A first version averaged
-- the generation side and the trade side over different sets of days, which
-- left France's decomposition 53 GWh short of its own demand change — the terms
-- have to come from the same rows to be allowed to add up.

-- CREATE OR REPLACE VIEW cannot rename or reorder columns, so drop in
-- dependency order first.
drop view if exists public.v_heatwave_imports_vs_gas;
drop view if exists public.v_heatwave_demand_sources;
drop view if exists public.v_heatwave_trade_position;
drop view if exists public.v_heatwave_daily_balance;

-- ── One row per country-day: demand, generation by fuel, net imports ───────
create view public.v_heatwave_daily_balance as
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

-- ── Trade position, normal vs heatwave ─────────────────────────────────────
create view public.v_heatwave_trade_position as
with agg as (
  select country_code, month, in_heatwave,
         avg(net_import_gwh) as imp_gwh,
         avg(demand_gwh)     as dem_gwh,
         count(*)            as days
  from public.v_heatwave_daily_balance
  group by 1, 2, 3
),
paired as (
  select h.country_code, h.month,
         h.imp_gwh as hw_imp, n.imp_gwh as base_imp,
         h.dem_gwh as hw_dem, n.dem_gwh as base_dem,
         h.days as hw_days, n.days as base_days
  from agg h
  join agg n on n.country_code = h.country_code and n.month = h.month
            and n.in_heatwave = false
  where h.in_heatwave = true
)
select
  country_code,
  sum(hw_days)                               as heatwave_days,
  round(avg(base_imp)::numeric, 1)           as normal_net_import_gwh,
  round(avg(hw_imp)::numeric, 1)             as heatwave_net_import_gwh,
  round(avg(hw_imp - base_imp)::numeric, 1)  as delta_net_import_gwh,
  -- Export view of the same figure, so exporters read positively.
  round(-avg(base_imp)::numeric, 1)          as normal_net_export_gwh,
  round(-avg(hw_imp)::numeric, 1)            as heatwave_net_export_gwh,
  round(-avg(hw_imp - base_imp)::numeric, 1) as delta_net_export_gwh,
  round(avg(hw_dem - base_dem)::numeric, 1)  as extra_demand_gwh
from paired
where base_days >= 5
group by 1;

-- ── Where the extra demand came from ───────────────────────────────────────
create view public.v_heatwave_demand_sources as
with agg as (
  select country_code, month, in_heatwave,
         avg(demand_gwh) as dem, avg(gas_gwh) as gas, avg(solar_gwh) as solar,
         avg(wind_gwh) as wind, avg(other_gwh) as other,
         avg(net_import_gwh) as imp, count(*) as days
  from public.v_heatwave_daily_balance
  group by 1, 2, 3
),
paired as (
  select h.country_code, h.month,
         h.dem - n.dem     as d_dem,
         h.gas - n.gas     as d_gas,
         h.solar - n.solar as d_solar,
         h.wind - n.wind   as d_wind,
         h.other - n.other as d_other,
         h.imp - n.imp     as d_imp,
         h.days as hw_days, n.days as base_days
  from agg h
  join agg n on n.country_code = h.country_code and n.month = h.month
            and n.in_heatwave = false
  where h.in_heatwave = true
)
select
  country_code,
  sum(hw_days)                                              as heatwave_days,
  round(avg(d_dem)::numeric, 1)                             as extra_demand_gwh,
  round(avg(d_gas)::numeric, 1)                             as extra_gas_gwh,
  round(avg(d_solar)::numeric, 1)                           as extra_solar_gwh,
  round(avg(d_wind)::numeric, 1)                            as extra_wind_gwh,
  round(avg(d_other)::numeric, 1)                           as extra_other_gwh,
  round(avg(d_imp)::numeric, 1)                             as extra_imports_gwh,
  -- Generation + imports minus demand. Non-zero means the country's reported
  -- generation is incomplete or losses/pumping move with heat; shown, not hidden.
  round(avg(d_gas + d_solar + d_wind + d_other + d_imp - d_dem)::numeric, 1) as residual_gwh,
  round((100.0 * avg(d_gas) / nullif(avg(d_dem), 0))::numeric, 0)            as gas_pct_of_extra_demand
from paired
where base_days >= 5
group by 1;

-- ── Do imports and gas move together? ──────────────────────────────────────
-- Correlational only. Electricity is fungible, so this cannot show that imports
-- DISPLACED gas — only whether the two move in opposite directions.
create view public.v_heatwave_imports_vs_gas as
select country_code, heatwave_days, extra_demand_gwh,
       extra_imports_gwh, extra_gas_gwh
from public.v_heatwave_demand_sources
where heatwave_days >= 20;

grant select on public.v_heatwave_daily_balance   to anon;
grant select on public.v_heatwave_trade_position  to anon;
grant select on public.v_heatwave_demand_sources  to anon;
grant select on public.v_heatwave_imports_vs_gas  to anon;

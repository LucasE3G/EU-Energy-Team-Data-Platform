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

-- v_heatwave_daily_balance is NOT defined here any more — it is owned by
-- heatwave_balance_mv.sql, which backs it with a materialized view. Three views
-- read it (this file's two plus gap_coverage) and recomputing the join on every
-- call cost enough, under the page's fourteen parallel queries, to trip the
-- anon statement timeout. Redefining it here would silently undo that.
--
-- CREATE OR REPLACE VIEW cannot rename or reorder columns, so drop in
-- dependency order first. gap_coverage depends on trade_position, so it goes
-- first and heatwave_gap_coverage.sql must be re-run after this file.
drop view if exists public.v_heatwave_gap_coverage;
drop view if exists public.v_heatwave_imports_vs_gas;
drop view if exists public.v_heatwave_demand_sources;
drop view if exists public.v_heatwave_trade_position;

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

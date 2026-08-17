-- Country-by-country energy balance under heat: what happened to demand, what
-- each fuel did about it, and what the interconnectors had to cover.
--
-- Percentages alone cannot be read as a story. "+27% solar" in a country with
-- a 200 MW fleet is irrelevant next to "-20% wind" in one with 40 GW. This
-- expresses every change in MW so the terms reconcile:
--
--     d(demand)  ~=  sum of d(generation by fuel)  +  d(net imports)
--
-- The residual is reported as implied_net_import_delta_mw. It is a RESIDUAL,
-- not a measurement: it also absorbs grid losses, pumped-storage consumption
-- and any unreported units. Once electricity_crossborder_flows is populated it
-- can be checked against measured net imports, and a large divergence means
-- the country's generation reporting is incomplete rather than that the
-- interconnectors did something surprising.
--
-- All comparisons are heatwave days vs non-heatwave days in the SAME country
-- and SAME calendar month.

-- ── Per-fuel MW delta ───────────────────────────────────────────────────────
create or replace view public.v_heatwave_fuel_delta as
with joined as (
  select
    g.country_code, g.fuel, g.avg_mw,
    extract(month from g.date)::int as month,
    (w.heatwave_id is not null)     as in_heatwave
  from public.mv_generation_daily_warm g
  join public.weather_country_daily w
    on w.country_code = g.country_code and w.date = g.date
),
agg as (
  select country_code, fuel, month, in_heatwave,
         avg(avg_mw) as mw, count(*) as days
  from joined group by 1, 2, 3, 4
),
paired as (
  select h.country_code, h.fuel, h.month,
         h.mw as hw_mw, n.mw as base_mw, h.days as hw_days, n.days as base_days
  from agg h
  join agg n
    on n.country_code = h.country_code and n.fuel = h.fuel
   and n.month = h.month and n.in_heatwave = false
  where h.in_heatwave = true
)
select
  country_code, fuel,
  sum(hw_days)                                                     as heatwave_days,
  round(avg(base_mw)::numeric, 0)                                  as normal_mw,
  round(avg(hw_mw)::numeric, 0)                                    as heatwave_mw,
  round(avg(hw_mw - base_mw)::numeric, 0)                          as delta_mw,
  round(avg(100.0 * (hw_mw / nullif(base_mw, 0) - 1))::numeric, 1) as change_pct
from paired
where base_days >= 5
group by 1, 2;

-- ── Demand MW delta, same matching ─────────────────────────────────────────
create or replace view public.v_heatwave_demand_delta as
with matched as (
  select country_code, month, in_heatwave,
         avg(avg_load_mw) as mw, count(*) as days
  from public.v_heatwave_load_daily
  where samples >= 20
  group by 1, 2, 3
),
paired as (
  select h.country_code, h.month, h.mw as hw_mw, n.mw as base_mw,
         h.days as hw_days, n.days as base_days
  from matched h
  join matched n
    on n.country_code = h.country_code and n.month = h.month and n.in_heatwave = false
  where h.in_heatwave = true
)
select
  country_code,
  sum(hw_days)                                                     as heatwave_days,
  round(avg(base_mw)::numeric, 0)                                  as normal_demand_mw,
  round(avg(hw_mw)::numeric, 0)                                    as heatwave_demand_mw,
  round(avg(hw_mw - base_mw)::numeric, 0)                          as demand_delta_mw,
  round(avg(100.0 * (hw_mw / nullif(base_mw, 0) - 1))::numeric, 1) as demand_change_pct
from paired
where base_days >= 5
group by 1;

-- ── One readable row per country ───────────────────────────────────────────
create or replace view public.v_heatwave_country_balance as
with pivot as (
  select
    country_code,
    sum(delta_mw) filter (where fuel = 'solar')   as solar_delta_mw,
    sum(delta_mw) filter (where fuel = 'wind')    as wind_delta_mw,
    sum(delta_mw) filter (where fuel = 'nuclear') as nuclear_delta_mw,
    sum(delta_mw) filter (where fuel = 'hydro')   as hydro_delta_mw,
    sum(delta_mw) filter (where fuel = 'gas')     as gas_delta_mw,
    sum(delta_mw) filter (where fuel = 'coal')    as coal_delta_mw,
    sum(delta_mw) filter (where fuel = 'biomass') as biomass_delta_mw,
    sum(delta_mw) filter (where fuel = 'other')   as other_delta_mw,
    sum(delta_mw)                                 as generation_delta_mw
  from public.v_heatwave_fuel_delta
  group by 1
)
select
  d.country_code,
  d.heatwave_days,
  d.normal_demand_mw,
  d.demand_delta_mw,
  d.demand_change_pct,
  p.solar_delta_mw,
  p.wind_delta_mw,
  p.nuclear_delta_mw,
  p.hydro_delta_mw,
  p.gas_delta_mw,
  p.coal_delta_mw,
  p.biomass_delta_mw,
  p.other_delta_mw,
  p.generation_delta_mw,
  d.demand_delta_mw - p.generation_delta_mw as implied_net_import_delta_mw
from public.v_heatwave_demand_delta d
left join pivot p on p.country_code = d.country_code
order by d.demand_change_pct desc nulls last;

grant select on public.v_heatwave_fuel_delta      to anon;
grant select on public.v_heatwave_demand_delta    to anon;
grant select on public.v_heatwave_country_balance to anon;

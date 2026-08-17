-- Complete the year+month matching.
--
-- The first pass converted six views and missed the ones behind the coverage
-- chart, so the page ended up showing year-matched and month-matched figures
-- side by side — French nuclear reading -117 MW in one chart and -45.5 GWh/day
-- (-1,896 MW) in another. A partial methodology fix is worse than none.
--
-- These are every remaining view the Heatwaves page reads that compares
-- heatwave days against normal days.

drop view if exists public.v_heatwave_gap_coverage;
drop view if exists public.v_heatwave_imports_vs_gas;
drop view if exists public.v_heatwave_demand_sources;
drop view if exists public.v_heatwave_trade_position;
drop view if exists public.v_heatwave_helpers;
drop view if exists public.v_heatwave_beneficiaries;
drop view if exists public.v_heatwave_help_pairs;

-- ── Trade position ─────────────────────────────────────────────────────────
create view public.v_heatwave_trade_position as
with agg as (
  select country_code,
         extract(year from date)::int as yr, extract(month from date)::int as mo,
         in_heatwave,
         avg(net_import_gwh) as imp, avg(demand_gwh) as dem, count(*) as n
  from public.mv_heatwave_daily_balance
  group by 1, 2, 3, 4
),
paired as (
  select h.country_code, h.imp hw_imp, n.imp base_imp,
         h.dem hw_dem, n.dem base_dem, h.n hw_days, n.n base_days
  from agg h
  join agg n on n.country_code = h.country_code and n.yr = h.yr and n.mo = h.mo
            and n.in_heatwave = false
  where h.in_heatwave
)
select country_code,
       sum(hw_days)                               as heatwave_days,
       round(avg(base_imp)::numeric, 1)           as normal_net_import_gwh,
       round(avg(hw_imp)::numeric, 1)             as heatwave_net_import_gwh,
       round(avg(hw_imp - base_imp)::numeric, 1)  as delta_net_import_gwh,
       round(-avg(base_imp)::numeric, 1)          as normal_net_export_gwh,
       round(-avg(hw_imp)::numeric, 1)            as heatwave_net_export_gwh,
       round(-avg(hw_imp - base_imp)::numeric, 1) as delta_net_export_gwh,
       round(avg(hw_dem - base_dem)::numeric, 1)  as extra_demand_gwh
from paired where base_days >= 3 group by 1;

-- ── Where the extra demand came from ───────────────────────────────────────
create view public.v_heatwave_demand_sources as
with agg as (
  select country_code,
         extract(year from date)::int as yr, extract(month from date)::int as mo,
         in_heatwave,
         avg(demand_gwh) d, avg(gas_gwh) gas, avg(solar_gwh) solar,
         avg(wind_gwh) wind, avg(other_gwh) other, avg(net_import_gwh) imp,
         count(*) n
  from public.mv_heatwave_daily_balance
  group by 1, 2, 3, 4
),
paired as (
  select h.country_code,
         h.d-n.d dd, h.gas-n.gas dgas, h.solar-n.solar dsol, h.wind-n.wind dwind,
         h.other-n.other doth, h.imp-n.imp dimp, h.n hw_days, n.n base_days
  from agg h
  join agg n on n.country_code = h.country_code and n.yr = h.yr and n.mo = h.mo
            and n.in_heatwave = false
  where h.in_heatwave
)
select country_code,
       sum(hw_days)                                                  as heatwave_days,
       round(avg(dd)::numeric, 1)                                    as extra_demand_gwh,
       round(avg(dgas)::numeric, 1)                                  as extra_gas_gwh,
       round(avg(dsol)::numeric, 1)                                  as extra_solar_gwh,
       round(avg(dwind)::numeric, 1)                                 as extra_wind_gwh,
       round(avg(doth)::numeric, 1)                                  as extra_other_gwh,
       round(avg(dimp)::numeric, 1)                                  as extra_imports_gwh,
       round(avg(dgas+dsol+dwind+doth+dimp-dd)::numeric, 1)          as residual_gwh,
       round((100.0*avg(dgas)/nullif(avg(dd),0))::numeric, 0)        as gas_pct_of_extra_demand
from paired where base_days >= 3 group by 1;

create view public.v_heatwave_imports_vs_gas as
select country_code, heatwave_days, extra_demand_gwh, extra_imports_gwh, extra_gas_gwh
from public.v_heatwave_demand_sources where heatwave_days >= 20;

-- ── Fuel-by-fuel coverage ──────────────────────────────────────────────────
create view public.v_heatwave_gap_coverage as
with j as (
  select c.country_code, c.component, c.gwh,
         extract(year from c.date)::int as yr, extract(month from c.date)::int as mo,
         (w.heatwave_id is not null) as hw
  from public.mv_heatwave_component_daily c
  join public.weather_country_daily w
    on w.country_code = c.country_code and w.date = c.date
  join public.mv_load_daily_warm l
    on l.country_code = c.country_code and l.date = c.date and l.samples >= 20
),
agg as (select country_code, component, yr, mo, hw, avg(gwh) gwh, count(*) n
        from j group by 1,2,3,4,5),
paired as (
  select h.country_code, h.component, h.gwh - n.gwh as delta,
         h.n hw_days, n.n base_days
  from agg h
  join agg n on n.country_code=h.country_code and n.component=h.component
            and n.yr=h.yr and n.mo=h.mo and n.hw=false
  where h.hw
),
per_component as (
  select country_code, component, sum(hw_days) as heatwave_days,
         round(avg(delta)::numeric, 1) as delta_gwh
  from paired where base_days >= 3 group by 1, 2
),
closure as (
  select p.country_code,
         max(d.extra_demand_gwh) as extra_demand_gwh,
         round(abs(sum(p.delta_gwh) - max(d.extra_demand_gwh))::numeric, 1) as gap_gwh,
         round((100.0*abs(sum(p.delta_gwh) - max(d.extra_demand_gwh))
                / nullif(abs(max(d.extra_demand_gwh)),0))::numeric, 0)      as gap_pct
  from per_component p
  join public.v_heatwave_trade_position d on d.country_code = p.country_code
  group by 1
)
select p.country_code, p.component, p.heatwave_days, p.delta_gwh,
       c.extra_demand_gwh, c.gap_gwh, c.gap_pct
from per_component p join closure c on c.country_code = p.country_code;

-- ── Interconnector assistance ──────────────────────────────────────────────
create view public.v_heatwave_help_pairs as
with classified as (
  select n.from_country as helper, n.to_country as beneficiary, n.net_export_gwh,
         extract(year from n.date)::int as yr, extract(month from n.date)::int as mo,
         (wb.heatwave_id is not null)                as ben_hw,
         coalesce(wh.heatwave_id is not null, false) as helper_hw
  from public.mv_crossborder_net_daily n
  join public.weather_country_daily wb on wb.country_code=n.to_country and wb.date=n.date
  left join public.weather_country_daily wh on wh.country_code=n.from_country and wh.date=n.date
),
baseline as (
  select helper, beneficiary, yr, mo, avg(net_export_gwh) base_gwh, count(*) base_days
  from classified where not ben_hw and not helper_hw group by 1,2,3,4
),
during as (
  select helper, beneficiary, yr, mo, helper_hw,
         avg(net_export_gwh) hw_gwh, count(*) hw_days
  from classified where ben_hw group by 1,2,3,4,5
)
select d.helper, d.beneficiary, d.helper_hw as helper_also_in_heatwave,
       sum(d.hw_days)                                             as heatwave_days,
       round(avg(d.hw_gwh)::numeric, 2)                           as mean_net_export_gwh,
       round(avg(b.base_gwh)::numeric, 2)                         as baseline_net_export_gwh,
       round(avg(d.hw_gwh - b.base_gwh)::numeric, 2)              as extra_gwh_per_day,
       round(sum((d.hw_gwh - b.base_gwh) * d.hw_days)::numeric, 1) as total_extra_gwh
from during d
join baseline b on b.helper=d.helper and b.beneficiary=d.beneficiary
               and b.yr=d.yr and b.mo=d.mo
where b.base_days >= 3
group by 1,2,3;

create view public.v_heatwave_helpers as
select helper as country_code,
       count(distinct beneficiary) as neighbours_helped,
       sum(heatwave_days)          as neighbour_heatwave_days,
       round(sum(total_extra_gwh) filter (where not helper_also_in_heatwave)::numeric,1)
                                   as extra_gwh_spare_capacity,
       round(sum(total_extra_gwh) filter (where helper_also_in_heatwave)::numeric,1)
                                   as extra_gwh_shared_stress,
       round(sum(total_extra_gwh)::numeric,1) as extra_gwh_total
from public.v_heatwave_help_pairs group by 1;

create view public.v_heatwave_beneficiaries as
select beneficiary as country_code,
       count(distinct helper) as neighbours_drawn_on,
       sum(heatwave_days)     as own_heatwave_days,
       round(sum(total_extra_gwh) filter (where not helper_also_in_heatwave)::numeric,1)
                              as extra_gwh_from_unstressed,
       round(sum(total_extra_gwh) filter (where helper_also_in_heatwave)::numeric,1)
                              as extra_gwh_from_stressed,
       round(sum(total_extra_gwh)::numeric,1) as extra_gwh_total
from public.v_heatwave_help_pairs group by 1;

grant select on public.v_heatwave_trade_position  to anon;
grant select on public.v_heatwave_demand_sources  to anon;
grant select on public.v_heatwave_imports_vs_gas  to anon;
grant select on public.v_heatwave_gap_coverage    to anon;
grant select on public.v_heatwave_help_pairs      to anon;
grant select on public.v_heatwave_helpers         to anon;
grant select on public.v_heatwave_beneficiaries   to anon;

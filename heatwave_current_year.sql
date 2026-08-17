-- Scope the whole Heatwaves analysis to the current year.
--
-- Pooling 2021-2026 kept producing numbers about systems that no longer exist.
-- The clearest case: Germany shut its last reactors in April 2023, yet the
-- coverage chart showed German nuclear at -8.7 GWh/day — a figure resting on
-- ten heatwave days from 2021 and 2022, describing a fleet that is gone.
--
-- Year-matching (comparing 2022 heatwave days against 2022 normal days) fixed
-- the comparison but not the reporting: the output was still an average across
-- years, so a fleet present in two of six years still appeared in the answer.
--
-- Restricting to the current year makes that impossible by construction, and
-- 2026 has enough data on its own: 961 heatwave country-days against 2279
-- normal ones, with every country holding at least 30 non-heatwave warm-season
-- days.
--
-- The (year, month) join is kept even though one year is in scope: it costs
-- nothing and keeps the views correct if the window is ever widened.

set statement_timeout = '900s';

-- ── Per-component deltas: the single source for fuel + coverage charts ─────
drop materialized view if exists public.mv_heatwave_component_delta cascade;
create materialized view public.mv_heatwave_component_delta as
with j as (
  select c.country_code, c.component, c.gwh,
         extract(year from c.date)::int  as yr,
         extract(month from c.date)::int as mo,
         (w.heatwave_id is not null)     as hw
  from public.mv_heatwave_component_daily c
  join public.weather_country_daily w
    on w.country_code = c.country_code and w.date = c.date
  join public.mv_load_daily_warm l
    on l.country_code = c.country_code and l.date = c.date and l.samples >= 20
  where c.date >= date_trunc('year', current_date)
),
agg as (select country_code, component, yr, mo, hw, avg(gwh) gwh, count(*) n
        from j group by 1,2,3,4,5),
paired as (
  select h.country_code, h.component, h.gwh hw_gwh, n.gwh base_gwh,
         h.n hw_days, n.n base_days
  from agg h
  join agg n on n.country_code=h.country_code and n.component=h.component
            and n.yr=h.yr and n.mo=h.mo and n.hw=false
  where h.hw
)
select country_code, component,
       sum(hw_days)                                                as heatwave_days,
       round(avg(base_gwh)::numeric, 2)                            as normal_gwh,
       round(avg(hw_gwh)::numeric, 2)                              as heatwave_gwh,
       round(avg(hw_gwh - base_gwh)::numeric, 2)                   as delta_gwh,
       round((avg(hw_gwh - base_gwh)*1000/24)::numeric, 0)         as delta_mw,
       round(avg(100.0*(hw_gwh/nullif(base_gwh,0)-1))::numeric, 1) as change_pct
from paired
where base_days >= 3 and base_gwh <> 0
group by 1,2;

create unique index if not exists mv_heatwave_component_delta_key
  on public.mv_heatwave_component_delta (country_code, component);

create view public.v_heatwave_fuel_resilience as
select country_code, component as fuel, heatwave_days,
       round((heatwave_gwh*1000/24)::numeric,0) as mean_mw_heatwave,
       round((normal_gwh*1000/24)::numeric,0)   as mean_mw_normal,
       change_pct as output_change_pct
from public.mv_heatwave_component_delta
where component <> 'imports' and abs(normal_gwh) > 0.02;

create view public.v_heatwave_fuel_delta as
select country_code, component as fuel, heatwave_days,
       round((normal_gwh*1000/24)::numeric,0)   as normal_mw,
       round((heatwave_gwh*1000/24)::numeric,0) as heatwave_mw,
       delta_mw, change_pct
from public.mv_heatwave_component_delta
where component <> 'imports';

-- ── Trade position ─────────────────────────────────────────────────────────
drop view if exists public.v_heatwave_gap_coverage;
drop view if exists public.v_heatwave_imports_vs_gas;
drop view if exists public.v_heatwave_demand_sources;
drop view if exists public.v_heatwave_trade_position;

create view public.v_heatwave_trade_position as
with agg as (
  select country_code, extract(year from date)::int yr, extract(month from date)::int mo,
         in_heatwave, avg(net_import_gwh) imp, avg(demand_gwh) dem, count(*) n
  from public.mv_heatwave_daily_balance
  -- flows_complete: these two views put imports and demand in one statement, so
  -- they may only use days where every border reported. Charts that never touch
  -- imports read the unrestricted day set instead.
  where date >= date_trunc('year', current_date) and flows_complete
  group by 1,2,3,4
),
paired as (
  select h.country_code, h.imp hw_imp, n.imp base_imp, h.dem hw_dem, n.dem base_dem,
         h.n hw_days, n.n base_days
  from agg h join agg n on n.country_code=h.country_code and n.yr=h.yr and n.mo=h.mo
                       and n.in_heatwave=false
  where h.in_heatwave
)
select country_code, sum(hw_days) as heatwave_days,
       round(avg(base_imp)::numeric,1)           as normal_net_import_gwh,
       round(avg(hw_imp)::numeric,1)             as heatwave_net_import_gwh,
       round(avg(hw_imp-base_imp)::numeric,1)    as delta_net_import_gwh,
       round(-avg(base_imp)::numeric,1)          as normal_net_export_gwh,
       round(-avg(hw_imp)::numeric,1)            as heatwave_net_export_gwh,
       round(-avg(hw_imp-base_imp)::numeric,1)   as delta_net_export_gwh,
       round(avg(hw_dem-base_dem)::numeric,1)    as extra_demand_gwh
from paired where base_days >= 3 group by 1;

create view public.v_heatwave_demand_sources as
with agg as (
  select country_code, extract(year from date)::int yr, extract(month from date)::int mo,
         in_heatwave, avg(demand_gwh) d, avg(gas_gwh) gas, avg(solar_gwh) solar,
         avg(wind_gwh) wind, avg(other_gwh) other, avg(net_import_gwh) imp, count(*) n
  from public.mv_heatwave_daily_balance
  -- flows_complete: these two views put imports and demand in one statement, so
  -- they may only use days where every border reported. Charts that never touch
  -- imports read the unrestricted day set instead.
  where date >= date_trunc('year', current_date) and flows_complete
  group by 1,2,3,4
),
paired as (
  select h.country_code, h.d-n.d dd, h.gas-n.gas dgas, h.solar-n.solar dsol,
         h.wind-n.wind dwind, h.other-n.other doth, h.imp-n.imp dimp,
         h.n hw_days, n.n base_days
  from agg h join agg n on n.country_code=h.country_code and n.yr=h.yr and n.mo=h.mo
                       and n.in_heatwave=false
  where h.in_heatwave
)
select country_code, sum(hw_days) as heatwave_days,
       round(avg(dd)::numeric,1)   as extra_demand_gwh,
       round(avg(dgas)::numeric,1) as extra_gas_gwh,
       round(avg(dsol)::numeric,1) as extra_solar_gwh,
       round(avg(dwind)::numeric,1) as extra_wind_gwh,
       round(avg(doth)::numeric,1)  as extra_other_gwh,
       round(avg(dimp)::numeric,1)  as extra_imports_gwh,
       round(avg(dgas+dsol+dwind+doth+dimp-dd)::numeric,1) as residual_gwh,
       round((100.0*avg(dgas)/nullif(avg(dd),0))::numeric,0) as gas_pct_of_extra_demand
from paired where base_days >= 3 group by 1;

create view public.v_heatwave_imports_vs_gas as
select country_code, heatwave_days, extra_demand_gwh, extra_imports_gwh, extra_gas_gwh
from public.v_heatwave_demand_sources where heatwave_days >= 10;

create view public.v_heatwave_gap_coverage as
with closure as (
  select d.country_code, max(t.extra_demand_gwh) as extra_demand_gwh,
         round(abs(sum(d.delta_gwh)-max(t.extra_demand_gwh))::numeric,1) as gap_gwh,
         round((100.0*abs(sum(d.delta_gwh)-max(t.extra_demand_gwh))
                / nullif(abs(max(t.extra_demand_gwh)),0))::numeric,0)    as gap_pct
  from public.mv_heatwave_component_delta d
  join public.v_heatwave_trade_position t on t.country_code=d.country_code
  group by 1
)
select d.country_code, d.component, d.heatwave_days, d.delta_gwh,
       c.extra_demand_gwh, c.gap_gwh, c.gap_pct
from public.mv_heatwave_component_delta d
join closure c on c.country_code=d.country_code;

-- ── Demand uplift, renewable share, price, gas sector ──────────────────────
drop view if exists public.v_heatwave_demand_uplift;
create view public.v_heatwave_demand_uplift as
with m as (
  select country_code, extract(year from date)::int yr, extract(month from date)::int mo,
         in_heatwave, avg(avg_load_mw) mw, avg(peak_load_mw) pk, count(*) n
  from public.v_heatwave_load_daily
  where samples >= 20 and date >= date_trunc('year', current_date)
  group by 1,2,3,4
),
p as (
  select h.country_code, h.mw hw_mw, n.mw base_mw, h.pk hw_pk, n.pk base_pk,
         h.n hw_days, n.n base_days
  from m h join m n on n.country_code=h.country_code and n.yr=h.yr and n.mo=h.mo
                   and n.in_heatwave=false
  where h.in_heatwave
)
select country_code, sum(hw_days) as heatwave_days,
       round(avg(100.0*(hw_mw/nullif(base_mw,0)-1))::numeric,2) as mean_demand_uplift_pct,
       round(avg(100.0*(hw_pk/nullif(base_pk,0)-1))::numeric,2) as peak_demand_uplift_pct
from p where base_days >= 3 group by 1;

drop view if exists public.v_heatwave_renewable;
create view public.v_heatwave_renewable as
with j as (
  select r.country_code, r.renewable_pct,
         extract(year from r.date)::int yr, extract(month from r.date)::int mo,
         (w.heatwave_id is not null) hw
  from public.mv_renewable_daily_warm r
  join public.weather_country_daily w on w.country_code=r.country_code and w.date=r.date
  where r.date >= date_trunc('year', current_date)
),
a as (select country_code, yr, mo, hw, avg(renewable_pct) pct, count(*) n from j group by 1,2,3,4),
p as (
  select h.country_code, h.pct hw_pct, n.pct base_pct, h.n hw_days, n.n base_days
  from a h join a n on n.country_code=h.country_code and n.yr=h.yr and n.mo=h.mo and n.hw=false
  where h.hw
)
select country_code, sum(hw_days) as heatwave_days,
       round(avg(base_pct)::numeric,1)        as normal_renewable_pct,
       round(avg(hw_pct)::numeric,1)          as heatwave_renewable_pct,
       round(avg(hw_pct-base_pct)::numeric,1) as delta_pp
from p where base_days >= 3 group by 1;

drop view if exists public.v_heatwave_price;
create view public.v_heatwave_price as
with j as (
  select p.country_code, p.avg_price, p.peak_price,
         extract(year from p.date)::int yr, extract(month from p.date)::int mo,
         (w.heatwave_id is not null) hw
  from public.mv_price_daily_warm p
  join public.weather_country_daily w on w.country_code=p.country_code and w.date=p.date
  where p.date >= date_trunc('year', current_date)
),
a as (select country_code, yr, mo, hw, avg(avg_price) av, avg(peak_price) pk, count(*) n
      from j group by 1,2,3,4),
pr as (
  select h.country_code, h.av hw_a, n.av base_a, h.pk hw_pk, n.pk base_pk,
         h.n hw_days, n.n base_days
  from a h join a n on n.country_code=h.country_code and n.yr=h.yr and n.mo=h.mo and n.hw=false
  where h.hw
)
select country_code, sum(hw_days) as heatwave_days,
       round(avg(base_a)::numeric,1)                          as normal_price_eur,
       round(avg(hw_a)::numeric,1)                            as heatwave_price_eur,
       round(avg(hw_a-base_a)::numeric,1)                     as delta_eur,
       round(avg(100.0*(hw_a/nullif(base_a,0)-1))::numeric,1) as change_pct,
       round(avg(hw_pk-base_pk)::numeric,1)                   as peak_delta_eur
from pr where base_days >= 3 and base_a > 1 group by 1;

drop view if exists public.v_heatwave_gas_sector;
create view public.v_heatwave_gas_sector as
with j as (
  select g.country_code, g.total_mwh, g.power_mwh, g.household_mwh, g.industry_mwh,
         extract(year from g.gas_day)::int yr, extract(month from g.gas_day)::int mo,
         (w.heatwave_id is not null) hw
  from public.gas_demand_daily g
  join public.weather_country_daily w on w.country_code=g.country_code and w.date=g.gas_day
  where g.total_mwh is not null
    and extract(month from g.gas_day) between 5 and 9
    and g.gas_day >= date_trunc('year', current_date)
),
a as (select country_code, yr, mo, hw, avg(total_mwh) t, avg(power_mwh) p,
             avg(household_mwh) hh, avg(industry_mwh) ind, count(*) n
      from j group by 1,2,3,4),
p as (
  select h.country_code, h.t hw_t, n.t base_t, h.p hw_p, n.p base_p,
         h.hh hw_hh, n.hh base_hh, h.ind hw_ind, n.ind base_ind,
         h.n hw_days, n.n base_days
  from a h join a n on n.country_code=h.country_code and n.yr=h.yr and n.mo=h.mo and n.hw=false
  where h.hw
)
select country_code, sum(hw_days) as heatwave_days,
       round(avg(base_t/1000.0)::numeric,1)                    as normal_total_gwh,
       round(avg((hw_t-base_t)/1000.0)::numeric,1)             as delta_total_gwh,
       round(avg((hw_p-base_p)/1000.0)::numeric,1)             as delta_power_gwh,
       round(avg((hw_hh-base_hh)/1000.0)::numeric,1)           as delta_household_gwh,
       round(avg((hw_ind-base_ind)/1000.0)::numeric,1)         as delta_industry_gwh,
       round(avg(100.0*(hw_p/nullif(base_p,0)-1))::numeric,1)  as power_change_pct,
       round(avg(100.0*(hw_t/nullif(base_t,0)-1))::numeric,1)  as total_change_pct
from p where base_days >= 3 group by 1;

-- ── Cooling response curve + EU view ───────────────────────────────────────
create or replace view public.v_heatwave_response_curve as
with b as (
  select country_code, (floor((tmax_c-10)/3.0)*3+11.5)::int as tmax_bin,
         avg(avg_load_mw) mw, count(*) days
  from public.v_heatwave_load_daily
  where samples >= 20 and tmax_c is not null and date >= date_trunc('year', current_date)
  group by 1,2
),
ref as (select country_code, sum(mw*days)/nullif(sum(days),0) ref_mw
        from b where tmax_bin between 16 and 20 group by 1)
select b.country_code, b.tmax_bin, b.days,
       round((100.0*b.mw/r.ref_mw)::numeric,1) as demand_index
from b join ref r on r.country_code=b.country_code
where b.days >= 4 and r.ref_mw > 0;

create or replace view public.v_eu_heatwave_response as
select case when countries_in_heatwave = 0 then '0'
            when countries_in_heatwave between 1 and 3 then '1-3'
            when countries_in_heatwave between 4 and 7 then '4-7'
            else '8+' end as bucket,
       count(*)                                 as days,
       round(avg(eu_load_mw)::numeric,0)        as mean_eu_load_mw,
       round(avg(eu_renewable_pct)::numeric,1)  as mean_renewable_pct,
       round(avg(max_anomaly_c)::numeric,1)     as mean_max_anomaly_c
from public.v_eu_heatwave_daily
where eu_load_mw is not null and countries_reporting >= 25
  and date >= date_trunc('year', current_date)
group by 1;

grant select on public.mv_heatwave_component_delta to anon;
grant select on public.v_heatwave_fuel_resilience  to anon;
grant select on public.v_heatwave_fuel_delta       to anon;
grant select on public.v_heatwave_trade_position   to anon;
grant select on public.v_heatwave_demand_sources   to anon;
grant select on public.v_heatwave_imports_vs_gas   to anon;
grant select on public.v_heatwave_gap_coverage     to anon;
grant select on public.v_heatwave_demand_uplift    to anon;
grant select on public.v_heatwave_renewable        to anon;
grant select on public.v_heatwave_price            to anon;
grant select on public.v_heatwave_gas_sector       to anon;
grant select on public.v_heatwave_response_curve   to anon;
grant select on public.v_eu_heatwave_response      to anon;

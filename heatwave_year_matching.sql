-- Match heatwave days against normal days in the same YEAR as well as the same
-- month.
--
-- Matching on month alone controls for season but not for structural change
-- between years, and several fleets changed enormously over 2021-2026:
--
--   French nuclear   38.5 GW (2021) -> 26.0 (2022 corrosion crisis) -> 38.3 (2026)
--   Solar capacity   grew every year across almost every country
--
-- Heatwave days are not evenly spread across those years — France has 46 of its
-- 107 in 2026 alone — so a month-matched comparison silently reads fleet
-- changes as heat effects. It made French nuclear look like -768 MW on heatwave
-- days when the same-year comparison gives -117 MW, and at EU level it turned a
-- +2.0% nuclear response into -4.4%, i.e. invented a derating that is not there.
--
-- Every paired view below now joins on (year, month) instead of (month).
-- The cost is smaller comparison groups, so the minimum-days floor drops to 3.

-- v_heatwave_country_balance is dropped outright: the chart it fed was removed
-- for duplicating the coverage chart on a looser day set and with net imports
-- as a residual rather than a measurement.
drop view if exists public.v_heatwave_country_balance;
drop view if exists public.v_heatwave_demand_delta;

-- ── Fuel resilience ────────────────────────────────────────────────────────
drop view if exists public.v_heatwave_fuel_resilience;
create view public.v_heatwave_fuel_resilience as
with j as (
  select g.country_code, g.fuel, g.avg_mw,
         extract(year from g.date)::int  as yr,
         extract(month from g.date)::int as mo,
         (w.heatwave_id is not null)     as hw
  from public.mv_generation_daily_warm g
  join public.weather_country_daily w
    on w.country_code = g.country_code and w.date = g.date
),
agg as (select country_code, fuel, yr, mo, hw, avg(avg_mw) mw, count(*) n from j group by 1,2,3,4,5),
paired as (
  select h.country_code, h.fuel, h.mw hw_mw, n.mw base_mw, h.n hw_days, n.n base_days
  from agg h
  join agg n on n.country_code=h.country_code and n.fuel=h.fuel
            and n.yr=h.yr and n.mo=h.mo and n.hw=false
  where h.hw
)
select country_code, fuel,
       sum(hw_days)                                                    as heatwave_days,
       round(avg(hw_mw)::numeric, 0)                                   as mean_mw_heatwave,
       round(avg(base_mw)::numeric, 0)                                 as mean_mw_normal,
       round(avg(100.0*(hw_mw/nullif(base_mw,0)-1))::numeric, 1)       as output_change_pct
from paired where base_days >= 3 and base_mw > 1 group by 1,2;

-- ── Fuel delta in MW (feeds the coverage chart's sibling views) ────────────
drop view if exists public.v_heatwave_fuel_delta;
create view public.v_heatwave_fuel_delta as
with j as (
  select g.country_code, g.fuel, g.avg_mw,
         extract(year from g.date)::int as yr, extract(month from g.date)::int as mo,
         (w.heatwave_id is not null) as hw
  from public.mv_generation_daily_warm g
  join public.weather_country_daily w on w.country_code=g.country_code and w.date=g.date
),
agg as (select country_code, fuel, yr, mo, hw, avg(avg_mw) mw, count(*) n from j group by 1,2,3,4,5),
paired as (
  select h.country_code, h.fuel, h.mw hw_mw, n.mw base_mw, h.n hw_days, n.n base_days
  from agg h join agg n on n.country_code=h.country_code and n.fuel=h.fuel
                       and n.yr=h.yr and n.mo=h.mo and n.hw=false
  where h.hw
)
select country_code, fuel,
       sum(hw_days)                                              as heatwave_days,
       round(avg(base_mw)::numeric, 0)                           as normal_mw,
       round(avg(hw_mw)::numeric, 0)                             as heatwave_mw,
       round(avg(hw_mw-base_mw)::numeric, 0)                     as delta_mw,
       round(avg(100.0*(hw_mw/nullif(base_mw,0)-1))::numeric, 1) as change_pct
from paired where base_days >= 3 group by 1,2;

-- ── Demand uplift ──────────────────────────────────────────────────────────
drop view if exists public.v_heatwave_demand_uplift;
create view public.v_heatwave_demand_uplift as
with m as (
  select country_code,
         extract(year from date)::int as yr, extract(month from date)::int as mo,
         in_heatwave, avg(avg_load_mw) mw, avg(peak_load_mw) pk, count(*) n
  from public.v_heatwave_load_daily where samples >= 20
  group by 1,2,3,4
),
paired as (
  select h.country_code, h.mw hw_mw, n.mw base_mw, h.pk hw_pk, n.pk base_pk,
         h.n hw_days, n.n base_days
  from m h join m n on n.country_code=h.country_code and n.yr=h.yr and n.mo=h.mo
                   and n.in_heatwave=false
  where h.in_heatwave
)
select country_code,
       sum(hw_days)                                              as heatwave_days,
       round(avg(100.0*(hw_mw/nullif(base_mw,0)-1))::numeric, 2) as mean_demand_uplift_pct,
       round(avg(100.0*(hw_pk/nullif(base_pk,0)-1))::numeric, 2) as peak_demand_uplift_pct
from paired where base_days >= 3 group by 1;

-- ── Renewable share ────────────────────────────────────────────────────────
drop view if exists public.v_heatwave_renewable;
create view public.v_heatwave_renewable as
with j as (
  select r.country_code, r.renewable_pct,
         extract(year from r.date)::int as yr, extract(month from r.date)::int as mo,
         (w.heatwave_id is not null) as hw
  from public.mv_renewable_daily_warm r
  join public.weather_country_daily w on w.country_code=r.country_code and w.date=r.date
),
agg as (select country_code, yr, mo, hw, avg(renewable_pct) pct, count(*) n from j group by 1,2,3,4),
paired as (
  select h.country_code, h.pct hw_pct, n.pct base_pct, h.n hw_days, n.n base_days
  from agg h join agg n on n.country_code=h.country_code and n.yr=h.yr and n.mo=h.mo and n.hw=false
  where h.hw
)
select country_code, sum(hw_days) as heatwave_days,
       round(avg(base_pct)::numeric,1)        as normal_renewable_pct,
       round(avg(hw_pct)::numeric,1)          as heatwave_renewable_pct,
       round(avg(hw_pct-base_pct)::numeric,1) as delta_pp
from paired where base_days >= 3 group by 1;

-- ── Day-ahead price ────────────────────────────────────────────────────────
drop view if exists public.v_heatwave_price;
create view public.v_heatwave_price as
with j as (
  select p.country_code, p.avg_price, p.peak_price,
         extract(year from p.date)::int as yr, extract(month from p.date)::int as mo,
         (w.heatwave_id is not null) as hw
  from public.mv_price_daily_warm p
  join public.weather_country_daily w on w.country_code=p.country_code and w.date=p.date
),
agg as (select country_code, yr, mo, hw, avg(avg_price) a, avg(peak_price) pk, count(*) n
        from j group by 1,2,3,4),
paired as (
  select h.country_code, h.a hw_a, n.a base_a, h.pk hw_pk, n.pk base_pk,
         h.n hw_days, n.n base_days
  from agg h join agg n on n.country_code=h.country_code and n.yr=h.yr and n.mo=h.mo and n.hw=false
  where h.hw
)
select country_code, sum(hw_days) as heatwave_days,
       round(avg(base_a)::numeric,1)                           as normal_price_eur,
       round(avg(hw_a)::numeric,1)                             as heatwave_price_eur,
       round(avg(hw_a-base_a)::numeric,1)                      as delta_eur,
       round(avg(100.0*(hw_a/nullif(base_a,0)-1))::numeric,1)  as change_pct,
       round(avg(hw_pk-base_pk)::numeric,1)                    as peak_delta_eur
from paired where base_days >= 3 and base_a > 1 group by 1;

-- ── Gas demand by sector ───────────────────────────────────────────────────
drop view if exists public.v_heatwave_gas_sector;
create view public.v_heatwave_gas_sector as
with j as (
  select g.country_code, g.total_mwh, g.power_mwh, g.household_mwh, g.industry_mwh,
         extract(year from g.gas_day)::int as yr, extract(month from g.gas_day)::int as mo,
         (w.heatwave_id is not null) as hw
  from public.gas_demand_daily g
  join public.weather_country_daily w on w.country_code=g.country_code and w.date=g.gas_day
  where g.total_mwh is not null and extract(month from g.gas_day) between 5 and 9
),
agg as (
  select country_code, yr, mo, hw, avg(total_mwh) t, avg(power_mwh) p,
         avg(household_mwh) hh, avg(industry_mwh) ind, count(*) n
  from j group by 1,2,3,4
),
paired as (
  select h.country_code, h.t hw_t, n.t base_t, h.p hw_p, n.p base_p,
         h.hh hw_hh, n.hh base_hh, h.ind hw_ind, n.ind base_ind,
         h.n hw_days, n.n base_days
  from agg h join agg n on n.country_code=h.country_code and n.yr=h.yr and n.mo=h.mo and n.hw=false
  where h.hw
)
select country_code,
       sum(hw_days)                                                        as heatwave_days,
       round(avg(base_t/1000.0)::numeric,1)                                as normal_total_gwh,
       round(avg((hw_t-base_t)/1000.0)::numeric,1)                         as delta_total_gwh,
       round(avg((hw_p-base_p)/1000.0)::numeric,1)                         as delta_power_gwh,
       round(avg((hw_hh-base_hh)/1000.0)::numeric,1)                       as delta_household_gwh,
       round(avg((hw_ind-base_ind)/1000.0)::numeric,1)                     as delta_industry_gwh,
       round(avg(100.0*(hw_p/nullif(base_p,0)-1))::numeric,1)              as power_change_pct,
       round(avg(100.0*(hw_t/nullif(base_t,0)-1))::numeric,1)              as total_change_pct
from paired where base_days >= 3 group by 1;

grant select on public.v_heatwave_fuel_resilience to anon;
grant select on public.v_heatwave_fuel_delta      to anon;
grant select on public.v_heatwave_demand_uplift   to anon;
grant select on public.v_heatwave_renewable       to anon;
grant select on public.v_heatwave_price           to anon;
grant select on public.v_heatwave_gas_sector      to anon;

-- SUPERSEDED by heatwave_pooled_baseline.sql. Do not run this file: the
-- nearest-neighbour matching below was replaced by a plain pooled average
-- (all heatwave days vs all non-heatwave days, May-September, same country,
-- weekday-vs-weekday / weekend-vs-weekend), which answers the same question
-- and can be explained in one sentence.
--
-- Replace calendar-month pairing with a nearest-neighbour baseline.
--
-- THE PROBLEM
--
-- Every paired view compared heatwave days against non-heatwave days in the
-- SAME CALENDAR MONTH. That silently deleted the hottest part of the summer:
--
--            May        Jun        Jul        Aug
--   Spain    8 hw/20    17/10      31 hw/0    16 hw/0
--   Italy   11 hw/20    19/11      31 hw/0    16 hw/0
--
-- Spain and Italy were in heatwave EVERY day of July and August 2026, so those
-- months had no within-month reference day and dropped out entirely. Spain's
-- "78 heatwave days" was in truth 25 days, all in May and June — the two
-- coolest months of the window — while July and August were discarded. Greece
-- lost August the same way (14 heatwave days against 2 normal).
--
-- The month boundary is also arbitrary in itself: 31 July and 1 August are one
-- day apart but were never comparable, while 1 and 31 August always were.
--
-- THE FIX
--
-- Match each heatwave day to its 15 nearest non-heatwave days within 45 days,
-- in the same country and the same year. Seasonality is still controlled — the
-- comparison stays local in time — but a hot month can borrow reference days
-- from just outside itself instead of having none. This lifts Italy from ~30 to
-- 52 usable heatwave days and Spain from ~25 to 47, and it is what lets Greece
-- keep the days the flows filter would otherwise cost it.
--
-- Where a country genuinely has no nearby normal day, it still drops out. That
-- is a real limit of summer 2026, not something to paper over: Cyprus was hot
-- almost continuously and cannot be measured against itself.
--
-- This file is the SINGLE definition of every heatwave-vs-normal view.
-- heatwave_current_year.sql, heatwave_year_matching*.sql, heatwave_single_source.sql
-- and heatwave_ratio_fix.sql all defined overlapping subsets of these and are
-- superseded; re-running any of them will reintroduce month pairing.

set statement_timeout = '1800s';

-- Drop every view this file owns first, so re-running it is idempotent and no
-- stale month-paired definition can survive underneath.
drop view if exists public.v_heatwave_imports_vs_gas   cascade;
drop view if exists public.v_heatwave_demand_sources   cascade;
drop view if exists public.v_heatwave_trade_position   cascade;
drop view if exists public.v_heatwave_gap_coverage     cascade;
drop view if exists public.v_heatwave_fuel_resilience  cascade;
drop view if exists public.v_heatwave_fuel_delta       cascade;
drop view if exists public.v_heatwave_demand_uplift    cascade;
drop view if exists public.v_heatwave_renewable        cascade;
drop view if exists public.v_heatwave_price            cascade;
drop view if exists public.v_heatwave_gas_sector       cascade;
drop view if exists public.v_heatwave_helpers          cascade;
drop view if exists public.v_heatwave_beneficiaries    cascade;
drop view if exists public.v_heatwave_help_pairs       cascade;

-- ── The matched day set ────────────────────────────────────────────────────
drop materialized view if exists public.mv_heatwave_matched_days cascade;
create materialized view public.mv_heatwave_matched_days as
with cal as (
  select country_code, date, (heatwave_id is not null) as hw,
         (extract(dow from date) in (0, 6))            as weekend
  from public.weather_country_daily
  where date >= date_trunc('year', current_date)
    and extract(month from date) between 5 and 9
)
select h.country_code, h.date as hw_date, b.date as base_date,
       abs(b.date - h.date) as day_gap
from cal h
cross join lateral (
  select n.date
  from cal n
  where n.country_code = h.country_code
    and not n.hw
    -- Same day type. Germany's heatwave days were 40% weekends against 27% of
    -- their reference days, and German weekend demand runs 9.5 GW below a
    -- weekday — enough to manufacture the 2.3% demand DROP that Germany showed
    -- during heatwaves. The window is wider for weekends because only two days
    -- in seven qualify.
    and n.weekend = h.weekend
    and n.date between h.date - (case when h.weekend then 75 else 45 end)
                   and h.date + (case when h.weekend then 75 else 45 end)
  order by abs(n.date - h.date), n.date
  limit 15
) b
where h.hw;

create index if not exists mv_heatwave_matched_days_key
  on public.mv_heatwave_matched_days (country_code, hw_date);
create index if not exists mv_heatwave_matched_days_base
  on public.mv_heatwave_matched_days (country_code, base_date);

-- A heatwave day needs this many reference days carrying real data to be used.
-- Fifteen are offered; five surviving the source's own filters is the floor.
-- MIN_BASE = 5, applied as `n_base >= 5` in each view below.

-- ── Per-component deltas: the single source for the fuel charts ────────────
drop materialized view if exists public.mv_heatwave_component_delta cascade;
create materialized view public.mv_heatwave_component_delta as
with src as (
  select c.country_code, c.date, c.component, c.gwh
  from public.mv_heatwave_component_daily c
  join public.mv_load_daily_warm l
    on l.country_code = c.country_code and l.date = c.date and l.samples >= 20
  where c.date >= date_trunc('year', current_date)
),
base as (
  select m.country_code, m.hw_date, s.component,
         avg(s.gwh) as base_gwh, count(*) as n_base
  from public.mv_heatwave_matched_days m
  join src s on s.country_code = m.country_code and s.date = m.base_date
  group by 1, 2, 3
),
per_day as (
  select h.country_code, h.component, h.gwh as hw_gwh, b.base_gwh
  from src h
  join base b on b.country_code = h.country_code and b.hw_date = h.date
             and b.component = h.component and b.n_base >= 5
)
select country_code, component,
       count(*)                                            as heatwave_days,
       round(avg(base_gwh)::numeric, 2)                    as normal_gwh,
       round(avg(hw_gwh)::numeric, 2)                      as heatwave_gwh,
       round(avg(hw_gwh - base_gwh)::numeric, 2)           as delta_gwh,
       round((avg(hw_gwh - base_gwh)*1000/24)::numeric, 0) as delta_mw,
       -- Ratio of means, not mean of ratios: one near-zero baseline day would
       -- otherwise dominate the average.
       round((100.0*(avg(hw_gwh)/nullif(avg(base_gwh),0) - 1))::numeric, 1) as change_pct
from per_day
group by 1, 2;

create unique index if not exists mv_heatwave_component_delta_key
  on public.mv_heatwave_component_delta (country_code, component);

create view public.v_heatwave_fuel_resilience as
select country_code, component as fuel, heatwave_days,
       round((heatwave_gwh*1000/24)::numeric,0) as mean_mw_heatwave,
       round((normal_gwh*1000/24)::numeric,0)   as mean_mw_normal,
       change_pct as output_change_pct
from public.mv_heatwave_component_delta
-- Below ~40 MW a percentage is noise; these were distorting the EU roll-up.
where component <> 'imports' and normal_gwh >= 1.0;

create view public.v_heatwave_fuel_delta as
select country_code, component as fuel, heatwave_days,
       round((normal_gwh*1000/24)::numeric,0)   as normal_mw,
       round((heatwave_gwh*1000/24)::numeric,0) as heatwave_mw,
       delta_mw, change_pct
from public.mv_heatwave_component_delta
where component <> 'imports';

-- ── Trade position ─────────────────────────────────────────────────────────
-- Imports and demand appear in one statement here, so both the heatwave day and
-- its reference days must have every border reporting.
create view public.v_heatwave_trade_position as
with src as (
  select country_code, date, demand_gwh, net_import_gwh
  from public.mv_heatwave_daily_balance
  where date >= date_trunc('year', current_date) and flows_complete
),
base as (
  select m.country_code, m.hw_date,
         avg(s.net_import_gwh) base_imp, avg(s.demand_gwh) base_dem, count(*) n_base
  from public.mv_heatwave_matched_days m
  join src s on s.country_code = m.country_code and s.date = m.base_date
  group by 1, 2
),
per_day as (
  select h.country_code, h.net_import_gwh hw_imp, b.base_imp,
         h.demand_gwh hw_dem, b.base_dem
  from src h
  join base b on b.country_code = h.country_code and b.hw_date = h.date and b.n_base >= 5
)
select country_code, count(*) as heatwave_days,
       round(avg(base_imp)::numeric,1)           as normal_net_import_gwh,
       round(avg(hw_imp)::numeric,1)             as heatwave_net_import_gwh,
       round(avg(hw_imp-base_imp)::numeric,1)    as delta_net_import_gwh,
       round(-avg(base_imp)::numeric,1)          as normal_net_export_gwh,
       round(-avg(hw_imp)::numeric,1)            as heatwave_net_export_gwh,
       round(-avg(hw_imp-base_imp)::numeric,1)   as delta_net_export_gwh,
       round(avg(hw_dem-base_dem)::numeric,1)    as extra_demand_gwh
from per_day group by 1;

create view public.v_heatwave_demand_sources as
with src as (
  select country_code, date, demand_gwh, gas_gwh, solar_gwh, wind_gwh,
         other_gwh, net_import_gwh
  from public.mv_heatwave_daily_balance
  where date >= date_trunc('year', current_date) and flows_complete
),
base as (
  select m.country_code, m.hw_date, avg(s.demand_gwh) d, avg(s.gas_gwh) gas,
         avg(s.solar_gwh) sol, avg(s.wind_gwh) wind, avg(s.other_gwh) oth,
         avg(s.net_import_gwh) imp, count(*) n_base
  from public.mv_heatwave_matched_days m
  join src s on s.country_code = m.country_code and s.date = m.base_date
  group by 1, 2
),
per_day as (
  select h.country_code,
         h.demand_gwh-b.d dd, h.gas_gwh-b.gas dgas, h.solar_gwh-b.sol dsol,
         h.wind_gwh-b.wind dwind, h.other_gwh-b.oth doth, h.net_import_gwh-b.imp dimp
  from src h
  join base b on b.country_code = h.country_code and b.hw_date = h.date and b.n_base >= 5
)
select country_code, count(*) as heatwave_days,
       round(avg(dd)::numeric,1)   as extra_demand_gwh,
       round(avg(dgas)::numeric,1) as extra_gas_gwh,
       round(avg(dsol)::numeric,1) as extra_solar_gwh,
       round(avg(dwind)::numeric,1) as extra_wind_gwh,
       round(avg(doth)::numeric,1)  as extra_other_gwh,
       round(avg(dimp)::numeric,1)  as extra_imports_gwh,
       round(avg(dgas+dsol+dwind+doth+dimp-dd)::numeric,1)     as residual_gwh,
       round((100.0*avg(dgas)/nullif(avg(dd),0))::numeric,0)   as gas_pct_of_extra_demand
from per_day group by 1;

create view public.v_heatwave_imports_vs_gas as
select country_code, heatwave_days, extra_demand_gwh, extra_imports_gwh, extra_gas_gwh
from public.v_heatwave_demand_sources where heatwave_days >= 10;

-- ── Coverage: components and demand on ONE day set, so the identity closes ──
drop materialized view if exists public.mv_heatwave_coverage_delta cascade;
create materialized view public.mv_heatwave_coverage_delta as
with days as (
  select country_code, date, demand_gwh
  from public.mv_heatwave_daily_balance
  where date >= date_trunc('year', current_date) and flows_complete
),
comp as (
  select c.country_code, c.date, c.component, c.gwh
  from public.mv_heatwave_component_daily c
  join days d on d.country_code = c.country_code and d.date = c.date
),
base_c as (
  select m.country_code, m.hw_date, s.component, avg(s.gwh) base_gwh, count(*) n_base
  from public.mv_heatwave_matched_days m
  join comp s on s.country_code = m.country_code and s.date = m.base_date
  group by 1, 2, 3
),
base_d as (
  select m.country_code, m.hw_date, avg(s.demand_gwh) base_dem, count(*) n_base
  from public.mv_heatwave_matched_days m
  join days s on s.country_code = m.country_code and s.date = m.base_date
  group by 1, 2
),
per_comp as (
  select h.country_code, h.component, count(*) heatwave_days,
         round(avg(h.gwh - b.base_gwh)::numeric,1) delta_gwh
  from comp h
  join base_c b on b.country_code=h.country_code and b.hw_date=h.date
               and b.component=h.component and b.n_base >= 5
  group by 1, 2
),
per_dem as (
  select h.country_code, round(avg(h.demand_gwh - b.base_dem)::numeric,1) extra_demand_gwh
  from days h
  join base_d b on b.country_code=h.country_code and b.hw_date=h.date and b.n_base >= 5
  group by 1
)
select p.country_code, p.component, p.heatwave_days, p.delta_gwh, d.extra_demand_gwh,
       round(abs(sum(p.delta_gwh) over (partition by p.country_code)
                 - d.extra_demand_gwh)::numeric, 1) as gap_gwh,
       round((100.0*abs(sum(p.delta_gwh) over (partition by p.country_code)
                        - d.extra_demand_gwh)
              / nullif(abs(d.extra_demand_gwh),0))::numeric, 0) as gap_pct
from per_comp p join per_dem d on d.country_code = p.country_code;

create unique index if not exists mv_heatwave_coverage_delta_key
  on public.mv_heatwave_coverage_delta (country_code, component);

create view public.v_heatwave_gap_coverage as
select * from public.mv_heatwave_coverage_delta;

-- ── Demand uplift ──────────────────────────────────────────────────────────
drop view if exists public.v_heatwave_demand_uplift;
create view public.v_heatwave_demand_uplift as
with src as (
  select country_code, date, avg_load_mw, peak_load_mw
  from public.v_heatwave_load_daily
  where samples >= 20 and date >= date_trunc('year', current_date)
),
base as (
  select m.country_code, m.hw_date, avg(s.avg_load_mw) mw, avg(s.peak_load_mw) pk,
         count(*) n_base
  from public.mv_heatwave_matched_days m
  join src s on s.country_code = m.country_code and s.date = m.base_date
  group by 1, 2
),
per_day as (
  select h.country_code, h.avg_load_mw hw_mw, b.mw base_mw,
         h.peak_load_mw hw_pk, b.pk base_pk
  from src h
  join base b on b.country_code = h.country_code and b.hw_date = h.date and b.n_base >= 5
)
select country_code, count(*) as heatwave_days,
       round((100.0*(avg(hw_mw)/nullif(avg(base_mw),0)-1))::numeric,2) as mean_demand_uplift_pct,
       round((100.0*(avg(hw_pk)/nullif(avg(base_pk),0)-1))::numeric,2) as peak_demand_uplift_pct
from per_day group by 1;

-- ── Renewable share ────────────────────────────────────────────────────────
drop view if exists public.v_heatwave_renewable;
create view public.v_heatwave_renewable as
with src as (
  select country_code, date, renewable_pct
  from public.mv_renewable_daily_warm
  where date >= date_trunc('year', current_date)
),
base as (
  select m.country_code, m.hw_date, avg(s.renewable_pct) pct, count(*) n_base
  from public.mv_heatwave_matched_days m
  join src s on s.country_code = m.country_code and s.date = m.base_date
  group by 1, 2
),
per_day as (
  select h.country_code, h.renewable_pct hw_pct, b.pct base_pct
  from src h
  join base b on b.country_code = h.country_code and b.hw_date = h.date and b.n_base >= 5
)
select country_code, count(*) as heatwave_days,
       round(avg(base_pct)::numeric,1)              as normal_renewable_pct,
       round(avg(hw_pct)::numeric,1)                as heatwave_renewable_pct,
       round(avg(hw_pct-base_pct)::numeric,1)       as delta_pp
from per_day group by 1;

-- ── Day-ahead price ────────────────────────────────────────────────────────
drop view if exists public.v_heatwave_price;
create view public.v_heatwave_price as
with src as (
  select country_code, date, avg_price, peak_price
  from public.mv_price_daily_warm
  where date >= date_trunc('year', current_date)
),
base as (
  select m.country_code, m.hw_date, avg(s.avg_price) av, avg(s.peak_price) pk,
         count(*) n_base
  from public.mv_heatwave_matched_days m
  join src s on s.country_code = m.country_code and s.date = m.base_date
  group by 1, 2
),
per_day as (
  select h.country_code, h.avg_price hw_a, b.av base_a, h.peak_price hw_pk, b.pk base_pk
  from src h
  join base b on b.country_code = h.country_code and b.hw_date = h.date and b.n_base >= 5
)
select country_code, count(*) as heatwave_days,
       round(avg(base_a)::numeric,1)                             as normal_price_eur,
       round(avg(hw_a)::numeric,1)                               as heatwave_price_eur,
       round(avg(hw_a-base_a)::numeric,1)                        as delta_eur,
       round((100.0*(avg(hw_a)/nullif(avg(base_a),0)-1))::numeric,1) as change_pct,
       round(avg(hw_pk-base_pk)::numeric,1)                      as peak_delta_eur
from per_day group by 1 having avg(base_a) > 1;

-- ── Gas by sector ──────────────────────────────────────────────────────────
drop view if exists public.v_heatwave_gas_sector;
create view public.v_heatwave_gas_sector as
with src as (
  select country_code, gas_day as date, total_mwh, power_mwh, household_mwh, industry_mwh
  from public.gas_demand_daily
  where total_mwh is not null
    and extract(month from gas_day) between 5 and 9
    and gas_day >= date_trunc('year', current_date)
),
base as (
  select m.country_code, m.hw_date, avg(s.total_mwh) t, avg(s.power_mwh) p,
         avg(s.household_mwh) hh, avg(s.industry_mwh) ind, count(*) n_base
  from public.mv_heatwave_matched_days m
  join src s on s.country_code = m.country_code and s.date = m.base_date
  group by 1, 2
),
per_day as (
  select h.country_code, h.total_mwh hw_t, b.t base_t, h.power_mwh hw_p, b.p base_p,
         h.household_mwh hw_hh, b.hh base_hh, h.industry_mwh hw_ind, b.ind base_ind
  from src h
  join base b on b.country_code = h.country_code and b.hw_date = h.date and b.n_base >= 5
)
select country_code, count(*) as heatwave_days,
       round(avg(base_t/1000.0)::numeric,1)            as normal_total_gwh,
       round(avg((hw_t-base_t)/1000.0)::numeric,1)     as delta_total_gwh,
       round(avg((hw_p-base_p)/1000.0)::numeric,1)     as delta_power_gwh,
       round(avg((hw_hh-base_hh)/1000.0)::numeric,1)   as delta_household_gwh,
       round(avg((hw_ind-base_ind)/1000.0)::numeric,1) as delta_industry_gwh,
       round((100.0*(avg(hw_p)/nullif(avg(base_p),0)-1))::numeric,1) as power_change_pct,
       round((100.0*(avg(hw_t)/nullif(avg(base_t),0)-1))::numeric,1) as total_change_pct
from per_day group by 1;

-- ── Interconnector assistance ──────────────────────────────────────────────
drop view if exists public.v_heatwave_helpers;
drop view if exists public.v_heatwave_beneficiaries;
drop view if exists public.v_heatwave_help_pairs;
create view public.v_heatwave_help_pairs as
with flows as (
  select n.from_country as helper, n.to_country as beneficiary, n.date, n.net_export_gwh,
         (wb.heatwave_id is not null)                as ben_hw,
         coalesce(wh.heatwave_id is not null, false) as helper_hw
  from public.mv_crossborder_net_daily n
  join public.weather_country_daily wb
    on wb.country_code = n.to_country and wb.date = n.date
  left join public.weather_country_daily wh
    on wh.country_code = n.from_country and wh.date = n.date
  where n.date >= date_trunc('year', current_date)
    and extract(month from n.date) between 5 and 9
),
-- The reference for a border is days when NEITHER end was in a heatwave.
base as (
  select m.country_code as beneficiary, m.hw_date, f.helper,
         avg(f.net_export_gwh) base_gwh, count(*) n_base
  from public.mv_heatwave_matched_days m
  join flows f on f.beneficiary = m.country_code and f.date = m.base_date
              and not f.helper_hw
  group by 1, 2, 3
),
per_day as (
  select f.helper, f.beneficiary, f.helper_hw,
         f.net_export_gwh - b.base_gwh as extra
  from flows f
  join base b on b.beneficiary = f.beneficiary and b.hw_date = f.date
             and b.helper = f.helper and b.n_base >= 5
  where f.ben_hw
)
select helper, beneficiary, helper_hw as helper_also_in_heatwave,
       count(*)                                as heatwave_days,
       round(avg(extra)::numeric, 2)           as extra_gwh_per_day,
       round(sum(extra)::numeric, 1)           as total_extra_gwh
from per_day group by 1, 2, 3;

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

grant select on public.mv_heatwave_matched_days   to anon;
grant select on public.mv_heatwave_component_delta to anon;
grant select on public.mv_heatwave_coverage_delta  to anon;
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
grant select on public.v_heatwave_help_pairs       to anon;
grant select on public.v_heatwave_helpers          to anon;
grant select on public.v_heatwave_beneficiaries    to anon;

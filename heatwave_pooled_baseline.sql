-- One average against another. Nothing else.
--
-- Heatwave side:  every heatwave day, May-September 2026, with data.
-- Reference side: every non-heatwave day, May-September 2026, with data.
-- Same country. Weekdays are compared with weekdays and weekends with
-- weekends (Germany's heatwave days were 40% weekends against ~28% of the
-- season, and German weekend demand runs 9.5 GW below a weekday - enough to
-- invent a demand DECLINE during heatwaves). The two strata are then combined
-- weighted by the country's own heatwave-day mix.
--
-- This replaces the nearest-neighbour matching (15 nearest days within a
-- window), which was harder to explain than the question deserved. The pooled
-- reference uses ALL available non-heatwave days.
--
-- THE single definition of every heatwave-vs-normal view. Everything earlier
-- (heatwave_current_year.sql, heatwave_year_matching*.sql,
-- heatwave_single_source.sql, heatwave_ratio_fix.sql,
-- heatwave_matched_baseline.sql) is superseded.

set statement_timeout = '1800s';

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
drop view if exists public.v_heatwave_baseline_quality cascade;
drop view if exists public.v_heatwave_days             cascade;
drop materialized view if exists public.mv_heatwave_matched_days   cascade;
drop materialized view if exists public.mv_heatwave_component_delta cascade;
drop materialized view if exists public.mv_heatwave_coverage_delta  cascade;

-- -- The two pools ----------------------------------------------------------
create view public.v_heatwave_days as
select country_code, date,
       (heatwave_id is not null)          as hw,
       (extract(dow from date) in (0, 6)) as wkend
from public.weather_country_daily
where date >= date_trunc('year', current_date)
  and extract(month from date) between 5 and 9;

-- -- Per-fuel deltas (fuel chart + coverage chart both read this) -----------
create materialized view public.mv_heatwave_component_delta as
with j as (
  select c.country_code, c.component, c.gwh, p.hw, p.wkend
  from public.mv_heatwave_component_daily c
  join public.v_heatwave_days p
    on p.country_code = c.country_code and p.date = c.date
  join public.mv_load_daily_warm l
    on l.country_code = c.country_code and l.date = c.date and l.samples >= 20
),
strata as (
  select country_code, component, wkend,
         avg(gwh) filter (where hw)     as hw_gwh,
         count(*) filter (where hw)     as hw_n,
         avg(gwh) filter (where not hw) as ref_gwh,
         count(*) filter (where not hw) as ref_n
  from j group by 1, 2, 3
),
ok as (select * from strata where hw_n > 0 and ref_n >= 3)
select country_code, component,
       sum(hw_n)::int                                            as heatwave_days,
       round((sum(ref_gwh*hw_n)/sum(hw_n))::numeric, 2)          as normal_gwh,
       round((sum(hw_gwh*hw_n)/sum(hw_n))::numeric, 2)           as heatwave_gwh,
       round((sum((hw_gwh-ref_gwh)*hw_n)/sum(hw_n))::numeric, 2) as delta_gwh,
       round((sum((hw_gwh-ref_gwh)*hw_n)/sum(hw_n)*1000/24)::numeric, 0) as delta_mw,
       round((100.0*((sum(hw_gwh*hw_n)/sum(hw_n))
              / nullif(sum(ref_gwh*hw_n)/sum(hw_n), 0) - 1))::numeric, 1) as change_pct
from ok group by 1, 2;

create unique index if not exists mv_heatwave_component_delta_key
  on public.mv_heatwave_component_delta (country_code, component);

create view public.v_heatwave_fuel_resilience as
select country_code, component as fuel, heatwave_days,
       round((heatwave_gwh*1000/24)::numeric,0) as mean_mw_heatwave,
       round((normal_gwh*1000/24)::numeric,0)   as mean_mw_normal,
       change_pct as output_change_pct
from public.mv_heatwave_component_delta
where component <> 'imports' and normal_gwh >= 1.0;   -- <40 MW fleets are noise

create view public.v_heatwave_fuel_delta as
select country_code, component as fuel, heatwave_days,
       round((normal_gwh*1000/24)::numeric,0)   as normal_mw,
       round((heatwave_gwh*1000/24)::numeric,0) as heatwave_mw,
       delta_mw, change_pct
from public.mv_heatwave_component_delta
where component <> 'imports';

-- NL is excluded from every load-based view: TenneT's reported load steps down
-- 4.3 GW overnight on 21 July 2026 (11.8 -> 7.5 GW daily average) and stays
-- there. Both our ingest paths agree on overlapping days, so the break is in
-- the source data, not the pipeline. Any average built across that break
-- measures the reporting change, not heat.

-- -- Coverage: components and demand on ONE day set so the identity closes --
create materialized view public.mv_heatwave_coverage_delta as
with days as (
  select b.country_code, b.date, b.demand_gwh, p.hw, p.wkend
  from public.mv_heatwave_daily_balance b
  join public.v_heatwave_days p on p.country_code = b.country_code and p.date = b.date
  where b.flows_complete and b.country_code <> 'NL'
),
comp as (
  select c.country_code, c.component, c.gwh, d.hw, d.wkend
  from public.mv_heatwave_component_daily c
  join days d on d.country_code = c.country_code and d.date = c.date
),
cs as (
  select country_code, component, wkend,
         avg(gwh) filter (where hw)     hw_gwh,  count(*) filter (where hw)     hw_n,
         avg(gwh) filter (where not hw) ref_gwh, count(*) filter (where not hw) ref_n
  from comp group by 1, 2, 3
),
ds as (
  select country_code, wkend,
         avg(demand_gwh) filter (where hw)     hw_dem,  count(*) filter (where hw)     hw_n,
         avg(demand_gwh) filter (where not hw) ref_dem, count(*) filter (where not hw) ref_n
  from days group by 1, 2
),
per_comp as (
  select country_code, component, sum(hw_n)::int heatwave_days,
         round((sum((hw_gwh-ref_gwh)*hw_n)/sum(hw_n))::numeric,1) delta_gwh
  from cs where hw_n > 0 and ref_n >= 3 group by 1, 2
),
per_dem as (
  select country_code,
         round((sum((hw_dem-ref_dem)*hw_n)/sum(hw_n))::numeric,1) extra_demand_gwh
  from ds where hw_n > 0 and ref_n >= 3 group by 1
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

-- -- Trade position ---------------------------------------------------------
create view public.v_heatwave_trade_position as
with days as (
  select b.country_code, b.net_import_gwh, b.demand_gwh, p.hw, p.wkend
  from public.mv_heatwave_daily_balance b
  join public.v_heatwave_days p on p.country_code = b.country_code and p.date = b.date
  where b.flows_complete and b.country_code <> 'NL'
),
s as (
  select country_code, wkend,
         avg(net_import_gwh) filter (where hw)     hw_imp,
         avg(net_import_gwh) filter (where not hw) ref_imp,
         avg(demand_gwh)     filter (where hw)     hw_dem,
         avg(demand_gwh)     filter (where not hw) ref_dem,
         count(*) filter (where hw) hw_n, count(*) filter (where not hw) ref_n
  from days group by 1, 2
),
ok as (select * from s where hw_n > 0 and ref_n >= 3)
select country_code, sum(hw_n)::int as heatwave_days,
       round((sum(ref_imp*hw_n)/sum(hw_n))::numeric,1)          as normal_net_import_gwh,
       round((sum(hw_imp*hw_n)/sum(hw_n))::numeric,1)           as heatwave_net_import_gwh,
       round((sum((hw_imp-ref_imp)*hw_n)/sum(hw_n))::numeric,1) as delta_net_import_gwh,
       round((-sum(ref_imp*hw_n)/sum(hw_n))::numeric,1)         as normal_net_export_gwh,
       round((-sum(hw_imp*hw_n)/sum(hw_n))::numeric,1)          as heatwave_net_export_gwh,
       round((-sum((hw_imp-ref_imp)*hw_n)/sum(hw_n))::numeric,1) as delta_net_export_gwh,
       round((sum((hw_dem-ref_dem)*hw_n)/sum(hw_n))::numeric,1)  as extra_demand_gwh
from ok group by 1;

-- -- Where the extra demand went / came from --------------------------------
create view public.v_heatwave_demand_sources as
with days as (
  select b.country_code, b.demand_gwh, b.gas_gwh, b.solar_gwh, b.wind_gwh,
         b.other_gwh, b.net_import_gwh, p.hw, p.wkend
  from public.mv_heatwave_daily_balance b
  join public.v_heatwave_days p on p.country_code = b.country_code and p.date = b.date
  where b.flows_complete and b.country_code <> 'NL'
),
s as (
  select country_code, wkend,
         count(*) filter (where hw) hw_n, count(*) filter (where not hw) ref_n,
         avg(demand_gwh)     filter (where hw) h_d,   avg(demand_gwh)     filter (where not hw) r_d,
         avg(gas_gwh)        filter (where hw) h_gas, avg(gas_gwh)        filter (where not hw) r_gas,
         avg(solar_gwh)      filter (where hw) h_sol, avg(solar_gwh)      filter (where not hw) r_sol,
         avg(wind_gwh)       filter (where hw) h_wnd, avg(wind_gwh)       filter (where not hw) r_wnd,
         avg(other_gwh)      filter (where hw) h_oth, avg(other_gwh)      filter (where not hw) r_oth,
         avg(net_import_gwh) filter (where hw) h_imp, avg(net_import_gwh) filter (where not hw) r_imp
  from days group by 1, 2
),
ok as (select * from s where hw_n > 0 and ref_n >= 3)
select country_code, sum(hw_n)::int as heatwave_days,
       round((sum(r_d*hw_n)/sum(hw_n))::numeric,1)               as normal_demand_gwh,
       round((sum((h_d-r_d)*hw_n)/sum(hw_n))::numeric,1)         as extra_demand_gwh,
       round((100.0*(sum((h_d-r_d)*hw_n)/sum(hw_n))
              / nullif(sum(r_d*hw_n)/sum(hw_n),0))::numeric,1)   as uplift_pct,
       round((sum((h_gas-r_gas)*hw_n)/sum(hw_n))::numeric,1)     as extra_gas_gwh,
       round((sum((h_sol-r_sol)*hw_n)/sum(hw_n))::numeric,1)     as extra_solar_gwh,
       round((sum((h_wnd-r_wnd)*hw_n)/sum(hw_n))::numeric,1)     as extra_wind_gwh,
       round((sum((h_oth-r_oth)*hw_n)/sum(hw_n))::numeric,1)     as extra_other_gwh,
       round((sum((h_imp-r_imp)*hw_n)/sum(hw_n))::numeric,1)     as extra_imports_gwh,
       round((sum(((h_gas-r_gas)+(h_sol-r_sol)+(h_wnd-r_wnd)+(h_oth-r_oth)
                   +(h_imp-r_imp)-(h_d-r_d))*hw_n)/sum(hw_n))::numeric,1) as residual_gwh,
       round((100.0*(sum((h_gas-r_gas)*hw_n)/sum(hw_n))
              / nullif(sum((h_d-r_d)*hw_n)/sum(hw_n),0))::numeric,0) as gas_pct_of_extra_demand
from ok group by 1;

create view public.v_heatwave_imports_vs_gas as
select country_code, heatwave_days, extra_demand_gwh, extra_imports_gwh, extra_gas_gwh
from public.v_heatwave_demand_sources where heatwave_days >= 10;

-- -- Demand uplift, with the min/mean/max of both pools in the open ---------
create view public.v_heatwave_demand_uplift as
with days as (
  select l.country_code, l.avg_load_mw, l.peak_load_mw, p.hw, p.wkend
  from public.v_heatwave_load_daily l
  join public.v_heatwave_days p on p.country_code = l.country_code and p.date = l.date
  where l.samples >= 20 and l.country_code <> 'NL'
),
s as (
  select country_code, wkend,
         avg(avg_load_mw)  filter (where hw)     h_mw,
         avg(avg_load_mw)  filter (where not hw) r_mw,
         avg(peak_load_mw) filter (where hw)     h_pk,
         avg(peak_load_mw) filter (where not hw) r_pk,
         count(*) filter (where hw) hw_n, count(*) filter (where not hw) ref_n
  from days group by 1, 2
),
ok as (select * from s where hw_n > 0 and ref_n >= 3),
core as (
  select country_code, sum(hw_n)::int heatwave_days,
         sum(r_mw*hw_n)/sum(hw_n) normal_mw, sum(h_mw*hw_n)/sum(hw_n) heatwave_mw,
         sum(r_pk*hw_n)/sum(hw_n) normal_pk, sum(h_pk*hw_n)/sum(hw_n) heatwave_pk
  from ok group by 1
),
extremes as (
  select country_code,
         count(*) filter (where not hw)          normal_days,
         min(avg_load_mw) filter (where not hw)  normal_min_mw,
         max(avg_load_mw) filter (where not hw)  normal_max_mw,
         min(avg_load_mw) filter (where hw)      heatwave_min_mw,
         max(avg_load_mw) filter (where hw)      heatwave_max_mw
  from days group by 1
)
select c.country_code, c.heatwave_days, e.normal_days,
       round(c.normal_mw::numeric,0)   as normal_mean_mw,
       round(c.heatwave_mw::numeric,0) as heatwave_mean_mw,
       round(e.normal_min_mw::numeric,0)   as normal_min_mw,
       round(e.normal_max_mw::numeric,0)   as normal_max_mw,
       round(e.heatwave_min_mw::numeric,0) as heatwave_min_mw,
       round(e.heatwave_max_mw::numeric,0) as heatwave_max_mw,
       round((100.0*(c.heatwave_mw/nullif(c.normal_mw,0)-1))::numeric,2) as mean_demand_uplift_pct,
       round((100.0*(c.heatwave_pk/nullif(c.normal_pk,0)-1))::numeric,2) as peak_demand_uplift_pct
from core c join extremes e on e.country_code = c.country_code;

-- -- Renewable share --------------------------------------------------------
create view public.v_heatwave_renewable as
with days as (
  select r.country_code, r.renewable_pct, p.hw, p.wkend
  from public.mv_renewable_daily_warm r
  join public.v_heatwave_days p on p.country_code = r.country_code and p.date = r.date
),
s as (
  select country_code, wkend,
         avg(renewable_pct) filter (where hw)     h, avg(renewable_pct) filter (where not hw) r,
         count(*) filter (where hw) hw_n, count(*) filter (where not hw) ref_n
  from days group by 1, 2
),
ok as (select * from s where hw_n > 0 and ref_n >= 3)
select country_code, sum(hw_n)::int as heatwave_days,
       round((sum(r*hw_n)/sum(hw_n))::numeric,1)     as normal_renewable_pct,
       round((sum(h*hw_n)/sum(hw_n))::numeric,1)     as heatwave_renewable_pct,
       round((sum((h-r)*hw_n)/sum(hw_n))::numeric,1) as delta_pp
from ok group by 1;

-- -- Day-ahead price --------------------------------------------------------
create view public.v_heatwave_price as
with days as (
  select q.country_code, q.avg_price, q.peak_price, p.hw, p.wkend
  from public.mv_price_daily_warm q
  join public.v_heatwave_days p on p.country_code = q.country_code and p.date = q.date
),
s as (
  select country_code, wkend,
         avg(avg_price)  filter (where hw)     h_a, avg(avg_price)  filter (where not hw) r_a,
         avg(peak_price) filter (where hw)     h_p, avg(peak_price) filter (where not hw) r_p,
         count(*) filter (where hw) hw_n, count(*) filter (where not hw) ref_n
  from days group by 1, 2
),
ok as (select * from s where hw_n > 0 and ref_n >= 3)
select country_code, sum(hw_n)::int as heatwave_days,
       round((sum(r_a*hw_n)/sum(hw_n))::numeric,1)          as normal_price_eur,
       round((sum(h_a*hw_n)/sum(hw_n))::numeric,1)          as heatwave_price_eur,
       round((sum((h_a-r_a)*hw_n)/sum(hw_n))::numeric,1)    as delta_eur,
       round((100.0*((sum(h_a*hw_n)/sum(hw_n))
              / nullif(sum(r_a*hw_n)/sum(hw_n),0)-1))::numeric,1) as change_pct,
       round((sum((h_p-r_p)*hw_n)/sum(hw_n))::numeric,1)    as peak_delta_eur
from ok group by 1 having sum(r_a*hw_n)/sum(hw_n) > 1;

-- -- Gas demand by sector ---------------------------------------------------
create view public.v_heatwave_gas_sector as
with days as (
  select g.country_code, g.total_mwh, g.power_mwh, g.household_mwh, g.industry_mwh,
         p.hw, p.wkend
  from public.gas_demand_daily g
  join public.v_heatwave_days p on p.country_code = g.country_code and p.date = g.gas_day
  where g.total_mwh is not null
),
s as (
  select country_code, wkend,
         avg(total_mwh)     filter (where hw) h_t,  avg(total_mwh)     filter (where not hw) r_t,
         avg(power_mwh)     filter (where hw) h_p,  avg(power_mwh)     filter (where not hw) r_p,
         avg(household_mwh) filter (where hw) h_hh, avg(household_mwh) filter (where not hw) r_hh,
         avg(industry_mwh)  filter (where hw) h_i,  avg(industry_mwh)  filter (where not hw) r_i,
         count(*) filter (where hw) hw_n, count(*) filter (where not hw) ref_n
  from days group by 1, 2
),
ok as (select * from s where hw_n > 0 and ref_n >= 3)
select country_code, sum(hw_n)::int as heatwave_days,
       round((sum(r_t*hw_n)/sum(hw_n)/1000.0)::numeric,1)          as normal_total_gwh,
       round((sum((h_t-r_t)*hw_n)/sum(hw_n)/1000.0)::numeric,1)    as delta_total_gwh,
       round((sum((h_p-r_p)*hw_n)/sum(hw_n)/1000.0)::numeric,1)    as delta_power_gwh,
       round((sum((h_hh-r_hh)*hw_n)/sum(hw_n)/1000.0)::numeric,1)  as delta_household_gwh,
       round((sum((h_i-r_i)*hw_n)/sum(hw_n)/1000.0)::numeric,1)    as delta_industry_gwh,
       round((100.0*((sum(h_p*hw_n)/sum(hw_n))
              / nullif(sum(r_p*hw_n)/sum(hw_n),0)-1))::numeric,1)  as power_change_pct,
       round((100.0*((sum(h_t*hw_n)/sum(hw_n))
              / nullif(sum(r_t*hw_n)/sum(hw_n),0)-1))::numeric,1)  as total_change_pct
from ok group by 1;

-- -- Interconnector assistance ----------------------------------------------
create view public.v_heatwave_help_pairs as
with flows as (
  select n.from_country as helper, n.to_country as beneficiary, n.net_export_gwh,
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
base as (
  select helper, beneficiary, avg(net_export_gwh) base_gwh, count(*) base_n
  from flows where not ben_hw and not helper_hw group by 1, 2
),
during as (
  select helper, beneficiary, helper_hw, avg(net_export_gwh) hw_gwh, count(*) hw_n
  from flows where ben_hw group by 1, 2, 3
)
select d.helper, d.beneficiary, d.helper_hw as helper_also_in_heatwave,
       d.hw_n                                   as heatwave_days,
       round(d.hw_gwh::numeric, 2)              as mean_net_export_gwh,
       round(b.base_gwh::numeric, 2)            as baseline_net_export_gwh,
       round((d.hw_gwh - b.base_gwh)::numeric, 2)        as extra_gwh_per_day,
       round(((d.hw_gwh - b.base_gwh)*d.hw_n)::numeric,1) as total_extra_gwh
from during d
join base b on b.helper = d.helper and b.beneficiary = d.beneficiary
where b.base_n >= 5;

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

-- -- What each country's reference actually is, in the open -----------------
create view public.v_heatwave_baseline_quality as
select p.country_code,
       count(*) filter (where p.hw)                        as heatwave_days,
       count(*) filter (where not p.hw)                    as reference_days,
       round(avg(w.tmax_c) filter (where p.hw)::numeric,1)     as hw_tmax_c,
       round(avg(w.tmax_c) filter (where not p.hw)::numeric,1) as ref_tmax_c,
       round((avg(w.tmax_c) filter (where p.hw)
              - avg(w.tmax_c) filter (where not p.hw))::numeric,1) as temp_gap_c,
       -- 100 means the whole reference pool sits before July: after that,
       -- every day was a heatwave.
       round((100.0*count(*) filter (where not p.hw and extract(month from p.date) <= 6)
              / nullif(count(*) filter (where not p.hw),0))::numeric,0) as pct_ref_before_jul
from public.v_heatwave_days p
join public.weather_country_daily w
  on w.country_code = p.country_code and w.date = p.date
group by 1;

grant select on public.v_heatwave_days             to anon;
grant select on public.mv_heatwave_component_delta to anon;
grant select on public.mv_heatwave_coverage_delta  to anon;
grant select on public.v_heatwave_fuel_resilience  to anon;
grant select on public.v_heatwave_fuel_delta       to anon;
grant select on public.v_heatwave_gap_coverage     to anon;
grant select on public.v_heatwave_trade_position   to anon;
grant select on public.v_heatwave_demand_sources   to anon;
grant select on public.v_heatwave_imports_vs_gas   to anon;
grant select on public.v_heatwave_demand_uplift    to anon;
grant select on public.v_heatwave_renewable        to anon;
grant select on public.v_heatwave_price            to anon;
grant select on public.v_heatwave_gas_sector       to anon;
grant select on public.v_heatwave_help_pairs       to anon;
grant select on public.v_heatwave_helpers          to anon;
grant select on public.v_heatwave_beneficiaries    to anon;
grant select on public.v_heatwave_baseline_quality to anon;

-- Two fixes to the per-fuel deltas.
--
-- 1. change_pct was avg(hw/base - 1) across month groups: a mean of ratios. One
--    month with a near-zero baseline makes that explode — Croatia's coal read
--    +1,050,345% when it actually went 85 -> 153 MW, i.e. +80%. Use the ratio
--    of the means, which is what the two MW columns beside it already show.
--
-- 2. Fuel deltas use every day with generation and load. The imports component
--    additionally needs every border to have reported, so the coverage chart —
--    which sums components against demand and must balance — gets its own
--    computation over complete-flow days only. Keeping these separate is what
--    lets Italy keep its 31 baseline days for the fuel charts.

set statement_timeout = '900s';

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
  -- Year AND month: fleets change structurally between years, so a month-only
  -- comparison reads those changes as heat effects.
  select h.country_code, h.component, h.gwh hw_gwh, n.gwh base_gwh,
         h.n hw_days, n.n base_days
  from agg h
  join agg n on n.country_code=h.country_code and n.component=h.component
            and n.yr=h.yr and n.mo=h.mo and n.hw=false
  where h.hw
)
select country_code, component,
       sum(hw_days)                                        as heatwave_days,
       round(avg(base_gwh)::numeric, 2)                    as normal_gwh,
       round(avg(hw_gwh)::numeric, 2)                      as heatwave_gwh,
       round(avg(hw_gwh - base_gwh)::numeric, 2)           as delta_gwh,
       round((avg(hw_gwh - base_gwh)*1000/24)::numeric, 0) as delta_mw,
       round((100.0*(avg(hw_gwh)/nullif(avg(base_gwh),0) - 1))::numeric, 1) as change_pct
from paired
where base_days >= 3 and base_gwh <> 0
group by 1,2;

create unique index if not exists mv_heatwave_component_delta_key
  on public.mv_heatwave_component_delta (country_code, component);

-- Fleets below ~40 MW average are excluded: a percentage on a fleet that small
-- is noise and was distorting the EU roll-up.
create view public.v_heatwave_fuel_resilience as
select country_code, component as fuel, heatwave_days,
       round((heatwave_gwh*1000/24)::numeric,0) as mean_mw_heatwave,
       round((normal_gwh*1000/24)::numeric,0)   as mean_mw_normal,
       change_pct as output_change_pct
from public.mv_heatwave_component_delta
where component <> 'imports' and normal_gwh >= 1.0;

create view public.v_heatwave_fuel_delta as
select country_code, component as fuel, heatwave_days,
       round((normal_gwh*1000/24)::numeric,0)   as normal_mw,
       round((heatwave_gwh*1000/24)::numeric,0) as heatwave_mw,
       delta_mw, change_pct
from public.mv_heatwave_component_delta
where component <> 'imports';

-- ── Coverage: one day set, complete borders, so the identity can close ─────
drop materialized view if exists public.mv_heatwave_coverage_delta cascade;
create materialized view public.mv_heatwave_coverage_delta as
with days as (
  select country_code, date, extract(year from date)::int yr,
         extract(month from date)::int mo, in_heatwave hw, demand_gwh
  from public.mv_heatwave_daily_balance
  where date >= date_trunc('year', current_date) and flows_complete
),
comp as (
  select d.country_code, d.yr, d.mo, d.hw, c.component, c.gwh, d.date
  from days d
  join public.mv_heatwave_component_daily c
    on c.country_code = d.country_code and c.date = d.date
),
agg as (select country_code, component, yr, mo, hw, avg(gwh) gwh, count(*) n
        from comp group by 1,2,3,4,5),
dem as (select country_code, yr, mo, hw, avg(demand_gwh) dem, count(*) n
        from days group by 1,2,3,4),
paired_c as (
  select h.country_code, h.component, h.gwh - n.gwh delta, h.n hw_days, n.n base_days
  from agg h join agg n on n.country_code=h.country_code and n.component=h.component
                       and n.yr=h.yr and n.mo=h.mo and n.hw=false
  where h.hw
),
paired_d as (
  select h.country_code, h.dem - n.dem ddem, n.n base_days
  from dem h join dem n on n.country_code=h.country_code and n.yr=h.yr and n.mo=h.mo
                       and n.hw=false
  where h.hw
),
per_comp as (
  select country_code, component, sum(hw_days) heatwave_days,
         round(avg(delta)::numeric,1) delta_gwh
  from paired_c where base_days >= 3 group by 1,2
),
per_dem as (
  select country_code, round(avg(ddem)::numeric,1) extra_demand_gwh
  from paired_d where base_days >= 3 group by 1
)
select p.country_code, p.component, p.heatwave_days, p.delta_gwh,
       d.extra_demand_gwh,
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

grant select on public.mv_heatwave_component_delta to anon;
grant select on public.mv_heatwave_coverage_delta  to anon;
grant select on public.v_heatwave_fuel_resilience  to anon;
grant select on public.v_heatwave_fuel_delta       to anon;
grant select on public.v_heatwave_gap_coverage     to anon;

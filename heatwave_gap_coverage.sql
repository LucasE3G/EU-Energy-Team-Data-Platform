-- How each country covered its heatwave demand increase, fuel by fuel.
--
-- Two things a reader wants at once: of the extra demand, how much came from
-- gas; and of the generation that FELL, what had to be replaced. Both are the
-- same arithmetic viewed from either side of zero, so one diverging stack
-- carries them.
--
-- Fuels are broken out individually rather than bundled into "other", because
-- "other fell by 57 GWh/day" is exactly the answer the question is asking for.
--
-- Crucially this also reports how well the terms close. Generation plus imports
-- should equal the demand change; the shortfall is pumped-storage consumption
-- (counted as generation, never netted off), transmission losses, and any plant
-- that reports patchily. Where that gap is large the decomposition should not
-- be shown at all — France's is 172% of its own demand change — so the closure
-- travels with the row and the chart filters on it instead of drawing an
-- "unexplained" block.

drop view if exists public.v_heatwave_gap_coverage;
create view public.v_heatwave_gap_coverage as
with gen as (
  select country_code, date, fuel, avg_mw * 24.0 / 1000.0 as gwh
  from public.mv_generation_daily_warm
),
imp as (
  select to_country as country_code, date, sum(net_export_gwh) as gwh
  from public.mv_crossborder_net_daily group by 1, 2
),
-- One long table of every component, so imports sit alongside the fuels.
comp as (
  select g.country_code, g.date, g.fuel as component, g.gwh from gen g
  union all
  select i.country_code, i.date, 'imports', i.gwh from imp i
),
j as (
  select c.country_code, c.date, c.component, c.gwh,
         extract(month from c.date)::int as month,
         (w.heatwave_id is not null) as hw
  from comp c
  join public.weather_country_daily w
    on w.country_code = c.country_code and w.date = c.date
  join public.mv_load_daily_warm l
    on l.country_code = c.country_code and l.date = c.date and l.samples >= 20
),
agg as (
  select country_code, component, month, hw, avg(gwh) as gwh, count(*) as days
  from j group by 1, 2, 3, 4
),
paired as (
  select h.country_code, h.component, h.month,
         h.gwh - n.gwh as delta, h.days as hw_days, n.days as base_days
  from agg h
  join agg n on n.country_code = h.country_code and n.component = h.component
            and n.month = h.month and n.hw = false
  where h.hw = true
),
per_component as (
  select country_code, component,
         sum(hw_days)                  as heatwave_days,
         round(avg(delta)::numeric, 1) as delta_gwh
  from paired where base_days >= 5
  group by 1, 2
),
demand as (
  select country_code, extra_demand_gwh
  from public.v_heatwave_trade_position
),
closure as (
  select p.country_code,
         sum(p.delta_gwh)                                              as total_delta,
         max(d.extra_demand_gwh)                                       as extra_demand_gwh,
         round(abs(sum(p.delta_gwh) - max(d.extra_demand_gwh))::numeric, 1) as gap_gwh,
         round((100.0 * abs(sum(p.delta_gwh) - max(d.extra_demand_gwh))
                / nullif(abs(max(d.extra_demand_gwh)), 0))::numeric, 0)     as gap_pct
  from per_component p
  join demand d on d.country_code = p.country_code
  group by 1
)
select
  p.country_code,
  p.component,
  p.heatwave_days,
  p.delta_gwh,
  c.extra_demand_gwh,
  c.gap_gwh,
  c.gap_pct
from per_component p
join closure c on c.country_code = p.country_code;

grant select on public.v_heatwave_gap_coverage to anon;

-- One source of truth for every per-fuel heatwave delta.
--
-- Two problems this closes:
--
-- 1. The fuel chart and the coverage chart disagreed on French nuclear even
--    after both were year-matched — -357 MW against -496 MW — because they used
--    different day sets. The fuel chart needed only generation and weather; the
--    coverage chart also required load and cross-border flows, which drops days.
--    Two charts on one page must not answer the same question differently, so
--    they now read the SAME paired table.
--
-- 2. Year-matching multiplied the number of groups and pushed gap_coverage back
--    over the anon statement timeout. Materializing the paired result fixes
--    that permanently — the output is a few hundred rows.

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
),
agg as (
  select country_code, component, yr, mo, hw, avg(gwh) gwh, count(*) n
  from j group by 1, 2, 3, 4, 5
),
paired as (
  -- Year AND month: fleets changed structurally between years (French nuclear
  -- ran 26 GW in 2022 against 38 GW in 2026), so a month-only comparison reads
  -- those changes as heat effects.
  select h.country_code, h.component,
         h.gwh as hw_gwh, n.gwh as base_gwh,
         h.n as hw_days, n.n as base_days
  from agg h
  join agg n on n.country_code = h.country_code and n.component = h.component
            and n.yr = h.yr and n.mo = h.mo and n.hw = false
  where h.hw
)
select
  country_code,
  component,
  sum(hw_days)                                                   as heatwave_days,
  round(avg(base_gwh)::numeric, 2)                               as normal_gwh,
  round(avg(hw_gwh)::numeric, 2)                                 as heatwave_gwh,
  round(avg(hw_gwh - base_gwh)::numeric, 2)                      as delta_gwh,
  round((avg(hw_gwh - base_gwh) * 1000 / 24)::numeric, 0)        as delta_mw,
  round(avg(100.0*(hw_gwh/nullif(base_gwh,0)-1))::numeric, 1)    as change_pct
from paired
where base_days >= 3 and base_gwh <> 0
group by 1, 2;

create unique index if not exists mv_heatwave_component_delta_key
  on public.mv_heatwave_component_delta (country_code, component);

-- ── Both charts now read that one table ────────────────────────────────────
drop view if exists public.v_heatwave_fuel_resilience;
create view public.v_heatwave_fuel_resilience as
select country_code,
       component                          as fuel,
       heatwave_days,
       round((heatwave_gwh*1000/24)::numeric, 0) as mean_mw_heatwave,
       round((normal_gwh*1000/24)::numeric, 0)   as mean_mw_normal,
       change_pct                         as output_change_pct
from public.mv_heatwave_component_delta
where component <> 'imports' and abs(normal_gwh) > 0.02;

drop view if exists public.v_heatwave_gap_coverage;
create view public.v_heatwave_gap_coverage as
with closure as (
  select d.country_code,
         max(t.extra_demand_gwh) as extra_demand_gwh,
         round(abs(sum(d.delta_gwh) - max(t.extra_demand_gwh))::numeric, 1) as gap_gwh,
         round((100.0*abs(sum(d.delta_gwh) - max(t.extra_demand_gwh))
                / nullif(abs(max(t.extra_demand_gwh)),0))::numeric, 0)      as gap_pct
  from public.mv_heatwave_component_delta d
  join public.v_heatwave_trade_position t on t.country_code = d.country_code
  group by 1
)
select d.country_code, d.component, d.heatwave_days, d.delta_gwh,
       c.extra_demand_gwh, c.gap_gwh, c.gap_pct
from public.mv_heatwave_component_delta d
join closure c on c.country_code = d.country_code;

drop view if exists public.v_heatwave_fuel_delta;
create view public.v_heatwave_fuel_delta as
select country_code, component as fuel, heatwave_days,
       round((normal_gwh*1000/24)::numeric, 0)   as normal_mw,
       round((heatwave_gwh*1000/24)::numeric, 0) as heatwave_mw,
       delta_mw, change_pct
from public.mv_heatwave_component_delta
where component <> 'imports';

create or replace function public.refresh_heatwave_delta_mv()
returns void language plpgsql security definer set search_path = public as $$
begin
  refresh materialized view concurrently public.mv_heatwave_component_delta;
end;
$$;
grant execute on function public.refresh_heatwave_delta_mv() to service_role;

grant select on public.mv_heatwave_component_delta to anon;
grant select on public.v_heatwave_fuel_resilience  to anon;
grant select on public.v_heatwave_gap_coverage     to anon;
grant select on public.v_heatwave_fuel_delta       to anon;

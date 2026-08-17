-- Net imports were summing one side of a two-sided table.
--
-- mv_crossborder_net_daily stores each border twice — (FR,IT) and (IT,FR) —
-- as sign-mirrored rows, but the two directions do NOT have the same day
-- coverage: in August 2026 (FR,IT) had 17 days and (IT,FR) had 1. The balance
-- MV built net imports as `sum(net_export_gwh) where to_country = X`, so on a
-- day where only some mirrors existed the missing borders silently dropped out
-- and the country looked far more balanced than it was. France came out at
-- 2.9 GW of net export against 7.3 GW actually on its interconnectors, and
-- read as EXPORTING more during heatwaves when it in fact imports heavily.
--
-- Fix: collapse each border to ONE canonical value per day, taking whichever
-- direction was recorded, then read both endpoints off that.
--
-- Border completeness is tracked but NOT used to gate the whole balance. Demand,
-- generation and renewable share do not depend on flows, and gating everything
-- on flow completeness cost Italy 28 of its 31 baseline days for no reason.
-- Only the charts that actually spend the imports number require complete days.

set statement_timeout = '900s';

drop materialized view if exists public.mv_crossborder_country_daily cascade;
create materialized view public.mv_crossborder_country_daily as
with pair as (
  -- One row per border per day. The two stored directions are mirrors, so
  -- max() picks the recorded one and agrees with itself when both are present.
  select date,
         least(from_country, to_country)    as a,
         greatest(from_country, to_country) as b,
         max(case when from_country < to_country then net_export_gwh
                  else -net_export_gwh end) as net_a_to_b
  from public.mv_crossborder_net_daily
  group by 1, 2, 3
),
endpoints as (
  select a as country_code, date, b as neighbour,  net_a_to_b as net_export_gwh from pair
  union all
  select b,                date, a,               -net_a_to_b                  from pair
),
-- Completeness is judged by how much FLOW is present, not how many borders.
-- Counting borders treats Italy's 166 MW Greek link the same as its 2 GW Swiss
-- one: requiring the Greek link (which ENTSO-E publishes sparsely — no May 2026
-- data at all) cost Italy 28 of its 31 baseline days to protect 3% of its
-- trade. The same link is half of GREECE's trade, so Greece must still require
-- it. Weighting by typical flow gets both cases right from one rule.
weights as (
  select country_code, neighbour, avg(abs(net_export_gwh)) as w
  from endpoints group by 1, 2
),
totals as (select country_code, sum(w) as total_w from weights group by 1),
daily as (
  select e.country_code, e.date,
         -sum(e.net_export_gwh) as net_import_gwh,
         count(*)               as borders_reporting,
         sum(wt.w)              as reported_w
  from endpoints e
  join weights wt on wt.country_code = e.country_code and wt.neighbour = e.neighbour
  group by 1, 2
)
select d.country_code, d.date, d.net_import_gwh, d.borders_reporting,
       round((100.0 * d.reported_w / nullif(t.total_w, 0))::numeric, 0) as flow_coverage_pct,
       (d.reported_w >= 0.90 * t.total_w) as complete
from daily d join totals t on t.country_code = d.country_code;

create unique index if not exists mv_crossborder_country_daily_key
  on public.mv_crossborder_country_daily (country_code, date);

-- ── Balance: flows are optional, and flagged when partial ──────────────────
drop materialized view if exists public.mv_heatwave_daily_balance cascade;
create materialized view public.mv_heatwave_daily_balance as
with gen as (
  select country_code, date,
         sum(avg_mw) filter (where fuel = 'gas')   * 24.0 / 1000.0 as gas_gwh,
         sum(avg_mw) filter (where fuel = 'solar') * 24.0 / 1000.0 as solar_gwh,
         sum(avg_mw) filter (where fuel = 'wind')  * 24.0 / 1000.0 as wind_gwh,
         sum(avg_mw) filter (where fuel not in ('gas','solar','wind'))
                                                   * 24.0 / 1000.0 as other_gwh
  from public.mv_generation_daily_warm
  group by 1, 2
)
select
  w.country_code,
  w.date,
  extract(month from w.date)::int  as month,
  (w.heatwave_id is not null)      as in_heatwave,
  l.avg_load_mw * 24.0 / 1000.0    as demand_gwh,
  g.gas_gwh, g.solar_gwh, g.wind_gwh, g.other_gwh,
  -- Null rather than a short number: a partial border set understates flows,
  -- and a wrong import figure is worse than a missing one.
  case when i.complete then i.net_import_gwh end as net_import_gwh,
  coalesce(i.complete, false)                    as flows_complete
from public.weather_country_daily w
join public.mv_load_daily_warm l on l.country_code = w.country_code and l.date = w.date
join gen g on g.country_code = w.country_code and g.date = w.date
left join public.mv_crossborder_country_daily i
  on i.country_code = w.country_code and i.date = w.date
where l.samples >= 20;

create unique index if not exists mv_heatwave_daily_balance_key
  on public.mv_heatwave_daily_balance (country_code, date);

drop materialized view if exists public.mv_heatwave_component_daily cascade;
create materialized view public.mv_heatwave_component_daily as
select g.country_code, g.date, g.fuel as component, g.avg_mw * 24.0 / 1000.0 as gwh
from public.mv_generation_daily_warm g
union all
select b.country_code, b.date, 'imports', b.net_import_gwh
from public.mv_heatwave_daily_balance b
where b.flows_complete;

create unique index if not exists mv_heatwave_component_daily_key
  on public.mv_heatwave_component_daily (country_code, date, component);

grant select on public.mv_crossborder_country_daily to anon;
grant select on public.mv_heatwave_daily_balance    to anon;
grant select on public.mv_heatwave_component_daily  to anon;

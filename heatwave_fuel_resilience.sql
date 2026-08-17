-- Which generation sources hold up under heat, and which fade.
--
-- Replaces the plain view of the same intent, which timed out: it aggregated
-- the full ~30M-row electricity_generation_snapshots table on every query.
-- Materialized and restricted to the warm season, since heatwave events only
-- exist in May-September, this drops to a few hundred thousand rows.
--
-- The question it answers: on heatwave days, does a country's nuclear output
-- fall (river-cooling limits and derating) while solar rises (clear skies,
-- partly offset by panel efficiency losses)? That contrast is the difference
-- between a thermal-heavy and a renewables-heavy system under heat stress.

set statement_timeout = '900s';

drop materialized view if exists public.mv_generation_daily_warm cascade;
create materialized view public.mv_generation_daily_warm as
select
  case when g.zone_id ~ '^(DK|NO|SE)[0-9]$' then left(g.zone_id, 2) else g.zone_id end as country_code,
  (g.ts at time zone 'UTC')::date as date,
  case
    when g.psr_type in ('B18','B19')       then 'wind'
    when g.psr_type = 'B16'                then 'solar'
    when g.psr_type in ('B10','B11','B12') then 'hydro'
    when g.psr_type = 'B14'                then 'nuclear'
    when g.psr_type = 'B04'                then 'gas'
    when g.psr_type in ('B02','B05')       then 'coal'
    when g.psr_type in ('B01','B17')       then 'biomass'
    else 'other'
  end as fuel,
  avg(g.mw) as avg_mw,
  max(g.mw) as max_mw,
  count(*)  as samples
from public.electricity_generation_snapshots g
where g.source = 'entsoe'
  and extract(month from (g.ts at time zone 'UTC')) between 5 and 9
group by 1, 2, 3;

create unique index if not exists mv_generation_daily_warm_key
  on public.mv_generation_daily_warm (country_code, date, fuel);

-- ── Per-fuel response to heat, against a matched same-month baseline ────────
-- Dropped rather than replaced: the earlier definition in
-- heatwave_analysis_views.sql exposed a different column list, and CREATE OR
-- REPLACE VIEW cannot change column names.
drop view if exists public.v_heatwave_fuel_resilience;
create view public.v_heatwave_fuel_resilience as
with joined as (
  select
    g.country_code, g.fuel, g.date, g.avg_mw,
    extract(month from g.date)::int as month,
    (w.heatwave_id is not null)     as in_heatwave
  from public.mv_generation_daily_warm g
  join public.weather_country_daily w
    on w.country_code = g.country_code and w.date = g.date
),
agg as (
  select country_code, fuel, month, in_heatwave,
         avg(avg_mw) as mw, count(*) as days
  from joined
  group by 1, 2, 3, 4
),
paired as (
  select
    h.country_code, h.fuel, h.month,
    h.mw as hw_mw, n.mw as base_mw,
    h.days as hw_days, n.days as base_days
  from agg h
  join agg n
    on n.country_code = h.country_code and n.fuel = h.fuel
   and n.month = h.month and n.in_heatwave = false
  where h.in_heatwave = true
)
select
  country_code,
  fuel,
  sum(hw_days)                                                     as heatwave_days,
  round(avg(hw_mw)::numeric, 0)                                    as mean_mw_heatwave,
  round(avg(base_mw)::numeric, 0)                                  as mean_mw_normal,
  round(avg(100.0 * (hw_mw / nullif(base_mw, 0) - 1))::numeric, 1) as output_change_pct
from paired
where base_days >= 5 and base_mw > 1      -- ignore fuels a country barely runs
group by 1, 2;

create or replace function public.refresh_generation_warm_mv()
returns void language plpgsql security definer set search_path = public as $$
begin
  refresh materialized view concurrently public.mv_generation_daily_warm;
end;
$$;
grant execute on function public.refresh_generation_warm_mv() to service_role;

grant select on public.mv_generation_daily_warm  to anon;
grant select on public.v_heatwave_fuel_resilience to anon;

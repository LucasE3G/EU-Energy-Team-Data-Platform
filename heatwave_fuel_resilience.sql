-- Daily warm-season generation by fuel group.
--
-- CORRECTION (important): this previously did avg(mw) grouped by
-- (country, date, fuel). Several fuel groups bundle multiple ENTSO-E psr_types
-- — wind = B18+B19, hydro = B10+B11+B12, coal = B02+B05, biomass = B01+B17,
-- and "other" bundles many — so averaging across BOTH time and psr_type
-- returned the mean of a single type rather than the group's total output.
--
-- The understatement scaled with how many types a country's mix spans:
-- Austria 56% of true generation, Germany 66%, Poland 71%, Spain 104% (Spain
-- barely affected because solar/gas/nuclear are single-type). That artefact was
-- initially mistaken for ENTSO-E omitting distributed plant.
--
-- Correct order: sum psr_types within each timestamp, THEN average over time.

set statement_timeout = '900s';

drop materialized view if exists public.mv_generation_daily_warm cascade;
create materialized view public.mv_generation_daily_warm as
with per_ts as (
  select
    case when zone_id ~ '^(DK|NO|SE)[0-9]$' then left(zone_id, 2) else zone_id end as country_code,
    (ts at time zone 'UTC')::date as date,
    case
      when psr_type in ('B18','B19')       then 'wind'
      when psr_type = 'B16'                then 'solar'
      when psr_type in ('B10','B11','B12') then 'hydro'
      when psr_type = 'B14'                then 'nuclear'
      when psr_type = 'B04'                then 'gas'
      when psr_type in ('B02','B05')       then 'coal'
      when psr_type in ('B01','B17')       then 'biomass'
      else 'other'
    end as fuel,
    ts,
    sum(mw) as mw           -- total across psr_types at this instant
  from public.electricity_generation_snapshots
  where source = 'entsoe'
    and extract(month from (ts at time zone 'UTC')) between 5 and 9
  group by 1, 2, 3, 4
)
select country_code, date, fuel,
       avg(mw) as avg_mw,   -- then average that total over the day
       max(mw) as max_mw,
       count(*) as samples
from per_ts
group by 1, 2, 3;

create unique index if not exists mv_generation_daily_warm_key
  on public.mv_generation_daily_warm (country_code, date, fuel);

-- ── Per-fuel response to heat, against a matched same-month baseline ────────
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
where base_days >= 5 and base_mw > 1
group by 1, 2;

create or replace function public.refresh_generation_warm_mv()
returns void language plpgsql security definer set search_path = public as $$
begin
  refresh materialized view concurrently public.mv_generation_daily_warm;
end;
$$;
grant execute on function public.refresh_generation_warm_mv() to service_role;

grant select on public.mv_generation_daily_warm   to anon;
grant select on public.v_heatwave_fuel_resilience to anon;

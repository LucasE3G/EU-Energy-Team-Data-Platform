-- European-level heatwave views + renewable share response.
-- Complements the country-level views; requires weather_country_daily.

-- ── Renewable share: heatwave days vs matched normal days ──────────────────
-- Answers "was the system dirtier or cleaner when it was under heat stress?"
create or replace view public.v_heatwave_renewable as
with zc as (
  select zone_id,
         case when zone_id ~ '^(DK|NO|SE)[0-9]$' then left(zone_id, 2) else zone_id end as cc
  from (select distinct zone_id from public.energy_mix_snapshots) z
),
daily as (
  select zc.cc as country_code,
         (m.ts at time zone 'UTC')::date as date,
         avg(m.renewable_percent) as ren_pct
  from public.energy_mix_snapshots m
  join zc on zc.zone_id = m.zone_id
  where m.source = 'entsoe' and m.renewable_percent is not null and zc.cc <> 'EU'
  group by 1, 2
),
j as (
  select d.country_code, d.date, d.ren_pct,
         extract(month from d.date)::int as month,
         (w.heatwave_id is not null) as hw
  from daily d
  join public.weather_country_daily w
    on w.country_code = d.country_code and w.date = d.date
  where extract(month from d.date) between 5 and 9
),
agg as (select country_code, month, hw, avg(ren_pct) as pct, count(*) as days
        from j group by 1, 2, 3),
paired as (
  select h.country_code, h.month, h.pct as hw_pct, n.pct as base_pct,
         h.days as hw_days, n.days as base_days
  from agg h
  join agg n on n.country_code = h.country_code and n.month = h.month and n.hw = false
  where h.hw = true
)
select
  country_code,
  sum(hw_days)                              as heatwave_days,
  round(avg(base_pct)::numeric, 1)          as normal_renewable_pct,
  round(avg(hw_pct)::numeric, 1)            as heatwave_renewable_pct,
  round(avg(hw_pct - base_pct)::numeric, 1) as delta_pp
from paired
where base_days >= 5
group by 1;

-- ── One row per day for the whole continent ────────────────────────────────
-- `countries_in_heatwave` is the intensity measure: a Europe-wide event is one
-- where many national systems are stressed at the same time, which is exactly
-- when interconnection stops being able to help.
create or replace view public.v_eu_heatwave_daily as
with hw as (
  select date,
         count(*) filter (where heatwave_id is not null) as countries_in_heatwave,
         round(max(anomaly_c)::numeric, 1)               as max_anomaly_c
  from public.weather_country_daily
  group by 1
),
dem as (
  select date, sum(avg_load_mw) as eu_load_mw, count(*) as countries_reporting
  from public.v_heatwave_load_daily
  where samples >= 20
  group by 1
),
ren as (
  select (ts at time zone 'UTC')::date as date, avg(renewable_percent) as eu_renewable_pct
  from public.energy_mix_snapshots
  where zone_id = 'EU' and source = 'entsoe' and renewable_percent is not null
  group by 1
)
select
  hw.date,
  hw.countries_in_heatwave,
  hw.max_anomaly_c,
  round(dem.eu_load_mw::numeric, 0)      as eu_load_mw,
  dem.countries_reporting,
  round(ren.eu_renewable_pct::numeric, 1) as eu_renewable_pct
from hw
left join dem on dem.date = hw.date
left join ren on ren.date = hw.date
where extract(month from hw.date) between 5 and 9;

-- ── Europe-wide response, by how many countries were stressed at once ───────
-- Bucketed rather than a single heatwave/normal split: the interesting question
-- is whether the response scales with how much of the continent is hot.
create or replace view public.v_eu_heatwave_response as
with base as (
  select *,
    case
      when countries_in_heatwave = 0            then '0'
      when countries_in_heatwave between 1 and 3  then '1-3'
      when countries_in_heatwave between 4 and 7  then '4-7'
      when countries_in_heatwave between 8 and 12 then '8-12'
      else '13+'
    end as bucket,
    extract(month from date)::int as month
  from public.v_eu_heatwave_daily
  where eu_load_mw is not null and countries_reporting >= 25
)
select
  bucket,
  count(*)                                    as days,
  round(avg(eu_load_mw)::numeric, 0)          as mean_eu_load_mw,
  round(avg(eu_renewable_pct)::numeric, 1)    as mean_renewable_pct,
  round(avg(max_anomaly_c)::numeric, 1)       as mean_max_anomaly_c
from base
group by 1;

grant select on public.v_heatwave_renewable   to anon;
grant select on public.v_eu_heatwave_daily    to anon;
grant select on public.v_eu_heatwave_response to anon;

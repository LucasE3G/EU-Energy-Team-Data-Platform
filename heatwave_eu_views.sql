-- European-level heatwave views + renewable share response.
-- Requires weather_country_daily, mv_load_daily_warm, mv_generation_daily_warm.

-- ── Renewable share per country: heatwave vs matched normal days ───────────
create or replace view public.v_heatwave_renewable as
with j as (
  select r.country_code, r.date, r.renewable_pct,
         extract(month from r.date)::int as month,
         (w.heatwave_id is not null) as hw
  from public.mv_renewable_daily_warm r
  join public.weather_country_daily w
    on w.country_code = r.country_code and w.date = r.date
),
agg as (select country_code, month, hw, avg(renewable_pct) as pct, count(*) as days
        from j group by 1, 2, 3),
paired as (
  select h.country_code, h.month, h.pct as hw_pct, n.pct as base_pct,
         h.days as hw_days, n.days as base_days
  from agg h
  join agg n on n.country_code = h.country_code and n.month = h.month and n.hw = false
  where h.hw = true
)
select country_code,
       sum(hw_days) as heatwave_days,
       round(avg(base_pct)::numeric, 1)          as normal_renewable_pct,
       round(avg(hw_pct)::numeric, 1)            as heatwave_renewable_pct,
       round(avg(hw_pct - base_pct)::numeric, 1) as delta_pp
from paired where base_days >= 5 group by 1;

-- ── One row per day for the whole continent ────────────────────────────────
-- `countries_in_heatwave` is the intensity measure: a Europe-wide event is one
-- where many national systems are stressed at once, which is exactly when
-- interconnection has least spare capacity to redistribute.
--
-- The EU renewable share is DERIVED from per-country generation, not read from
-- the `zone_id = 'EU'` aggregate row. That row exists for only ~108 of 873
-- warm-season days, so a chart built on it averaged a small arbitrary subset —
-- and once bucket boundaries moved, two buckets held no rows at all and
-- rendered as 0.0%.
create or replace view public.v_eu_heatwave_daily as
with hw as (
  select date,
         count(*) filter (where heatwave_id is not null) as countries_in_heatwave,
         round(max(anomaly_c)::numeric, 1)               as max_anomaly_c
  from public.weather_country_daily group by 1
),
dem as (
  select date, sum(avg_load_mw) as eu_load_mw, count(*) as countries_reporting
  from public.mv_load_daily_warm where samples >= 20 group by 1
),
gen as (
  select date,
         sum(avg_mw)                                                          as total_mw,
         sum(avg_mw) filter (where fuel in ('wind','solar','hydro','biomass')) as renewable_mw,
         count(distinct country_code)                                          as gen_countries
  from public.mv_generation_daily_warm group by 1
)
select
  hw.date,
  hw.countries_in_heatwave,
  hw.max_anomaly_c,
  round(dem.eu_load_mw::numeric, 0)       as eu_load_mw,
  dem.countries_reporting,
  round((100.0 * gen.renewable_mw / nullif(gen.total_mw, 0))::numeric, 1) as eu_renewable_pct,
  gen.gen_countries
from hw
left join dem on dem.date = hw.date
left join gen on gen.date = hw.date
where extract(month from hw.date) between 5 and 9;

-- ── Europe-wide response, four buckets ─────────────────────────────────────
-- Four, not five: the old top bucket ('13+') held only 8 days against 299 in
-- the first and its value bounced back up, drawing as a trend reversal that was
-- pure noise. The day count travels with each row so the chart can show what
-- every point rests on.
create or replace view public.v_eu_heatwave_response as
select
  case
    when countries_in_heatwave = 0             then '0'
    when countries_in_heatwave between 1 and 3 then '1-3'
    when countries_in_heatwave between 4 and 7 then '4-7'
    else '8+'
  end as bucket,
  count(*)                                 as days,
  round(avg(eu_load_mw)::numeric, 0)       as mean_eu_load_mw,
  round(avg(eu_renewable_pct)::numeric, 1) as mean_renewable_pct,
  round(avg(max_anomaly_c)::numeric, 1)    as mean_max_anomaly_c
from public.v_eu_heatwave_daily
where eu_load_mw is not null and countries_reporting >= 25
group by 1;

grant select on public.v_heatwave_renewable   to anon;
grant select on public.v_eu_heatwave_daily    to anon;
grant select on public.v_eu_heatwave_response to anon;

-- Daily warm-season rollups so the Heatwaves page can query live.
--
-- The analysis views were built for ad-hoc SQL and aggregate the 15-minute
-- tables on every call: load (~4M rows), prices (~2.4M), energy mix (~3M).
-- That is fine from a terminal and far too slow behind a page load. These
-- materialize the daily grain once; the views on top then join tiny tables.
--
-- All are warm-season only (May-September) because heatwave events exist
-- nowhere else, which removes ~60% of the rows for free.

set statement_timeout = '900s';

-- ── Daily load per country ─────────────────────────────────────────────────
drop materialized view if exists public.mv_load_daily_warm cascade;
create materialized view public.mv_load_daily_warm as
select
  case when zone_id ~ '^(DK|NO|SE)[0-9]$' then left(zone_id, 2) else zone_id end as country_code,
  (ts at time zone 'UTC')::date as date,
  avg(load_mw) as avg_load_mw,
  max(load_mw) as peak_load_mw,
  count(*)     as samples
from public.electricity_load_snapshots
where source = 'entsoe' and load_mw > 0
  and extract(month from (ts at time zone 'UTC')) between 5 and 9
group by 1, 2;
create unique index if not exists mv_load_daily_warm_key
  on public.mv_load_daily_warm (country_code, date);

-- ── Daily day-ahead price per country ──────────────────────────────────────
drop materialized view if exists public.mv_price_daily_warm cascade;
create materialized view public.mv_price_daily_warm as
select
  case when zone_id ~ '^(DK|NO|SE)[0-9]$' then left(zone_id, 2) else zone_id end as country_code,
  (ts at time zone 'UTC')::date as date,
  avg(price_eur_per_mwh) as avg_price,
  max(price_eur_per_mwh) as peak_price
from public.electricity_day_ahead_prices
where source = 'entsoe' and price_eur_per_mwh is not null
  and extract(month from (ts at time zone 'UTC')) between 5 and 9
group by 1, 2;
create unique index if not exists mv_price_daily_warm_key
  on public.mv_price_daily_warm (country_code, date);

-- ── Daily renewable share per country (+ the EU aggregate zone) ────────────
drop materialized view if exists public.mv_renewable_daily_warm cascade;
create materialized view public.mv_renewable_daily_warm as
select
  case when zone_id ~ '^(DK|NO|SE)[0-9]$' then left(zone_id, 2) else zone_id end as country_code,
  (ts at time zone 'UTC')::date as date,
  avg(renewable_percent) as renewable_pct
from public.energy_mix_snapshots
where source = 'entsoe' and renewable_percent is not null
  and extract(month from (ts at time zone 'UTC')) between 5 and 9
group by 1, 2;
create unique index if not exists mv_renewable_daily_warm_key
  on public.mv_renewable_daily_warm (country_code, date);

-- ── Rewire the analysis views onto the fast rollups ────────────────────────
create or replace view public.v_heatwave_load_daily as
select
  d.country_code,
  d.date,
  d.avg_load_mw,
  d.peak_load_mw,
  d.avg_load_mw * 24.0 / 1000.0 as consumption_gwh,
  d.samples,
  w.tmax_c,
  w.anomaly_c,
  w.is_hot_day,
  w.heatwave_id is not null as in_heatwave,
  w.heatwave_day,
  w.heatwave_length,
  extract(month from d.date)::int as month,
  extract(isodow from d.date)::int >= 6 as is_weekend
from public.mv_load_daily_warm d
join public.weather_country_daily w
  on w.country_code = d.country_code and w.date = d.date;

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

create or replace view public.v_heatwave_price as
with j as (
  select p.country_code, p.date, p.avg_price, p.peak_price,
         extract(month from p.date)::int as month,
         (w.heatwave_id is not null) as hw
  from public.mv_price_daily_warm p
  join public.weather_country_daily w
    on w.country_code = p.country_code and w.date = p.date
),
agg as (select country_code, month, hw, avg(avg_price) as p_avg,
               avg(peak_price) as p_peak, count(*) as days
        from j group by 1, 2, 3),
paired as (
  select h.country_code, h.month, h.p_avg as hw_avg, n.p_avg as base_avg,
         h.p_peak as hw_peak, n.p_peak as base_peak,
         h.days as hw_days, n.days as base_days
  from agg h
  join agg n on n.country_code = h.country_code and n.month = h.month and n.hw = false
  where h.hw = true
)
select country_code,
       sum(hw_days) as heatwave_days,
       round(avg(base_avg)::numeric, 1)                                   as normal_price_eur,
       round(avg(hw_avg)::numeric, 1)                                     as heatwave_price_eur,
       round(avg(hw_avg - base_avg)::numeric, 1)                          as delta_eur,
       round(avg(100.0 * (hw_avg / nullif(base_avg, 0) - 1))::numeric, 1) as change_pct,
       round(avg(hw_peak - base_peak)::numeric, 1)                        as peak_delta_eur
from paired where base_days >= 5 and base_avg > 1 group by 1;

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
ren as (
  select date, renewable_pct as eu_renewable_pct
  from public.mv_renewable_daily_warm where country_code = 'EU'
)
select hw.date, hw.countries_in_heatwave, hw.max_anomaly_c,
       round(dem.eu_load_mw::numeric, 0) as eu_load_mw,
       dem.countries_reporting,
       round(ren.eu_renewable_pct::numeric, 1) as eu_renewable_pct
from hw
left join dem on dem.date = hw.date
left join ren on ren.date = hw.date
where extract(month from hw.date) between 5 and 9;

-- ── Europe-wide response, four buckets ─────────────────────────────────────
-- Was five: '8-12' and '13+' are merged because '13+' held only 8 days and its
-- value bounced back up, which drew as a trend reversal beside a bucket of 299
-- days. Merged it is 51 days and the series is monotonic. The day count is
-- carried on the row so the chart can show what each point rests on.
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

grant select on public.v_eu_heatwave_response to anon;

-- ── Refresh helper: call after any heatwave/energy ingest ──────────────────
create or replace function public.refresh_heatwave_mvs()
returns void language plpgsql security definer set search_path = public as $$
begin
  refresh materialized view concurrently public.mv_load_daily_warm;
  refresh materialized view concurrently public.mv_price_daily_warm;
  refresh materialized view concurrently public.mv_renewable_daily_warm;
  refresh materialized view concurrently public.mv_generation_daily_warm;
end;
$$;
grant execute on function public.refresh_heatwave_mvs() to service_role;

grant select on public.mv_load_daily_warm      to anon;
grant select on public.mv_price_daily_warm     to anon;
grant select on public.mv_renewable_daily_warm to anon;
grant select on public.v_heatwave_load_daily   to anon;
grant select on public.v_heatwave_renewable    to anon;
grant select on public.v_heatwave_price        to anon;
grant select on public.v_eu_heatwave_daily     to anon;

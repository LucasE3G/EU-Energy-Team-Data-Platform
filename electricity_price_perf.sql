-- Aggregations for day-ahead prices

-- Per-zone daily average price (EUR/MWh)
create or replace view public.electricity_price_daily as
select
  zone_id,
  date_trunc('day', ts) as ts,
  avg(price_eur_per_mwh) as price_eur_per_mwh,
  min(price_eur_per_mwh) as price_min_eur_per_mwh,
  max(price_eur_per_mwh) as price_max_eur_per_mwh,
  count(*) as hours
from public.electricity_day_ahead_prices
where source = 'entsoe'
group by 1, 2;

-- Per-zone weekly average price (EUR/MWh)
create or replace view public.electricity_price_weekly as
select
  zone_id,
  date_trunc('week', ts) as ts,
  avg(price_eur_per_mwh) as price_eur_per_mwh,
  min(price_eur_per_mwh) as price_min_eur_per_mwh,
  max(price_eur_per_mwh) as price_max_eur_per_mwh,
  count(*) as hours
from public.electricity_day_ahead_prices
where source = 'entsoe'
group by 1, 2;

-- EU simple average price across zones per hour (unweighted)
create materialized view if not exists public.electricity_eu_price_hourly_mv as
with dedup as (
  select distinct on (zone_id, ts)
    zone_id, ts, price_eur_per_mwh
  from public.electricity_day_ahead_prices
  where source='entsoe'
  order by zone_id, ts, id desc
)
select
  ts,
  avg(price_eur_per_mwh) as price_eur_per_mwh,
  count(*) as zones
from dedup
group by 1;

create index if not exists electricity_eu_price_hourly_mv_ts_desc
  on public.electricity_eu_price_hourly_mv (ts desc);

-- EU daily avg from hourly MV
create materialized view if not exists public.electricity_eu_price_daily_mv as
select
  date_trunc('day', ts) as ts,
  avg(price_eur_per_mwh) as price_eur_per_mwh,
  min(price_eur_per_mwh) as price_min_eur_per_mwh,
  max(price_eur_per_mwh) as price_max_eur_per_mwh
from public.electricity_eu_price_hourly_mv
group by 1;

create index if not exists electricity_eu_price_daily_mv_ts_desc
  on public.electricity_eu_price_daily_mv (ts desc);

-- EU weekly avg from hourly MV
create materialized view if not exists public.electricity_eu_price_weekly_mv as
select
  date_trunc('week', ts) as ts,
  avg(price_eur_per_mwh) as price_eur_per_mwh,
  min(price_eur_per_mwh) as price_min_eur_per_mwh,
  max(price_eur_per_mwh) as price_max_eur_per_mwh
from public.electricity_eu_price_hourly_mv
group by 1;

create index if not exists electricity_eu_price_weekly_mv_ts_desc
  on public.electricity_eu_price_weekly_mv (ts desc);

-- Grants for public dashboard reads (views/MVs don't have RLS policies)
grant select on public.electricity_day_ahead_prices to anon;
grant select on public.electricity_price_daily to anon;
grant select on public.electricity_price_weekly to anon;
grant select on public.electricity_eu_price_hourly_mv to anon;
grant select on public.electricity_eu_price_daily_mv to anon;
grant select on public.electricity_eu_price_weekly_mv to anon;


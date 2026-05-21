-- Electricity Load (ENTSO-E) performance helpers + aggregates
-- Run in Supabase SQL editor after `electricity_load_schema.sql`.

create index if not exists electricity_load_snapshots_zone_source_ts_desc
  on public.electricity_load_snapshots (zone_id, source, ts desc);

-- Downsampled history views (for 6m/1y/5y charts without timeouts)
create or replace view public.electricity_load_daily as
select
  zone_id,
  source,
  date_trunc('day', ts) as ts,
  avg(load_mw)::double precision as load_mw
from public.electricity_load_snapshots
group by zone_id, source, date_trunc('day', ts);

create or replace view public.electricity_load_weekly as
select
  zone_id,
  source,
  date_trunc('week', ts) as ts,
  avg(load_mw)::double precision as load_mw
from public.electricity_load_snapshots
group by zone_id, source, date_trunc('week', ts);

-- EU aggregate from per-zone loads.
-- Two-step hourly aggregation: avg(load_mw) per zone per hour → sum across zones.
-- This avoids spikes from zones that report at 15-min or 30-min resolution being
-- summed at timestamps where only a subset of zones have data.
drop materialized view if exists public.electricity_eu_load_daily_mv cascade;
drop materialized view if exists public.electricity_eu_load_weekly_mv cascade;
drop materialized view if exists public.electricity_eu_load_15m_mv;
create materialized view public.electricity_eu_load_15m_mv as
with zone_hour as (
  select
    date_trunc('hour', ts) as ts,
    zone_id,
    avg(load_mw) as avg_mw
  from public.electricity_load_snapshots
  where source = 'entsoe'
    and zone_id <> 'EU'
    and load_mw is not null
    and load_mw > 0
  group by 1, 2
)
select ts, sum(avg_mw)::double precision as load_mw
from zone_hour
group by 1;

create index if not exists electricity_eu_load_15m_mv_ts_desc
  on public.electricity_eu_load_15m_mv (ts desc);

drop materialized view if exists public.electricity_eu_load_daily_mv;
create materialized view public.electricity_eu_load_daily_mv as
select
  date_trunc('day', ts) as ts,
  avg(load_mw)::double precision as load_mw
from public.electricity_eu_load_15m_mv
group by 1;

create index if not exists electricity_eu_load_daily_mv_ts_desc
  on public.electricity_eu_load_daily_mv (ts desc);

drop materialized view if exists public.electricity_eu_load_weekly_mv;
create materialized view public.electricity_eu_load_weekly_mv as
select
  date_trunc('week', ts) as ts,
  avg(load_mw)::double precision as load_mw
from public.electricity_eu_load_15m_mv
group by 1;

create index if not exists electricity_eu_load_weekly_mv_ts_desc
  on public.electricity_eu_load_weekly_mv (ts desc);

-- Allow anon reads (PostgREST) for public dashboard charts.
-- Tables have RLS policies; views/MVs need explicit grants.
grant select on public.electricity_load_snapshots to anon;
grant select on public.electricity_load_daily to anon;
grant select on public.electricity_load_weekly to anon;
grant select on public.electricity_eu_load_15m_mv to anon;
grant select on public.electricity_eu_load_daily_mv to anon;
grant select on public.electricity_eu_load_weekly_mv to anon;


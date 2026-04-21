-- EU total generation aggregates for the Electricity tab (fast day/week/month/6m/1y/5y charts).
--
-- IMPORTANT: build the EU aggregate from PER-ZONE snapshots (exclude zone_id='EU'),
-- exactly like renewable % does in `energy_meter_perf.sql`. This gives full
-- historical coverage as long as you have per-zone snapshots, and avoids
-- spikes caused by duplicate `EU` rows.
--
-- These MVs compute:
--   EU total MW = sum(raw.totalMw) across zones at each timestamp bucket
--
-- Run in Supabase SQL editor after edits. Refresh MVs daily alongside the
-- existing `energy_eu_*_mv` (renewable %) views.

drop materialized view if exists public.energy_eu_total_daily_mv cascade;
create materialized view public.energy_eu_total_daily_mv as
with per_15m as (
  select
    date_bin(interval '15 minutes', ts, '2000-01-01'::timestamptz) as ts_15m,
    sum(((coalesce(raw->>'totalMw', raw->>'total_mw'))::double precision)) as total_mw
  from public.energy_mix_snapshots
  where source = 'entsoe'
    and zone_id <> 'EU'
    and (raw ? 'totalMw' or raw ? 'total_mw')
  group by 1
)
select
  date_trunc('day', ts_15m) as ts,
  avg(total_mw) as total_mw
from per_15m
group by 1;

create index if not exists energy_eu_total_daily_mv_ts_desc
  on public.energy_eu_total_daily_mv (ts desc);

drop materialized view if exists public.energy_eu_total_weekly_mv cascade;
create materialized view public.energy_eu_total_weekly_mv as
with per_15m as (
  select
    date_bin(interval '15 minutes', ts, '2000-01-01'::timestamptz) as ts_15m,
    sum(((coalesce(raw->>'totalMw', raw->>'total_mw'))::double precision)) as total_mw
  from public.energy_mix_snapshots
  where source = 'entsoe'
    and zone_id <> 'EU'
    and (raw ? 'totalMw' or raw ? 'total_mw')
  group by 1
)
select
  date_trunc('week', ts_15m) as ts,
  avg(total_mw) as total_mw
from per_15m
group by 1;

create index if not exists energy_eu_total_weekly_mv_ts_desc
  on public.energy_eu_total_weekly_mv (ts desc);

drop materialized view if exists public.energy_eu_total_15m_mv cascade;
create materialized view public.energy_eu_total_15m_mv as
select
  date_bin(interval '15 minutes', ts, '2000-01-01'::timestamptz) as ts,
  sum(((coalesce(raw->>'totalMw', raw->>'total_mw'))::double precision)) as total_mw
from public.energy_mix_snapshots
where source = 'entsoe'
  and zone_id <> 'EU'
  and (raw ? 'totalMw' or raw ? 'total_mw')
group by 1;

create index if not exists energy_eu_total_15m_mv_ts_desc
  on public.energy_eu_total_15m_mv (ts desc);

-- Per-zone: average per (zone_id, source, day) after deduping per (zone_id, ts)
create or replace view public.energy_zone_total_daily as
with z as (
  select distinct on (zone_id, ts)
    zone_id,
    source,
    ts,
    (coalesce(raw->>'totalMw', raw->>'total_mw'))::double precision as total_mw
  from public.energy_mix_snapshots
  where source = 'entsoe'
    and zone_id <> 'EU'
    and (raw ? 'totalMw' or raw ? 'total_mw')
  order by zone_id asc, ts asc, id desc
)
select
  zone_id,
  source,
  date_trunc('day', ts) as ts,
  avg(total_mw) as total_mw
from z
group by zone_id, source, date_trunc('day', ts);

create or replace view public.energy_zone_total_weekly as
with z as (
  select distinct on (zone_id, ts)
    zone_id,
    source,
    ts,
    (coalesce(raw->>'totalMw', raw->>'total_mw'))::double precision as total_mw
  from public.energy_mix_snapshots
  where source = 'entsoe'
    and zone_id <> 'EU'
    and (raw ? 'totalMw' or raw ? 'total_mw')
  order by zone_id asc, ts asc, id desc
)
select
  zone_id,
  source,
  date_trunc('week', ts) as ts,
  avg(total_mw) as total_mw
from z
group by zone_id, source, date_trunc('week', ts);

-- After loading historical snapshots:
--   refresh materialized view concurrently public.energy_eu_total_daily_mv;
--   refresh materialized view concurrently public.energy_eu_total_weekly_mv;
--   refresh materialized view concurrently public.energy_eu_total_15m_mv;
-- (Add unique indexes first if you use CONCURRENTLY — see Postgres docs.)

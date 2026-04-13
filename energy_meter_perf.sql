-- Energy Meter performance helpers
-- Run in Supabase SQL editor.

-- Helps global "latest" queries
create index if not exists energy_mix_snapshots_ts_desc
  on public.energy_mix_snapshots (ts desc);

create index if not exists energy_mix_snapshots_source_ts_desc
  on public.energy_mix_snapshots (source, ts desc);

create index if not exists energy_mix_snapshots_zone_source_ts_desc
  on public.energy_mix_snapshots (zone_id, source, ts desc);

-- Latest row per zone (fast to query for the table)
create or replace view public.energy_mix_latest as
select distinct on (zone_id)
  id,
  zone_id,
  country_code,
  ts,
  renewable_percent,
  carbon_intensity_g_per_kwh,
  source
from public.energy_mix_snapshots
order by zone_id, ts desc;

-- Downsampled history views (for 6m/1y/5y charts without timeouts)
-- NOTE: These are simple views (computed on read). For very large datasets,
-- consider materializing + scheduled refresh, but this already fixes the
-- "can't go past 6 months" UI limitation.

create or replace view public.energy_mix_daily as
select
  zone_id,
  source,
  date_trunc('day', ts) as ts,
  avg(renewable_percent)::double precision as renewable_percent,
  avg(carbon_intensity_g_per_kwh)::double precision as carbon_intensity_g_per_kwh
from public.energy_mix_snapshots
group by zone_id, source, date_trunc('day', ts);

create or replace view public.energy_mix_weekly as
select
  zone_id,
  source,
  date_trunc('week', ts) as ts,
  avg(renewable_percent)::double precision as renewable_percent,
  avg(carbon_intensity_g_per_kwh)::double precision as carbon_intensity_g_per_kwh
from public.energy_mix_snapshots
group by zone_id, source, date_trunc('week', ts);

-- EU aggregate computed from per-zone MW totals (avoids needing historical EU rows)
-- Requires ENTSO-E ingester to store raw.totalMw and raw.renewableMw on per-zone rows.
create or replace view public.energy_eu_daily as
select
  date_trunc('day', ts) as ts,
  (sum(((coalesce(raw->>'renewableMw', raw->>'renewable_mw'))::double precision))
    / nullif(sum(((coalesce(raw->>'totalMw', raw->>'total_mw'))::double precision)), 0)) * 100
    as renewable_percent
from public.energy_mix_snapshots
where source = 'entsoe'
  and zone_id <> 'EU'
  and (raw ? 'totalMw' or raw ? 'total_mw')
  and (raw ? 'renewableMw' or raw ? 'renewable_mw')
group by date_trunc('day', ts);

create or replace view public.energy_eu_weekly as
select
  date_trunc('week', ts) as ts,
  (sum(((coalesce(raw->>'renewableMw', raw->>'renewable_mw'))::double precision))
    / nullif(sum(((coalesce(raw->>'totalMw', raw->>'total_mw'))::double precision)), 0)) * 100
    as renewable_percent
from public.energy_mix_snapshots
where source = 'entsoe'
  and zone_id <> 'EU'
  and (raw ? 'totalMw' or raw ? 'total_mw')
  and (raw ? 'renewableMw' or raw ? 'renewable_mw')
group by date_trunc('week', ts);

-- Materialized versions for fast chart queries (avoids statement timeouts)
-- Run these once after loading historical data, and refresh periodically (e.g. daily).
drop materialized view if exists public.energy_eu_daily_mv;
create materialized view public.energy_eu_daily_mv as
select
  date_trunc('day', ts) as ts,
  (sum(((coalesce(raw->>'renewableMw', raw->>'renewable_mw'))::double precision))
    / nullif(sum(((coalesce(raw->>'totalMw', raw->>'total_mw'))::double precision)), 0)) * 100
    as renewable_percent
from public.energy_mix_snapshots
where source = 'entsoe'
  and zone_id <> 'EU'
  and (raw ? 'totalMw' or raw ? 'total_mw')
  and (raw ? 'renewableMw' or raw ? 'renewable_mw')
group by date_trunc('day', ts);

create index if not exists energy_eu_daily_mv_ts_desc
  on public.energy_eu_daily_mv (ts desc);

drop materialized view if exists public.energy_eu_weekly_mv;
create materialized view public.energy_eu_weekly_mv as
select
  date_trunc('week', ts) as ts,
  (sum(((coalesce(raw->>'renewableMw', raw->>'renewable_mw'))::double precision))
    / nullif(sum(((coalesce(raw->>'totalMw', raw->>'total_mw'))::double precision)), 0)) * 100
    as renewable_percent
from public.energy_mix_snapshots
where source = 'entsoe'
  and zone_id <> 'EU'
  and (raw ? 'totalMw' or raw ? 'total_mw')
  and (raw ? 'renewableMw' or raw ? 'renewable_mw')
group by date_trunc('week', ts);

create index if not exists energy_eu_weekly_mv_ts_desc
  on public.energy_eu_weekly_mv (ts desc);

-- High-granularity EU aggregate (15-min buckets) for day/week/month charts
drop materialized view if exists public.energy_eu_15m_mv;
create materialized view public.energy_eu_15m_mv as
select
  date_bin(interval '15 minutes', ts, '2000-01-01'::timestamptz) as ts,
  (sum(((coalesce(raw->>'renewableMw', raw->>'renewable_mw'))::double precision))
    / nullif(sum(((coalesce(raw->>'totalMw', raw->>'total_mw'))::double precision)), 0)) * 100
    as renewable_percent
from public.energy_mix_snapshots
where source = 'entsoe'
  and zone_id <> 'EU'
  and (raw ? 'totalMw' or raw ? 'total_mw')
  and (raw ? 'renewableMw' or raw ? 'renewable_mw')
group by 1;

create index if not exists energy_eu_15m_mv_ts_desc
  on public.energy_eu_15m_mv (ts desc);


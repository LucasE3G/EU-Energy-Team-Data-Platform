-- Electricity generation by type: MWh materialized views for stacked charts.
-- Run in Supabase SQL editor after electricity_generation_schema.sql + backfill.
-- Uses avg(mw)*24 instead of lead() window functions — safe because ENTSO-E
-- publishes at uniform intervals (15min/30min/1h) so avg×24h = true daily MWh.
-- Dependency order: daily → weekly → eu_daily → eu_weekly.

drop materialized view if exists public.electricity_eu_generation_weekly_mwh;
drop materialized view if exists public.electricity_eu_generation_daily_mwh;
drop materialized view if exists public.electricity_generation_weekly_mwh;
drop materialized view if exists public.electricity_generation_daily_mwh;
drop materialized view if exists public.electricity_eu_generation_15m_mv;

-- ── EU 15-min aggregate by type (for "day" range charts) ─────────────────────
create materialized view public.electricity_eu_generation_15m_mv as
select
  date_bin(interval '15 minutes', ts, '2000-01-01'::timestamptz) as ts,
  psr_type,
  sum(mw)::double precision as mw
from public.electricity_generation_snapshots
where source = 'entsoe'
group by 1, 2;

create unique index on public.electricity_eu_generation_15m_mv (ts, psr_type);
create index on public.electricity_eu_generation_15m_mv (ts desc);

-- ── Per-zone daily production (MWh) by type ──────────────────────────────────
-- avg(mw) * 24 works for any resolution: 24 hourly readings, 96 quarter-hourly,
-- or 48 half-hourly all yield the same avg(mw)*24 = true MWh for the day.
create materialized view public.electricity_generation_daily_mwh as
select
  zone_id,
  source,
  psr_type,
  date_trunc('day', ts) as ts,
  (avg(mw) * 24.0)::double precision as production_mwh
from public.electricity_generation_snapshots
where mw >= 0
group by zone_id, source, psr_type, date_trunc('day', ts);

create unique index on public.electricity_generation_daily_mwh (zone_id, source, psr_type, ts);
create index on public.electricity_generation_daily_mwh (zone_id, ts desc);

-- ── Per-zone weekly production (MWh) by type ─────────────────────────────────
create materialized view public.electricity_generation_weekly_mwh as
select
  zone_id,
  source,
  psr_type,
  date_trunc('week', ts) as ts,
  sum(production_mwh)::double precision as production_mwh
from public.electricity_generation_daily_mwh
group by zone_id, source, psr_type, date_trunc('week', ts);

create unique index on public.electricity_generation_weekly_mwh (zone_id, source, psr_type, ts);
create index on public.electricity_generation_weekly_mwh (zone_id, ts desc);

-- ── EU aggregate daily production (MWh) by type ──────────────────────────────
create materialized view public.electricity_eu_generation_daily_mwh as
select
  psr_type,
  ts,
  sum(production_mwh)::double precision as production_mwh
from public.electricity_generation_daily_mwh
group by psr_type, ts;

create unique index on public.electricity_eu_generation_daily_mwh (psr_type, ts);
create index on public.electricity_eu_generation_daily_mwh (ts desc);

-- ── EU aggregate weekly production (MWh) by type ─────────────────────────────
create materialized view public.electricity_eu_generation_weekly_mwh as
select
  psr_type,
  ts,
  sum(production_mwh)::double precision as production_mwh
from public.electricity_generation_weekly_mwh
group by psr_type, ts;

create unique index on public.electricity_eu_generation_weekly_mwh (psr_type, ts);
create index on public.electricity_eu_generation_weekly_mwh (ts desc);

-- ── Refresh ───────────────────────────────────────────────────────────────────
-- refresh materialized view concurrently public.electricity_eu_generation_15m_mv;
-- refresh materialized view concurrently public.electricity_generation_daily_mwh;
-- refresh materialized view concurrently public.electricity_generation_weekly_mwh;
-- refresh materialized view concurrently public.electricity_eu_generation_daily_mwh;
-- refresh materialized view concurrently public.electricity_eu_generation_weekly_mwh;

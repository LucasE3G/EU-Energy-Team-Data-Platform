-- Electricity load: true daily/weekly consumption (MWh) materialized views
-- Run in Supabase SQL editor after electricity_load_schema.sql.
--
-- Uses avg(load_mw)*24 instead of lead() window functions — safe because ENTSO-E
-- A65 publishes at uniform intervals (1h typically, sometimes 15min/30min) so
-- avg×24h = true daily MWh. Avoids sorting the full table and refreshes quickly.

-- Drop in reverse-dependency order before recreating
drop materialized view if exists public.electricity_eu_load_weekly_mwh;
drop materialized view if exists public.electricity_eu_load_daily_mwh;
drop materialized view if exists public.electricity_load_weekly_mwh;
drop materialized view if exists public.electricity_load_daily_mwh;

-- ── Per-zone: daily consumption (MWh) ────────────────────────────────────────
create materialized view public.electricity_load_daily_mwh as
select
  zone_id,
  source,
  date_trunc('day', ts) as ts,
  (avg(load_mw) * 24.0)::double precision as consumption_mwh
from public.electricity_load_snapshots
where load_mw is not null
  and load_mw > 0
group by zone_id, source, date_trunc('day', ts);

create unique index on public.electricity_load_daily_mwh (zone_id, source, ts);
create index on public.electricity_load_daily_mwh (zone_id, ts desc);

-- ── Per-zone: weekly consumption (MWh) ───────────────────────────────────────
create materialized view public.electricity_load_weekly_mwh as
select
  zone_id,
  source,
  date_trunc('week', ts) as ts,
  sum(consumption_mwh)::double precision as consumption_mwh
from public.electricity_load_daily_mwh
group by zone_id, source, date_trunc('week', ts);

create unique index on public.electricity_load_weekly_mwh (zone_id, source, ts);
create index on public.electricity_load_weekly_mwh (zone_id, ts desc);

-- ── EU aggregate: daily consumption (MWh) ────────────────────────────────────
create materialized view public.electricity_eu_load_daily_mwh as
select
  ts,
  sum(consumption_mwh)::double precision as consumption_mwh
from public.electricity_load_daily_mwh
group by ts;

create unique index on public.electricity_eu_load_daily_mwh (ts);

-- ── EU aggregate: weekly consumption (MWh) ───────────────────────────────────
create materialized view public.electricity_eu_load_weekly_mwh as
select
  ts,
  sum(consumption_mwh)::double precision as consumption_mwh
from public.electricity_load_weekly_mwh
group by ts;

create unique index on public.electricity_eu_load_weekly_mwh (ts);

-- ── Refresh (run after new load data is ingested) ────────────────────────────
-- refresh materialized view concurrently public.electricity_load_daily_mwh;
-- refresh materialized view concurrently public.electricity_load_weekly_mwh;
-- refresh materialized view concurrently public.electricity_eu_load_daily_mwh;
-- refresh materialized view concurrently public.electricity_eu_load_weekly_mwh;

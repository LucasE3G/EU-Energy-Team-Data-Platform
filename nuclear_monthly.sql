-- Monthly nuclear generation per country, for the 2026-against-range chart.
--
-- Monthly energy is built from DAILY means (average MW that day x 24 h) rather
-- than from raw readings, so a day with partial reporting contributes its
-- average level instead of a short total. `days` is exposed so consumers can
-- drop incomplete months - the month in progress, and April 2021 where the
-- record begins mid-month.
--
-- Materialized, not a plain view: aggregating five years of quarter-hourly
-- snapshots takes long enough to blow the anon role's statement timeout, which
-- is a 500 at the API rather than a slow chart.

set statement_timeout = '900s';

drop view if exists public.v_nuclear_monthly cascade;
drop materialized view if exists public.mv_nuclear_monthly cascade;

create materialized view public.mv_nuclear_monthly as
with daily as (
  select zone_id as country_code,
         (ts at time zone 'UTC')::date as date,
         avg(mw) * 24.0 / 1000.0       as gwh
  from public.electricity_generation_snapshots
  where psr_type = 'B14'
    and length(zone_id) = 2
    and mw is not null
  group by 1, 2
)
select country_code,
       extract(year from date)::int  as year,
       extract(month from date)::int as month,
       round((sum(gwh) / 1000.0)::numeric, 4) as twh,
       count(*)::int                          as days
from daily
group by 1, 2, 3;

create unique index if not exists mv_nuclear_monthly_key
  on public.mv_nuclear_monthly (country_code, year, month);

create view public.v_nuclear_monthly as
select * from public.mv_nuclear_monthly;

grant select on public.mv_nuclear_monthly to anon;
grant select on public.v_nuclear_monthly  to anon;

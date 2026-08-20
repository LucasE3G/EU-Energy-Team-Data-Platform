-- Daily nuclear generation per country, for the zoomed summer chart.
--
-- Daily energy is the day's mean output times 24 h, so a day with partial
-- reporting contributes its average level rather than a short total. `samples`
-- is exposed so consumers can drop thin days.
--
-- Materialized for the same reason as the monthly view: aggregating five years
-- of quarter-hourly snapshots on demand blows the anon statement timeout.

set statement_timeout = '900s';

drop view if exists public.v_nuclear_daily cascade;
drop materialized view if exists public.mv_nuclear_daily cascade;

create materialized view public.mv_nuclear_daily as
select zone_id                                  as country_code,
       (ts at time zone 'UTC')::date            as date,
       round((avg(mw) * 24.0 / 1000.0)::numeric, 3) as gwh,
       round(avg(mw)::numeric, 1)                   as mean_mw,
       count(*)::int                                as samples
from public.electricity_generation_snapshots
where psr_type = 'B14'
  and length(zone_id) = 2
  and mw is not null
group by 1, 2;

create unique index if not exists mv_nuclear_daily_key
  on public.mv_nuclear_daily (country_code, date);

create view public.v_nuclear_daily as
select * from public.mv_nuclear_daily;

create or replace function public.refresh_nuclear_mvs()
returns void language plpgsql security definer set search_path = public as $$
begin
  refresh materialized view concurrently public.mv_nuclear_daily;
  refresh materialized view concurrently public.mv_nuclear_monthly;
end;
$$;
grant execute on function public.refresh_nuclear_mvs() to service_role;

grant select on public.mv_nuclear_daily to anon;
grant select on public.v_nuclear_daily  to anon;

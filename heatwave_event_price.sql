-- Day-ahead price by hour on exactly the days behind mv_hw_event_profile, so a
-- price panel can sit under the dispatch panel and describe the same hours.
--
-- Each country's own heatwave days inside 28 July - 12 August 2026, local hour.

set statement_timeout = '900s';

drop materialized view if exists public.mv_hw_event_price cascade;
create materialized view public.mv_hw_event_price as
with params as (select date '2026-07-28' as d0, date '2026-08-12' as d1),
tz as (
  select * from (values
    ('FR','Europe/Paris'), ('IT','Europe/Rome'), ('HU','Europe/Budapest'),
    ('RO','Europe/Bucharest'), ('GR','Europe/Athens'), ('ES','Europe/Madrid'),
    ('DE','Europe/Berlin'), ('PL','Europe/Warsaw'), ('BE','Europe/Brussels')
  ) as t(country_code, zone)
),
days as (
  select w.country_code, w.date
  from public.weather_country_daily w, params p
  where w.country_code in (select country_code from tz)
    and w.date between p.d0 and p.d1
    and w.heatwave_id is not null
),
px as (
  select q.zone_id as country_code,
         (q.ts at time zone t.zone)::date                 as date,
         extract(hour from q.ts at time zone t.zone)::int  as hour,
         avg(q.price_eur_per_mwh) as eur
  from public.electricity_day_ahead_prices q
  join tz t on t.country_code = q.zone_id
  join params p on true
  where q.ts >= p.d0 and q.ts < p.d1 + 1 and q.price_eur_per_mwh is not null
  group by 1, 2, 3
)
select px.country_code, px.hour,
       count(*)                        as days,
       round(avg(px.eur)::numeric, 1)  as eur,
       round(min(px.eur)::numeric, 1)  as eur_min,
       round(max(px.eur)::numeric, 1)  as eur_max
from px
join days d on d.country_code = px.country_code and d.date = px.date
group by 1, 2;

create unique index if not exists mv_hw_event_price_key
  on public.mv_hw_event_price (country_code, hour);

create view public.v_hw_event_price as select * from public.mv_hw_event_price;

grant select on public.mv_hw_event_price to anon;
grant select on public.v_hw_event_price  to anon;

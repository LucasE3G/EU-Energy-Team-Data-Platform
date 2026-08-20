-- The merit-order effect: what solar does to the day-ahead price, by hour.
--
-- A daily average hides this entirely. Solar's effect on price is a MIDDAY
-- phenomenon - it displaces the marginal unit for six hours and then stops -
-- so the only way to see it is to keep the hours apart.
--
-- Hours are local (Europe/Berlin): Germany, Spain and Italy all sit on CEST
-- through the warm season, so their clocks agree and midday means midday.
--
-- Two scopes are emitted per country:
--   'heatwave' - that country's own heatwave days
--   'all'      - every warm-season day, which serves as the control. The
--                pattern below is not an artefact of the three countries having
--                different heatwave dates: on identical calendar days Germany
--                and Italy price within 20% of each other at 03:00 and 21:00,
--                and differ 3-5x at 13:00.

set statement_timeout = '900s';

drop materialized view if exists public.mv_solar_price_intraday cascade;
create materialized view public.mv_solar_price_intraday as
with px as (
  select zone_id as country_code,
         (ts at time zone 'Europe/Berlin')::date              as date,
         extract(hour from ts at time zone 'Europe/Berlin')::int as hour,
         avg(price_eur_per_mwh) as eur
  from public.electricity_day_ahead_prices
  where ts >= date_trunc('year', current_date)
    and length(zone_id) = 2
    and price_eur_per_mwh is not null
  group by 1, 2, 3
),
gen as (
  select zone_id as country_code,
         (ts at time zone 'Europe/Berlin')::date              as date,
         extract(hour from ts at time zone 'Europe/Berlin')::int as hour,
         sum(mw) filter (where psr_type = 'B16') as solar_mw,
         sum(mw)                                 as total_mw
  from public.electricity_generation_snapshots
  where ts >= date_trunc('year', current_date)
    and length(zone_id) = 2
  group by 1, 2, 3
),
j as (
  select p.country_code, p.date, p.hour, p.eur,
         g.solar_mw, g.total_mw,
         (w.heatwave_id is not null) as hw
  from px p
  join gen g on g.country_code = p.country_code and g.date = p.date and g.hour = p.hour
  join public.weather_country_daily w
    on w.country_code = p.country_code and w.date = p.date
  where extract(month from p.date) between 5 and 9
    and g.total_mw > 0
),
scoped as (
  select country_code, hour, 'heatwave' as scope, eur, solar_mw, total_mw, date from j where hw
  union all
  select country_code, hour, 'all',              eur, solar_mw, total_mw, date from j
)
select country_code, scope, hour,
       count(distinct date)                                          as days,
       round(avg(eur)::numeric, 1)                                   as price_eur,
       -- Share from summed megawatts, not a mean of hourly ratios: a low-output
       -- hour must not weigh the same as a high-output one.
       round((100.0 * sum(solar_mw) / nullif(sum(total_mw), 0))::numeric, 1) as solar_share_pct
from scoped
group by 1, 2, 3
having count(distinct date) >= 5;

create unique index if not exists mv_solar_price_intraday_key
  on public.mv_solar_price_intraday (country_code, scope, hour);

create view public.v_solar_price_intraday as
select * from public.mv_solar_price_intraday;

grant select on public.mv_solar_price_intraday to anon;
grant select on public.v_solar_price_intraday  to anon;

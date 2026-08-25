-- Hourly demand on NON-heatwave days, for the dotted reference line on the
-- event profile chart.
--
-- mv_hw_event_profile covers heatwave days inside 28 July - 12 August 2026.
-- mv_hw_window_profile covers 1 July - 15 August but averages ALL days in it,
-- heatwave days included, so it cannot serve as a baseline for them.
--
-- The comparison set is the one every other figure in this analysis uses:
-- non-heatwave days from the same May-September season. Weekdays and weekends
-- are averaged separately and then weighted back together using the mix of the
-- heatwave days actually being drawn, so the line is not a working week set
-- against an average that is two sevenths weekend. The mix differs by country -
-- 3 of France's 8 heatwave days fall at a weekend against 3 of Romania's 13 -
-- which is exactly why it is computed here rather than assumed.
create or replace view public.v_hw_event_baseline_demand as
with tz(country_code, zone) as (
  values ('FR','Europe/Paris'), ('IT','Europe/Rome'), ('HU','Europe/Budapest'),
         ('RO','Europe/Bucharest'), ('GR','Europe/Athens')
),
event_mix as (
  -- The same filter mv_hw_event_profile applies, so the weighting describes the
  -- days the solid line is drawn from.
  select w.country_code,
         avg(case when extract(isodow from w.date) >= 6 then 1.0 else 0.0 end)
           as weekend_share,
         count(*) as event_days
  from public.weather_country_daily w
  where w.date between '2026-07-28' and '2026-08-12'
    and w.heatwave_id is not null
  group by 1
),
days as (
  select w.country_code, w.date
  from public.weather_country_daily w
  join tz on tz.country_code = w.country_code
  where w.date >= '2026-05-01' and w.date <= '2026-09-30'
    and w.heatwave_id is null
),
load_hourly as (
  select l.zone_id as country_code,
         (l.ts at time zone t.zone)::date as date,
         extract(hour from (l.ts at time zone t.zone))::int as hour,
         avg(l.load_mw) as mw
  from public.electricity_load_snapshots l
  join tz t on t.country_code = l.zone_id
  where l.ts >= '2026-05-01' and l.ts < '2026-10-01'
    and l.load_mw is not null
  group by 1, 2, 3
),
by_type as (
  select lh.country_code,
         lh.hour,
         (extract(isodow from lh.date) >= 6) as is_weekend,
         avg(lh.mw) as mw,
         count(*)   as obs
  from load_hourly lh
  join days d on d.country_code = lh.country_code and d.date = lh.date
  group by 1, 2, 3
)
select b.country_code,
       b.hour,
       sum(b.obs)      as obs,
       m.event_days,
       round((sum(b.mw * case when b.is_weekend then m.weekend_share
                              else 1 - m.weekend_share end)
              / nullif(sum(case when b.is_weekend then m.weekend_share
                                else 1 - m.weekend_share end), 0)
              / 1000.0)::numeric, 3) as gwh
from by_type b
join event_mix m on m.country_code = b.country_code
group by 1, 2, 4;

grant select on public.v_hw_event_baseline_demand to anon, authenticated;

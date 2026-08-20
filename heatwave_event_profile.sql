-- Average hourly demand and generation by source during a named heatwave.
--
-- Built to match the Ember small-multiples format: one panel per country, hour
-- of day on x, GWh on y, generation stacked by source with demand as a line.
--
-- Window: 28 July - 12 August 2026, restricted to each country's own heatwave
-- days inside it (GR 16 of 16, IT 16, HU 14, RO 13, FR 8).
--
-- Hours are LOCAL. Romania and Greece run EEST (UTC+3) while France, Italy and
-- Hungary run CEST (UTC+2); using one clock would shift the Balkan solar peak
-- an hour against the others and make the panels lie about midday.
--
-- Storage here is DISCHARGE ONLY. ENTSO-E publishes pumping as a separate
-- consumption series we do not ingest, so unlike Ember's version the storage
-- band never goes negative. Hungary and Romania report no pumped storage at
-- all, so they have no storage band - the same gap Ember footnotes.

set statement_timeout = '900s';

drop materialized view if exists public.mv_hw_event_profile cascade;
create materialized view public.mv_hw_event_profile as
with params as (
  select date '2026-07-28' as d0, date '2026-08-12' as d1
),
tz as (
  select * from (values
    ('FR','Europe/Paris'), ('IT','Europe/Rome'), ('HU','Europe/Budapest'),
    ('RO','Europe/Bucharest'), ('GR','Europe/Athens')
  ) as t(country_code, zone)
),
days as (
  -- The event days: heatwave days for that country inside the window.
  select w.country_code, w.date
  from public.weather_country_daily w, params p
  where w.country_code in (select country_code from tz)
    and w.date between p.d0 and p.d1
    and w.heatwave_id is not null
),
gen as (
  select g.zone_id as country_code,
         (g.ts at time zone t.zone)::date            as date,
         extract(hour from g.ts at time zone t.zone)::int as hour,
         case
           when g.psr_type = 'B16' then 'solar'
           when g.psr_type = 'B10' then 'storage'
           when g.psr_type = 'B14' then 'nuclear'
           when g.psr_type in ('B01','B09','B11','B12','B13','B15','B17','B18','B19','B25')
                then 'other_renewables'
           else 'fossil'
         end as category,
         g.mw, g.ts
  from public.electricity_generation_snapshots g
  join tz t on t.country_code = g.zone_id
  join params p on true
  where g.ts >= p.d0 and g.ts < p.d1 + 1
),
-- Sum across psr_types AT EACH TIMESTAMP first, then average over the hour.
-- Averaging the raw rows instead would divide every multi-type category by its
-- number of types: 'fossil' holds five codes, so Italian fossil came out five
-- times too small and supply fell 29 GWh short of demand at the evening peak.
gen_per_ts as (
  select country_code, date, hour, category, ts, sum(mw) as mw
  from gen group by 1, 2, 3, 4, 5
),
gen_hourly as (
  select country_code, date, hour, category, avg(mw) as mw
  from gen_per_ts group by 1, 2, 3, 4
),
-- Net imports from directed physical flows: what came in minus what went out.
--
-- Borders must be collapsed to the hour SEPARATELY before they are summed.
-- Reporting resolution varies by border - Italy's Swiss interconnector posts
-- hourly while its French and Austrian ones post every 15 minutes - so summing
-- at the raw timestamp drops any border with no row at that instant. That cost
-- Italy 2.45 GW of Swiss imports on three of every four timestamps and opened a
-- false gap between generation and demand.
flow_border_ts as (
  select t.country_code,
         case when f.from_zone = t.country_code then f.to_zone else f.from_zone end as neighbour,
         (f.ts at time zone t.zone)::date                 as date,
         extract(hour from f.ts at time zone t.zone)::int as hour,
         f.ts,
         sum(case when f.to_zone   = t.country_code then f.mw else 0 end)
       - sum(case when f.from_zone = t.country_code then f.mw else 0 end) as net_mw
  from public.electricity_crossborder_flows f
  join tz t on t.country_code in (f.from_zone, f.to_zone)
  join params p on true
  where f.ts >= p.d0 and f.ts < p.d1 + 1 and f.mw is not null
  group by 1, 2, 3, 4, 5
),
flow_border_hour as (
  select country_code, neighbour, date, hour, avg(net_mw) as net_mw
  from flow_border_ts group by 1, 2, 3, 4
),
flow_hourly as (
  select country_code, date, hour, 'net_import' as category, sum(net_mw) as mw
  from flow_border_hour group by 1, 2, 3
),
load_hourly as (
  select l.zone_id as country_code,
         (l.ts at time zone t.zone)::date            as date,
         extract(hour from l.ts at time zone t.zone)::int as hour,
         avg(l.load_mw) as mw
  from public.electricity_load_snapshots l
  join tz t on t.country_code = l.zone_id
  join params p on true
  where l.ts >= p.d0 and l.ts < p.d1 + 1 and l.load_mw is not null
  group by 1, 2, 3
),
combined as (
  select g.country_code, g.hour, g.category, g.mw
  from gen_hourly g join days d on d.country_code = g.country_code and d.date = g.date
  union all
  select f.country_code, f.hour, f.category, f.mw
  from flow_hourly f join days d on d.country_code = f.country_code and d.date = f.date
  union all
  select l.country_code, l.hour, 'demand', l.mw
  from load_hourly l join days d on d.country_code = l.country_code and d.date = l.date
)
select country_code, hour, category,
       count(*)                                   as obs,
       round((avg(mw) / 1000.0)::numeric, 3)      as gwh,
       round(avg(mw)::numeric, 0)                 as mw
from combined
group by 1, 2, 3;

create unique index if not exists mv_hw_event_profile_key
  on public.mv_hw_event_profile (country_code, hour, category);

create view public.v_hw_event_profile as
select * from public.mv_hw_event_profile;

grant select on public.mv_hw_event_profile to anon;
grant select on public.v_hw_event_profile  to anon;

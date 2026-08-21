-- Average hourly demand and generation by source over a fixed calendar window,
-- 1 July - 15 August 2026.
--
-- The sibling of mv_hw_event_profile, which averages over each country's own
-- HEATWAVE days. This one takes EVERY day in the window, so it lines up exactly
-- with the nuclear charts built on the same dates and the two can be read side
-- by side.
--
-- Hours are local: Romania and Greece run EEST while the rest run CEST, and one
-- shared clock would shift their solar peak against everyone else's.
--
-- Two aggregation traps, both already paid for elsewhere in this schema:
--   * generation is summed across psr_types AT EACH TIMESTAMP before being
--     averaged over the hour; averaging the raw rows divides every multi-type
--     category by its number of types.
--   * each border is collapsed to the hour SEPARATELY before the borders are
--     summed, because reporting resolution differs by border - Italy's Swiss
--     link posts hourly while its French one posts every fifteen minutes, and
--     summing at the raw timestamp silently drops whichever is absent.

set statement_timeout = '900s';

drop materialized view if exists public.mv_hw_window_profile cascade;
create materialized view public.mv_hw_window_profile as
with params as (
  select date '2026-07-01' as d0, date '2026-08-15' as d1
),
tz as (
  select * from (values
    ('FR','Europe/Paris'), ('IT','Europe/Rome'), ('HU','Europe/Budapest'),
    ('RO','Europe/Bucharest'), ('GR','Europe/Athens'), ('ES','Europe/Madrid'),
    ('DE','Europe/Berlin'), ('PL','Europe/Warsaw'), ('BG','Europe/Sofia'),
    ('CZ','Europe/Prague'), ('SK','Europe/Bratislava'), ('AT','Europe/Vienna'),
    ('HR','Europe/Zagreb'), ('SI','Europe/Ljubljana'), ('BE','Europe/Brussels'),
    ('PT','Europe/Lisbon'), ('CH','Europe/Zurich')
  ) as t(country_code, zone)
),
gen as (
  select g.zone_id as country_code,
         (g.ts at time zone t.zone)::date                 as date,
         extract(hour from g.ts at time zone t.zone)::int  as hour,
         g.ts,
         case
           when g.psr_type = 'B16' then 'solar'
           when g.psr_type = 'B10' then 'storage'
           when g.psr_type = 'B14' then 'nuclear'
           when g.psr_type in ('B01','B09','B11','B12','B13','B15','B17','B18','B19','B25')
                then 'other_renewables'
           else 'fossil'
         end as category,
         g.mw
  from public.electricity_generation_snapshots g
  join tz t on t.country_code = g.zone_id
  join params p on true
  where g.ts >= p.d0 and g.ts < p.d1 + 1
),
gen_per_ts as (
  select country_code, date, hour, category, ts, sum(mw) as mw
  from gen group by 1, 2, 3, 4, 5
),
gen_hourly as (
  select country_code, date, hour, category, avg(mw) as mw
  from gen_per_ts group by 1, 2, 3, 4
),
flow_border_ts as (
  select t.country_code,
         case when f.from_zone = t.country_code then f.to_zone else f.from_zone end as neighbour,
         (f.ts at time zone t.zone)::date                 as date,
         extract(hour from f.ts at time zone t.zone)::int  as hour,
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
         (l.ts at time zone t.zone)::date                 as date,
         extract(hour from l.ts at time zone t.zone)::int  as hour,
         avg(l.load_mw) as mw
  from public.electricity_load_snapshots l
  join tz t on t.country_code = l.zone_id
  join params p on true
  where l.ts >= p.d0 and l.ts < p.d1 + 1 and l.load_mw is not null
  group by 1, 2, 3
),
combined as (
  select country_code, hour, category, mw from gen_hourly
  union all
  select country_code, hour, category, mw from flow_hourly
  union all
  select country_code, hour, 'demand', mw from load_hourly
)
select country_code, hour, category,
       count(*)                              as obs,
       round((avg(mw) / 1000.0)::numeric, 3) as gwh,
       round(avg(mw)::numeric, 0)            as mw
from combined
group by 1, 2, 3;

create unique index if not exists mv_hw_window_profile_key
  on public.mv_hw_window_profile (country_code, hour, category);

create view public.v_hw_window_profile as
select * from public.mv_hw_window_profile;

grant select on public.mv_hw_window_profile to anon;
grant select on public.v_hw_window_profile  to anon;

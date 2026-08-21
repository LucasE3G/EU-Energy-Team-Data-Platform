-- Daily energy by component for the outage waterfall.
--
-- Runs to 31 August so the Romanian shutdown, which begins on 14 August, has
-- more than the two days that fit inside the 1 July - 15 August window.
--
-- Same two aggregation rules as everywhere else in this schema: generation is
-- summed across psr_types at each timestamp before being averaged over the day,
-- and each border is collapsed to the day separately before the borders are
-- summed, because reporting resolution differs by border.

set statement_timeout = '900s';

drop materialized view if exists public.mv_outage_daily cascade;
create materialized view public.mv_outage_daily as
with params as (select date '2026-07-01' as d0, date '2026-08-31' as d1),
cc as (select unnest(array['HU','RO']) as country_code),
gen_ts as (
  select g.zone_id as country_code,
         (g.ts at time zone 'UTC')::date as date,
         g.ts,
         case
           when g.psr_type = 'B16' then 'solar'
           when g.psr_type = 'B14' then 'nuclear'
           when g.psr_type in ('B10','B11','B12') then 'hydro'
           when g.psr_type in ('B18','B19') then 'wind'
           when g.psr_type in ('B01','B09','B13','B15','B17','B25') then 'other_renewables'
           else 'fossil'
         end as category,
         sum(g.mw) as mw
  from public.electricity_generation_snapshots g
  join cc on cc.country_code = g.zone_id
  join params p on true
  where g.ts >= p.d0 and g.ts < p.d1 + 1
  group by 1, 2, 3, 4
),
gen_daily as (
  select country_code, date, category, avg(mw) * 24.0 / 1000.0 as gwh
  from gen_ts group by 1, 2, 3
),
flow_border_ts as (
  select cc.country_code,
         case when f.from_zone = cc.country_code then f.to_zone else f.from_zone end as neighbour,
         (f.ts at time zone 'UTC')::date as date, f.ts,
         sum(case when f.to_zone   = cc.country_code then f.mw else 0 end)
       - sum(case when f.from_zone = cc.country_code then f.mw else 0 end) as net_mw
  from public.electricity_crossborder_flows f
  join cc on cc.country_code in (f.from_zone, f.to_zone)
  join params p on true
  where f.ts >= p.d0 and f.ts < p.d1 + 1 and f.mw is not null
  group by 1, 2, 3, 4
),
flow_daily as (
  select country_code, date, 'net_import' as category,
         sum(net_mw) * 24.0 / 1000.0 as gwh
  from (select country_code, neighbour, date, avg(net_mw) as net_mw
        from flow_border_ts group by 1, 2, 3) t
  group by 1, 2
),
load_daily as (
  select l.zone_id as country_code, (l.ts at time zone 'UTC')::date as date,
         'demand' as category, avg(l.load_mw) * 24.0 / 1000.0 as gwh
  from public.electricity_load_snapshots l
  join cc on cc.country_code = l.zone_id
  join params p on true
  where l.ts >= p.d0 and l.ts < p.d1 + 1 and l.load_mw is not null
  group by 1, 2
)
select country_code, date, category, round(gwh::numeric, 3) as gwh
from (
  select * from gen_daily
  union all select * from flow_daily
  union all select * from load_daily
) u;

create unique index if not exists mv_outage_daily_key
  on public.mv_outage_daily (country_code, date, category);

create view public.v_outage_daily as select * from public.mv_outage_daily;

grant select on public.mv_outage_daily to anon;
grant select on public.v_outage_daily  to anon;

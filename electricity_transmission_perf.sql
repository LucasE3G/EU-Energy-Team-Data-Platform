-- Aggregations for cross-border flows

-- Net imports per zone per timestamp (MW)
create or replace view public.electricity_net_imports_mw as
with flows as (
  select ts, from_zone, to_zone, mw
  from public.electricity_crossborder_flows
  where source='entsoe' and mw is not null
),
imports as (
  select ts, to_zone as zone_id, sum(mw) as mw
  from flows
  group by 1, 2
),
exports as (
  select ts, from_zone as zone_id, sum(mw) as mw
  from flows
  group by 1, 2
)
select
  coalesce(i.ts, e.ts) as ts,
  coalesce(i.zone_id, e.zone_id) as zone_id,
  coalesce(i.mw, 0) - coalesce(e.mw, 0) as net_mw
from imports i
full outer join exports e
  on i.ts = e.ts and i.zone_id = e.zone_id;

-- Daily net imports (MWh/day) using average net MW * 24
create or replace view public.electricity_net_imports_daily_mwh as
select
  zone_id,
  date_trunc('day', ts) as ts,
  avg(net_mw) * 24.0 as net_mwh
from public.electricity_net_imports_mw
group by 1, 2;

-- Weekly net imports (MWh/week) using avg net MW * 24 * 7
create or replace view public.electricity_net_imports_weekly_mwh as
select
  zone_id,
  date_trunc('week', ts) as ts,
  avg(net_mw) * 24.0 * 7.0 as net_mwh
from public.electricity_net_imports_mw
group by 1, 2;

grant select on public.electricity_crossborder_flows to anon;
grant select on public.electricity_net_imports_mw to anon;
grant select on public.electricity_net_imports_daily_mwh to anon;
grant select on public.electricity_net_imports_weekly_mwh to anon;


-- Who helped whom across interconnectors during heatwaves.
-- Requires: electricity_crossborder_flows populated (see
-- python/electricity_crossborder_backfill_entsoe.py) and weather_country_daily.
--
-- The central distinction this encodes: heatwaves are regional, so a neighbour
-- is very often inside the SAME heatwave. Power flowing between two stressed
-- systems is competition for scarce supply, not assistance. Every result is
-- therefore split by whether the exporter was itself in a heatwave:
--
--   'spare capacity' - exporter NOT in a heatwave -> genuine help
--   'shared stress'  - exporter also in a heatwave -> contested supply
--
-- Everything is also measured against a same-month baseline of days when
-- NEITHER country was in a heatwave. A country that always exports 1 GWh/day
-- to its neighbour is not helping more just because it is hot; only the
-- deviation from its normal pattern is evidence of anything.

-- ── Daily net energy per directed country pair ──────────────────────────────
-- Materialized: the source table holds tens of millions of 15-minute rows,
-- while this is daily-grain and small (~80 pairs x ~2000 days).
drop materialized view if exists public.mv_crossborder_net_daily cascade;
create materialized view public.mv_crossborder_net_daily as
with zone_daily as (
  select
    from_zone, to_zone,
    (ts at time zone 'UTC')::date as date,
    sum(mw)   as sum_mw,
    count(*)  as points
  from public.electricity_crossborder_flows
  where source = 'entsoe' and mw is not null
  group by 1, 2, 3
),
-- Interval length is self-calibrated per border-day from the union of
-- timestamps across BOTH directions. Borders report at 15, 30 or 60 minutes
-- and the resolution is not stored on the row, so assuming one would silently
-- scale some borders by 4x.
border_slots as (
  select
    least(from_zone, to_zone)    as z1,
    greatest(from_zone, to_zone) as z2,
    (ts at time zone 'UTC')::date as date,
    count(distinct ts)            as slots
  from public.electricity_crossborder_flows
  where source = 'entsoe'
  group by 1, 2, 3
),
zone_energy as (
  select
    d.from_zone, d.to_zone, d.date,
    -- Absent intervals are genuine zeros (ENTSO-E publishes a direction only
    -- while flow runs that way), so dividing the day into `slots` and summing
    -- the reported MW is the correct energy, not an average over present rows.
    d.sum_mw * (24.0 / nullif(b.slots, 0)) / 1000.0 as gwh
  from zone_daily d
  join border_slots b
    on b.z1 = least(d.from_zone, d.to_zone)
   and b.z2 = greatest(d.from_zone, d.to_zone)
   and b.date = d.date
),
country_energy as (
  select
    case when from_zone ~ '^(DK|NO|SE)[0-9]$' then left(from_zone, 2) else from_zone end as from_country,
    case when to_zone   ~ '^(DK|NO|SE)[0-9]$' then left(to_zone, 2)   else to_zone   end as to_country,
    date,
    sum(gwh) as gwh
  from zone_energy
  group by 1, 2, 3
)
select
  a.from_country,
  a.to_country,
  a.date,
  a.gwh                              as gross_export_gwh,
  coalesce(b.gwh, 0)                 as gross_import_gwh,
  a.gwh - coalesce(b.gwh, 0)         as net_export_gwh
from country_energy a
left join country_energy b
  on b.from_country = a.to_country
 and b.to_country   = a.from_country
 and b.date         = a.date
where a.from_country <> a.to_country;

create unique index if not exists mv_crossborder_net_daily_key
  on public.mv_crossborder_net_daily (from_country, to_country, date);
create index if not exists mv_crossborder_net_daily_date
  on public.mv_crossborder_net_daily (date desc);

-- ── Every day a country was helped while in a heatwave ──────────────────────
create or replace view public.v_heatwave_help_events as
select
  n.date,
  n.from_country                              as helper,
  n.to_country                                as beneficiary,
  round(n.net_export_gwh::numeric, 2)         as net_help_gwh,
  round(wb.tmax_c::numeric, 1)                as beneficiary_tmax_c,
  round(wb.anomaly_c::numeric, 1)             as beneficiary_anomaly_c,
  wb.heatwave_day,
  wb.heatwave_length,
  (wh.heatwave_id is not null)                as helper_also_in_heatwave,
  case when wh.heatwave_id is not null then 'shared stress' else 'spare capacity' end as help_type
from public.mv_crossborder_net_daily n
join public.weather_country_daily wb
  on wb.country_code = n.to_country and wb.date = n.date
left join public.weather_country_daily wh
  on wh.country_code = n.from_country and wh.date = n.date
where wb.heatwave_id is not null
  and n.net_export_gwh > 0;

-- ── Pairwise: how much MORE than normal did the helper send? ────────────────
create or replace view public.v_heatwave_help_pairs as
with classified as (
  select
    n.from_country as helper,
    n.to_country   as beneficiary,
    n.date,
    n.net_export_gwh,
    extract(month from n.date)::int              as month,
    (wb.heatwave_id is not null)                 as ben_hw,
    coalesce(wh.heatwave_id is not null, false)  as helper_hw
  from public.mv_crossborder_net_daily n
  join public.weather_country_daily wb
    on wb.country_code = n.to_country and wb.date = n.date
  left join public.weather_country_daily wh
    on wh.country_code = n.from_country and wh.date = n.date
),
baseline as (
  select helper, beneficiary, month,
         avg(net_export_gwh) as base_gwh,
         count(*)            as base_days
  from classified
  where not ben_hw and not helper_hw
  group by 1, 2, 3
),
during as (
  select helper, beneficiary, month, helper_hw,
         avg(net_export_gwh) as hw_gwh,
         count(*)            as hw_days
  from classified
  where ben_hw
  group by 1, 2, 3, 4
)
select
  d.helper,
  d.beneficiary,
  d.helper_hw                                              as helper_also_in_heatwave,
  sum(d.hw_days)                                           as heatwave_days,
  round(avg(d.hw_gwh)::numeric, 2)                         as mean_net_export_gwh,
  round(avg(b.base_gwh)::numeric, 2)                       as baseline_net_export_gwh,
  round(avg(d.hw_gwh - b.base_gwh)::numeric, 2)            as extra_gwh_per_day,
  round(sum((d.hw_gwh - b.base_gwh) * d.hw_days)::numeric, 1) as total_extra_gwh
from during d
join baseline b
  on b.helper = d.helper and b.beneficiary = d.beneficiary and b.month = d.month
where b.base_days >= 5
group by 1, 2, 3;

-- ── Who helped most (rolled up over all their neighbours) ───────────────────
create or replace view public.v_heatwave_helpers as
select
  helper                                   as country_code,
  count(distinct beneficiary)              as neighbours_helped,
  sum(heatwave_days)                       as neighbour_heatwave_days,
  round(sum(total_extra_gwh) filter (where not helper_also_in_heatwave)::numeric, 1)
                                           as extra_gwh_spare_capacity,
  round(sum(total_extra_gwh) filter (where helper_also_in_heatwave)::numeric, 1)
                                           as extra_gwh_shared_stress,
  round(sum(total_extra_gwh)::numeric, 1)  as extra_gwh_total
from public.v_heatwave_help_pairs
group by 1
order by extra_gwh_spare_capacity desc nulls last;

-- ── Who leaned on their neighbours most during their own heatwaves ──────────
create or replace view public.v_heatwave_beneficiaries as
select
  beneficiary                              as country_code,
  count(distinct helper)                   as neighbours_drawn_on,
  sum(heatwave_days)                       as own_heatwave_days,
  round(sum(total_extra_gwh) filter (where not helper_also_in_heatwave)::numeric, 1)
                                           as extra_gwh_from_unstressed,
  round(sum(total_extra_gwh) filter (where helper_also_in_heatwave)::numeric, 1)
                                           as extra_gwh_from_stressed,
  round(sum(total_extra_gwh)::numeric, 1)  as extra_gwh_total
from public.v_heatwave_help_pairs
group by 1
order by extra_gwh_total desc nulls last;

-- Refresh after each flows backfill / ingest run.
create or replace function public.refresh_crossborder_mvs()
returns void language plpgsql security definer set search_path = public as $$
begin
  refresh materialized view concurrently public.mv_crossborder_net_daily;
end;
$$;
grant execute on function public.refresh_crossborder_mvs() to service_role;

grant select on public.mv_crossborder_net_daily     to anon;
grant select on public.v_heatwave_help_events       to anon;
grant select on public.v_heatwave_help_pairs        to anon;
grant select on public.v_heatwave_helpers           to anon;
grant select on public.v_heatwave_beneficiaries     to anon;

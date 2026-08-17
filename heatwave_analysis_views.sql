-- Heatwave impact analysis views.
-- Requires weather_heatwave_schema.sql and a populated weather_country_daily.
--
-- Method note that applies to every view here: heatwave days are always
-- compared against non-heatwave days in the SAME country, SAME calendar month
-- and SAME day type (weekday vs weekend). Comparing a July heatwave against
-- an annual mean would mostly measure summer, and comparing against all days
-- would mostly measure the weekly demand cycle. The month + day-type control
-- is what isolates the heat signal.

-- ── Daily electricity demand per zone, with the heatwave flag attached ───────
-- Zone -> country: DK1/DK2 -> DK, NO1..5 -> NO, SE1..4 -> SE, everything else
-- is already a country code.
create or replace view public.v_heatwave_load_daily as
with zone_country as (
  select
    zone_id,
    case
      when zone_id ~ '^(DK|NO|SE)[0-9]$' then left(zone_id, 2)
      else zone_id
    end as country_code
  from (select distinct zone_id from public.electricity_load_snapshots) z
),
daily as (
  select
    zc.country_code,
    (l.ts at time zone 'UTC')::date as date,
    avg(l.load_mw)                  as avg_load_mw,
    max(l.load_mw)                  as peak_load_mw,
    avg(l.load_mw) * 24.0 / 1000.0  as consumption_gwh,
    count(*)                        as samples
  from public.electricity_load_snapshots l
  join zone_country zc on zc.zone_id = l.zone_id
  where l.source = 'entsoe' and l.load_mw > 0
  group by 1, 2
)
select
  d.country_code,
  d.date,
  d.avg_load_mw,
  d.peak_load_mw,
  d.consumption_gwh,
  d.samples,
  w.tmax_c,
  w.anomaly_c,
  w.is_hot_day,
  w.heatwave_id is not null as in_heatwave,
  w.heatwave_day,
  w.heatwave_length,
  extract(month from d.date)::int as month,
  extract(isodow from d.date)::int >= 6 as is_weekend
from daily d
join public.weather_country_daily w
  on w.country_code = d.country_code and w.date = d.date;

-- ── Demand uplift during heatwaves, country by country ──────────────────────
create or replace view public.v_heatwave_demand_uplift as
with matched as (
  select
    country_code, month, is_weekend, in_heatwave,
    avg(avg_load_mw)  as avg_load_mw,
    avg(peak_load_mw) as peak_load_mw,
    count(*)          as days
  from public.v_heatwave_load_daily
  where samples >= 20              -- drop partially-reported days
  group by 1, 2, 3, 4
),
paired as (
  select
    h.country_code, h.month, h.is_weekend,
    h.avg_load_mw  as hw_load,  n.avg_load_mw  as base_load,
    h.peak_load_mw as hw_peak,  n.peak_load_mw as base_peak,
    h.days as hw_days, n.days as base_days
  from matched h
  join matched n
    on n.country_code = h.country_code
   and n.month = h.month
   and n.is_weekend = h.is_weekend
   and n.in_heatwave = false
  where h.in_heatwave = true
)
select
  country_code,
  sum(hw_days)                                                     as heatwave_days,
  round(avg(100.0 * (hw_load / nullif(base_load, 0) - 1))::numeric, 2)  as mean_demand_uplift_pct,
  round(avg(100.0 * (hw_peak / nullif(base_peak, 0) - 1))::numeric, 2)  as peak_demand_uplift_pct
from paired
where base_days >= 5               -- need a credible comparison group
group by 1
order by mean_demand_uplift_pct desc nulls last;

-- ── Generation mix on heatwave days vs matched normal days ──────────────────
-- Answers "how did the system serve the extra load, and what held up?"
create or replace view public.v_heatwave_generation_mix as
with zone_country as (
  select
    zone_id,
    case when zone_id ~ '^(DK|NO|SE)[0-9]$' then left(zone_id, 2) else zone_id end as country_code
  from (select distinct zone_id from public.electricity_generation_snapshots) z
),
daily as (
  select
    zc.country_code,
    (g.ts at time zone 'UTC')::date as date,
    case
      when g.psr_type in ('B18','B19')              then 'wind'
      when g.psr_type = 'B16'                       then 'solar'
      when g.psr_type in ('B10','B11','B12')        then 'hydro'
      when g.psr_type = 'B14'                       then 'nuclear'
      when g.psr_type = 'B04'                       then 'gas'
      when g.psr_type in ('B02','B05')              then 'coal'
      when g.psr_type in ('B01','B17')              then 'biomass'
      else 'other'
    end as fuel,
    avg(g.mw) as avg_mw
  from public.electricity_generation_snapshots g
  join zone_country zc on zc.zone_id = g.zone_id
  where g.source = 'entsoe'
  group by 1, 2, 3
)
select
  d.country_code,
  d.fuel,
  extract(month from d.date)::int as month,
  w.heatwave_id is not null as in_heatwave,
  avg(d.avg_mw) as avg_mw,
  count(*)      as days
from daily d
join public.weather_country_daily w
  on w.country_code = d.country_code and w.date = d.date
group by 1, 2, 3, 4;

-- ── Per-fuel response to heat: which sources hold up, which fade ────────────
create or replace view public.v_heatwave_fuel_resilience as
with paired as (
  select
    h.country_code, h.fuel, h.month,
    h.avg_mw as hw_mw, n.avg_mw as base_mw, h.days as hw_days, n.days as base_days
  from public.v_heatwave_generation_mix h
  join public.v_heatwave_generation_mix n
    on n.country_code = h.country_code and n.fuel = h.fuel
   and n.month = h.month and n.in_heatwave = false
  where h.in_heatwave = true
)
select
  country_code,
  fuel,
  sum(hw_days) as heatwave_days,
  round(avg(100.0 * (hw_mw / nullif(base_mw, 0) - 1))::numeric, 2) as output_change_pct
from paired
where base_days >= 5 and base_mw > 0
group by 1, 2
order by country_code, fuel;

-- ── Gas demand and day-ahead price on heatwave days ─────────────────────────
create or replace view public.v_heatwave_gas_and_price as
with gas as (
  select
    g.country_code,
    g.gas_day as date,
    g.total_mwh,
    g.power_mwh,
    extract(month from g.gas_day)::int as month
  from public.gas_demand_daily g
  where g.method_version = 'v2_bruegel_power_entsoe'
),
price as (
  select
    case when zone_id ~ '^(DK|NO|SE)[0-9]$' then left(zone_id, 2) else zone_id end as country_code,
    (ts at time zone 'UTC')::date as date,
    avg(price_eur_per_mwh) as avg_price,
    max(price_eur_per_mwh) as peak_price
  from public.electricity_day_ahead_prices
  where source = 'entsoe'
  group by 1, 2
)
select
  w.country_code,
  w.date,
  w.heatwave_id is not null as in_heatwave,
  extract(month from w.date)::int as month,
  g.total_mwh,
  g.power_mwh,
  p.avg_price,
  p.peak_price
from public.weather_country_daily w
left join gas   g on g.country_code = w.country_code and g.date = w.date
left join price p on p.country_code = w.country_code and p.date = w.date;

grant select on public.v_heatwave_load_daily        to anon;
grant select on public.v_heatwave_demand_uplift     to anon;
grant select on public.v_heatwave_generation_mix    to anon;
grant select on public.v_heatwave_fuel_resilience   to anon;
grant select on public.v_heatwave_gas_and_price     to anon;

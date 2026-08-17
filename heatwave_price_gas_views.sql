-- Price and gas-sector response to heatwaves.
-- Requires weather_country_daily; matched on country + calendar month throughout.

-- ── Day-ahead electricity price on heatwave days ───────────────────────────
create or replace view public.v_heatwave_price as
with zc as (
  select zone_id,
         case when zone_id ~ '^(DK|NO|SE)[0-9]$' then left(zone_id, 2) else zone_id end as cc
  from (select distinct zone_id from public.electricity_day_ahead_prices) z
),
daily as (
  select zc.cc as country_code,
         (p.ts at time zone 'UTC')::date as date,
         avg(p.price_eur_per_mwh) as avg_price,
         max(p.price_eur_per_mwh) as peak_price
  from public.electricity_day_ahead_prices p
  join zc on zc.zone_id = p.zone_id
  where p.source = 'entsoe' and p.price_eur_per_mwh is not null
  group by 1, 2
),
j as (
  select d.*, extract(month from d.date)::int as month,
         (w.heatwave_id is not null) as hw
  from daily d
  join public.weather_country_daily w
    on w.country_code = d.country_code and w.date = d.date
  where extract(month from d.date) between 5 and 9
),
agg as (
  select country_code, month, hw,
         avg(avg_price) as p_avg, avg(peak_price) as p_peak, count(*) as days
  from j group by 1, 2, 3
),
paired as (
  select h.country_code, h.month,
         h.p_avg as hw_avg, n.p_avg as base_avg,
         h.p_peak as hw_peak, n.p_peak as base_peak,
         h.days as hw_days, n.days as base_days
  from agg h
  join agg n on n.country_code = h.country_code and n.month = h.month and n.hw = false
  where h.hw = true
)
select
  country_code,
  sum(hw_days)                                                       as heatwave_days,
  round(avg(base_avg)::numeric, 1)                                   as normal_price_eur,
  round(avg(hw_avg)::numeric, 1)                                     as heatwave_price_eur,
  round(avg(hw_avg - base_avg)::numeric, 1)                          as delta_eur,
  round(avg(100.0 * (hw_avg / nullif(base_avg, 0) - 1))::numeric, 1) as change_pct,
  round(avg(hw_peak - base_peak)::numeric, 1)                        as peak_delta_eur
from paired
where base_days >= 5 and base_avg > 1     -- avoid % blowups on near-zero prices
group by 1;

-- ── Gas demand by sector on heatwave days ──────────────────────────────────
-- Nine countries (CY, CZ, FI, GR, IE, LT, MT, SE, SK) have no daily native gas
-- source and are derived from Eurostat monthly data, which publishes ~4 months
-- in arrears; their recent rows are NULL placeholders. Those are excluded here
-- rather than counted as zero.
create or replace view public.v_heatwave_gas_sector as
with j as (
  select g.country_code, g.gas_day as date,
         g.total_mwh, g.power_mwh, g.household_mwh, g.industry_mwh,
         extract(month from g.gas_day)::int as month,
         (w.heatwave_id is not null) as hw
  from public.gas_demand_daily g
  join public.weather_country_daily w
    on w.country_code = g.country_code and w.date = g.gas_day
  where g.total_mwh is not null
    and extract(month from g.gas_day) between 5 and 9
),
agg as (
  select country_code, month, hw,
         avg(total_mwh)     as total,
         avg(power_mwh)     as power,
         avg(household_mwh) as household,
         avg(industry_mwh)  as industry,
         count(*)           as days
  from j group by 1, 2, 3
),
paired as (
  select h.country_code, h.month,
         h.total as hw_total, n.total as base_total,
         h.power as hw_power, n.power as base_power,
         h.household as hw_hh, n.household as base_hh,
         h.industry as hw_ind, n.industry as base_ind,
         h.days as hw_days, n.days as base_days
  from agg h
  join agg n on n.country_code = h.country_code and n.month = h.month and n.hw = false
  where h.hw = true
)
select
  country_code,
  sum(hw_days)                                                          as heatwave_days,
  round(avg(base_total / 1000.0)::numeric, 1)                           as normal_total_gwh,
  round(avg((hw_total - base_total) / 1000.0)::numeric, 1)              as delta_total_gwh,
  round(avg((hw_power - base_power) / 1000.0)::numeric, 1)              as delta_power_gwh,
  round(avg((hw_hh - base_hh) / 1000.0)::numeric, 1)                    as delta_household_gwh,
  round(avg((hw_ind - base_ind) / 1000.0)::numeric, 1)                  as delta_industry_gwh,
  round(avg(100.0 * (hw_power / nullif(base_power, 0) - 1))::numeric, 1) as power_change_pct,
  round(avg(100.0 * (hw_total / nullif(base_total, 0) - 1))::numeric, 1) as total_change_pct
from paired
where base_days >= 5
group by 1;

-- ── TTF gas price against how much of Europe is in a heatwave ──────────────
create or replace view public.v_heatwave_ttf as
select
  e.bucket,
  count(*)                                     as days,
  round(avg(t.close_eur_per_mwh)::numeric, 1)  as mean_ttf_eur
from (
  select date, countries_in_heatwave,
    case
      when countries_in_heatwave = 0             then '0'
      when countries_in_heatwave between 1 and 3 then '1-3'
      when countries_in_heatwave between 4 and 7 then '4-7'
      when countries_in_heatwave between 8 and 12 then '8-12'
      else '13+'
    end as bucket
  from public.v_eu_heatwave_daily
) e
join public.gas_price_ttf_daily t on t.ts = e.date
group by 1;

grant select on public.v_heatwave_price      to anon;
grant select on public.v_heatwave_gas_sector to anon;
grant select on public.v_heatwave_ttf        to anon;

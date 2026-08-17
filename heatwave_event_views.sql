-- Each country's largest heatwave of the current year, plus the daily series
-- around it, for the event-anatomy panel.
--
-- Both views report the threshold that is ACTUALLY applied, not the raw 90th
-- percentile. A day counts as hot when it exceeds the local percentile OR
-- reaches the absolute floor, so the effective threshold is the lower of the
-- two. Reporting the percentile alone drew France's line at 31.6 C while days
-- at 30-31.6 C were correctly flagged as heatwave days but sat visibly below
-- it — the chart contradicted its own rule.
--
-- ABSOLUTE_HOT_C is 30.0 in python/weather_heatwave_days.py; keep them in step.

drop view if exists public.v_heatwave_event_series;
drop view if exists public.v_heatwave_event_top;

create view public.v_heatwave_event_top as
with ev as (
  select
    country_code,
    heatwave_id,
    min(date)                                                          as start_date,
    max(date)                                                          as end_date,
    max(heatwave_length)                                               as length_days,
    round(max(tmax_c)::numeric, 1)                                     as peak_tmax_c,
    round(max(tmax_c - least(threshold_p90_c, 30.0))::numeric, 1)      as peak_anomaly_c,
    round(min(least(threshold_p90_c, 30.0))::numeric, 1)               as threshold_c,
    row_number() over (
      partition by country_code
      order by max(heatwave_length) desc, max(anomaly_c) desc
    ) as rn
  from public.weather_country_daily
  where heatwave_id is not null
    and date >= date_trunc('year', current_date)
  group by 1, 2
)
select country_code, heatwave_id, start_date, end_date,
       length_days, peak_tmax_c, peak_anomaly_c, threshold_c
from ev
where rn = 1;

create view public.v_heatwave_event_series as
select
  w.country_code,
  w.date,
  round(w.tmax_c::numeric, 1)                        as tmax_c,
  round(w.threshold_p90_c::numeric, 1)               as threshold_p90_c,
  round(least(w.threshold_p90_c, 30.0)::numeric, 1)  as threshold_c,
  (w.heatwave_id is not null)                        as in_heatwave,
  round(l.avg_load_mw::numeric, 0)                   as avg_load_mw,
  round(sum(g.avg_mw) filter (where g.fuel = 'solar')::numeric, 0)   as solar_mw,
  round(sum(g.avg_mw) filter (where g.fuel = 'wind')::numeric, 0)    as wind_mw,
  round(sum(g.avg_mw) filter (where g.fuel = 'hydro')::numeric, 0)   as hydro_mw,
  round(sum(g.avg_mw) filter (where g.fuel = 'nuclear')::numeric, 0) as nuclear_mw,
  round(sum(g.avg_mw) filter (where g.fuel = 'gas')::numeric, 0)     as gas_mw,
  round(sum(g.avg_mw) filter (where g.fuel = 'coal')::numeric, 0)    as coal_mw,
  round(sum(g.avg_mw) filter (where g.fuel = 'biomass')::numeric, 0) as biomass_mw,
  round(sum(g.avg_mw) filter (where g.fuel = 'other')::numeric, 0)   as other_mw
from public.weather_country_daily w
join public.v_heatwave_event_top t on t.country_code = w.country_code
left join public.mv_load_daily_warm l
  on l.country_code = w.country_code and l.date = w.date
left join public.mv_generation_daily_warm g
  on g.country_code = w.country_code and g.date = w.date
where w.date between t.start_date - 7 and t.end_date + 7
group by w.country_code, w.date, w.tmax_c, w.threshold_p90_c, w.heatwave_id, l.avg_load_mw;

-- Heatwave burden for the current year, per country.
create or replace view public.v_heatwave_burden as
select
  country_code,
  count(*) filter (where heatwave_id is not null) as heatwave_days,
  count(distinct heatwave_id)                     as events,
  round(max(tmax_c)::numeric, 1)                  as peak_tmax_c,
  round(max(anomaly_c)::numeric, 1)               as peak_anomaly_c
from public.weather_country_daily
where date >= date_trunc('year', current_date)
group by 1
having count(*) filter (where heatwave_id is not null) > 0;

grant select on public.v_heatwave_event_top    to anon;
grant select on public.v_heatwave_event_series to anon;
grant select on public.v_heatwave_burden       to anon;

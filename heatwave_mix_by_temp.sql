-- EU generation mix as a function of how hot Europe is.
--
-- The x-axis needs one heat number for the whole continent, which is awkward:
-- a plain average of 30 countries mixes Finland with Greece and describes
-- nowhere. Weighting each country by its own mean electricity demand gives an
-- index anchored to where the load actually is - Germany, France, Italy and
-- Spain dominate it, exactly the systems whose mix the chart is about. It runs
-- 15.8 C to 34.1 C over the 2026 warm season.
--
-- Shares are computed from summed megawatts per bin, not as an average of daily
-- shares: a mean of ratios would let a low-generation day count as much as a
-- high one.

set statement_timeout = '900s';

drop view if exists public.v_eu_mix_by_temp cascade;
create view public.v_eu_mix_by_temp as
with wt as (
  select country_code, avg(avg_load_mw) as w
  from public.mv_load_daily_warm
  where date >= date_trunc('year', current_date)
  group by 1
),
eu_temp as (
  select t.date, sum(t.tmax_c * wt.w) / nullif(sum(wt.w), 0) as eu_tmax
  from public.weather_country_daily t
  join wt on wt.country_code = t.country_code
  where t.date >= date_trunc('year', current_date)
    and extract(month from t.date) between 5 and 9
  group by 1
  -- Only days where nearly every country reported, so the index means the
  -- same thing from one day to the next.
  having count(*) >= 25
),
gen as (
  select date, fuel, sum(avg_mw) as mw
  from public.mv_generation_daily_warm
  where date >= date_trunc('year', current_date)
  group by 1, 2
),
binned as (
  select
    -- 2 C bins, labelled at their midpoint.
    (floor(e.eu_tmax / 2.0) * 2 + 1)::numeric as bin_c,
    g.fuel, g.mw, e.date
  from eu_temp e
  join gen g on g.date = e.date
  -- Start at 22 C (first bin 23). Below that the bins hold three to eight days
  -- each and are cool May weather rather than heat: they widened the axis
  -- without telling us anything about how the system responds to temperature.
  -- The floor keeps 85 of the 108 warm-season days, every bin at 9 or more.
  where e.eu_tmax >= 22.0
),
per_bin as (
  select bin_c, fuel, sum(mw) as fuel_mw, count(distinct date) as days
  from binned group by 1, 2
),
bin_tot as (
  select bin_c, sum(fuel_mw) as tot_mw, max(days) as days
  from per_bin group by 1
)
select p.bin_c,
       b.days,
       p.fuel,
       round((p.fuel_mw / nullif(b.days, 0))::numeric, 0)              as mean_mw,
       round((100.0 * p.fuel_mw / nullif(b.tot_mw, 0))::numeric, 1)    as share_pct
from per_bin p
join bin_tot b on b.bin_c = p.bin_c
-- Three days is the floor for a bin to be drawn at all.
where b.days >= 3;

grant select on public.v_eu_mix_by_temp to anon;

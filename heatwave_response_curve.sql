-- Cooling response curve, binned server-side.
-- The page was pulling ~20k daily rows and binning them in the browser, which
-- cost ~6 s of paged requests on first load. This returns ~200 rows.
create or replace view public.v_heatwave_response_curve as
with b as (
  select country_code,
         (floor((tmax_c - 10) / 3.0) * 3 + 11.5)::int as tmax_bin,
         avg(avg_load_mw) as mw,
         count(*)         as days
  from public.v_heatwave_load_daily
  where samples >= 20 and tmax_c is not null
  group by 1, 2
),
ref as (
  select country_code, sum(mw * days) / nullif(sum(days), 0) as ref_mw
  from b where tmax_bin between 16 and 20 group by 1
)
select b.country_code,
       b.tmax_bin,
       b.days,
       round((100.0 * b.mw / r.ref_mw)::numeric, 1) as demand_index
from b join ref r on r.country_code = b.country_code
where b.days >= 8 and r.ref_mw > 0;

grant select on public.v_heatwave_response_curve to anon;
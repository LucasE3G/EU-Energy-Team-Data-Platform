create or replace view public.v_heatwave_baseline_quality as
select country_code,
       count(distinct hw_date)                as heatwave_days,
       round(avg(day_gap)::numeric,1)         as avg_ref_gap_days,
       max(day_gap)                           as max_ref_gap_days,
       -- 100 means every reference day precedes the heatwave, so any ordinary
       -- seasonal warming between the two is counted as a heat effect.
       round((100.0*count(*) filter (where base_date < hw_date)
              / nullif(count(*),0))::numeric,0) as pct_ref_before
from public.mv_heatwave_matched_days group by 1;
grant select on public.v_heatwave_baseline_quality to anon;
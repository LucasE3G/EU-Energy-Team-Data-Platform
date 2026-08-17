-- Population-weighted daily temperature + heatwave flags per country.
-- Built by python/weather_heatwave_days.py. Run once in the SQL editor.
--
-- One row per country per day, shaped to join directly onto the energy tables:
--   electricity_load_snapshots  on (country/zone, ts::date)
--   gas_demand_daily            on (country_code, gas_day)
--
-- `threshold_p90_c` is the 90th percentile of daily maximum temperature for
-- that calendar day over 1991-2020 (+/- 7 day window). A heatwave is >= 3
-- consecutive days above it — the EEA / Copernicus CTX90pct convention. The
-- threshold is per country and per calendar day on purpose: the energy system
-- responds to the local anomaly, not to an absolute temperature.

create table if not exists public.weather_country_daily (
  country_code     text        not null,
  date             date        not null,
  tmax_c           numeric,
  tmean_c          numeric,
  threshold_p90_c  numeric,
  anomaly_c        numeric,     -- tmax_c - threshold_p90_c
  is_hot_day       boolean     not null default false,
  heatwave_id      text,        -- e.g. 'PL-2026-07-14'; null outside an event
  heatwave_day     integer,     -- 1-based day within the event
  heatwave_length  integer,     -- total length of the event, repeated on each day
  source           text        not null default 'open_meteo_era5',
  inserted_at      timestamptz not null default now(),
  constraint weather_country_daily_pkey primary key (country_code, date)
);

create index if not exists weather_country_daily_date_idx
  on public.weather_country_daily (date desc);
create index if not exists weather_country_daily_country_date_idx
  on public.weather_country_daily (country_code, date desc);
-- Partial index: heatwave rows are the minority and are what queries filter on.
create index if not exists weather_country_daily_heatwave_idx
  on public.weather_country_daily (country_code, date) where heatwave_id is not null;

alter table public.weather_country_daily enable row level security;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'weather_country_daily'
      and policyname = 'weather_country_daily_read_anon'
  ) then
    create policy weather_country_daily_read_anon
      on public.weather_country_daily
      for select to anon using (true);
  end if;
end $$;

grant select on public.weather_country_daily to anon;

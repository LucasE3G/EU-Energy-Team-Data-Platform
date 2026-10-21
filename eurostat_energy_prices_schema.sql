-- Eurostat household energy prices + energy transition indicators
-- Sources: nrg_pc_202, nrg_pc_204, road_eqs_carpda, road_eqr_carpda,
--          nrg_bal_c (RA600), nrg_ind_ren, demo_pjan
-- Populated by python/eurostat_price_upload_supabase.py
-- Run via Supabase SQL editor or Management API.

-- Household retail prices, all bands / tax treatments / currencies.
-- One row per fuel-country-semester-band-tax-currency.
create table if not exists public.eurostat_household_energy_price (
  id bigserial primary key,
  geo text not null,                 -- Eurostat geo code (EL = Greece, EU27_2020 = aggregate)
  country text not null,
  fuel text not null,                -- 'Electricity' | 'Gas'
  period text not null,              -- semester, e.g. '2025-S2'
  band text not null,                -- KWH2500-4999 (DC), GJ20-199 (D2), ...
  band_label text null,
  tax text not null,                 -- X_TAX | X_VAT | I_TAX
  tax_label text null,
  currency text not null,            -- EUR | PPS
  price_per_kwh numeric null,        -- in `currency` per kWh
  source text not null default 'eurostat',
  inserted_at timestamptz not null default now()
);

create unique index if not exists eurostat_household_energy_price_unique
  on public.eurostat_household_energy_price (fuel, geo, period, band, tax, currency);

create index if not exists eurostat_household_energy_price_geo_period
  on public.eurostat_household_energy_price (geo, period desc);

create index if not exists eurostat_household_energy_price_headline
  on public.eurostat_household_energy_price (period desc, fuel, band, tax, currency);

-- Annual energy-transition context: heat pumps, EVs, renewables, population.
-- One row per country-year-indicator.
create table if not exists public.eurostat_energy_transition_indicator (
  id bigserial primary key,
  geo text not null,
  country text not null,
  year integer not null,
  indicator text not null,
  unit text null,
  value numeric null,
  source text null,                  -- originating Eurostat dataset
  inserted_at timestamptz not null default now()
);

create unique index if not exists eurostat_energy_transition_indicator_unique
  on public.eurostat_energy_transition_indicator (geo, year, indicator);

create index if not exists eurostat_energy_transition_indicator_lookup
  on public.eurostat_energy_transition_indicator (indicator, year desc);

-- Macro and energy-consumption panel, long form, all frequencies in one table.
-- freq: 'M' monthly, 'Q' quarterly, 'A' annual, 'S' semester (aggregated onto
-- the price series' semesters, so it joins straight onto the ratio).
-- period is the Eurostat period label for that frequency: '2025-03', '2025-Q1',
-- '2025', '2025-S2'.
create table if not exists public.eurostat_macro_energy_panel (
  id bigserial primary key,
  geo text not null,
  country text not null,
  freq text not null check (freq in ('M', 'Q', 'A', 'S')),
  period text not null,
  series text not null,              -- machine key, e.g. gas_consumption_tj_gcv
  label text null,                   -- human label with unit
  value numeric null,
  inserted_at timestamptz not null default now()
);

create unique index if not exists eurostat_macro_energy_panel_unique
  on public.eurostat_macro_energy_panel (geo, freq, period, series);

create index if not exists eurostat_macro_energy_panel_series
  on public.eurostat_macro_energy_panel (series, freq, period);

create index if not exists eurostat_macro_energy_panel_geo
  on public.eurostat_macro_energy_panel (geo, freq, period);

-- Headline ratio: band DC electricity over band D2 gas, all taxes included, EUR.
-- Band DC is the electricity counterpart of gas band D2 (there is no D2 in the
-- electricity nomenclature, whose bands run DA-DE).
create or replace view public.eurostat_household_price_ratio as
with headline as (
  select geo, country, period, fuel, currency, tax, price_per_kwh
  from public.eurostat_household_energy_price
  where (fuel = 'Electricity' and band = 'KWH2500-4999')
     or (fuel = 'Gas'         and band = 'GJ20-199')
)
select
  e.geo,
  e.country,
  e.period,
  e.currency,
  e.tax,
  e.price_per_kwh as electricity_price_per_kwh,
  g.price_per_kwh as gas_price_per_kwh,
  case when g.price_per_kwh > 0
       then round(e.price_per_kwh / g.price_per_kwh, 4)
  end as ratio
from headline e
join headline g
  on  g.geo = e.geo
  and g.period = e.period
  and g.currency = e.currency
  and g.tax = e.tax
  and g.fuel = 'Gas'
where e.fuel = 'Electricity';

comment on view public.eurostat_household_price_ratio is
  'Household electricity/gas retail price ratio (band DC over band D2). The ratio '
  'is the break-even seasonal COP for electrifying heat: a heat pump above it is '
  'cheaper to run than a gas boiler. Cyprus, Finland and Malta have no household '
  'gas series; Poland reports gas only to 2023-S2.';

-- Each macro series lined up against the price ratio on the same semester,
-- long form, so no pivoting is needed to correlate one against the other.
create or replace view public.eurostat_semester_macro_vs_ratio as
select
  r.geo,
  r.country,
  r.period,
  r.ratio,
  r.electricity_price_per_kwh,
  r.gas_price_per_kwh,
  m.series,
  m.label,
  m.value
from public.eurostat_household_price_ratio r
join public.eurostat_macro_energy_panel m
  on  m.geo = r.geo
  and m.freq = 'S'
  and m.period = r.period
where r.tax = 'I_TAX'
  and r.currency = 'EUR'
  and m.series not in ('price_ratio', 'electricity_price_eur_kwh', 'gas_price_eur_kwh');

comment on view public.eurostat_semester_macro_vs_ratio is
  'Household electricity/gas price ratio joined to each macro and energy series '
  'on the same semester. Correlate on year-on-year changes rather than levels: '
  'nearly every series here trends, and correlating trends overstates the link.';

alter table public.eurostat_household_energy_price enable row level security;
alter table public.eurostat_energy_transition_indicator enable row level security;
alter table public.eurostat_macro_energy_panel enable row level security;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'eurostat_household_energy_price'
      and policyname = 'eurostat_household_energy_price_read_anon'
  ) then
    create policy eurostat_household_energy_price_read_anon
      on public.eurostat_household_energy_price
      for select
      to anon
      using (true);
  end if;

  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'eurostat_energy_transition_indicator'
      and policyname = 'eurostat_energy_transition_indicator_read_anon'
  ) then
    create policy eurostat_energy_transition_indicator_read_anon
      on public.eurostat_energy_transition_indicator
      for select
      to anon
      using (true);
  end if;

  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'eurostat_macro_energy_panel'
      and policyname = 'eurostat_macro_energy_panel_read_anon'
  ) then
    create policy eurostat_macro_energy_panel_read_anon
      on public.eurostat_macro_energy_panel
      for select
      to anon
      using (true);
  end if;
end $$;

-- Data API grants. From 2026-10-30 Supabase no longer grants new public tables
-- to the API roles automatically, so a fresh run of this file needs these.
-- anon reads go through the policies above; service_role is what
-- eurostat_price_upload_supabase.py writes with.
grant select on public.eurostat_household_energy_price,
                public.eurostat_energy_transition_indicator,
                public.eurostat_macro_energy_panel
  to anon, authenticated;
grant all on public.eurostat_household_energy_price,
             public.eurostat_energy_transition_indicator,
             public.eurostat_macro_energy_panel
  to service_role;
grant usage, select on sequence public.eurostat_household_energy_price_id_seq,
                                public.eurostat_energy_transition_indicator_id_seq,
                                public.eurostat_macro_energy_panel_id_seq
  to service_role;

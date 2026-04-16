-- EU Gas implied demand by sector (daily)
-- Run this in the Supabase SQL editor.

create table if not exists public.gas_demand_daily (
  id bigserial primary key,
  country_code text not null,
  gas_day date not null,

  -- All values in MWh/day (energy), not volume.
  total_mwh numeric null,
  power_mwh numeric null,
  household_mwh numeric null,
  industry_mwh numeric null,

  -- Provenance / transparency
  source_total text not null default 'entsog+gie',
  source_power text not null default 'estimated',
  source_split text not null default 'fixed_shares',
  method_version text not null default 'v1',
  quality_flag text null, -- e.g. observed_total, estimated_power, missing_component
  raw jsonb null,

  inserted_at timestamptz not null default now()
);

create unique index if not exists gas_demand_daily_unique
  on public.gas_demand_daily (method_version, country_code, gas_day);

create index if not exists gas_demand_daily_country_day
  on public.gas_demand_daily (country_code, gas_day desc);

alter table public.gas_demand_daily enable row level security;

-- Allow reading to anon (public dashboard / map).
do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'gas_demand_daily'
      and policyname = 'gas_demand_daily_read_anon'
  ) then
    create policy gas_demand_daily_read_anon
      on public.gas_demand_daily
      for select
      to anon
      using (true);
  end if;
end $$;


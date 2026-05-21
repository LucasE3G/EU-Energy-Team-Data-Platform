-- EU gas storage (GIE AGSI+) — daily aggregate
-- Run in Supabase SQL editor.

create table if not exists public.gas_storage_eu_daily (
  id               bigserial primary key,
  gas_day          date        not null,
  gas_in_storage_twh  double precision,  -- TWh of gas currently stored
  full_pct         double precision,     -- % of working gas volume filled
  trend_pct        double precision,     -- day-over-day change in fill %
  injection_twh    double precision,     -- TWh injected on this gas day
  withdrawal_twh   double precision,     -- TWh withdrawn on this gas day
  working_gas_volume_twh double precision, -- total EU working gas capacity (TWh)
  status           text,                 -- C = confirmed, E = estimated, etc.
  source           text not null default 'gie_agsi',
  created_at       timestamptz not null default now(),
  unique (source, gas_day)
);

create index if not exists gas_storage_eu_daily_gas_day_desc
  on public.gas_storage_eu_daily (gas_day desc);

-- RLS (read-only for anon)
alter table public.gas_storage_eu_daily enable row level security;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where tablename = 'gas_storage_eu_daily' and policyname = 'allow_anon_select'
  ) then
    create policy allow_anon_select on public.gas_storage_eu_daily
      for select using (true);
  end if;
end $$;

grant select on public.gas_storage_eu_daily to anon;

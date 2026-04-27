-- Day-ahead electricity prices (ENTSO-E A44 / A01)
-- Run via Supabase SQL editor or Management API.

create table if not exists public.electricity_day_ahead_prices (
  id bigserial primary key,
  zone_id text not null,
  ts timestamptz not null,
  price_eur_per_mwh numeric null,
  currency text not null default 'EUR',
  source text not null default 'entsoe',
  raw jsonb null,
  inserted_at timestamptz not null default now()
);

create unique index if not exists electricity_day_ahead_prices_unique
  on public.electricity_day_ahead_prices (source, zone_id, ts);

create index if not exists electricity_day_ahead_prices_zone_ts_desc
  on public.electricity_day_ahead_prices (zone_id, ts desc);

create index if not exists electricity_day_ahead_prices_ts_desc
  on public.electricity_day_ahead_prices (ts desc);

alter table public.electricity_day_ahead_prices enable row level security;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'electricity_day_ahead_prices'
      and policyname = 'electricity_day_ahead_prices_read_anon'
  ) then
    create policy electricity_day_ahead_prices_read_anon
      on public.electricity_day_ahead_prices
      for select
      to anon
      using (true);
  end if;
end $$;


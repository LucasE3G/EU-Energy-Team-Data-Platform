-- Gas storage by country (GIE AGSI+ country-level data).
-- Run once in Supabase SQL Editor, then deploy updated gas_ingest_storage_eu function.

create table if not exists public.gas_storage_country_daily (
  id               bigserial primary key,
  gas_day          date not null,
  country          text not null,
  gas_in_storage_twh   numeric,
  full_pct             numeric,
  trend_pct            numeric,
  injection_twh        numeric,
  withdrawal_twh       numeric,
  working_gas_volume_twh numeric,
  status           text,
  source           text not null default 'gie_agsi',
  created_at       timestamptz not null default now()
);

create unique index if not exists gas_storage_country_daily_uq
  on public.gas_storage_country_daily (source, country, gas_day);

create index if not exists gas_storage_country_daily_day_idx
  on public.gas_storage_country_daily (gas_day desc);

create index if not exists gas_storage_country_daily_country_idx
  on public.gas_storage_country_daily (country, gas_day desc);

alter table public.gas_storage_country_daily enable row level security;

do $$ begin
  if not exists (
    select 1 from pg_policies
    where tablename = 'gas_storage_country_daily' and policyname = 'anon read'
  ) then
    create policy "anon read" on public.gas_storage_country_daily
      for select to anon using (true);
  end if;
end $$;

grant select on public.gas_storage_country_daily to anon;

-- Electricity generation by PSR type (ENTSO-E A75 actual generation per production type).
-- One row per (zone, timestamp, psrType). Run in Supabase SQL editor.

create table if not exists public.electricity_generation_snapshots (
  id          bigserial primary key,
  zone_id     text not null,
  ts          timestamptz not null,
  psr_type    text not null,   -- ENTSO-E code: B01 biomass, B04 gas, B14 nuclear, B16 solar, B18/B19 wind …
  mw          double precision not null,
  source      text not null default 'entsoe',
  inserted_at timestamptz not null default now()
);

create unique index if not exists electricity_generation_snapshots_unique
  on public.electricity_generation_snapshots (source, zone_id, ts, psr_type);

create index if not exists electricity_generation_snapshots_zone_ts
  on public.electricity_generation_snapshots (zone_id, ts desc);

create index if not exists electricity_generation_snapshots_type_ts
  on public.electricity_generation_snapshots (psr_type, ts desc);

alter table public.electricity_generation_snapshots enable row level security;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename  = 'electricity_generation_snapshots'
      and policyname = 'electricity_generation_snapshots_read_anon'
  ) then
    create policy electricity_generation_snapshots_read_anon
      on public.electricity_generation_snapshots
      for select to anon using (true);
  end if;
end $$;

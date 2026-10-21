-- Electricity Load (ENTSO-E) schema
-- Run this in Supabase SQL editor.

create table if not exists public.electricity_load_snapshots (
  id bigserial primary key,
  zone_id text not null,
  country_code text null,
  ts timestamptz not null,
  load_mw double precision null,
  source text not null default 'entsoe',
  raw jsonb null,
  inserted_at timestamptz not null default now()
);

create unique index if not exists electricity_load_snapshots_unique
  on public.electricity_load_snapshots (source, zone_id, ts);

create index if not exists electricity_load_snapshots_zone_ts
  on public.electricity_load_snapshots (zone_id, ts desc);

create index if not exists electricity_load_snapshots_ts_desc
  on public.electricity_load_snapshots (ts desc);

alter table public.electricity_load_snapshots enable row level security;

-- Allow reading snapshots to anyone with anon key (public dashboard).
do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'electricity_load_snapshots'
      and policyname = 'electricity_load_snapshots_read_anon'
  ) then
    create policy electricity_load_snapshots_read_anon
      on public.electricity_load_snapshots
      for select
      to anon
      using (true);
  end if;
end $$;

-- Data API grants. From 2026-10-30 Supabase no longer grants new public tables
-- to the API roles automatically, so a fresh run of this file needs these.
-- anon reads go through the policy above; service_role is what the ingest
-- function writes with.
grant select on public.electricity_load_snapshots to anon, authenticated;
grant all on public.electricity_load_snapshots to service_role;
grant usage, select on sequence public.electricity_load_snapshots_id_seq to service_role;


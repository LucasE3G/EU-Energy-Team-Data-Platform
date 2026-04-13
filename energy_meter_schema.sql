-- Energy Meter (ENTSO-E) schema
-- Run this in Supabase SQL editor.

create table if not exists public.energy_mix_snapshots (
  id bigserial primary key,
  zone_id text not null,
  country_code text null,
  ts timestamptz not null,
  renewable_percent numeric null,
  carbon_intensity_g_per_kwh numeric null,
  source text not null default 'entsoe',
  raw jsonb null,
  inserted_at timestamptz not null default now()
);

create unique index if not exists energy_mix_snapshots_unique
  on public.energy_mix_snapshots (source, zone_id, ts);

create index if not exists energy_mix_snapshots_zone_ts
  on public.energy_mix_snapshots (zone_id, ts desc);

alter table public.energy_mix_snapshots enable row level security;

-- Allow reading snapshots to anyone with anon key (public dashboard).
do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'energy_mix_snapshots'
      and policyname = 'energy_mix_snapshots_read_anon'
  ) then
    create policy energy_mix_snapshots_read_anon
      on public.energy_mix_snapshots
      for select
      to anon
      using (true);
  end if;
end $$;


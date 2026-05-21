-- Cross-border physical flows (ENTSO-E A11 / A16)

create table if not exists public.electricity_crossborder_flows (
  id bigserial primary key,
  ts timestamptz not null,
  from_zone text not null,
  to_zone text not null,
  mw numeric null,
  source text not null default 'entsoe',
  raw jsonb null,
  inserted_at timestamptz not null default now(),
  constraint electricity_crossborder_flows_pair_check check (from_zone <> to_zone)
);

create unique index if not exists electricity_crossborder_flows_unique
  on public.electricity_crossborder_flows (source, from_zone, to_zone, ts);

create index if not exists electricity_crossborder_flows_ts_desc
  on public.electricity_crossborder_flows (ts desc);

create index if not exists electricity_crossborder_flows_from_ts_desc
  on public.electricity_crossborder_flows (from_zone, ts desc);

create index if not exists electricity_crossborder_flows_to_ts_desc
  on public.electricity_crossborder_flows (to_zone, ts desc);

alter table public.electricity_crossborder_flows enable row level security;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname='public'
      and tablename='electricity_crossborder_flows'
      and policyname='electricity_crossborder_flows_read_anon'
  ) then
    create policy electricity_crossborder_flows_read_anon
      on public.electricity_crossborder_flows
      for select
      to anon
      using (true);
  end if;
end $$;


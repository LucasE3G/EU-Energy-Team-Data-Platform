-- TTF natural gas daily closing prices (sourced from Yahoo Finance TTF=F).
-- Run once in Supabase SQL Editor before deploying the ingest function.

create table if not exists public.gas_price_ttf_daily (
  ts                  date             not null,
  close_eur_per_mwh   double precision,
  open_eur_per_mwh    double precision,
  high_eur_per_mwh    double precision,
  low_eur_per_mwh     double precision,
  source              text             not null default 'yahoo_finance',
  constraint gas_price_ttf_daily_pkey primary key (source, ts)
);

create index if not exists gas_price_ttf_daily_ts_idx on public.gas_price_ttf_daily (ts desc);

-- Public read access (no RLS policy needed, just grant + enable)
alter table public.gas_price_ttf_daily enable row level security;
create policy "public read" on public.gas_price_ttf_daily for select using (true);
grant select on public.gas_price_ttf_daily to anon;

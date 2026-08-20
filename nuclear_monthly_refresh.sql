create or replace function public.refresh_nuclear_monthly()
returns void language plpgsql security definer set search_path = public as $$
begin
  refresh materialized view concurrently public.mv_nuclear_monthly;
end;
$$;
grant execute on function public.refresh_nuclear_monthly() to service_role;
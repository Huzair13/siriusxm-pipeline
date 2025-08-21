-- sql/hello_world_proc.sql
create or replace procedure public.hello_world()
language plpgsql
as $$
begin
  raise notice 'Hello from Redshift!';
end;
$$;

grant execute on procedure public.hello_world() to public;

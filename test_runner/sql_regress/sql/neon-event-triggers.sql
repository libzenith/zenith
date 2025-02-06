create or replace function admin_proc()
    returns event_trigger
    language plpgsql as
$$
begin
    raise notice 'admin event trigger is executed for %', current_user;
end;
$$;

create role neon_superuser;
create role neon_admin login inherit createrole createdb in role neon_superuser;
grant create on schema public to neon_admin;
create database neondb with owner neon_admin;
grant all privileges on database neondb to neon_superuser;

create role neon_user;
grant create on schema public to neon_user;

create event trigger on_ddl1 on ddl_command_end
execute procedure admin_proc();

set role neon_user;

-- Non-priveleged neon user should not be able to create event trigers
create event trigger on_ddl2 on ddl_command_end
execute procedure admin_proc();

set role neon_admin;

-- neon_super user should be able to create event trigers

create or replace function neon_proc()
    returns event_trigger
    language plpgsql as
$$
begin
    raise notice 'neon event trigger is executed for %', current_user;
end;
$$;

create event trigger on_ddl2 on ddl_command_end
execute procedure neon_proc();


\c neondb neon_admin

create or replace function neondb_proc()
    returns event_trigger
    language plpgsql as
$$
begin
    raise notice 'neondb event trigger is executed for %', current_user;
end;
$$;

create or replace function neondb_secdef_proc()
    returns event_trigger
    language plpgsql as
$$
begin
    raise notice 'neondb secdef event trigger is executed for %', current_user;
end;
$$ security definer;

-- Neon superuser should be able to create event triggers
create event trigger on_ddl3 on ddl_command_end
execute procedure neondb_proc();

create event trigger on_ddl4 on ddl_command_end
execute procedure neondb_secdef_proc();

-- Check that event trigger is fired for neon_admin
create table t1(x integer);

\c regression cloud_admin
-- Check that event triggers are not fired for superuser
create table t2(x integer);

-- Now enable event triggers execution for superuser
set neon.enable_event_triggers_for_superuser=on;

-- Check that even trigger is fired in this case
create table t3(x integer);

\c neondb cloud_admin
-- Check that event triggers are not fired for superuser
create table t4(x integer);

-- Now enable event triggers execution for superuser
set neon.enable_event_triggers_for_superuser=on;

-- Check that even trigger is fired in this case
create table t5(x integer);

\c neondb neon_admin

-- Check that neon_admin can drop event triggers
drop event trigger on_ddl3;
drop event trigger on_ddl4;

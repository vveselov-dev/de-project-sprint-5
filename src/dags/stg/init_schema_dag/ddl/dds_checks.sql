select
    table_schema,
    table_name
from information_schema.tables
where table_schema = 'dds'
order by table_name;

select
    table_name,
    column_name,
    data_type,
    is_nullable,
    column_default
from information_schema.columns
where table_schema = 'dds'
order by table_name, ordinal_position;

select
    conrelid::regclass as table_name,
    conname as constraint_name,
    contype as constraint_type,
    pg_get_constraintdef(oid) as constraint_definition
from pg_constraint
where connamespace = 'dds'::regnamespace
order by conrelid::regclass::text, conname;


select
    table_name,
    column_name,
    data_type,
    is_nullable,
    column_default
from information_schema.columns
where table_schema = 'dds'
  and table_name in ('dm_couriers', 'dm_deliveries', 'dm_orders')
order by table_name, ordinal_position;

select
    conrelid::regclass as table_name,
    conname as constraint_name,
    contype as constraint_type,
    pg_get_constraintdef(oid) as constraint_definition
from pg_constraint
where connamespace = 'dds'::regnamespace
  and conrelid::regclass::text in (
      'dds.dm_couriers',
      'dds.dm_deliveries',
      'dds.dm_orders'
  )
order by table_name, conname;
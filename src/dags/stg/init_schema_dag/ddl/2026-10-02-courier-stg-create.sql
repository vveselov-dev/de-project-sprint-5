create table stg.couriers (
    id serial constraint couriers_pkey primary key,
    object_id varchar not null constraint couriers_object_id_key unique,
    object_value text not null,
    update_ts timestamp not null
);

create table stg.deliveries (
    id serial constraint deliveries_pkey primary key,
    object_id varchar not null constraint deliveries_object_id_key unique,
    object_value text not null,
    update_ts timestamp not null
);

-- Проверка корректности создания таблиц
select
    table_schema,
    table_name
from information_schema.tables
where table_schema = 'stg'
  and table_name in ('couriers', 'deliveries')
order by table_name;

select 'stg.couriers' as table_name, count(*) as row_count from stg.couriers
union all
select 'stg.deliveries', count(*) from stg.deliveries
union all
select 'dds.dm_couriers', count(*) from dds.dm_couriers
union all
select 'dds.dm_deliveries', count(*) from dds.dm_deliveries;



select
    count(*) as total_deliveries,
    count(*) filter (
        where o.id is not null
    ) as matched_orders,
    count(*) filter (
        where o.id is null
    ) as unmatched_orders
from stg.deliveries s
left join dds.dm_orders o
    on o.order_key = s.object_value::jsonb ->> 'order_id';

select count(*) as orders_count
from dds.dm_orders;


select count(*) as orders_count
from stg.ordersystem_orders;

select 'dds.dm_orders' as table_name, count(*) as row_count from dds.dm_orders
union all
select 'dds.dm_restaurants', count(*) from dds.dm_restaurants
union all
select 'dds.dm_users', count(*) from dds.dm_users
union all
select 'dds.dm_timestamps', count(*) from dds.dm_timestamps;

select
    table_schema,
    table_name
from information_schema.tables
where table_name = 'srv_wf_settings';

select
    column_name,
    data_type,
    is_nullable
from information_schema.columns
where table_schema = 'stg'
  and table_name = 'srv_wf_settings'
order by ordinal_position;

select
    id,
    workflow_key,
    workflow_settings
from stg.srv_wf_settings
order by id desc
limit 20;

select table_schema, table_name
from information_schema.tables
where table_name = 'srv_wf_settings'
order by table_schema;
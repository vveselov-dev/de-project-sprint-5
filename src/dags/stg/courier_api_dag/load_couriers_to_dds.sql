
insert into dds.dm_couriers (
    courier_id,
    courier_name
)
select
    s.object_value::jsonb ->> '_id' as courier_id,
    s.object_value::jsonb ->> 'name' as courier_name
from stg.couriers s
where
    s.object_value::jsonb ->> '_id' is not null
    and s.object_value::jsonb ->> 'name' is not null
on conflict (courier_id) do update
set
    courier_name = excluded.courier_name
where
    dds.dm_couriers.courier_name is distinct from excluded.courier_name;
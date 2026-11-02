insert into dds.dm_deliveries (
    delivery_id,
    order_id,
    courier_id,
    order_ts,
    rate,
    tip_sum
)
select
    s.object_value::jsonb ->> 'delivery_id' as delivery_id,
    o.id as order_id,
    c.id as courier_id,
    (s.object_value::jsonb ->> 'order_ts')::timestamp as order_ts,
    (s.object_value::jsonb ->> 'rate')::integer as rate,
    (s.object_value::jsonb ->> 'tip_sum')::numeric as tip_sum
from stg.deliveries s
join dds.dm_orders o
    on o.order_key = s.object_value::jsonb ->> 'order_id'
join dds.dm_couriers c
    on c.courier_id = s.object_value::jsonb ->> 'courier_id'
where
    s.object_value::jsonb ->> 'delivery_id' is not null
on conflict (delivery_id) do update
set
    order_id = excluded.order_id,
    courier_id = excluded.courier_id,
    order_ts = excluded.order_ts,
    rate = excluded.rate,
    tip_sum = excluded.tip_sum
where
    (dds.dm_deliveries.order_id,
     dds.dm_deliveries.courier_id,
     dds.dm_deliveries.order_ts,
     dds.dm_deliveries.rate,
     dds.dm_deliveries.tip_sum)
    is distinct from
    (excluded.order_id,
     excluded.courier_id,
     excluded.order_ts,
     excluded.rate,
     excluded.tip_sum);
create table dds.dm_couriers (
id serial primary key,
courier_id varchar not null unique,
courier_name varchar not null
);

create table dds.dm_deliveries (
id serial primary key,
delivery_id varchar not null unique,
order_id integer not null,
courier_id integer not null,
order_ts timestamp not null,
rate integer not null,
tip_sum numeric not null,


constraint dm_deliveries_order_fk
    foreign key (order_id)
    references dds.dm_orders (id),

constraint dm_deliveries_courier_fk
    foreign key (courier_id)
    references dds.dm_couriers (id)


);

alter table dds.dm_orders
add column courier_id integer;

alter table dds.dm_orders
add constraint dm_orders_courier_fk
foreign key (courier_id)
references dds.dm_couriers (id);

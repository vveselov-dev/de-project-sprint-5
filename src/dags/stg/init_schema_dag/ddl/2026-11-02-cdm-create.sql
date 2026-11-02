create schema if not exists cdm;

create table if not exists cdm.dm_courier_ledger (
    id integer generated always as identity primary key,
    courier_id integer not null,
    courier_name varchar not null,
    settlement_year integer not null,
    settlement_month integer not null,
    orders_count integer not null,
    orders_total_sum numeric(14, 2) not null,
    rate_avg numeric(5, 3) not null,
    order_processing_fee numeric(14, 2) not null,
    courier_order_sum numeric(14, 2) not null,
    courier_tips_sum numeric(14, 2) not null,
    courier_reward_sum numeric(14, 2) not null,
    constraint dm_courier_ledger_courier_fk
        foreign key (courier_id)
        references dds.dm_couriers (id),
    constraint dm_courier_ledger_month_chk
        check (settlement_month between 1 and 12),
    constraint dm_courier_ledger_courier_month_uq
        unique (courier_id, settlement_year, settlement_month)
);
INSERT INTO cdm.dm_courier_ledger (
    courier_id,
    courier_name,
    settlement_year,
    settlement_month,
    orders_count,
    orders_total_sum,
    rate_avg,
    order_processing_fee,
    courier_order_sum,
    courier_tips_sum,
    courier_reward_sum
)
WITH order_sums AS (
    SELECT
        order_id,
        SUM(total_sum) AS order_total_sum
    FROM dds.fct_product_sales
    GROUP BY order_id
),
monthly_courier_stats AS (
    SELECT
        d.courier_id,
        c.courier_name,
        EXTRACT(YEAR FROM d.order_ts)::integer AS settlement_year,
        EXTRACT(MONTH FROM d.order_ts)::integer AS settlement_month,
        COUNT(DISTINCT d.order_id)::integer AS orders_count,
        SUM(os.order_total_sum) AS orders_total_sum,
        AVG(d.rate) AS rate_avg,
        SUM(d.tip_sum) AS courier_tips_sum
    FROM dds.dm_deliveries d
    JOIN dds.dm_couriers c
        ON c.id = d.courier_id
    JOIN order_sums os
        ON os.order_id = d.order_id
    GROUP BY
        d.courier_id,
        c.courier_name,
        EXTRACT(YEAR FROM d.order_ts),
        EXTRACT(MONTH FROM d.order_ts)
),
order_payouts AS (
    SELECT
        m.courier_id,
        m.settlement_year,
        m.settlement_month,
        SUM(
            GREATEST(
                CASE
                    WHEN m.rate_avg < 4
                        THEN os.order_total_sum * 0.05
                    WHEN m.rate_avg < 4.5
                        THEN os.order_total_sum * 0.07
                    WHEN m.rate_avg < 4.9
                        THEN os.order_total_sum * 0.08
                    ELSE os.order_total_sum * 0.10
                END,
                CASE
                    WHEN m.rate_avg < 4 THEN 100
                    WHEN m.rate_avg < 4.5 THEN 150
                    WHEN m.rate_avg < 4.9 THEN 175
                    ELSE 200
                END
            )
        ) AS courier_order_sum
    FROM monthly_courier_stats m
    JOIN dds.dm_deliveries d
        ON d.courier_id = m.courier_id
       AND EXTRACT(YEAR FROM d.order_ts)::integer = m.settlement_year
       AND EXTRACT(MONTH FROM d.order_ts)::integer = m.settlement_month
    JOIN order_sums os
        ON os.order_id = d.order_id
    GROUP BY
        m.courier_id,
        m.settlement_year,
        m.settlement_month
),
calculated AS (
    SELECT
        m.*,
        m.orders_total_sum * 0.25 AS order_processing_fee,
        p.courier_order_sum
    FROM monthly_courier_stats m
    JOIN order_payouts p
        ON p.courier_id = m.courier_id
       AND p.settlement_year = m.settlement_year
       AND p.settlement_month = m.settlement_month
)
SELECT
    courier_id,
    courier_name,
    settlement_year,
    settlement_month,
    orders_count,
    orders_total_sum,
    rate_avg,
    order_processing_fee,
    courier_order_sum,
    courier_tips_sum,
    courier_order_sum + courier_tips_sum * 0.95 AS courier_reward_sum
FROM calculated
ON CONFLICT (courier_id, settlement_year, settlement_month)
DO UPDATE SET
    courier_name = EXCLUDED.courier_name,
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    rate_avg = EXCLUDED.rate_avg,
    order_processing_fee = EXCLUDED.order_processing_fee,
    courier_order_sum = EXCLUDED.courier_order_sum,
    courier_tips_sum = EXCLUDED.courier_tips_sum,
    courier_reward_sum = EXCLUDED.courier_reward_sum;

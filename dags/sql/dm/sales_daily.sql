-- Пересборка витрины продаж за последние params.lookback_days дней (включая текущий).
-- Всё выполняется одной транзакцией вместе с проверками качества: если проверка
-- не прошла, RAISE EXCEPTION откатывает транзакцию и читатели продолжают видеть
-- прежнюю версию витрины.
{% set window_start = "DATE '" ~ macros.ds_add(ds, -params.lookback_days) ~ "'" %}

DELETE FROM dm.sales_daily
WHERE report_date >= {{ window_start }};

INSERT INTO dm.sales_daily (report_date, customer_segment, orders_cnt, customers_cnt, revenue)
SELECT
    o.created_at::date                  AS report_date,
    coalesce(c.segment, 'unknown')      AS customer_segment,
    count(*)                            AS orders_cnt,
    count(DISTINCT o.customer_id)       AS customers_cnt,
    sum(o.amount)                       AS revenue
FROM ods.shop_orders o
LEFT JOIN ods.shop_customers c ON c.customer_id = o.customer_id
WHERE o.created_at >= {{ window_start }}
  AND o.status <> 'cancelled'
GROUP BY 1, 2;

DO $$
DECLARE
    v_rows       bigint;
    v_bad_rows   bigint;
    v_dm_revenue numeric;
    v_ods_revenue numeric;
BEGIN
    -- Один проход по окну витрины для всех построчных проверок.
    SELECT count(*),
           coalesce(sum(CASE WHEN report_date IS NULL OR customer_segment IS NULL OR orders_cnt <= 0
                                  OR revenue < 0 OR customers_cnt > orders_cnt THEN 1 ELSE 0 END), 0),
           coalesce(sum(revenue), 0)
    INTO v_rows, v_bad_rows, v_dm_revenue
    FROM dm.sales_daily
    WHERE report_date >= {{ window_start }};

    IF v_rows = 0 THEN
        RAISE EXCEPTION 'DQ: окно витрины с % пустое', {{ window_start }};
    END IF;
    IF v_bad_rows > 0 THEN
        RAISE EXCEPTION 'DQ: % некорректных строк (NULL, orders_cnt <= 0, revenue < 0, customers > orders)', v_bad_rows;
    END IF;

    SELECT coalesce(sum(amount), 0) INTO v_ods_revenue
    FROM ods.shop_orders
    WHERE created_at >= {{ window_start }} AND status <> 'cancelled';

    IF abs(v_dm_revenue - v_ods_revenue) >= 0.01 THEN
        RAISE EXCEPTION 'DQ: выручка витрины % не сходится с ODS %', v_dm_revenue, v_ods_revenue;
    END IF;
END
$$;

ANALYZE dm.sales_daily;

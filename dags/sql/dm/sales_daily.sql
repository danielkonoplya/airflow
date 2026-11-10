-- Пересборка витрины продаж за последние params.lookback_days дней (включая текущий).
-- Выполняется одной транзакцией: читатели видят либо старую, либо новую версию окна.
DELETE FROM dm.sales_daily
WHERE report_date >= DATE '{{ macros.ds_add(ds, -params.lookback_days) }}';

INSERT INTO dm.sales_daily (report_date, customer_segment, orders_cnt, customers_cnt, revenue)
SELECT
    o.created_at::date                  AS report_date,
    coalesce(c.segment, 'unknown')      AS customer_segment,
    count(*)                            AS orders_cnt,
    count(DISTINCT o.customer_id)       AS customers_cnt,
    sum(o.amount)                       AS revenue
FROM ods.shop_orders o
LEFT JOIN ods.shop_customers c ON c.customer_id = o.customer_id
WHERE o.created_at >= DATE '{{ macros.ds_add(ds, -params.lookback_days) }}'
  AND o.status <> 'cancelled'
GROUP BY 1, 2;

ANALYZE dm.sales_daily;

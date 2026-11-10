-- Демо-таблицы под конфиги из dags/configs.

-- Справочник клиентов: маленький, полная перезаливка.
CREATE TABLE IF NOT EXISTS ods.shop_customers (
    customer_id bigint NOT NULL,
    full_name   text,
    email       text,
    segment     text,
    created_at  timestamp,
    updated_at  timestamp,
    _loaded_at  timestamp DEFAULT now()
)
DISTRIBUTED BY (customer_id);

-- Заказы: постоянные upsert'ы (DELETE + INSERT), поэтому heap —
-- в AO-таблицах удаления копятся в visimap и требуют частого VACUUM.
CREATE TABLE IF NOT EXISTS ods.shop_orders (
    order_id    bigint NOT NULL,
    customer_id bigint,
    status      text,
    amount      numeric(18,2),
    created_at  timestamp,
    updated_at  timestamp,
    _loaded_at  timestamp DEFAULT now()
)
DISTRIBUTED BY (order_id);

-- События: append-only, большие объёмы — колоночное AO со сжатием и
-- дневными партициями. Без DEFAULT PARTITION: новые партиции создаёт gp_maintenance.
CREATE TABLE IF NOT EXISTS ods.web_events (
    event_id   bigint,
    user_id    bigint,
    event_type text,
    page_url   text,
    event_ts   timestamp,
    event_date date NOT NULL,
    _loaded_at timestamp DEFAULT now()
)
WITH (appendoptimized=true, orientation=column, compresstype=zstd, compresslevel=1)
DISTRIBUTED BY (event_id)
PARTITION BY RANGE (event_date)
(
    START (date '{{ macros.ds_add(ds, -7) }}') INCLUSIVE
    END (date '{{ macros.ds_add(ds, 8) }}') EXCLUSIVE
    EVERY (interval '1 day')
);

-- Маленький справочник — реплицируется на все сегменты, джойны без motion.
CREATE TABLE IF NOT EXISTS ods.partners (
    partner_id   bigint,
    partner_name text,
    country      text,
    _loaded_at   timestamp DEFAULT now()
)
DISTRIBUTED REPLICATED;

-- Витрина. RANDOMLY: распределение по дате дало бы перекос при фильтре по дню.
CREATE TABLE IF NOT EXISTS dm.sales_daily (
    report_date      date NOT NULL,
    customer_segment text NOT NULL,
    orders_cnt       bigint,
    customers_cnt    bigint,
    revenue          numeric(18,2),
    _built_at        timestamp DEFAULT now()
)
DISTRIBUTED RANDOMLY;

-- Слои хранилища и служебные объекты ETL.
CREATE SCHEMA IF NOT EXISTS etl;   -- служебные таблицы
CREATE SCHEMA IF NOT EXISTS stg;   -- staging для загрузок
CREATE SCHEMA IF NOT EXISTS ext;   -- внешние таблицы PXF / gpfdist
CREATE SCHEMA IF NOT EXISTS ods;   -- детальный слой
CREATE SCHEMA IF NOT EXISTS dm;    -- витрины

-- Журнал загрузок. Heap, а не AO: частые однострочные вставки.
CREATE TABLE IF NOT EXISTS etl.load_log (
    dag_id         text        NOT NULL,
    task_id        text        NOT NULL,
    run_id         text        NOT NULL,
    target_table   text        NOT NULL,
    rows_loaded    bigint      NOT NULL,
    interval_start timestamptz,
    interval_end   timestamptz,
    loaded_at      timestamptz NOT NULL DEFAULT now()
)
DISTRIBUTED RANDOMLY;

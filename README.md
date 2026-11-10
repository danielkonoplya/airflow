# Airflow ↔ Greenplum

DAG'и Apache Airflow для интеграции с Greenplum: загрузка из PostgreSQL, загрузка и выгрузка
через внешние таблицы (PXF / gpfdist), витрины с проверками качества, мониторинг и обслуживание кластера.

Рассчитано на **Airflow 2.10** и **Greenplum 6.x** (в т.ч. Arenadata DB 6).

## DAG'и

| DAG | Расписание | Что делает |
|---|---|---|
| `gp_bootstrap` | вручную | Схемы `etl/stg/ext/ods/dm`, журнал `etl.load_log`; опционально `CREATE EXTENSION pxf` и демо-таблицы |
| `gp_pg2gp__<name>` | из конфига | PostgreSQL → GP через `COPY`: инкрементальный upsert или полная перезаливка |
| `gp_ext_import__<name>` | из конфига | Загрузка через readable external table (PXF, gpfdist), ожидание файла в S3 |
| `gp_ext_export__<name>` | cron или Dataset | Выгрузка запроса через writable external table (PXF → S3) |
| `gp_dm_sales_daily` | Dataset'ы ODS | Пересборка витрины, DQ-проверки, сверка с ODS |
| `gp_healthcheck` | каждые 15 мин | Сегменты, свободное место, долгие запросы, блокировки |
| `gp_maintenance` | ежедневно 02:00 | Партиции, ANALYZE, bloat, skew, VACUUM каталога |

DAG'и `gp_pg2gp__*` публикуют Dataset `greenplum://dwh/<schema.table>`, витрины и выгрузки подписываются на него:

```
gp_pg2gp__shop_orders ────┐
                          ├─► gp_dm_sales_daily ─► gp_ext_export__sales_daily_s3
gp_pg2gp__shop_customers ─┘
```

## Структура

```
dags/
  gp_*.py           DAG'и и фабрики DAG'ов
  gp_lib/           общая библиотека: конфиги, SQL-билдеры, партиции, проверки, hook, оператор, алерты
  configs/          pg_to_gp.yaml, external_tables.yaml, ops.yaml
  sql/              bootstrap/ и dm/ — Jinja-шаблоны SQL
  .airflowignore    исключает gp_lib/, configs/, sql/ из парсинга DAG'ов
requirements.txt    провайдеры postgres, common-sql, amazon
```

## Деплой

Весь каталог `dags/` целиком кладётся в dags folder Airflow (git-sync или образ): DAG'и импортируют
`gp_lib` и читают `configs/` и `sql/` относительно себя. Провайдеры из `requirements.txt`
уже входят в официальный образ `apache/airflow`; если образ свой — их нужно доустановить.

### Connections

| Conn ID | Тип | Назначение |
|---|---|---|
| `greenplum_default` | Postgres | Greenplum |
| `oltp_postgres` | Postgres | источник для `gp_pg2gp__*` |
| `s3_default` | AWS | сенсор и очистка S3 для `gp_ext_*` |

Необязательная Variable `gp_alert_webhook_url` — webhook для алертов о падениях задач (тело `{"text": "..."}`).

### Что должно быть в Greenplum

Создаётся `gp_bootstrap` (`sql/bootstrap/001_core.sql`) или вашими миграциями:

- схема `stg` — staging-таблицы загрузок `stg.<schema>__<table>`;
- схема `ext` — временные внешние таблицы;
- таблица `etl.load_log` — журнал загрузок `gp_pg2gp__*`;
- `CREATE EXTENSION pxf` и PXF-серверы — для `gp_ext_*` через PXF;
- целевые таблицы из конфигов.

`sql/bootstrap/002_demo_tables.sql` создаёт демо-таблицы под примеры в конфигах.

## Загрузка PostgreSQL → Greenplum

Каждая запись в `configs/pg_to_gp.yaml` превращается в DAG `gp_pg2gp__<name>`:

```yaml
- name: shop_payments
  source_table: public.payments
  target_table: ods.shop_payments
  columns: [payment_id, order_id, amount, updated_at]
  primary_key: [payment_id]
  increment_column: updated_at
```

Как работает:

1. `COPY (SELECT ... WHERE updated_at >= start AND updated_at < end) TO STDOUT` из источника
   во временный файл воркера.
2. В одной транзакции GP: `TRUNCATE` staging → `COPY FROM STDIN` → `DELETE USING` + `INSERT`
   последней версии строки по ключу → запись в `etl.load_log`.

- **incremental** — окно = data interval DAG'а плюс `lookback_minutes`; повторные запуски и backfill идемпотентны.
- **full** — `TRUNCATE` + `INSERT`; падает, если источник вернул 0 строк (разрешается `allow_empty_full_load`).
- Первичная загрузка — ручной запуск с параметром `full_reload=true`: выгружается вся таблица, upsert по ключу.
- `distributed_by` по умолчанию = `primary_key`: staging и цель распределены одинаково, upsert идёт без motion.

Ограничения: `increment_column` в источнике должна обновляться при каждом изменении строки и иметь индекс;
удаления в источнике не ловятся (нужен soft delete или CDC). Данные идут через воркер Airflow — для больших
объёмов используйте PXF.

## Внешние таблицы

Записи `imports` / `exports` в `configs/external_tables.yaml`. Всё выполняется одной транзакцией:
`CREATE EXTERNAL TABLE ext.r_<name>` → загрузка → `DROP`.

Режимы импорта:

- `replace_slice` — удалить срез (`slice_column = '{{ ds }}'`) и вставить строки только этого среза;
- `append`;
- `full` — `TRUNCATE` + `INSERT`.

`reject_limit` включает `LOG ERRORS SEGMENT REJECT LIMIT`: битые строки не роняют загрузку и видны в
`gp_read_error_log('ext.r_<name>')`. Экспорт в S3 перед записью очищает префикс `s3_cleanup`,
так как запись во writable external table не транзакционна.

В `location`, `slice_value` и `s3_cleanup.prefix` работают Jinja-макросы Airflow (`{{ ds }}`, `{{ ds_nodash }}`).

## Мониторинг и обслуживание

Пороги и политики — в `configs/ops.yaml`.

- `gp_healthcheck` падает при недоступном сегменте или нехватке места (срабатывает алерт);
  несбалансированные сегменты, рассинхрон зеркал, долгие запросы и блокировки пишутся в лог как предупреждения.
- `gp_maintenance` создаёт партиции на `premake` периодов вперёд и удаляет старше `retention`
  только при `drop_enabled: true`. Таблицы должны быть без `DEFAULT PARTITION`.

## Особенности Greenplum 6

- Нет `INSERT ... ON CONFLICT` и `MERGE` — upsert через `DELETE USING` + `INSERT`.
- Партиции читаются из `pg_partitions`, которого нет в GP7.
- Staging не пересоздаётся на каждый запуск: `DROP/CREATE` раздувает системный каталог.

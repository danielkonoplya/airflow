# AGENTS.md

Инструкции для AI-ассистентов (Codex, Cursor, Copilot, Claude Code и др.) при работе с этим репозиторием.
Правила обязательны для всех; их соблюдение проверяет CI.

## Что это

DAG'и Airflow 2.10 для интеграции с Greenplum 6.x (в т.ч. Arenadata DB 6). Airflow уже развёрнут в Kubernetes:
в репозитории только DAG'и, их библиотека, тесты и CI (GitHub Actions), без инфраструктуры. Каталог `dags/` целиком
кладётся в dags folder (git-sync/образ). Подробности для людей — в `README.md`.

## Соглашения

Ветки, коммиты, заголовки PR и нейминг DAG'ов/задач/таблиц/SQL — строго по `CONTRIBUTING.md`; формат проверяет
CI (`scripts/check_conventions.py`, `tests/test_dag_integrity.py`). Кратко:

- Ветка = ключ задачи Jira в нижнем регистре через `_`: `GAU-23819` -> `gau_23819`, без префиксов и описания;
  от `main`, одна задача = одна ветка = один PR.
- Коммит и заголовок PR: `<type>(<scope>): <GAU-123> <описание по-русски>` (Conventional Commits, без точки).
  Ключ в заголовке PR = ключ из имени ветки (`gau_23819` -> `GAU-23819`). Слияние — squash.
- `dag_id` одиночного DAG'а = имя файла; из фабрики — `gp_<фабрика>__<name>`. `task_id` — `snake_case`, глагол.
- Описание PR — по `.github/pull_request_template.md`, включая «Как проверено».

## Команды

```bash
ruff check .                                        # линтер (правила в ruff.toml)
pytest -q tests/test_configs.py                     # валидация YAML-конфигов, нужен только PyYAML
python scripts/check_conventions.py title "<заголовок PR>" --branch <ветка>
```

Проверка импорта DAG'ов и соглашений по неймингу требует установленного Airflow, а на нативном Windows он не работает —
её делает job `dagbag` в CI; локально — WSL/Linux:

```bash
pip install "apache-airflow==2.10.5" -r requirements.txt pytest \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt
pytest -q tests/test_dag_integrity.py
```

## Архитектура

- **Корень импорта — `dags/`.** DAG'и импортируют `gp_lib.*`; `.airflowignore` исключает `gp_lib/`, `configs/`, `sql/`
  из парсинга. Пути к `configs/` и `sql/` вычисляются от `__file__` (`config.CONFIG_DIR`, `defaults.SQL_DIR`).
- **Разделение `gp_lib`:** `config`, `sql_builders`, `partitions`, `checks` — чистый Python без импорта Airflow
  (их можно проверять без окружения); `hooks`, `operators`, `callbacks`, `defaults` зависят от Airflow.
  Сохраняйте это разделение.
- **Конфиг-ориентированные фабрики.** `gp_pg2gp.py` и `gp_external_tables.py` на этапе парсинга читают YAML
  (`configs/pg_to_gp.yaml`, `configs/external_tables.yaml`) и регистрируют DAG на каждую запись через
  `globals()[dag_id]`. `gp_healthcheck.py`/`gp_maintenance.py` берут параметры из `configs/ops.yaml`.
  Новая таблица/загрузка — это запись в YAML, а не новый DAG-файл.
- **Валидация конфигов** (`gp_lib/config.py`): dataclass'ы + строгие проверки (неизвестные ключи, идентификаторы
  `^[a-z_][a-z0-9_]*$`, `schema.table`, запрет `'`/`;` в строках, которые попадают в SQL-литералы).
  `sql_builders` подставляет провалидированные имена без экранирования; имена из системного каталога
  экранируются через `quote_ident`/`quote_table`. Новые поля конфига, попадающие в SQL, нужно так же валидировать.
- **Связи между DAG'ами — через Datasets** `greenplum://dwh/<schema.table>` (`defaults.gp_dataset`):
  загрузки публикуют их в `outlets`, витрина `gp_dm_sales_daily` и экспорты (`schedule_on`) подписываются.
- **PostgreSQL → GP** (`operators.PostgresToGreenplumOperator`): выгрузка `COPY ... TO STDOUT` в отдельном потоке
  пишет в `os.pipe`, основной поток читает его в `COPY FROM STDIN` staging GP (без диска). Ошибка выгрузки
  пробрасывается после `join` и откатывает транзакцию; при падении GP закрытие reader'а будит поток через
  BrokenPipe. Дальше в той же транзакции: upsert → строка в `etl.load_log`. Окно = data interval + lookback;
  параметр запуска `full_reload`.
- **Внешние таблицы**: список SQL-операторов (`CREATE EXTERNAL` → load → `DROP`) выполняется
  `SQLExecuteQueryOperator` в одной транзакции; Jinja (`{{ ds }}`) рендерится в этих строках.
  `replace_partition` грузит в `stg.<schema>__<table>__part` и делает `EXCHANGE PARTITION ... WITHOUT VALIDATION`.
- **Минимум задач = минимум подов** (Airflow в Kubernetes): healthcheck, витрина и экспорт — по одной задаче;
  DQ витрины — `DO`-блок с `RAISE EXCEPTION` внутри транзакции сборки (`sql/dm/sales_daily.sql`), не отдельные
  check-операторы; S3-сенсор — `deferrable=True` (нужен triggerer). Хуки/провайдеры/`requests` импортируются
  внутри задач, а не на уровне модулей, чтобы не замедлять парсинг.
- **Общие `default_args`** (`defaults.DEFAULT_ARGS`): owner `data-platform`, ретраи, `notify_failure`
  (webhook из Variable `gp_alert_webhook_url`). Все DAG'и с тегом `greenplum`, `catchup=False`.

## Ограничения Greenplum 6, на которые завязан код

- Нет `INSERT ... ON CONFLICT`/`MERGE` → upsert только `DELETE USING` + `INSERT` с дедупликацией `row_number()`.
- Партиции читаются из `pg_partitions` (нет в GP7), DDL партиций — legacy-синтаксис; таблицы без `DEFAULT PARTITION`.
- Не создавать/удалять объекты на каждом запуске (каталог раздувается): staging создаётся `IF NOT EXISTS` и чистится `TRUNCATE`.
- `VACUUM` — только вне транзакции (`GreenplumHook.run_autocommit`).
- Staging распределяется так же, как целевая таблица (`distributed_by`, по умолчанию = PK).

## Git

Работа идёт в фича-ветках на `origin`; `main` меняется только слиянием PR. В `main` напрямую не пушить.

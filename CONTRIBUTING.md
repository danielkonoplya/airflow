# Соглашения по разработке

Единые правила для людей и агентов (Claude читает их через `CLAUDE.md`). Формат веток, коммитов и заголовков PR
проверяет CI (`.github/workflows/ci.yml`), поэтому PR с нарушениями не сливается.

## Поток работы

1. Задача заведена в Jira с критериями приёмки. Ключ задачи (`PROJ-123`) — сквозной идентификатор: он есть в имени
   ветки, в заголовке PR и в коммитах.
2. Ветка создаётся от `main` (пока код не слит в `main` — от актуальной фича-ветки).
3. Один PR = одна задача. `main` меняется только через PR с зелёным CI и approve; в `main` напрямую не пушить.
4. Слияние — **squash merge**: в `main` попадает один коммит, его сообщение = заголовок PR.
5. После слияния git-sync подтягивает `dags/` в Airflow — отдельного шага деплоя нет.

## Ветки

```
feature/PROJ-123-short-desc   новая функциональность
fix/PROJ-123-short-desc       исправление
hotfix/PROJ-123-short-desc    срочное исправление прода
chore/short-desc              обслуживание без задачи в Jira (зависимости, CI, документация)
docs/short-desc
ci/short-desc
```

Описание — латиница в нижнем регистре через дефис, 2–5 слов. Регулярка из CI:
`^(feature|fix|hotfix)/[A-Z][A-Z0-9]+-[0-9]+-[a-z0-9-]+$` или `^(chore|docs|ci)/[a-z0-9-]+$`.

## Коммиты и заголовки PR

[Conventional Commits](https://www.conventionalcommits.org/), описание — на русском, без точки в конце:

```
<type>(<scope>): <KEY-123> <что сделано>

feat(pg2gp): PROJ-123 потоковый COPY без промежуточного файла
fix(healthcheck): PROJ-130 не падать на пустом pg_partitions
perf(dm): PROJ-141 EXCHANGE PARTITION вместо DELETE в sales_daily
chore(ci): добавить проверку DagBag
```

- `type`: `feat`, `fix`, `perf`, `refactor`, `test`, `docs`, `chore`, `ci`, `revert`. `!` после scope — ломающее
  изменение (например, переименование `dag_id` или колонок витрины).
- `scope`: `dags`, `gp_lib`, `configs`, `sql`, `pg2gp`, `ext`, `dm`, `healthcheck`, `maintenance`, `bootstrap`,
  `ci`, `docs`, `deps`.
- Ключ Jira обязателен в заголовке PR из веток `feature/`, `fix/`, `hotfix/` и должен совпадать с ключом в имени
  ветки. В промежуточных коммитах ветки ключ желателен, но не обязателен — они схлопываются при squash.
- Тело коммита (необязательно): *почему* сделано изменение, а не пересказ диффа.

## Нейминг в коде

| Что | Правило | Пример |
|---|---|---|
| Файл DAG'а | `dags/gp_<назначение>.py` | `gp_dm_sales_daily.py` |
| `dag_id` одиночного DAG'а | = имя файла без `.py` | `gp_healthcheck` |
| `dag_id` из YAML-фабрики | `gp_<фабрика>__<name>` (двойное подчёркивание) | `gp_pg2gp__shop_orders`, `gp_ext_import__web_events_s3` |
| `dag_id` витрины | `gp_dm_<сущность>_<частота>` | `gp_dm_sales_daily` |
| `name` в YAML | `snake_case`, уникален в файле | `partners_gpfdist` |
| `task_id` | `<глагол>[_<объект>]`, `snake_case` | `load`, `wait_for_source`, `build_sales_daily` |
| Схемы GP | `stg` — staging, `ods` — копии источников, `dm` — витрины, `ext` — внешние таблицы, `etl` — служебные | `ods.shop_orders` |
| Внешние таблицы | `ext.r_<name>` — чтение, `ext.w_<name>` — запись | `ext.r_partners_gpfdist` |
| Staging партиций | `stg.<schema>__<table>__part` | `stg.ods__web_events__part` |
| SQL-файлы | `dags/sql/<слой>/<объект>.sql`; bootstrap — `NNN_<описание>.sql` | `sql/dm/sales_daily.sql` |
| Идентификаторы | `^[a-z_][a-z0-9_]*$` (валидирует `gp_lib/config.py`) | |

Все DAG'и: тег `greenplum`, `catchup=False`, `default_args=defaults.DEFAULT_ARGS`.

## Кейсы: где описывать

- **Что и зачем делать** — в задаче Jira: контекст, критерии приёмки, затрагиваемые таблицы/DAG'и. Агент берёт
  задачу в работу только при наличии критериев приёмки.
- **Как устроено** — в `CLAUDE.md` (кратко, для агентов) и `README.md` (для людей). Новое архитектурное решение
  дописывается туда в том же PR.
- **Как проверено** — в описании PR по шаблону `.github/pull_request_template.md`.

## Локальная проверка

```bash
pip install pre-commit && pre-commit install   # ruff, LF-окончания, YAML, проверка формата коммита
pre-commit run --all-files
```

Полную проверку импорта DAG'ов (DagBag) делает CI на Linux; локально — только в WSL/Linux, см. `CLAUDE.md`.

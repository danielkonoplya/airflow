"""
### gp_maintenance

Ночное обслуживание Greenplum (настройки — `configs/ops.yaml`):

1. **партиции** — создание партиций наперёд и удаление старше retention (только при `drop_enabled`);
2. **статистика** — ANALYZE таблиц из `gp_toolkit.gp_stats_missing`;
3. **bloat** — отчёт по `gp_toolkit.gp_bloat_diag`, опционально VACUUM;
4. **skew** — перекос по размеру данных на сегментах (без полного прохода по таблицам);
5. **каталог** — VACUUM ANALYZE системного каталога (после DDL, раздувающего каталог).
"""

import logging
from datetime import timedelta
from typing import Dict, List

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

from gp_lib import checks, partitions
from gp_lib.config import PartitionPolicy, load_ops
from gp_lib.defaults import DEFAULT_ARGS
from gp_lib.sql_builders import quote_table

log = logging.getLogger(__name__)

_, MAINT = load_ops()


def _hook():
    from gp_lib.hooks import GreenplumHook

    return GreenplumHook(MAINT.gp_conn_id)


def partition_statements(hook, policy: PartitionPolicy, today: pendulum.Date) -> List[str]:
    schema, table = policy.table.split(".")
    rows = hook.fetch_dicts(partitions.PARTITIONS_SQL, (schema, table))
    if not rows:
        raise AirflowFailException(f"{policy.table} не партиционирована или не существует")
    if any(r["partitionisdefault"] for r in rows):
        raise AirflowFailException(
            f"У {policy.table} есть DEFAULT PARTITION — ADD PARTITION невозможен, нужен SPLIT DEFAULT PARTITION"
        )

    existing = [
        partitions.PartitionRange(
            partitions.parse_range_bound(r["partitionrangestart"]),
            partitions.parse_range_bound(r["partitionrangeend"]),
            r["partitionname"],
        )
        for r in rows
    ]
    plan = partitions.plan_partitions(existing, today, policy.granularity, policy.premake, policy.retention)

    statements = [partitions.add_partition_sql(policy.table, p) for p in plan.to_add]
    if plan.to_drop and not policy.drop_enabled:
        log.warning("%s: %s партиций старше retention, удаление выключено (drop_enabled=false)",
                    policy.table, len(plan.to_drop))
    elif plan.to_drop:
        statements += [partitions.drop_partition_sql(policy.table, p) for p in plan.to_drop]
    return statements


@dag(
    dag_id="gp_maintenance",
    schedule=MAINT.schedule,
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=5),
    default_args={**DEFAULT_ARGS, "retries": 1},
    tags=["greenplum", "maintenance"],
    doc_md=__doc__,
)
def gp_maintenance():
    @task(execution_timeout=timedelta(minutes=30))
    def manage_partitions(ds: str = None) -> Dict[str, int]:
        """Все таблицы — одна задача; каждая таблица в своей транзакции, ошибка одной не мешает остальным."""
        hook, today = _hook(), pendulum.parse(ds).date()
        changed, failed = {}, []
        for policy in MAINT.partitions:
            try:
                statements = partition_statements(hook, policy, today)
                if statements:
                    hook.run(statements)  # одна транзакция на таблицу
                for stmt in statements:
                    log.info(stmt)
                changed[policy.table] = len(statements)
            except Exception:  # noqa: BLE001
                log.exception("Не удалось обработать партиции %s", policy.table)
                failed.append(policy.table)
        if failed:
            raise AirflowFailException(f"Ошибки управления партициями: {failed}")
        return changed

    @task(execution_timeout=timedelta(hours=2))
    def analyze_missing_stats() -> int:
        hook = _hook()
        rows = hook.fetch_dicts(checks.STATS_MISSING_SQL, {"limit": MAINT.analyze_missing_stats_limit})
        statements = [f"ANALYZE {quote_table(r['smischema'], r['smitable'])}" for r in rows]
        hook.run_autocommit(statements)
        log.info("Проанализировано таблиц: %s", len(statements))
        return len(statements)

    @task(execution_timeout=timedelta(hours=2))
    def bloat_report() -> List[str]:
        hook = _hook()
        rows = hook.fetch_dicts(checks.BLOAT_SQL)
        tables = [quote_table(r["bdinspname"], r["bdirelname"]) for r in rows]
        for r in rows:
            log.warning("Bloat %s.%s: %s (страниц %s, ожидалось %s)", r["bdinspname"], r["bdirelname"],
                        r["bdidiag"], r["bdirelpages"], r["bdiexppages"])
        if MAINT.vacuum_bloated:
            hook.run_autocommit([f"VACUUM ANALYZE {t}" for t in tables])
        return tables

    @task(execution_timeout=timedelta(minutes=30))
    def skew_report() -> Dict[str, float]:
        hook = _hook()
        segments = [r[0] for r in hook.get_records(checks.PRIMARY_SEGMENTS_SQL)]
        report = {}
        for table in MAINT.skew_tables:
            schema, name = table.split(".")
            oids = [r[0] for r in hook.get_records(
                checks.SKEW_RELATIONS_SQL, {"table": table, "schema": schema, "name": name}
            )]
            sizes = dict(hook.get_records(checks.SEGMENT_SIZES_SQL, {"oids": oids}))
            coef = checks.skew_coefficient([sizes.get(s, 0) for s in segments])
            report[table] = round(coef, 4)
            if coef > MAINT.skew_threshold:
                log.warning("Перекос %s: %.3f > %.3f — проверьте ключ распределения", table, coef, MAINT.skew_threshold)
        return report

    @task(execution_timeout=timedelta(hours=1))
    def vacuum_catalog() -> None:
        _hook().run_autocommit([f"VACUUM ANALYZE pg_catalog.{t}" for t in checks.CATALOG_TABLES])

    ddl = manage_partitions() if MAINT.partitions else None
    analyze = analyze_missing_stats()
    if ddl is not None:
        ddl >> analyze
    if MAINT.catalog_vacuum:
        analyze >> vacuum_catalog()
    bloat_report()
    if MAINT.skew_tables:
        skew_report()


gp_maintenance()

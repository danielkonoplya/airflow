"""
### gp_healthcheck

Мониторинг Greenplum каждые 15 минут (настройки — `configs/ops.yaml`):

* сегменты: down, не в preferred role, рассинхрон primary/mirror;
* свободное место на сегментных хостах (`gp_toolkit.gp_disk_free`);
* долгие активные запросы и ожидающие блокировки.

Критичные проблемы роняют задачу `summarize` → срабатывает алерт.
Предупреждения только пишутся в лог.
"""

import logging
from datetime import timedelta
from typing import Dict, List

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

from gp_lib import checks
from gp_lib.config import load_ops
from gp_lib.defaults import DEFAULT_ARGS
from gp_lib.hooks import GreenplumHook

log = logging.getLogger(__name__)

HEALTH, _ = load_ops()


@dag(
    dag_id="gp_healthcheck",
    schedule=HEALTH.schedule,
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args={**DEFAULT_ARGS, "retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["greenplum", "monitoring"],
    doc_md=__doc__,
)
def gp_healthcheck():
    def hook() -> GreenplumHook:
        return GreenplumHook(HEALTH.gp_conn_id)

    @task(execution_timeout=timedelta(minutes=2))
    def check_connection() -> str:
        version = hook().get_first("SELECT version()")[0]
        log.info("Greenplum: %s", version)
        return version

    @task(execution_timeout=timedelta(minutes=2))
    def check_segments() -> Dict[str, List[str]]:
        return checks.evaluate_segments(hook().fetch_dicts(checks.SEGMENTS_SQL)).as_dict()

    @task(execution_timeout=timedelta(minutes=5))
    def check_disk() -> Dict[str, List[str]]:
        rows = hook().fetch_dicts(checks.DISK_FREE_SQL)
        return checks.evaluate_disk(rows, HEALTH.min_free_disk_gb).as_dict()

    @task(execution_timeout=timedelta(minutes=2))
    def check_long_queries() -> Dict[str, List[str]]:
        rows = hook().fetch_dicts(checks.LONG_QUERIES_SQL, {"threshold": HEALTH.long_query_minutes})
        result = checks.CheckResult()
        for r in rows:
            result.warnings.append(f"Долгий запрос pid={r['pid']} user={r['usename']} {r['duration']}: {r['query']}")
        return result.as_dict()

    @task(execution_timeout=timedelta(minutes=2))
    def check_waiting_locks() -> Dict[str, List[str]]:
        waiting = hook().get_first(checks.WAITING_LOCKS_SQL)[0]
        result = checks.CheckResult()
        if waiting > HEALTH.waiting_locks_threshold:
            result.warnings.append(f"Ожидающих блокировок: {waiting} (порог {HEALTH.waiting_locks_threshold})")
        return result.as_dict()

    @task
    def summarize(results: List[Dict[str, List[str]]]) -> Dict[str, List[str]]:
        merged = checks.CheckResult.merge(results)
        for w in merged.warnings:
            log.warning(w)
        if merged.critical:
            raise AirflowFailException("Критичные проблемы Greenplum:\n" + "\n".join(merged.critical))
        return merged.as_dict()

    connection = check_connection()
    results = [check_segments(), check_disk(), check_long_queries(), check_waiting_locks()]
    connection >> results
    summarize(results)


gp_healthcheck()

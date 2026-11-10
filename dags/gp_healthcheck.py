"""
### gp_healthcheck

Мониторинг Greenplum каждые 15 минут (настройки — `configs/ops.yaml`):

* сегменты: down, не в preferred role, рассинхрон primary/mirror;
* свободное место на сегментных хостах (`gp_toolkit.gp_disk_free`);
* долгие активные запросы и ожидающие блокировки.

Все проверки — одна задача и одно подключение: в Kubernetes каждая задача
стоит отдельного пода, а сами запросы к каталогу занимают доли секунды.
Сбой одной проверки не мешает остальным. Критичные проблемы и ошибки проверок
роняют задачу (срабатывает алерт), предупреждения пишутся в лог.
"""

import logging
from contextlib import closing
from datetime import timedelta
from typing import Callable, Dict, List

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

from gp_lib import checks
from gp_lib.config import load_ops
from gp_lib.defaults import DEFAULT_ARGS

log = logging.getLogger(__name__)

HEALTH, _ = load_ops()


def _fetch(cur, sql: str, params=None) -> List[Dict]:
    cur.execute(sql, params)
    names = [d[0] for d in cur.description]
    return [dict(zip(names, row)) for row in cur.fetchall()]


def check_segments(cur) -> checks.CheckResult:
    return checks.evaluate_segments(_fetch(cur, checks.SEGMENTS_SQL))


def check_disk(cur) -> checks.CheckResult:
    return checks.evaluate_disk(_fetch(cur, checks.DISK_FREE_SQL), HEALTH.min_free_disk_gb)


def check_long_queries(cur) -> checks.CheckResult:
    result = checks.CheckResult()
    for r in _fetch(cur, checks.LONG_QUERIES_SQL, {"threshold": HEALTH.long_query_minutes}):
        result.warnings.append(f"Долгий запрос pid={r['pid']} user={r['usename']} {r['duration']}: {r['query']}")
    return result


def check_waiting_locks(cur) -> checks.CheckResult:
    result = checks.CheckResult()
    cur.execute(checks.WAITING_LOCKS_SQL)
    waiting = cur.fetchone()[0]
    if waiting > HEALTH.waiting_locks_threshold:
        result.warnings.append(f"Ожидающих блокировок: {waiting} (порог {HEALTH.waiting_locks_threshold})")
    return result


CHECKS: List[Callable] = [check_segments, check_disk, check_long_queries, check_waiting_locks]


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
    @task(execution_timeout=timedelta(minutes=5))
    def healthcheck() -> Dict[str, List[str]]:
        from gp_lib.hooks import GreenplumHook

        merged = checks.CheckResult()
        with closing(GreenplumHook(HEALTH.gp_conn_id).get_conn()) as conn:
            conn.autocommit = True  # ошибка одной проверки не должна обрывать транзакцию остальных
            with closing(conn.cursor()) as cur:
                cur.execute("SET statement_timeout = '60s'")
                for check in CHECKS:
                    try:
                        result = check(cur)
                    except Exception as exc:  # noqa: BLE001
                        log.exception("Проверка %s упала", check.__name__)
                        result = checks.CheckResult(critical=[f"{check.__name__}: {exc}"])
                    merged.critical.extend(result.critical)
                    merged.warnings.extend(result.warnings)

        for w in merged.warnings:
            log.warning(w)
        if merged.critical:
            raise AirflowFailException("Критичные проблемы Greenplum:\n" + "\n".join(merged.critical))
        return merged.as_dict()

    healthcheck()


gp_healthcheck()

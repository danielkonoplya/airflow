"""Операторы интеграции с Greenplum."""

from __future__ import annotations

import tempfile
from contextlib import closing
from datetime import timedelta
from typing import Any, Optional, Sequence

from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from gp_lib import sql_builders as sb
from gp_lib.config import DEFAULT_GP_CONN_ID
from gp_lib.hooks import GreenplumHook


class PostgresToGreenplumOperator(BaseOperator):
    """Переливает таблицу PostgreSQL в Greenplum через COPY.

    Источник -> COPY TO STDOUT -> spooled-файл -> COPY FROM STDIN в staging GP
    -> upsert (incremental) или TRUNCATE+INSERT (full) в одной транзакции.

    incremental: окно [data_interval_start - lookback, data_interval_end) по
    increment_column. Повторный запуск того же интервала идемпотентен.
    Параметр запуска full_reload=true выгружает всю таблицу и делает upsert —
    для первичной загрузки или восстановления после сбоя.
    """

    template_fields = ("source_table", "target_table")
    ui_color = "#d8ecd8"

    def __init__(
        self,
        *,
        source_conn_id: str,
        source_table: str,
        target_table: str,
        columns: Sequence[str],
        primary_key: Sequence[str],
        distributed_by: Sequence[str],
        mode: str = "incremental",
        increment_column: Optional[str] = None,
        lookback: timedelta = timedelta(0),
        allow_empty_full_load: bool = False,
        gp_conn_id: str = DEFAULT_GP_CONN_ID,
        spool_max_bytes: int = 256 * 1024 * 1024,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.source_conn_id = source_conn_id
        self.source_table = source_table
        self.target_table = target_table
        self.columns = list(columns)
        self.primary_key = list(primary_key)
        self.distributed_by = list(distributed_by)
        self.mode = mode
        self.increment_column = increment_column
        self.lookback = lookback
        self.allow_empty_full_load = allow_empty_full_load
        self.gp_conn_id = gp_conn_id
        self.spool_max_bytes = spool_max_bytes

    def execute(self, context) -> int:
        full_reload = bool(context["params"].get("full_reload", False))
        windowed = self.mode == "incremental" and not full_reload
        start = context["data_interval_start"] - self.lookback
        end = context["data_interval_end"]

        query = sb.extract_query(self.source_table, self.columns, self.increment_column if windowed else None)
        params = {"start": start, "end": end} if windowed else None
        self.log.info("Режим %s%s, окно: %s", self.mode, " (full_reload)" if full_reload else "",
                      f"[{start}, {end})" if windowed else "вся таблица")

        with tempfile.SpooledTemporaryFile(max_size=self.spool_max_bytes, mode="w+b") as buf:
            self._extract(query, params, buf)
            self.log.info("Выгружено %.1f MB из %s", buf.tell() / 1024 / 1024, self.source_table)
            buf.seek(0)
            rows = self._load(buf, context, start if windowed else None, end if windowed else None)

        self.log.info("Загружено %s строк в %s", rows, self.target_table)
        return rows

    def _extract(self, query: str, params: Optional[dict], buf) -> None:
        hook = PostgresHook(postgres_conn_id=self.source_conn_id)
        with closing(hook.get_conn()) as conn, closing(conn.cursor()) as cur:
            sql = cur.mogrify(query, params).decode() if params else query
            cur.copy_expert(sb.copy_out(sql), buf)

    def _load(self, buf, context, interval_start, interval_end) -> int:
        staging = sb.staging_table(self.target_table)
        truncate_target = self.mode == "full" and not context["params"].get("full_reload", False)

        conn = GreenplumHook(self.gp_conn_id).get_conn()
        try:
            with closing(conn.cursor()) as cur:
                for stmt in sb.prepare_staging(staging, self.target_table, self.distributed_by):
                    cur.execute(stmt)
                cur.copy_expert(sb.copy_in(staging, self.columns), buf)
                rows = cur.rowcount

                if rows == 0 and truncate_target and not self.allow_empty_full_load:
                    raise AirflowFailException(
                        f"Источник {self.source_table} вернул 0 строк — полная перезаливка "
                        f"очистила бы {self.target_table}. Разрешите allow_empty_full_load, если это норма."
                    )

                if rows or truncate_target:
                    cur.execute(f"ANALYZE {staging}")
                    if truncate_target:
                        merge = sb.merge_full(self.target_table, staging, self.columns)
                    else:
                        merge = sb.merge_incremental(
                            self.target_table, staging, self.columns, self.primary_key, self.increment_column
                        )
                    for stmt in merge:
                        cur.execute(stmt)
                else:
                    self.log.info("Новых строк нет, merge пропущен")

                ti = context["task_instance"]
                cur.execute(
                    sb.LOAD_LOG_INSERT,
                    (ti.dag_id, ti.task_id, context["run_id"], self.target_table, rows, interval_start, interval_end),
                )
            conn.commit()
            return rows
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

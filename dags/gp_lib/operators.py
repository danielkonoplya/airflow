"""Операторы интеграции с Greenplum.

Хуки импортируются внутри execute: модуль импортируется при каждом парсинге
DAG-файлов, а провайдеры с драйверами нужны только при выполнении задачи.
"""

from __future__ import annotations

import os
import threading
from contextlib import closing
from datetime import timedelta
from typing import Any, List, Optional, Sequence

from airflow.exceptions import AirflowException, AirflowFailException
from airflow.models import BaseOperator

from gp_lib import sql_builders as sb
from gp_lib.config import DEFAULT_GP_CONN_ID

COPY_BUFFER_BYTES = 1024 * 1024


class PostgresToGreenplumOperator(BaseOperator):
    """Переливает таблицу PostgreSQL в Greenplum потоком через COPY.

    Источник COPY TO STDOUT и COPY FROM STDIN в staging GP работают одновременно
    через pipe: данные не копятся на диске воркера, время = max(выгрузка, загрузка).
    Затем в той же транзакции GP — upsert (incremental) или TRUNCATE+INSERT (full).

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

    def execute(self, context) -> int:
        from gp_lib.hooks import GreenplumHook

        full_reload = bool(context["params"].get("full_reload", False))
        windowed = self.mode == "incremental" and not full_reload
        truncate_target = self.mode == "full" and not full_reload
        start = context["data_interval_start"] - self.lookback if windowed else None
        end = context["data_interval_end"] if windowed else None

        query = sb.extract_query(self.source_table, self.columns, self.increment_column if windowed else None)
        params = {"start": start, "end": end} if windowed else None
        self.log.info("Режим %s%s, окно: %s", self.mode, " (full_reload)" if full_reload else "",
                      f"[{start}, {end})" if windowed else "вся таблица")

        staging = sb.staging_table(self.target_table)
        conn = GreenplumHook(self.gp_conn_id).get_conn()
        try:
            with closing(conn.cursor()) as cur:
                for stmt in sb.prepare_staging(staging, self.target_table, self.distributed_by):
                    cur.execute(stmt)
                rows = self._stream(query, params, cur, staging)
                self.log.info("Загружено в staging %s строк", rows)

                if rows == 0 and truncate_target and not self.allow_empty_full_load:
                    raise AirflowFailException(
                        f"Источник {self.source_table} вернул 0 строк — полная перезаливка "
                        f"очистила бы {self.target_table}. Разрешите allow_empty_full_load, если это норма."
                    )
                if rows or truncate_target:
                    for stmt in self._merge_statements(staging, truncate_target):
                        cur.execute(stmt)
                else:
                    self.log.info("Новых строк нет, merge пропущен")

                ti = context["task_instance"]
                cur.execute(
                    sb.LOAD_LOG_INSERT,
                    (ti.dag_id, ti.task_id, context["run_id"], self.target_table, rows, start, end),
                )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

        self.log.info("Загружено %s строк в %s", rows, self.target_table)
        return rows

    def _merge_statements(self, staging: str, truncate_target: bool) -> List[str]:
        # Статистика staging нужна планировщику для выбора join'а в DELETE USING.
        statements = [f"ANALYZE {staging}"]
        if truncate_target:
            return statements + sb.merge_full(self.target_table, staging, self.columns)
        return statements + sb.merge_incremental(
            self.target_table, staging, self.columns, self.primary_key, self.increment_column
        )

    def _stream(self, query: str, params: Optional[dict], gp_cursor, staging: str) -> int:
        """COPY источник -> pipe -> COPY staging GP. Возвращает число загруженных строк."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        source = PostgresHook(postgres_conn_id=self.source_conn_id)
        read_fd, write_fd = os.pipe()
        reader = os.fdopen(read_fd, "rb", buffering=COPY_BUFFER_BYTES)
        writer = os.fdopen(write_fd, "wb", buffering=COPY_BUFFER_BYTES)
        extract_errors: List[BaseException] = []

        def extract() -> None:
            try:
                with closing(source.get_conn()) as conn, closing(conn.cursor()) as cur:
                    sql = cur.mogrify(query, params).decode() if params else query
                    cur.copy_expert(sb.copy_out(sql), writer, size=COPY_BUFFER_BYTES)
            except BaseException as exc:  # noqa: BLE001 — ошибка передаётся в основной поток
                extract_errors.append(exc)
            finally:
                try:
                    writer.close()  # EOF для COPY FROM STDIN
                except OSError:
                    pass

        thread = threading.Thread(target=extract, name="pg-extract", daemon=True)
        thread.start()
        try:
            gp_cursor.copy_expert(sb.copy_in(staging, self.columns), reader, size=COPY_BUFFER_BYTES)
        finally:
            reader.close()  # если GP упал — выгрузка получит BrokenPipe и завершится
            thread.join()

        if extract_errors:
            # GP мог успеть принять часть данных до обрыва — транзакция откатится выше.
            raise AirflowException(f"Ошибка выгрузки из {self.source_table}") from extract_errors[0]
        return gp_cursor.rowcount

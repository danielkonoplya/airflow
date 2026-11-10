"""Хук Greenplum поверх PostgresHook (GP использует протокол PostgreSQL)."""

from __future__ import annotations

from contextlib import closing
from typing import Any, Dict, List, Optional

from airflow.providers.postgres.hooks.postgres import PostgresHook

from gp_lib.config import DEFAULT_GP_CONN_ID


class GreenplumHook(PostgresHook):
    def __init__(self, greenplum_conn_id: str = DEFAULT_GP_CONN_ID, **kwargs: Any) -> None:
        super().__init__(postgres_conn_id=greenplum_conn_id, **kwargs)

    def fetch_dicts(self, sql: str, parameters: Optional[Any] = None) -> List[Dict[str, Any]]:
        with closing(self.get_conn()) as conn, closing(conn.cursor()) as cur:
            cur.execute(sql, parameters)
            names = [d[0] for d in cur.description]
            return [dict(zip(names, row)) for row in cur.fetchall()]

    def run_autocommit(self, statements: List[str]) -> None:
        """VACUUM и подобные команды нельзя выполнять внутри транзакции."""
        if statements:
            self.run(statements, autocommit=True)

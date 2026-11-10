"""Общие настройки DAG'ов."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pendulum
from airflow.datasets import Dataset

from gp_lib.callbacks import notify_failure
from gp_lib.config import DEFAULT_GP_CONN_ID

GP_CONN_ID = DEFAULT_GP_CONN_ID
SQL_DIR = Path(__file__).resolve().parent.parent / "sql"

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": notify_failure,
}


def gp_dataset(table: str) -> Dataset:
    """Dataset таблицы GP: загрузки публикуют его, витрины на него подписываются."""
    return Dataset(f"greenplum://dwh/{table}")


def to_pendulum(d: date) -> pendulum.DateTime:
    return pendulum.datetime(d.year, d.month, d.day, tz="UTC")

"""Валидация YAML-конфигов. Нужен только PyYAML — Airflow не требуется."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "dags"))

from gp_lib import config  # noqa: E402


def test_pg_to_gp():
    config.load_pg_to_gp()


def test_external():
    config.load_external()


def test_ops():
    config.load_ops()

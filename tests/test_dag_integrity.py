"""Импорт всех DAG'ов и проверка соглашений из CONTRIBUTING.md. Требует установленного Airflow (Linux/WSL)."""

import re
from pathlib import Path

import pytest

pytest.importorskip("airflow")

DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"
FACTORY_DAG_ID_RE = re.compile(r"^gp_[a-z0-9]+(?:_[a-z0-9]+)*__[a-z0-9_]+$")
TASK_ID_RE = re.compile(r"^[a-z][a-z0-9_]*$")


@pytest.fixture(scope="session")
def dagbag():
    from airflow.models import DagBag

    return DagBag(str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dagbag):
    assert not dagbag.import_errors, "\n".join(f"{k}: {v}" for k, v in dagbag.import_errors.items())


def test_dags_found(dagbag):
    assert dagbag.dags, "в dags/ не найдено ни одного DAG'а"


def test_naming_and_defaults(dagbag):
    errors = []
    for dag_id, dag in sorted(dagbag.dags.items()):
        stem = Path(dag.fileloc).stem
        if "__" in dag_id:
            if not FACTORY_DAG_ID_RE.match(dag_id):
                errors.append(f"{dag_id}: dag_id фабрики должен быть gp_<фабрика>__<name>")
        elif dag_id != stem:
            errors.append(f"{dag_id}: dag_id одиночного DAG'а должен совпадать с именем файла ({stem})")
        if "greenplum" not in (dag.tags or []):
            errors.append(f"{dag_id}: нет тега 'greenplum'")
        if dag.catchup:
            errors.append(f"{dag_id}: catchup должен быть False")
        errors.extend(
            f"{dag_id}.{t.task_id}: task_id не snake_case" for t in dag.tasks if not TASK_ID_RE.match(t.task_id)
        )
    assert not errors, "\n".join(errors)

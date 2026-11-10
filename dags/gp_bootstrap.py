"""
### gp_bootstrap

Разовая инициализация Greenplum: схемы слоёв (etl, stg, ext, ods, dm),
журнал загрузок `etl.load_log`, опционально расширение PXF и демо-таблицы.

Запускается вручную; все объекты создаются через `IF NOT EXISTS`, повторный запуск безопасен.
"""

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from gp_lib.defaults import DEFAULT_ARGS, GP_CONN_ID, SQL_DIR


@dag(
    dag_id="gp_bootstrap",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={**DEFAULT_ARGS, "retries": 0},
    template_searchpath=[str(SQL_DIR)],
    params={
        "enable_pxf": Param(False, type="boolean", description="CREATE EXTENSION pxf (PXF должен быть установлен)"),
        "with_demo": Param(True, type="boolean", description="Создать демо-таблицы ods/dm"),
    },
    tags=["greenplum", "bootstrap"],
    doc_md=__doc__,
)
def gp_bootstrap():
    core = SQLExecuteQueryOperator(task_id="create_core_objects", conn_id=GP_CONN_ID, sql="bootstrap/001_core.sql")

    @task.short_circuit
    def pxf_requested(**context) -> bool:
        return bool(context["params"]["enable_pxf"])

    @task.short_circuit
    def demo_requested(**context) -> bool:
        return bool(context["params"]["with_demo"])

    enable_pxf = SQLExecuteQueryOperator(
        task_id="enable_pxf", conn_id=GP_CONN_ID, sql="CREATE EXTENSION IF NOT EXISTS pxf"
    )
    demo_tables = SQLExecuteQueryOperator(
        task_id="create_demo_tables", conn_id=GP_CONN_ID, sql="bootstrap/002_demo_tables.sql"
    )

    core >> pxf_requested() >> enable_pxf
    core >> demo_requested() >> demo_tables


gp_bootstrap()

"""
### gp_dm_sales_daily

Витрина `dm.sales_daily`. Запускается по Dataset'ам: после обновления
`ods.shop_orders` и `ods.shop_customers` загрузочными DAG'ами.

Одна задача и одна транзакция (`sql/dm/sales_daily.sql`): пересборка окна
последних `lookback_days` дней, проверки качества (NULL-ы, отрицательная
выручка, пустое окно) и сверка выручки с ODS. Если проверка не прошла,
транзакция откатывается и витрина остаётся в прежнем состоянии.
"""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from gp_lib.defaults import DEFAULT_ARGS, GP_CONN_ID, SQL_DIR, gp_dataset

with DAG(
    dag_id="gp_dm_sales_daily",
    schedule=[gp_dataset("ods.shop_orders"), gp_dataset("ods.shop_customers")],
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    default_args=DEFAULT_ARGS,
    template_searchpath=[str(SQL_DIR)],
    params={"lookback_days": 3},
    tags=["greenplum", "dm"],
    doc_md=__doc__,
) as dag:
    SQLExecuteQueryOperator(
        task_id="build_sales_daily",
        conn_id=GP_CONN_ID,
        sql="dm/sales_daily.sql",
        execution_timeout=timedelta(minutes=45),
        outlets=[gp_dataset("dm.sales_daily")],
    )

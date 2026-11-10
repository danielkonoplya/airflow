"""
### gp_dm_sales_daily

Витрина `dm.sales_daily`. Запускается по Dataset'ам: после обновления
`ods.shop_orders` и `ods.shop_customers` загрузочными DAG'ами.

1. пересборка окна последних `lookback_days` дней (sql/dm/sales_daily.sql);
2. проверки качества: NULL-ы, отрицательная выручка, непустое окно;
3. сверка суммы выручки витрины с детальным слоем.
"""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLColumnCheckOperator,
    SQLExecuteQueryOperator,
    SQLTableCheckOperator,
)
from airflow.utils.task_group import TaskGroup

from gp_lib.defaults import DEFAULT_ARGS, GP_CONN_ID, SQL_DIR, gp_dataset

LOOKBACK_DAYS = 3
WINDOW_START = "DATE '{{ macros.ds_add(ds, -params.lookback_days) }}'"

with DAG(
    dag_id="gp_dm_sales_daily",
    schedule=[gp_dataset("ods.shop_orders"), gp_dataset("ods.shop_customers")],
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    default_args=DEFAULT_ARGS,
    template_searchpath=[str(SQL_DIR)],
    params={"lookback_days": LOOKBACK_DAYS},
    tags=["greenplum", "dm"],
    doc_md=__doc__,
) as dag:
    build = SQLExecuteQueryOperator(
        task_id="build_sales_daily",
        conn_id=GP_CONN_ID,
        sql="dm/sales_daily.sql",
    )

    with TaskGroup("quality") as quality:
        columns = SQLColumnCheckOperator(
            task_id="column_checks",
            conn_id=GP_CONN_ID,
            table="dm.sales_daily",
            partition_clause=f"report_date >= {WINDOW_START}",
            column_mapping={
                "report_date": {"null_check": {"equal_to": 0}},
                "customer_segment": {"null_check": {"equal_to": 0}},
                "orders_cnt": {"min": {"greater_than": 0}},
                "revenue": {"min": {"geq_to": 0}},
            },
        )
        table = SQLTableCheckOperator(
            task_id="table_checks",
            conn_id=GP_CONN_ID,
            table="dm.sales_daily",
            partition_clause=f"report_date >= {WINDOW_START}",
            checks={
                "window_not_empty": {"check_statement": "COUNT(*) > 0"},
                "customers_le_orders": {
                    "check_statement": "SUM(CASE WHEN customers_cnt > orders_cnt THEN 1 ELSE 0 END) = 0"
                },
            },
        )

    reconcile = SQLCheckOperator(
        task_id="reconcile_with_ods",
        conn_id=GP_CONN_ID,
        sql=f"""
            SELECT abs(coalesce(o.revenue, 0) - coalesce(d.revenue, 0)) < 0.01
            FROM (SELECT sum(amount) AS revenue FROM ods.shop_orders
                  WHERE created_at >= {WINDOW_START} AND status <> 'cancelled') o,
                 (SELECT sum(revenue) AS revenue FROM dm.sales_daily
                  WHERE report_date >= {WINDOW_START}) d
        """,
        outlets=[gp_dataset("dm.sales_daily")],
    )

    build >> quality >> reconcile

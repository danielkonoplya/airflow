"""
Фабрика DAG'ов PostgreSQL -> Greenplum. Каждая запись `configs/pg_to_gp.yaml`
превращается в DAG `gp_pg2gp__<name>`, публикующий Dataset целевой таблицы.
"""

from datetime import timedelta

from airflow import DAG
from airflow.models.param import Param

from gp_lib.config import PgToGpTable, load_pg_to_gp
from gp_lib.defaults import DEFAULT_ARGS, gp_dataset, to_pendulum
from gp_lib.operators import PostgresToGreenplumOperator


def build_dag(cfg: PgToGpTable) -> DAG:
    doc = f"""
### {cfg.dag_id}

`{cfg.source_table}` (conn `{cfg.source_conn_id}`) → `{cfg.target_table}`, режим **{cfg.mode}**.

* ключ: `{', '.join(cfg.primary_key)}`, распределение: `{', '.join(cfg.distributed_by)}`
* инкремент: `{cfg.increment_column or '—'}`, lookback {cfg.lookback_minutes} мин
* ручной запуск с `full_reload=true` — выгрузка всей таблицы источника с upsert
"""
    with DAG(
        dag_id=cfg.dag_id,
        description=f"{cfg.source_table} -> {cfg.target_table} ({cfg.mode})",
        schedule=cfg.schedule,
        start_date=to_pendulum(cfg.start_date),
        catchup=cfg.catchup,
        max_active_runs=1,  # staging-таблица одна на целевую таблицу
        dagrun_timeout=timedelta(hours=3),
        default_args=DEFAULT_ARGS,
        params={"full_reload": Param(False, type="boolean")},
        tags=["greenplum", "load", "postgres"],
        doc_md=doc,
    ) as dag:
        PostgresToGreenplumOperator(
            task_id="load",
            source_conn_id=cfg.source_conn_id,
            source_table=cfg.source_table,
            target_table=cfg.target_table,
            columns=cfg.columns,
            primary_key=cfg.primary_key,
            distributed_by=cfg.distributed_by,
            mode=cfg.mode,
            increment_column=cfg.increment_column,
            lookback=timedelta(minutes=cfg.lookback_minutes),
            allow_empty_full_load=cfg.allow_empty_full_load,
            gp_conn_id=cfg.gp_conn_id,
            execution_timeout=timedelta(hours=2),
            outlets=[gp_dataset(cfg.target_table)],
        )
    return dag


for _cfg in load_pg_to_gp():
    globals()[_cfg.dag_id] = build_dag(_cfg)

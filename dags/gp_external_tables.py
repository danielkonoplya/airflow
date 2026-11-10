"""
Фабрика DAG'ов загрузки/выгрузки через внешние таблицы Greenplum (PXF, gpfdist).
Конфиг: `configs/external_tables.yaml`.

Данные идут напрямую между сегментами GP и источником, минуя воркер Airflow —
так грузятся большие объёмы. Airflow только оркестрирует DDL/DML.
"""

from datetime import timedelta
from typing import List, Optional

from airflow import DAG
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from gp_lib import sql_builders as sb
from gp_lib.config import ExternalExport, ExternalImport, load_external
from gp_lib.defaults import DEFAULT_ARGS, gp_dataset, to_pendulum


def import_statements(cfg: ExternalImport) -> List[str]:
    ext = cfg.external_table
    create = sb.create_readable_external(
        ext, [(c.name, c.type) for c in cfg.columns], cfg.location, cfg.format, cfg.format_options, cfg.reject_limit
    )
    if cfg.load_mode == "replace_partition":
        load = sb.replace_partition(
            cfg.target_table, ext, cfg.mapping, cfg.slice_column, cfg.slice_value, cfg.partition_storage
        )
    else:
        load = sb.load_from_external(
            cfg.target_table, ext, cfg.mapping, cfg.load_mode, cfg.slice_column, cfg.slice_value
        )
    return [*create, *load, sb.drop_external(ext)]


def build_import_dag(cfg: ExternalImport) -> DAG:
    with DAG(
        dag_id=cfg.dag_id,
        description=f"{cfg.location.split('?')[0]} -> {cfg.target_table} ({cfg.load_mode})",
        schedule=cfg.schedule,
        start_date=to_pendulum(cfg.start_date),
        catchup=False,
        max_active_runs=1,
        dagrun_timeout=timedelta(hours=8),
        default_args=DEFAULT_ARGS,
        tags=["greenplum", "load", "external", cfg.location.split(":", 1)[0]],
        doc_md=f"### {cfg.dag_id}\n\n`{cfg.location}` → `{cfg.target_table}`, режим **{cfg.load_mode}**.",
    ) as dag:
        load = SQLExecuteQueryOperator(
            task_id="load",
            conn_id=cfg.gp_conn_id,
            sql=import_statements(cfg),
            execution_timeout=timedelta(hours=4),
            outlets=[gp_dataset(cfg.target_table)],
        )

        if cfg.wait_for_s3:
            from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

            # deferrable: ожидание идёт в triggerer, а не отдельным подом на каждую проверку.
            S3KeySensor(
                task_id="wait_for_source",
                aws_conn_id=cfg.wait_for_s3.aws_conn_id,
                bucket_key=cfg.wait_for_s3.bucket_key,
                deferrable=True,
                poke_interval=300,
                timeout=cfg.wait_for_s3.timeout_minutes * 60,
            ) >> load
    return dag


@task(task_id="export", execution_timeout=timedelta(hours=4))
def export(
    gp_conn_id: str,
    statements: List[str],
    aws_conn_id: Optional[str] = None,
    bucket: Optional[str] = None,
    prefix: Optional[str] = None,
) -> int:
    """Очистка префикса S3 и выгрузка — одна задача, т.е. один под.

    Запись во writable external table не транзакционна для S3: при повторе старые
    файлы остались бы рядом с новыми, поэтому префикс чистится перед записью.
    """
    from gp_lib.hooks import GreenplumHook

    removed = 0
    if bucket:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        s3 = S3Hook(aws_conn_id=aws_conn_id)
        keys = s3.list_keys(bucket_name=bucket, prefix=prefix) or []
        if keys:
            s3.delete_objects(bucket=bucket, keys=keys)
        removed = len(keys)
    GreenplumHook(gp_conn_id).run(statements)
    return removed


def build_export_dag(cfg: ExternalExport) -> DAG:
    ext = cfg.external_table
    schedule = [gp_dataset(t) for t in cfg.schedule_on] if cfg.schedule_on else cfg.schedule
    cleanup = cfg.s3_cleanup
    with DAG(
        dag_id=cfg.dag_id,
        description=f"Greenplum -> {cfg.location.split('?')[0]}",
        schedule=schedule,
        start_date=to_pendulum(cfg.start_date),
        catchup=False,
        max_active_runs=1,
        dagrun_timeout=timedelta(hours=6),
        default_args=DEFAULT_ARGS,
        tags=["greenplum", "export", "external"],
        doc_md=f"### {cfg.dag_id}\n\nВыгрузка запроса в `{cfg.location}` через writable external table.",
    ) as dag:
        export(
            gp_conn_id=cfg.gp_conn_id,
            statements=[
                *sb.create_writable_external(
                    ext, [(c.name, c.type) for c in cfg.columns], cfg.location, cfg.format, cfg.format_options
                ),
                sb.export_to_external(ext, [c.name for c in cfg.columns], cfg.query),
                sb.drop_external(ext),
            ],
            aws_conn_id=cleanup.aws_conn_id if cleanup else None,
            bucket=cleanup.bucket if cleanup else None,
            prefix=cleanup.prefix if cleanup else None,
        )
    return dag


_imports, _exports = load_external()
for _cfg in _imports:
    globals()[_cfg.dag_id] = build_import_dag(_cfg)
for _cfg in _exports:
    globals()[_cfg.dag_id] = build_export_dag(_cfg)

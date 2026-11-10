"""
Фабрика DAG'ов загрузки/выгрузки через внешние таблицы Greenplum (PXF, gpfdist).
Конфиг: `configs/external_tables.yaml`.

Данные идут напрямую между сегментами GP и источником, минуя воркер Airflow —
так грузятся большие объёмы. Airflow только оркестрирует DDL/DML.
"""

from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from gp_lib import sql_builders as sb
from gp_lib.config import ExternalExport, ExternalImport, load_external
from gp_lib.defaults import DEFAULT_ARGS, gp_dataset, to_pendulum


def build_import_dag(cfg: ExternalImport) -> DAG:
    ext = cfg.external_table
    columns = [(c.name, c.type) for c in cfg.columns]
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
            sql=[
                *sb.create_readable_external(
                    ext, columns, cfg.location, cfg.format, cfg.format_options, cfg.reject_limit
                ),
                *sb.load_from_external(
                    cfg.target_table, ext, cfg.mapping, cfg.load_mode, cfg.slice_column, cfg.slice_value
                ),
                sb.drop_external(ext),
            ],
            execution_timeout=timedelta(hours=4),
            outlets=[gp_dataset(cfg.target_table)],
        )

        if cfg.wait_for_s3:
            from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

            S3KeySensor(
                task_id="wait_for_source",
                aws_conn_id=cfg.wait_for_s3.aws_conn_id,
                bucket_key=cfg.wait_for_s3.bucket_key,
                mode="reschedule",
                poke_interval=300,
                timeout=cfg.wait_for_s3.timeout_minutes * 60,
            ) >> load
    return dag


def build_export_dag(cfg: ExternalExport) -> DAG:
    ext = cfg.external_table
    columns = [(c.name, c.type) for c in cfg.columns]
    schedule = [gp_dataset(t) for t in cfg.schedule_on] if cfg.schedule_on else cfg.schedule
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
        export = SQLExecuteQueryOperator(
            task_id="export",
            conn_id=cfg.gp_conn_id,
            sql=[
                *sb.create_writable_external(ext, columns, cfg.location, cfg.format, cfg.format_options),
                sb.export_to_external(ext, [c.name for c in cfg.columns], cfg.query),
                sb.drop_external(ext),
            ],
            execution_timeout=timedelta(hours=4),
        )

        if cfg.s3_cleanup:
            # Запись во writable external table не транзакционна для S3: при повторе
            # старые файлы остались бы рядом с новыми, поэтому чистим префикс заранее.
            @task(task_id="cleanup_target_prefix")
            def cleanup_target_prefix(aws_conn_id: str, bucket: str, prefix: str) -> int:
                from airflow.providers.amazon.aws.hooks.s3 import S3Hook

                hook = S3Hook(aws_conn_id=aws_conn_id)
                keys = hook.list_keys(bucket_name=bucket, prefix=prefix) or []
                if keys:
                    hook.delete_objects(bucket=bucket, keys=keys)
                return len(keys)

            cleanup_target_prefix(cfg.s3_cleanup.aws_conn_id, cfg.s3_cleanup.bucket, cfg.s3_cleanup.prefix) >> export
    return dag


_imports, _exports = load_external()
for _cfg in _imports:
    globals()[_cfg.dag_id] = build_import_dag(_cfg)
for _cfg in _exports:
    globals()[_cfg.dag_id] = build_export_dag(_cfg)

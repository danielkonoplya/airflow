"""Генерация SQL для Greenplum 6.x.

Имена таблиц и колонок приходят из провалидированных конфигов (gp_lib.config),
поэтому подставляются как есть. Имена из системного каталога экранируются
через quote_ident/quote_table.

GP6 основан на PostgreSQL 9.4: нет INSERT ... ON CONFLICT и MERGE, поэтому
upsert делается как DELETE USING + INSERT в одной транзакции.
"""

from __future__ import annotations

from typing import List, Optional, Sequence, Tuple

LOAD_LOG_INSERT = (
    "INSERT INTO etl.load_log "
    "(dag_id, task_id, run_id, target_table, rows_loaded, interval_start, interval_end) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s)"
)


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def quote_table(schema: str, table: str) -> str:
    return f"{quote_ident(schema)}.{quote_ident(table)}"


def _cols(columns: Sequence[str]) -> str:
    return ", ".join(columns)


# --------------------------------------------------------------------------- PostgreSQL -> GP


def staging_table(target_table: str) -> str:
    schema, table = target_table.split(".")
    return f"stg.{schema}__{table}"


def extract_query(source_table: str, columns: Sequence[str], increment_column: Optional[str] = None) -> str:
    """SELECT для выгрузки из источника; параметры %(start)s / %(end)s подставляет mogrify."""
    sql = f"SELECT {_cols(columns)} FROM {source_table}"
    if increment_column:
        sql += f" WHERE {increment_column} >= %(start)s AND {increment_column} < %(end)s"
    return sql


def copy_out(query: str) -> str:
    return f"COPY ({query}) TO STDOUT WITH CSV"


def copy_in(table: str, columns: Sequence[str]) -> str:
    return f"COPY {table} ({_cols(columns)}) FROM STDIN WITH CSV"


def prepare_staging(staging: str, target: str, distributed_by: Sequence[str]) -> List[str]:
    """Staging создаётся один раз и далее только очищается.

    DROP/CREATE на каждый запуск раздувает системный каталог GP — это одна из
    типичных причин деградации кластера, поэтому TRUNCATE.
    Распределение staging совпадает с целевой таблицей, чтобы DELETE USING
    выполнялся без redistribute motion.
    """
    return [
        f"CREATE UNLOGGED TABLE IF NOT EXISTS {staging} (LIKE {target}) "
        f"WITH (appendoptimized=false) DISTRIBUTED BY ({_cols(distributed_by)})",
        f"TRUNCATE {staging}",
    ]


def merge_incremental(
    target: str,
    staging: str,
    columns: Sequence[str],
    primary_key: Sequence[str],
    order_column: Optional[str] = None,
) -> List[str]:
    """Upsert: удаляем изменившиеся ключи и вставляем последнюю версию строки из staging."""
    join = " AND ".join(f"t.{c} = s.{c}" for c in primary_key)
    order = f" ORDER BY {order_column} DESC" if order_column else ""
    cols = _cols(columns)
    return [
        f"DELETE FROM {target} t USING {staging} s WHERE {join}",
        f"INSERT INTO {target} ({cols}) "
        f"SELECT {cols} FROM ("
        f"SELECT {cols}, row_number() OVER (PARTITION BY {_cols(primary_key)}{order}) AS rn__ "
        f"FROM {staging}) d WHERE rn__ = 1",
    ]


def merge_full(target: str, staging: str, columns: Sequence[str]) -> List[str]:
    cols = _cols(columns)
    return [
        f"TRUNCATE {target}",
        f"INSERT INTO {target} ({cols}) SELECT {cols} FROM {staging}",
    ]


# --------------------------------------------------------------------------- external tables


def _column_defs(columns: Sequence[Tuple[str, str]]) -> str:
    return ", ".join(f"{name} {col_type}" for name, col_type in columns)


def _format_clause(fmt: str, format_options: str) -> str:
    clause = f"FORMAT '{fmt}'"
    if format_options:
        clause += f" ({format_options})"
    return clause


def drop_external(name: str) -> str:
    return f"DROP EXTERNAL TABLE IF EXISTS {name}"


def create_readable_external(
    name: str,
    columns: Sequence[Tuple[str, str]],
    location: str,
    fmt: str,
    format_options: str = "",
    reject_limit: Optional[int] = None,
) -> List[str]:
    ddl = (
        f"CREATE READABLE EXTERNAL TABLE {name} ({_column_defs(columns)}) "
        f"LOCATION ('{location}') {_format_clause(fmt, format_options)}"
    )
    if reject_limit:
        ddl += f" LOG ERRORS SEGMENT REJECT LIMIT {reject_limit} ROWS"
    return [drop_external(name), ddl]


def create_writable_external(
    name: str,
    columns: Sequence[Tuple[str, str]],
    location: str,
    fmt: str,
    format_options: str = "",
) -> List[str]:
    ddl = (
        f"CREATE WRITABLE EXTERNAL TABLE {name} ({_column_defs(columns)}) "
        f"LOCATION ('{location}') {_format_clause(fmt, format_options)} DISTRIBUTED RANDOMLY"
    )
    return [drop_external(name), ddl]


def load_from_external(
    target: str,
    external: str,
    mapping: Sequence[Tuple[str, str]],
    mode: str,
    slice_column: Optional[str] = None,
    slice_value: Optional[str] = None,
) -> List[str]:
    """Перенос данных из внешней таблицы в целевую.

    replace_slice — идемпотентная перезаливка среза (обычно дня): удаляем срез
    и вставляем только строки, попавшие в этот же срез.
    """
    cols = _cols([col for col, _ in mapping])
    select = ", ".join(f"{expr} AS {col}" for col, expr in mapping)
    source = f"SELECT {select} FROM {external}"

    if mode == "full":
        return [f"TRUNCATE {target}", f"INSERT INTO {target} ({cols}) {source}"]
    if mode == "append":
        return [f"INSERT INTO {target} ({cols}) {source}"]
    if mode == "replace_slice":
        if not slice_column or not slice_value:
            raise ValueError("replace_slice требует slice_column и slice_value")
        return [
            f"DELETE FROM {target} WHERE {slice_column} = '{slice_value}'",
            f"INSERT INTO {target} ({cols}) SELECT {cols} FROM ({source}) s "
            f"WHERE s.{slice_column} = '{slice_value}'",
        ]
    raise ValueError(f"Неизвестный режим загрузки: {mode!r}")


def export_to_external(external: str, columns: Sequence[str], query: str) -> str:
    return f"INSERT INTO {external} ({_cols(columns)}) {query}"

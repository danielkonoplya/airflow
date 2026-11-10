"""Загрузка и валидация YAML-конфигов интеграций.

Конфиги лежат в репозитории и проходят code review, поэтому SQL-выражения
в них (mapping, query) считаются доверенными. Валидация ловит опечатки и
случайные кавычки/точки с запятой в идентификаторах и путях.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

CONFIG_DIR = Path(__file__).resolve().parent.parent / "configs"
DEFAULT_GP_CONN_ID = "greenplum_default"
DEFAULT_START_DATE = date(2026, 11, 1)

_IDENT_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
_TYPE_RE = re.compile(r"^[a-z][a-z0-9_ ,()\[\]]*$")
_FORMATS = {"CSV", "TEXT", "CUSTOM"}


class ConfigError(ValueError):
    """Ошибка в YAML-конфиге интеграции."""


# --------------------------------------------------------------------------- helpers


def ident(value: Any, what: str) -> str:
    if not isinstance(value, str) or not _IDENT_RE.match(value):
        raise ConfigError(f"{what}: недопустимый идентификатор {value!r}")
    return value


def table_name(value: Any, what: str) -> str:
    parts = value.split(".") if isinstance(value, str) else []
    if len(parts) != 2:
        raise ConfigError(f"{what}: ожидается schema.table, получено {value!r}")
    for part in parts:
        ident(part, what)
    return value


def ident_list(values: Any, what: str, allow_empty: bool = False) -> Tuple[str, ...]:
    if values is None:
        values = []
    if not isinstance(values, list):
        raise ConfigError(f"{what}: ожидается список, получено {values!r}")
    if not values and not allow_empty:
        raise ConfigError(f"{what}: список не может быть пустым")
    result = tuple(ident(v, what) for v in values)
    if len(set(result)) != len(result):
        raise ConfigError(f"{what}: дублирующиеся значения в {list(result)}")
    return result


def safe_literal(value: Any, what: str) -> str:
    """Строка, которая подставляется в SQL внутри одинарных кавычек."""
    if not isinstance(value, str) or not value or "'" in value or ";" in value:
        raise ConfigError(f"{what}: недопустимое значение {value!r}")
    return value


def as_date(value: Any, what: str) -> date:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise ConfigError(f"{what}: ожидается дата YYYY-MM-DD, получено {value!r}") from exc


def _check_keys(item: Dict[str, Any], allowed: set, what: str) -> None:
    unknown = set(item) - allowed
    if unknown:
        raise ConfigError(f"{what}: неизвестные ключи {sorted(unknown)}")


def _check_unique(names: List[str], what: str) -> None:
    dupes = sorted({n for n in names if names.count(n) > 1})
    if dupes:
        raise ConfigError(f"{what}: повторяющиеся name {dupes}")


def load_yaml(path: Path) -> Dict[str, Any]:
    with open(path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"{path}: корень конфига должен быть словарём")
    return data


# --------------------------------------------------------------------------- PostgreSQL -> GP


@dataclass(frozen=True)
class PgToGpTable:
    name: str
    source_conn_id: str
    source_table: str
    target_table: str
    columns: Tuple[str, ...]
    primary_key: Tuple[str, ...]
    distributed_by: Tuple[str, ...]
    mode: str
    increment_column: Optional[str]
    lookback_minutes: int
    schedule: str
    start_date: date
    catchup: bool
    allow_empty_full_load: bool
    gp_conn_id: str

    @property
    def dag_id(self) -> str:
        return f"gp_pg2gp__{self.name}"


_PG_TO_GP_KEYS = {
    "name", "source_conn_id", "source_table", "target_table", "columns", "primary_key",
    "distributed_by", "mode", "increment_column", "lookback_minutes", "schedule",
    "start_date", "catchup", "allow_empty_full_load", "gp_conn_id",
}


def parse_pg_to_gp_table(item: Dict[str, Any]) -> PgToGpTable:
    what = f"pg_to_gp[{item.get('name')!r}]"
    _check_keys(item, _PG_TO_GP_KEYS, what)

    columns = ident_list(item.get("columns"), f"{what}.columns")
    primary_key = ident_list(item.get("primary_key"), f"{what}.primary_key")
    distributed_by = ident_list(item.get("distributed_by") or list(primary_key), f"{what}.distributed_by")
    for field_name, cols in (("primary_key", primary_key), ("distributed_by", distributed_by)):
        missing = set(cols) - set(columns)
        if missing:
            raise ConfigError(f"{what}.{field_name}: колонок {sorted(missing)} нет в columns")

    mode = item.get("mode", "incremental")
    if mode not in ("incremental", "full"):
        raise ConfigError(f"{what}.mode: ожидается incremental|full, получено {mode!r}")

    increment_column = item.get("increment_column")
    if mode == "incremental":
        ident(increment_column, f"{what}.increment_column")
        if increment_column not in columns:
            raise ConfigError(f"{what}.increment_column: {increment_column!r} нет в columns")
    else:
        increment_column = None

    lookback = item.get("lookback_minutes", 0)
    if not isinstance(lookback, int) or lookback < 0:
        raise ConfigError(f"{what}.lookback_minutes: ожидается целое >= 0")

    return PgToGpTable(
        name=ident(item.get("name"), f"{what}.name"),
        source_conn_id=str(item["source_conn_id"]),
        source_table=table_name(item.get("source_table"), f"{what}.source_table"),
        target_table=table_name(item.get("target_table"), f"{what}.target_table"),
        columns=columns,
        primary_key=primary_key,
        distributed_by=distributed_by,
        mode=mode,
        increment_column=increment_column,
        lookback_minutes=lookback,
        schedule=str(item.get("schedule", "@hourly")),
        start_date=as_date(item.get("start_date", DEFAULT_START_DATE), f"{what}.start_date"),
        catchup=bool(item.get("catchup", False)),
        allow_empty_full_load=bool(item.get("allow_empty_full_load", False)),
        gp_conn_id=str(item.get("gp_conn_id", DEFAULT_GP_CONN_ID)),
    )


def load_pg_to_gp(path: Optional[Path] = None) -> List[PgToGpTable]:
    data = load_yaml(path or CONFIG_DIR / "pg_to_gp.yaml")
    defaults = data.get("defaults") or {}
    tables = [parse_pg_to_gp_table({**defaults, **item}) for item in data.get("tables") or []]
    _check_unique([t.name for t in tables], "pg_to_gp")
    return tables


# --------------------------------------------------------------------------- external tables (PXF / gpfdist)


@dataclass(frozen=True)
class ExtColumn:
    name: str
    type: str


@dataclass(frozen=True)
class S3Wait:
    aws_conn_id: str
    bucket_key: str
    timeout_minutes: int


@dataclass(frozen=True)
class S3Prefix:
    aws_conn_id: str
    bucket: str
    prefix: str


@dataclass(frozen=True)
class ExternalImport:
    name: str
    schedule: str
    start_date: date
    location: str
    format: str
    format_options: str
    columns: Tuple[ExtColumn, ...]
    reject_limit: Optional[int]
    target_table: str
    load_mode: str
    slice_column: Optional[str]
    slice_value: str
    mapping: Tuple[Tuple[str, str], ...]
    wait_for_s3: Optional[S3Wait]
    gp_conn_id: str

    @property
    def dag_id(self) -> str:
        return f"gp_ext_import__{self.name}"

    @property
    def external_table(self) -> str:
        return f"ext.r_{self.name}"


@dataclass(frozen=True)
class ExternalExport:
    name: str
    schedule: Optional[str]
    schedule_on: Tuple[str, ...]
    start_date: date
    location: str
    format: str
    format_options: str
    columns: Tuple[ExtColumn, ...]
    query: str
    s3_cleanup: Optional[S3Prefix]
    gp_conn_id: str

    @property
    def dag_id(self) -> str:
        return f"gp_ext_export__{self.name}"

    @property
    def external_table(self) -> str:
        return f"ext.w_{self.name}"


def _parse_ext_columns(values: Any, what: str) -> Tuple[ExtColumn, ...]:
    if not isinstance(values, list) or not values:
        raise ConfigError(f"{what}: ожидается непустой список пар [name, type]")
    result = []
    for pair in values:
        if not isinstance(pair, list) or len(pair) != 2:
            raise ConfigError(f"{what}: ожидается пара [name, type], получено {pair!r}")
        col_type = str(pair[1]).lower()
        if not _TYPE_RE.match(col_type):
            raise ConfigError(f"{what}: недопустимый тип {pair[1]!r}")
        result.append(ExtColumn(ident(pair[0], what), col_type))
    ident_list([c.name for c in result], what)  # проверка дублей
    return tuple(result)


def _parse_format(item: Dict[str, Any], what: str) -> Tuple[str, str]:
    fmt = str(item.get("format", "")).upper()
    if fmt not in _FORMATS:
        raise ConfigError(f"{what}.format: ожидается одно из {sorted(_FORMATS)}, получено {fmt!r}")
    options = str(item.get("format_options", ""))
    if ";" in options:
        raise ConfigError(f"{what}.format_options: ';' не допускается")
    return fmt, options


_IMPORT_KEYS = {
    "name", "schedule", "start_date", "location", "format", "format_options", "columns",
    "reject_limit", "target_table", "load_mode", "slice_column", "slice_value", "mapping",
    "wait_for_s3", "gp_conn_id",
}


def parse_external_import(item: Dict[str, Any]) -> ExternalImport:
    what = f"imports[{item.get('name')!r}]"
    _check_keys(item, _IMPORT_KEYS, what)
    fmt, options = _parse_format(item, what)
    columns = _parse_ext_columns(item.get("columns"), f"{what}.columns")

    raw_mapping = item.get("mapping") or {c.name: c.name for c in columns}
    if not isinstance(raw_mapping, dict):
        raise ConfigError(f"{what}.mapping: ожидается словарь target_column: expression")
    mapping = tuple((ident(k, f"{what}.mapping"), str(v)) for k, v in raw_mapping.items())

    load_mode = item.get("load_mode", "replace_slice")
    if load_mode not in ("replace_slice", "append", "full"):
        raise ConfigError(f"{what}.load_mode: ожидается replace_slice|append|full")
    slice_column = item.get("slice_column")
    if load_mode == "replace_slice":
        ident(slice_column, f"{what}.slice_column")
        if slice_column not in dict(mapping):
            raise ConfigError(f"{what}.slice_column: {slice_column!r} должна быть в mapping")

    reject_limit = item.get("reject_limit")
    if reject_limit is not None and (not isinstance(reject_limit, int) or reject_limit < 2):
        raise ConfigError(f"{what}.reject_limit: ожидается целое >= 2")

    wait = item.get("wait_for_s3")
    wait_cfg = None
    if wait:
        wait_cfg = S3Wait(
            aws_conn_id=str(wait.get("aws_conn_id", "aws_default")),
            bucket_key=str(wait["bucket_key"]),
            timeout_minutes=int(wait.get("timeout_minutes", 360)),
        )

    return ExternalImport(
        name=ident(item.get("name"), f"{what}.name"),
        schedule=str(item.get("schedule", "@daily")),
        start_date=as_date(item.get("start_date", DEFAULT_START_DATE), f"{what}.start_date"),
        location=safe_literal(item.get("location"), f"{what}.location"),
        format=fmt,
        format_options=options,
        columns=columns,
        reject_limit=reject_limit,
        target_table=table_name(item.get("target_table"), f"{what}.target_table"),
        load_mode=load_mode,
        slice_column=slice_column if load_mode == "replace_slice" else None,
        slice_value=safe_literal(item.get("slice_value", "{{ ds }}"), f"{what}.slice_value"),
        mapping=mapping,
        wait_for_s3=wait_cfg,
        gp_conn_id=str(item.get("gp_conn_id", DEFAULT_GP_CONN_ID)),
    )


_EXPORT_KEYS = {
    "name", "schedule", "schedule_on", "start_date", "location", "format", "format_options",
    "columns", "query", "s3_cleanup", "gp_conn_id",
}


def parse_external_export(item: Dict[str, Any]) -> ExternalExport:
    what = f"exports[{item.get('name')!r}]"
    _check_keys(item, _EXPORT_KEYS, what)
    fmt, options = _parse_format(item, what)

    schedule_on = tuple(table_name(t, f"{what}.schedule_on") for t in item.get("schedule_on") or [])
    schedule = item.get("schedule")
    if bool(schedule) == bool(schedule_on):
        raise ConfigError(f"{what}: нужно указать ровно одно из schedule / schedule_on")

    query = str(item.get("query", "")).strip().rstrip(";")
    if not query:
        raise ConfigError(f"{what}.query: пустой запрос")

    cleanup = item.get("s3_cleanup")
    cleanup_cfg = None
    if cleanup:
        prefix = str(cleanup.get("prefix", ""))
        if not prefix.strip("/") or not prefix.endswith("/"):
            raise ConfigError(f"{what}.s3_cleanup.prefix: нужен непустой префикс, оканчивающийся на '/'")
        cleanup_cfg = S3Prefix(
            aws_conn_id=str(cleanup.get("aws_conn_id", "aws_default")),
            bucket=str(cleanup["bucket"]),
            prefix=prefix,
        )

    return ExternalExport(
        name=ident(item.get("name"), f"{what}.name"),
        schedule=str(schedule) if schedule else None,
        schedule_on=schedule_on,
        start_date=as_date(item.get("start_date", DEFAULT_START_DATE), f"{what}.start_date"),
        location=safe_literal(item.get("location"), f"{what}.location"),
        format=fmt,
        format_options=options,
        columns=_parse_ext_columns(item.get("columns"), f"{what}.columns"),
        query=query,
        s3_cleanup=cleanup_cfg,
        gp_conn_id=str(item.get("gp_conn_id", DEFAULT_GP_CONN_ID)),
    )


def load_external(path: Optional[Path] = None) -> Tuple[List[ExternalImport], List[ExternalExport]]:
    data = load_yaml(path or CONFIG_DIR / "external_tables.yaml")
    imports = [parse_external_import(i) for i in data.get("imports") or []]
    exports = [parse_external_export(e) for e in data.get("exports") or []]
    _check_unique([i.name for i in imports], "imports")
    _check_unique([e.name for e in exports], "exports")
    return imports, exports


# --------------------------------------------------------------------------- healthcheck / maintenance


@dataclass(frozen=True)
class HealthcheckConfig:
    schedule: str
    long_query_minutes: int
    waiting_locks_threshold: int
    min_free_disk_gb: int
    gp_conn_id: str


@dataclass(frozen=True)
class PartitionPolicy:
    table: str
    granularity: str
    premake: int
    retention: Optional[int]
    drop_enabled: bool


@dataclass(frozen=True)
class MaintenanceConfig:
    schedule: str
    catalog_vacuum: bool
    analyze_missing_stats_limit: int
    vacuum_bloated: bool
    partitions: Tuple[PartitionPolicy, ...]
    skew_tables: Tuple[str, ...]
    skew_threshold: float
    gp_conn_id: str


def _parse_partition_policy(item: Dict[str, Any]) -> PartitionPolicy:
    what = f"partitions[{item.get('table')!r}]"
    _check_keys(item, {"table", "granularity", "premake", "retention", "drop_enabled"}, what)
    granularity = item.get("granularity", "day")
    if granularity not in ("day", "month"):
        raise ConfigError(f"{what}.granularity: ожидается day|month")
    premake = int(item.get("premake", 7))
    retention = item.get("retention")
    if retention is not None and int(retention) < 1:
        raise ConfigError(f"{what}.retention: ожидается целое >= 1")
    drop_enabled = bool(item.get("drop_enabled", False))
    if drop_enabled and retention is None:
        raise ConfigError(f"{what}: drop_enabled требует retention")
    return PartitionPolicy(
        table=table_name(item.get("table"), f"{what}.table"),
        granularity=granularity,
        premake=premake,
        retention=int(retention) if retention is not None else None,
        drop_enabled=drop_enabled,
    )


def load_ops(path: Optional[Path] = None) -> Tuple[HealthcheckConfig, MaintenanceConfig]:
    data = load_yaml(path or CONFIG_DIR / "ops.yaml")
    gp_conn_id = str(data.get("gp_conn_id", DEFAULT_GP_CONN_ID))

    hc = data.get("healthcheck") or {}
    health = HealthcheckConfig(
        schedule=str(hc.get("schedule", "*/15 * * * *")),
        long_query_minutes=int(hc.get("long_query_minutes", 60)),
        waiting_locks_threshold=int(hc.get("waiting_locks_threshold", 20)),
        min_free_disk_gb=int(hc.get("min_free_disk_gb", 50)),
        gp_conn_id=gp_conn_id,
    )

    mt = data.get("maintenance") or {}
    skew = mt.get("skew_check") or {}
    maintenance = MaintenanceConfig(
        schedule=str(mt.get("schedule", "0 2 * * *")),
        catalog_vacuum=bool(mt.get("catalog_vacuum", True)),
        analyze_missing_stats_limit=int(mt.get("analyze_missing_stats_limit", 200)),
        vacuum_bloated=bool(mt.get("vacuum_bloated", False)),
        partitions=tuple(_parse_partition_policy(p) for p in mt.get("partitions") or []),
        skew_tables=tuple(table_name(t, "skew_check.tables") for t in skew.get("tables") or []),
        skew_threshold=float(skew.get("threshold", 0.2)),
        gp_conn_id=gp_conn_id,
    )
    return health, maintenance

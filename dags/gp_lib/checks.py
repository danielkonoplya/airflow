"""Оценка состояния кластера Greenplum по результатам запросов к каталогу."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Sequence

SEGMENTS_SQL = """
SELECT content, role, preferred_role, mode, status, hostname, port
FROM gp_segment_configuration
ORDER BY content, role
"""

DISK_FREE_SQL = "SELECT dfhostname, dfdevice, dfspace FROM gp_toolkit.gp_disk_free"

LONG_QUERIES_SQL = """
SELECT pid, usename, now() - query_start AS duration, left(query, 300) AS query
FROM pg_stat_activity
WHERE state = 'active'
  AND pid <> pg_backend_pid()
  AND now() - query_start > %(threshold)s * interval '1 minute'
ORDER BY query_start
"""

WAITING_LOCKS_SQL = "SELECT count(*) FROM pg_locks WHERE NOT granted"

STATS_MISSING_SQL = """
SELECT smischema, smitable
FROM gp_toolkit.gp_stats_missing
WHERE smischema NOT IN ('pg_catalog', 'information_schema', 'gp_toolkit')
  AND smischema NOT LIKE 'pg_temp%%'
ORDER BY smischema, smitable
LIMIT %(limit)s
"""

BLOAT_SQL = "SELECT bdinspname, bdirelname, bdirelpages, bdiexppages, bdidiag FROM gp_toolkit.gp_bloat_diag"

CATALOG_TABLES = (
    "pg_class", "pg_attribute", "pg_type", "pg_depend", "pg_index", "pg_constraint",
    "pg_namespace", "pg_proc", "pg_statistic", "pg_attrdef", "pg_partition_rule",
    "gp_distribution_policy",
)


@dataclass
class CheckResult:
    critical: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, List[str]]:
        return {"critical": list(self.critical), "warnings": list(self.warnings)}

    @classmethod
    def merge(cls, results: Iterable[Mapping[str, Sequence[str]]]) -> CheckResult:
        merged = cls()
        for r in results:
            merged.critical.extend(r.get("critical", []))
            merged.warnings.extend(r.get("warnings", []))
        return merged


def evaluate_segments(rows: Sequence[Mapping[str, Any]]) -> CheckResult:
    result = CheckResult()
    has_mirrors = any(r["role"] == "m" and r["content"] >= 0 for r in rows)

    for r in rows:
        where = f"content={r['content']} {r['hostname']}:{r['port']}"
        if r["status"] == "d":
            result.critical.append(f"Сегмент недоступен: {where} (role={r['role']})")
        elif r["role"] != r["preferred_role"]:
            result.warnings.append(
                f"Сегмент не в preferred role: {where} — нужен gprecoverseg -r для ребалансировки"
            )
        if has_mirrors and r["content"] >= 0 and r["mode"] != "s" and r["status"] == "u":
            result.warnings.append(f"Пара primary/mirror не синхронизирована: {where} (mode={r['mode']})")

    if not any(r["content"] == -1 and r["role"] == "p" and r["status"] == "u" for r in rows):
        result.critical.append("Не найден работающий coordinator/master (content=-1)")
    return result


def evaluate_disk(rows: Sequence[Mapping[str, Any]], min_free_gb: int) -> CheckResult:
    result = CheckResult()
    for r in rows:
        free_gb = int(r["dfspace"]) / 1024 / 1024  # dfspace в килобайтах
        if free_gb < min_free_gb:
            result.critical.append(
                f"Мало места на {r['dfhostname']}:{r['dfdevice']} — {free_gb:.1f} GB < {min_free_gb} GB"
            )
    return result


def skew_coefficient(counts: Sequence[int]) -> float:
    """Коэффициент вариации числа строк по сегментам (0 — идеально ровно)."""
    if not counts:
        return 0.0
    mean = sum(counts) / len(counts)
    if mean == 0:
        return 0.0
    variance = sum((c - mean) ** 2 for c in counts) / len(counts)
    return math.sqrt(variance) / mean

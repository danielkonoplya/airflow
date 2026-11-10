"""Планирование range-партиций по дате для Greenplum 6.x (legacy-синтаксис).

Таблица должна быть создана без DEFAULT PARTITION: при её наличии
ADD PARTITION невозможен, нужен SPLIT DEFAULT PARTITION.
"""

from __future__ import annotations

import calendar
import re
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import List, Optional, Sequence

# Партиции GP6 читаются из pg_partitions (в GP7 этого представления нет).
PARTITIONS_SQL = """
SELECT partitionname, partitionrangestart, partitionrangeend, partitionisdefault
FROM pg_partitions
WHERE schemaname = %s AND tablename = %s AND partitionlevel = 0
ORDER BY partitionposition
"""

_BOUND_RE = re.compile(r"'(\d{4}-\d{2}-\d{2})")


@dataclass(frozen=True)
class PartitionRange:
    start: date
    end: date
    name: Optional[str] = None


@dataclass(frozen=True)
class PartitionPlan:
    to_add: List[PartitionRange] = field(default_factory=list)
    to_drop: List[PartitionRange] = field(default_factory=list)


def parse_range_bound(expr: str) -> date:
    """'2026-11-01'::date или '2026-11-01 00:00:00'::timestamp without time zone -> date."""
    match = _BOUND_RE.search(expr or "")
    if not match:
        raise ValueError(f"Не удалось разобрать границу партиции: {expr!r}")
    return date.fromisoformat(match.group(1))


def period_start(d: date, granularity: str) -> date:
    return d.replace(day=1) if granularity == "month" else d


def add_period(d: date, granularity: str, n: int = 1) -> date:
    if granularity == "day":
        return d + timedelta(days=n)
    if granularity == "month":
        year, month0 = divmod(d.year * 12 + d.month - 1 + n, 12)
        day = min(d.day, calendar.monthrange(year, month0 + 1)[1])
        return date(year, month0 + 1, day)
    raise ValueError(f"Неизвестная гранулярность: {granularity!r}")


def partition_name(start: date, granularity: str) -> str:
    return f"p{start:%Y%m}" if granularity == "month" else f"p{start:%Y%m%d}"


def plan_partitions(
    existing: Sequence[PartitionRange],
    today: date,
    granularity: str,
    premake: int,
    retention: Optional[int] = None,
) -> PartitionPlan:
    """Какие партиции добавить (до today + premake периодов) и какие старше retention."""
    if not existing:
        raise ValueError("У таблицы нет партиций: создайте её с начальным диапазоном")

    current = period_start(today, granularity)
    horizon = add_period(current, granularity, premake + 1)

    to_add: List[PartitionRange] = []
    cursor = max(p.end for p in existing)
    while cursor < horizon:
        nxt = add_period(cursor, granularity)
        to_add.append(PartitionRange(cursor, nxt, partition_name(cursor, granularity)))
        cursor = nxt

    to_drop: List[PartitionRange] = []
    if retention is not None:
        cutoff = add_period(current, granularity, -retention)
        to_drop = [p for p in sorted(existing, key=lambda p: p.start) if p.end <= cutoff]
        if len(to_drop) == len(existing) and not to_add:
            to_drop = to_drop[:-1]  # таблица не должна остаться без партиций

    return PartitionPlan(to_add=to_add, to_drop=to_drop)


def add_partition_sql(table: str, rng: PartitionRange) -> str:
    return (
        f"ALTER TABLE {table} ADD PARTITION {rng.name} "
        f"START (date '{rng.start.isoformat()}') INCLUSIVE "
        f"END (date '{rng.end.isoformat()}') EXCLUSIVE"
    )


def drop_partition_sql(table: str, rng: PartitionRange) -> str:
    return f"ALTER TABLE {table} DROP PARTITION FOR (date '{rng.start.isoformat()}')"

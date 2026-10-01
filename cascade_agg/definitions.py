"""类型定义：事件、查询定义、物化行。"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

ALLOWED_WINDOWS = (60, 300, 900, 3600)
GROUP_FIELDS = ("event_type", "source_id")
SELECT_FIELDS = ("count", "sum", "min", "max")


@dataclass(frozen=True)
class Event:
    """原始事件。时间戳为整数秒，允许乱序与迟到。"""

    ts: int
    event_type: str
    source_id: str
    value: int


@dataclass(frozen=True)
class QueryDef:
    """查询定义，一次性加载，运行中不修改。

    source 为 None 表示读取原始事件；否则为上游查询名。
    select 仅在读取上游时使用，取上游行的 count/sum/min/max 之一作为输入值。
    仅当分组的 sum >= threshold 时发布该行。
    """

    name: str
    source: Optional[str]
    window: int
    group_by: Tuple[str, ...]
    threshold: int
    select: Optional[str] = None


@dataclass(frozen=True)
class Row:
    """一条可见物化行。group 与查询的 group_by 字段顺序对齐。"""

    query: str
    window_start: int
    group: Tuple[str, ...]
    count: int
    sum: int
    min: int
    max: int

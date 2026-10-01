"""Type definitions for the cascading window aggregation library."""

from dataclasses import dataclass
from typing import Optional, Tuple

GROUP_FIELDS = ("event_type", "source_id")
WINDOW_SIZES = (60, 300, 900, 3600)
UPSTREAM_VALUE_FIELDS = ("count", "sum", "min", "max")


@dataclass(frozen=True)
class Event:
    """A single deduplicated event delivered exactly once."""

    timestamp: int
    event_type: str
    source_id: str
    value: int


@dataclass(frozen=True)
class QueryDef:
    """Structured query definition, loaded once and immutable afterwards.

    source is None for queries reading raw events, otherwise the name of
    another query whose visible materialized rows are read.
    value_field is required when source is a query: one of count/sum/min/max.
    """

    name: str
    source: Optional[str]
    window_seconds: int
    group_by: Tuple[str, ...]
    threshold: int
    value_field: Optional[str] = None


@dataclass(frozen=True)
class MaterializedRow:
    """One visible row of a query.

    group holds the values of the query's group_by fields, in the same order.
    """

    window_start: int
    group: Tuple[str, ...]
    count: int
    sum: int
    min: int
    max: int

"""Engine: loads query definitions, ingests event batches, serves reads.

Each query keeps, per (window_start, group_key) cell, a multiset of integer
contributions. Raw queries append event values (events are never retracted).
Downstream queries apply diffs of upstream visible rows: an upstream row
contributes exactly one value (the configured field), and corrections replace
the old contribution instead of accumulating on top of it.
"""

from collections import defaultdict, deque
from typing import Dict, Iterable, List, Optional, Tuple

from .types import GROUP_FIELDS, UPSTREAM_VALUE_FIELDS, WINDOW_SIZES
from .types import Event, MaterializedRow, QueryDef

CellKey = Tuple[int, Tuple[str, ...]]


class _QueryState:
    """Runtime state of one query."""

    def __init__(self, qdef: QueryDef) -> None:
        self.qdef = qdef
        # cell key -> {value: multiplicity}
        self.cells: Dict[CellKey, Dict[int, int]] = {}
        # cell key -> currently visible row
        self.visible: Dict[CellKey, MaterializedRow] = {}
        # downstream only: upstream cell key -> (local cell key, value)
        self.applied: Dict[CellKey, Tuple[CellKey, int]] = {}

    def apply(self, cell: CellKey, value: int, sign: int) -> None:
        bucket = self.cells.setdefault(cell, {})
        remaining = bucket.get(value, 0) + sign
        if remaining < 0:
            raise RuntimeError("contribution underflow: retracting unknown input")
        if remaining:
            bucket[value] = remaining
        else:
            del bucket[value]
        if not bucket:
            del self.cells[cell]

    def aggregate(self, cell: CellKey) -> Optional[MaterializedRow]:
        bucket = self.cells.get(cell)
        if not bucket:
            return None
        count = sum(bucket.values())
        total = sum(value * mult for value, mult in bucket.items())
        row = MaterializedRow(
            window_start=cell[0],
            group=cell[1],
            count=count,
            sum=total,
            min=min(bucket),
            max=max(bucket),
        )
        return row if row.sum >= self.qdef.threshold else None


def _cell_key(qdef: QueryDef, timestamp: int, event_type: str, source_id: str) -> CellKey:
    window_start = (timestamp // qdef.window_seconds) * qdef.window_seconds
    values = {"event_type": event_type, "source_id": source_id}
    return window_start, tuple(values[field] for field in qdef.group_by)


class Engine:
    def __init__(self) -> None:
        self._states: Dict[str, _QueryState] = {}
        self._order: List[str] = []
        self._children: Dict[str, List[str]] = {}
        self._loaded = False

    # -- definition loading -------------------------------------------------

    def load_queries(self, defs: Iterable[QueryDef]) -> None:
        """Load all query definitions exactly once."""
        if self._loaded:
            raise RuntimeError("queries already loaded")
        defs = list(defs)
        names = [d.name for d in defs]
        if len(set(names)) != len(names):
            raise ValueError("duplicate query names")
        known = set(names)
        for d in defs:
            self._validate_shape(d)
        for d in defs:
            if d.source is not None:
                if d.source not in known:
                    raise ValueError(f"{d.name}: unknown source {d.source!r}")
        by_name = {d.name: d for d in defs}
        for d in defs:
            if d.source is not None:
                self._validate_against_upstream(d, by_name[d.source])
        self._order = self._topo_sort(defs)
        self._children = {d.name: [] for d in defs}
        for d in defs:
            if d.source is not None:
                self._children[d.source].append(d.name)
        self._states = {d.name: _QueryState(d) for d in defs}
        self._loaded = True

    @staticmethod
    def _validate_shape(d: QueryDef) -> None:
        if d.window_seconds not in WINDOW_SIZES:
            raise ValueError(f"{d.name}: window must be one of {WINDOW_SIZES}")
        for field in d.group_by:
            if field not in GROUP_FIELDS:
                raise ValueError(f"{d.name}: invalid group field {field!r}")
        if len(set(d.group_by)) != len(d.group_by):
            raise ValueError(f"{d.name}: duplicate group fields")
        if d.source is None:
            if d.value_field is not None:
                raise ValueError(f"{d.name}: raw queries take value from events")
        else:
            if d.value_field not in UPSTREAM_VALUE_FIELDS:
                raise ValueError(
                    f"{d.name}: value_field must be one of {UPSTREAM_VALUE_FIELDS}"
                )

    @staticmethod
    def _validate_against_upstream(d: QueryDef, up: QueryDef) -> None:
        if d.window_seconds < up.window_seconds:
            raise ValueError(f"{d.name}: window shorter than upstream {up.name}")
        if d.window_seconds % up.window_seconds != 0:
            raise ValueError(f"{d.name}: window not a multiple of upstream {up.name}")
        if not set(d.group_by) <= set(up.group_by):
            raise ValueError(f"{d.name}: group_by must be a subset of upstream {up.name}")

    @staticmethod
    def _topo_sort(defs: List[QueryDef]) -> List[str]:
        indegree = {d.name: 0 for d in defs}
        children: Dict[str, List[str]] = defaultdict(list)
        for d in defs:
            if d.source is not None:
                indegree[d.name] += 1
                children[d.source].append(d.name)
        queue = deque(sorted(n for n, deg in indegree.items() if deg == 0))
        order: List[str] = []
        while queue:
            name = queue.popleft()
            order.append(name)
            for child in children[name]:
                indegree[child] -= 1
                if indegree[child] == 0:
                    queue.append(child)
        if len(order) != len(defs):
            raise ValueError("query dependency cycle detected")
        return order

    # -- ingestion and propagation ------------------------------------------

    def ingest(self, events: Iterable[Event]) -> None:
        """Apply one batch of events and propagate corrections."""
        if not self._loaded:
            raise RuntimeError("load_queries must be called first")
        events = list(events)
        # pending[name] = list of (cell_key, value, sign)
        pending: Dict[str, List[Tuple[CellKey, int, int]]] = defaultdict(list)
        for name in self._order:
            state = self._states[name]
            if state.qdef.source is None:
                for event in events:
                    cell = _cell_key(
                        state.qdef, event.timestamp, event.event_type, event.source_id
                    )
                    pending[name].append((cell, event.value, 1))
        for name in self._order:
            changes = pending.get(name)
            if not changes:
                continue
            self._apply_and_propagate(name, changes, pending)

    def _apply_and_propagate(
        self,
        name: str,
        changes: List[Tuple[CellKey, int, int]],
        pending: Dict[str, List[Tuple[CellKey, int, int]]],
    ) -> None:
        state = self._states[name]
        touched = set()
        for cell, value, sign in changes:
            state.apply(cell, value, sign)
            touched.add(cell)
        diffs: List[Tuple[CellKey, Optional[MaterializedRow], Optional[MaterializedRow]]] = []
        for cell in touched:
            new_row = state.aggregate(cell)
            old_row = state.visible.get(cell)
            if new_row == old_row:
                continue
            if new_row is None:
                del state.visible[cell]
            else:
                state.visible[cell] = new_row
            diffs.append((cell, old_row, new_row))
        if not diffs:
            return
        for child_name in self._children[name]:
            child = self._states[child_name]
            for cell, old_row, new_row in diffs:
                if old_row is not None:
                    old_cell, old_value = child.applied.pop(cell)
                    pending[child_name].append((old_cell, old_value, -1))
                if new_row is not None:
                    new_cell, new_value = self._project(child.qdef, name, cell, new_row)
                    child.applied[cell] = (new_cell, new_value)
                    pending[child_name].append((new_cell, new_value, 1))

    def _project(
        self, child_def: QueryDef, upstream_name: str, upstream_cell: CellKey, row: MaterializedRow
    ) -> Tuple[CellKey, int]:
        upstream_def = self._states[upstream_name].qdef
        window_start = (row.window_start // child_def.window_seconds) * child_def.window_seconds
        positions = [upstream_def.group_by.index(f) for f in child_def.group_by]
        group = tuple(upstream_cell[1][i] for i in positions)
        value = getattr(row, child_def.value_field)
        return (window_start, group), value

    # -- reads ----------------------------------------------------------------

    def read(self, query_name: str) -> List[MaterializedRow]:
        """Return the currently visible rows of a query, sorted by window and group."""
        if query_name not in self._states:
            raise KeyError(f"unknown query {query_name!r}")
        rows = self._states[query_name].visible.values()
        return sorted(rows, key=lambda r: (r.window_start, r.group))

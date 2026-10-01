"""级联窗口聚合引擎。

核心机制：每个查询的每个 (分组, 窗口) 桶保存一份"贡献字典"
（贡献键 -> 数值）。原始事件的贡献键是事件序号（只增不减）；
下游查询的贡献键是上游行标识 (上游分组, 上游窗口起点)，
上游行变化时原位替换对应贡献，消失时删除，从而保证更正
是替换而非累加。每批事件处理时按拓扑序逐查询结算被触动的桶，
把可见行的差分（出现/消失/指标变化）传播给所有下游。
"""
from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Set, Tuple

from .definitions import (
    ALLOWED_WINDOWS,
    GROUP_FIELDS,
    SELECT_FIELDS,
    Event,
    QueryDef,
    Row,
)

GroupKey = Tuple[str, ...]
BucketKey = Tuple[GroupKey, int]  # (分组, 窗口起点)
ContribKey = Tuple  # 原始事件: ("__event__", id)；上游行: (上游分组, 上游窗口起点)


class _QueryState:
    __slots__ = ("defn", "buckets", "published")

    def __init__(self, defn: QueryDef) -> None:
        self.defn = defn
        # (分组, 窗口起点) -> {贡献键: 数值}
        self.buckets: Dict[BucketKey, Dict[ContribKey, int]] = {}
        # (分组, 窗口起点) -> 当前可见行（仅可见行）
        self.published: Dict[BucketKey, Row] = {}


def _align(ts: int, window: int) -> int:
    return (ts // window) * window


class CascadeEngine:
    """进程内事件流级联窗口聚合引擎。"""

    def __init__(self) -> None:
        self._states: Dict[str, _QueryState] = {}
        self._order: List[str] = []  # 拓扑序
        self._children: Dict[str, List[str]] = {}
        self._loaded = False
        self._next_event_id = 0

    # ---------------- 加载与校验 ----------------

    def load_queries(self, defs: Iterable[QueryDef]) -> None:
        if self._loaded:
            raise RuntimeError("查询定义只能加载一次")
        defs = list(defs)
        names = [d.name for d in defs]
        if len(set(names)) != len(names):
            raise ValueError("查询名重复")
        by_name = {d.name: d for d in defs}

        for d in defs:
            if d.window not in ALLOWED_WINDOWS:
                raise ValueError(f"{d.name}: 非法窗口长度 {d.window}")
            if any(g not in GROUP_FIELDS for g in d.group_by):
                raise ValueError(f"{d.name}: 非法分组字段 {d.group_by}")
            if len(set(d.group_by)) != len(d.group_by):
                raise ValueError(f"{d.name}: 分组字段重复")
            if d.source is None:
                if d.select is not None:
                    raise ValueError(f"{d.name}: 读取原始事件时不得指定 select")
            else:
                if d.source not in by_name:
                    raise ValueError(f"{d.name}: 未知上游 {d.source}")
                if d.select not in SELECT_FIELDS:
                    raise ValueError(f"{d.name}: 非法 select {d.select}")
                up = by_name[d.source]
                if not set(d.group_by) <= set(up.group_by):
                    raise ValueError(f"{d.name}: 分组字段必须是上游分组字段的子集")
                if d.window % up.window != 0:
                    raise ValueError(f"{d.name}: 窗口长度必须是上游窗口的整数倍")

        # 拓扑排序（Kahn），同时检测环
        indegree = {n: 0 for n in names}
        children: Dict[str, List[str]] = {n: [] for n in names}
        for d in defs:
            if d.source is not None:
                indegree[d.name] += 1
                children[d.source].append(d.name)
        queue = [n for n in names if indegree[n] == 0]
        order: List[str] = []
        while queue:
            n = queue.pop()
            order.append(n)
            for c in children[n]:
                indegree[c] -= 1
                if indegree[c] == 0:
                    queue.append(c)
        if len(order) != len(names):
            raise ValueError("查询依赖存在环")

        self._states = {d.name: _QueryState(d) for d in defs}
        self._order = order
        self._children = children
        self._loaded = True

    # ---------------- 事件接入 ----------------

    def ingest(self, events: Iterable[Event]) -> None:
        if not self._loaded:
            raise RuntimeError("请先加载查询定义")
        touched: Dict[str, Set[BucketKey]] = {n: set() for n in self._states}
        for ev in events:
            cid: ContribKey = ("__event__", self._next_event_id)
            self._next_event_id += 1
            for name in self._order:
                st = self._states[name]
                if st.defn.source is not None:
                    continue
                group = self._event_group(st.defn, ev)
                key = (group, _align(ev.ts, st.defn.window))
                st.buckets.setdefault(key, {})[cid] = ev.value
                touched[name].add(key)
        self._propagate(touched)

    # ---------------- 差分传播 ----------------

    def _propagate(self, touched: Dict[str, Set[BucketKey]]) -> None:
        for name in self._order:
            keys = touched[name]
            if not keys:
                continue
            st = self._states[name]
            for key in keys:
                old_row = st.published.get(key)
                new_row = self._finalize_bucket(st, key)
                if old_row == new_row:
                    continue
                if new_row is None:
                    del st.published[key]
                else:
                    st.published[key] = new_row
                for child_name in self._children[name]:
                    self._apply_diff(
                        self._states[child_name], key, old_row, new_row,
                        touched[child_name],
                    )

    def _finalize_bucket(self, st: _QueryState, key: BucketKey) -> Optional[Row]:
        contribs = st.buckets.get(key)
        if not contribs:
            st.buckets.pop(key, None)
            return None
        values = list(contribs.values())
        total = sum(values)
        if total < st.defn.threshold:
            return None  # 隐藏分组继续累计，但不发布
        group, ws = key
        return Row(
            query=st.defn.name,
            window_start=ws,
            group=group,
            count=len(values),
            sum=total,
            min=min(values),
            max=max(values),
        )

    def _apply_diff(
        self,
        child: _QueryState,
        up_key: BucketKey,
        old_row: Optional[Row],
        new_row: Optional[Row],
        child_touched: Set[BucketKey],
    ) -> None:
        defn = child.defn
        up_group, up_ws = up_key
        group = self._project_group(child.defn, up_group)
        key = (group, _align(up_ws, defn.window))
        contribs = child.buckets.setdefault(key, {})
        cid: ContribKey = up_key
        if old_row is not None:
            # 旧贡献必须存在；删除即"替换/撤回"，不会重复累加
            del contribs[cid]
        if new_row is not None:
            contribs[cid] = self._select_value(defn, new_row)
        if not contribs:
            child.buckets.pop(key, None)
        child_touched.add(key)

    # ---------------- 读取 ----------------

    def visible_rows(self, query: str) -> List[Row]:
        if query not in self._states:
            raise KeyError(f"未知查询: {query}")
        rows = list(self._states[query].published.values())
        rows.sort(key=lambda r: (r.window_start, r.group))
        return rows

    # ---------------- 辅助 ----------------

    @staticmethod
    def _event_group(defn: QueryDef, ev: Event) -> GroupKey:
        return tuple(getattr(ev, g) for g in defn.group_by)

    def _project_group(self, defn: QueryDef, up_group: GroupKey) -> GroupKey:
        up_fields = self._states[defn.source].defn.group_by
        index = {f: i for i, f in enumerate(up_fields)}
        return tuple(up_group[index[f]] for f in defn.group_by)

    @staticmethod
    def _select_value(defn: QueryDef, row: Row) -> int:
        return getattr(row, defn.select)

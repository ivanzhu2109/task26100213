"""差分测试：随机乱序事件分批注入，
每批之后将引擎结果与"按当前全部事件从头计算"的参考实现对比。"""
import random
import unittest
from collections import defaultdict

from cascade_agg import CascadeEngine, Event, QueryDef, Row


def reference_visible_rows(events, defs, name):
    """对全部事件从头计算某查询的可见行（按定义递归）。"""
    defs_by_name = {d.name: d for d in defs}
    memo = {}

    def visible(qname):
        if qname in memo:
            return memo[qname]
        d = defs_by_name[qname]
        buckets = defaultdict(list)  # (group, ws) -> [values]
        if d.source is None:
            for ev in events:
                group = tuple(getattr(ev, g) for g in d.group_by)
                ws = (ev.ts // d.window) * d.window
                buckets[(group, ws)].append(ev.value)
        else:
            up = defs_by_name[d.source]
            for row in visible(d.source):
                index = {f: i for i, f in enumerate(up.group_by)}
                group = tuple(row.group[index[f]] for f in d.group_by)
                ws = (row.window_start // d.window) * d.window
                buckets[(group, ws)].append(getattr(row, d.select))
        rows = []
        for (group, ws), values in buckets.items():
            total = sum(values)
            if total >= d.threshold:
                rows.append(Row(qname, ws, group, len(values),
                                total, min(values), max(values)))
        memo[qname] = rows
        return rows

    return sorted(visible(name), key=lambda r: (r.window_start, r.group))


DEFS = [
    QueryDef("m1", None, 60, ("event_type", "source_id"), 5),
    QueryDef("m2", "m1", 300, ("event_type",), 1, "sum"),
    QueryDef("m3", "m2", 900, (), 1, "max"),
    QueryDef("m4", "m1", 3600, ("source_id",), 3, "min"),
]


class DifferentialTest(unittest.TestCase):
    def test_random_batches_match_reference(self):
        for seed in range(20):
            rng = random.Random(seed)
            events = [
                Event(rng.randint(-1000, 5000),
                      rng.choice(["click", "view", "buy"]),
                      rng.choice(["s1", "s2", "s3"]),
                      rng.randint(-10, 15))
                for _ in range(120)
            ]
            eng = CascadeEngine()
            eng.load_queries(DEFS)
            i = 0
            while i < len(events):
                batch = events[i:i + rng.randint(1, 15)]
                i += len(batch)
                eng.ingest(batch)
                seen = events[:i]
                for q in ("m1", "m2", "m3", "m4"):
                    self.assertEqual(
                        eng.visible_rows(q),
                        reference_visible_rows(seen, DEFS, q),
                        f"seed={seed} 前 {i} 个事件后查询 {q} 不一致",
                    )


if __name__ == "__main__":
    unittest.main()

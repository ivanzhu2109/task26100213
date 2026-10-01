"""级联窗口聚合库的自动化测试。

运行方式（任选其一）：
    python3 -m unittest discover -s tests -v
    python3 -m pytest tests -v
"""
import unittest

from cascade_agg import CascadeEngine, Event, QueryDef


def make_engine(defs):
    eng = CascadeEngine()
    eng.load_queries(defs)
    return eng


class ThresholdExitAndReappearTest(unittest.TestCase):
    """迟到事件使上游分组退出阈值筛选后重新出现，
    下游 count/sum 及可见性必须同步恢复。"""

    def setUp(self):
        self.eng = make_engine([
            QueryDef(name="per_minute", source=None, window=60,
                     group_by=("event_type", "source_id"), threshold=10),
            QueryDef(name="dashboard", source="per_minute", window=300,
                     group_by=("event_type",), threshold=1, select="sum"),
        ])

    def dashboard_rows(self):
        return self.eng.visible_rows("dashboard")

    def test_exit_and_reappear(self):
        # 批次1：sum=13 >= 10，上游可见，下游收到 1 个输入
        self.eng.ingest([Event(10, "click", "s1", 8),
                         Event(20, "click", "s1", 5)])
        rows = self.eng.visible_rows("per_minute")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].sum, 13)
        rows = self.dashboard_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0].count, rows[0].sum), (1, 13))

        # 批次2：迟到负值事件使 sum=7 < 10，上游退出筛选，下游行同步消失
        self.eng.ingest([Event(30, "click", "s1", -6)])
        self.assertEqual(self.eng.visible_rows("per_minute"), [])
        self.assertEqual(self.dashboard_rows(), [])

        # 批次3：迟到正值事件使 sum=11 >= 10，上游重新出现，
        # 下游 count/sum 与可见性同步恢复
        self.eng.ingest([Event(40, "click", "s1", 4)])
        rows = self.eng.visible_rows("per_minute")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].sum, 11)
        rows = self.dashboard_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].window_start, 0)
        self.assertEqual(rows[0].group, ("click",))
        self.assertEqual((rows[0].count, rows[0].sum,
                          rows[0].min, rows[0].max), (1, 11, 11, 11))


class CorrectionCascadeTest(unittest.TestCase):
    """三层级联中的更正传播：
    - 原有唯一最小值被更正（替换而非累加）
    - 重复最大值之一消失，max 无残留
    - 末个输入消失，多层结果全部清空
    """

    def setUp(self):
        self.eng = make_engine([
            QueryDef(name="m1", source=None, window=60,
                     group_by=("event_type", "source_id"), threshold=5),
            QueryDef(name="m2", source="m1", window=300,
                     group_by=("event_type",), threshold=1, select="sum"),
            QueryDef(name="m3", source="m2", window=900,
                     group_by=(), threshold=1, select="max"),
        ])

    def row(self, query):
        rows = self.eng.visible_rows(query)
        self.assertEqual(len(rows), 1, f"{query} 应只有一行")
        return rows[0]

    def test_corrections(self):
        # 批次1：s1 两个事件（验证下游按"行"而非原始事件数展开），
        # s2、s3 提供重复最大值 8
        self.eng.ingest([Event(1, "view", "s1", 2),
                         Event(2, "view", "s1", 4),
                         Event(3, "view", "s2", 8),
                         Event(4, "view", "s3", 8)])
        r2 = self.row("m2")
        # m1 有 3 个可见行（s1 的 count=2 只算 1 个输入）
        self.assertEqual((r2.count, r2.sum, r2.min, r2.max), (3, 22, 6, 8))
        r3 = self.row("m3")
        self.assertEqual((r3.count, r3.sum, r3.min, r3.max), (1, 8, 8, 8))

        # 批次2：迟到事件更正唯一最小值 6 -> 7。
        # 若为累加而非替换，sum 会变成 22+7=29
        self.eng.ingest([Event(5, "view", "s1", 1)])
        r2 = self.row("m2")
        self.assertEqual((r2.count, r2.sum, r2.min, r2.max), (3, 23, 7, 8))

        # 批次3：重复最大值之一（s2=8）退出筛选，max 仍为 8，count 减一
        self.eng.ingest([Event(6, "view", "s2", -4)])  # s2: 8-4=4 < 5
        r2 = self.row("m2")
        self.assertEqual((r2.count, r2.sum, r2.min, r2.max), (2, 15, 7, 8))
        r3 = self.row("m3")  # m2 的 max 未变，m3 不受影响
        self.assertEqual((r3.count, r3.sum), (1, 8))

        # 批次4：末个输入消失，三层结果全部清空，无残留
        self.eng.ingest([Event(7, "view", "s1", -10),   # s1: 7-10=-3
                         Event(8, "view", "s3", -10)])  # s3: 8-10=-2
        self.assertEqual(self.eng.visible_rows("m1"), [])
        self.assertEqual(self.eng.visible_rows("m2"), [])
        self.assertEqual(self.eng.visible_rows("m3"), [])


class BasicSemanticsTest(unittest.TestCase):
    def test_late_event_updates_historical_window(self):
        eng = make_engine([
            QueryDef(name="q", source=None, window=60,
                     group_by=("source_id",), threshold=0),
        ])
        eng.ingest([Event(5000, "click", "s1", 3)])
        eng.ingest([Event(10, "click", "s1", 7)])  # 迟到进入历史窗口 0
        rows = {r.window_start: r for r in eng.visible_rows("q")}
        self.assertEqual(rows[0].sum, 7)
        self.assertEqual(rows[4980].sum, 3)

    def test_window_alignment_and_upstream_bucket(self):
        eng = make_engine([
            QueryDef(name="up", source=None, window=60,
                     group_by=("event_type",), threshold=1),
            QueryDef(name="down", source="up", window=300,
                     group_by=("event_type",), threshold=1, select="count"),
        ])
        # ts=599 落入窗口 [540,600)，其行归入下游窗口 [300,600)
        eng.ingest([Event(599, "click", "s1", 1),
                    Event(10, "click", "s1", 1)])
        rows = eng.visible_rows("down")
        self.assertEqual(len(rows), 2)
        self.assertEqual({r.window_start for r in rows}, {0, 300})
        # select=count：每个上游行贡献其 count 值
        self.assertEqual({r.window_start: r.sum for r in rows},
                         {0: 1, 300: 1})

    def test_no_zero_rows_for_empty_groups(self):
        eng = make_engine([
            QueryDef(name="q", source=None, window=60,
                     group_by=("source_id",), threshold=0),
        ])
        self.assertEqual(eng.visible_rows("q"), [])
        eng.ingest([Event(0, "click", "s1", 0)])  # sum=0 >= 0，有输入才发布
        rows = eng.visible_rows("q")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].count, 1)

    def test_hidden_group_keeps_accumulating(self):
        eng = make_engine([
            QueryDef(name="q", source=None, window=60,
                     group_by=("source_id",), threshold=10),
        ])
        eng.ingest([Event(0, "click", "s1", 4)])
        self.assertEqual(eng.visible_rows("q"), [])
        eng.ingest([Event(1, "click", "s1", 6)])  # 隐藏期间继续累计
        rows = eng.visible_rows("q")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].sum, 10)

    def test_shared_upstream_and_group_subset(self):
        eng = make_engine([
            QueryDef(name="base", source=None, window=60,
                     group_by=("event_type", "source_id"), threshold=1),
            QueryDef(name="by_type", source="base", window=300,
                     group_by=("event_type",), threshold=1, select="sum"),
            QueryDef(name="total", source="base", window=300,
                     group_by=(), threshold=1, select="count"),
        ])
        eng.ingest([Event(0, "a", "s1", 5), Event(1, "a", "s2", 7),
                    Event(2, "b", "s1", 11)])
        by_type = {r.group: r for r in eng.visible_rows("by_type")}
        self.assertEqual(by_type[("a",)].sum, 12)
        self.assertEqual(by_type[("b",)].sum, 11)
        total = eng.visible_rows("total")
        self.assertEqual(len(total), 1)
        self.assertEqual(total[0].sum, 3)  # 3 个可见上游行


class ValidationTest(unittest.TestCase):
    def test_load_only_once(self):
        eng = make_engine([])
        with self.assertRaises(RuntimeError):
            eng.load_queries([])

    def test_ingest_before_load(self):
        with self.assertRaises(RuntimeError):
            CascadeEngine().ingest([])

    def test_cycle_rejected(self):
        with self.assertRaises(ValueError):
            make_engine([
                QueryDef("a", "b", 60, (), 0, "sum"),
                QueryDef("b", "a", 60, (), 0, "sum"),
            ])

    def test_bad_window_rejected(self):
        with self.assertRaises(ValueError):
            make_engine([QueryDef("q", None, 120, (), 0)])

    def test_group_subset_rejected(self):
        with self.assertRaises(ValueError):
            make_engine([
                QueryDef("up", None, 60, ("event_type",), 0),
                QueryDef("down", "up", 300, ("source_id",), 0, "sum"),
            ])

    def test_window_multiple_rejected(self):
        with self.assertRaises(ValueError):
            make_engine([
                QueryDef("up", None, 300, (), 0),
                QueryDef("down", "up", 900, (), 0, "sum"),
                QueryDef("bad", "up", 60, (), 0, "sum"),  # 比上游短
            ])

    def test_definition_order_independent(self):
        # 定义顺序任意：下游先定义也能正常工作
        eng = make_engine([
            QueryDef("down", "up", 300, (), 1, "sum"),
            QueryDef("up", None, 60, (), 1),
        ])
        eng.ingest([Event(0, "a", "s1", 5)])
        self.assertEqual(eng.visible_rows("down")[0].sum, 5)


if __name__ == "__main__":
    unittest.main()

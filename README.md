# cascade — 事件流级联窗口聚合库

纯 Python、进程内存中的多层滚动窗口聚合引擎。上层分析任务直接调用：
一次性加载查询定义，分批接收事件，按查询读取当前可见的物化行。
迟到与乱序事件会持续更正所有受影响的下游结果，保证任意时刻每个查询的
可见行与"按当前全部事件从头计算"的结果完全一致。

## 快速开始

```python
from cascade import Engine, Event, QueryDef

engine = Engine()
engine.load_queries([
    # 第一层：按来源统计每分钟指标，sum 达到 10 才发布
    QueryDef("m1", source=None, window_seconds=60,
             group_by=("source_id",), threshold=10),
    # 第二层：把分钟行按窗口起点归入 5 分钟窗口，取上游 sum 作为输入
    QueryDef("m5", source="m1", window_seconds=300,
             group_by=("source_id",), threshold=0, value_field="sum"),
])

engine.ingest([
    Event(timestamp=5,  event_type="click", source_id="s1", value=8),
    Event(timestamp=10, event_type="click", source_id="s1", value=8),
])
print(engine.read("m1"))
# [MaterializedRow(window_start=0, group=('s1',), count=2, sum=16, min=8, max=8)]
print(engine.read("m5"))
# [MaterializedRow(window_start=0, group=('s1',), count=1, sum=16, min=16, max=16)]

# 迟到事件使上游分组退出阈值筛选，下游同步失效；恢复后下游自动复原
engine.ingest([Event(20, "click", "s1", -10)])
assert engine.read("m1") == [] and engine.read("m5") == []
```

## 概念与规则

- `Event`：`timestamp`（整数秒）、`event_type`、`source_id`、有符号整数 `value`。
  调用方已完成去重与迟到准入，每个事件只交付一次；时间戳允许乱序。
- `QueryDef`：
  - `source`：`None` 表示读取原始事件；否则为另一个查询名，读取其可见物化行。
  - `window_seconds`：60 / 300 / 900 / 3600，按 Unix 零点对齐，左闭右开。
    下游窗口必须是上游的整数倍且不得更短；上游行按其窗口起点归入下游窗口。
  - `group_by`：从 `event_type`、`source_id` 中选取，可为空（不分组）；
    下游只能保留上游分组字段的子集。
  - `threshold`：整数阈值，仅当 `sum >= threshold` 时该行可见。
    没有输入的分组不发布零值行；隐藏分组继续累计，可再次满足阈值。
  - `value_field`：仅读取上游时需要，`count` / `sum` / `min` / `max` 之一。
    每个可见上游行只贡献一个输入（不按其原始事件数展开）。
- 定义一次性加载（`load_queries` 只能调用一次），顺序任意，依赖必须无环，
  允许多个查询共享同一上游。
- 更正是替换而非累加：上游行出现、消失或指标变化，都会以差量形式传播到
  所有下游；末个输入消失后，下游不残留任何行。
- `read(name)` 返回该查询当前可见行，按 `(window_start, group)` 排序；
  不可见行不返回。

## 运行测试

```bash
python3 -m pytest tests -q
```

测试覆盖：阈值退出后重新出现并同步恢复下游、唯一最小值被更正、
重复最大值之一消失、末个输入消失无残留、窗口对齐与分组子集投影、
共享上游、定义校验，以及随机化对照"从头重算"参考实现的一致性校验。

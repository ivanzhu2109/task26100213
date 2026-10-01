# cascade_agg — 事件流级联窗口聚合库

纯 Python、零依赖、进程内存中的级联窗口聚合库。分析任务直接调用：
一次性加载查询定义，分批接收事件，按查询读取当前可见的物化行。
每批事件处理完毕后，任何查询的可见行都与"按当前全部事件从头计算"
的结果完全一致——迟到与乱序事件会持续更正已发布的多层结果。

## 运行环境

- Python 3.7+（仅标准库，无需安装任何依赖或服务）
- 运行测试：`python3 -m unittest discover -s tests -v`

## 数据模型

```python
from cascade_agg import CascadeEngine, Event, QueryDef

Event(ts=1700000005,            # 整数秒时间戳，允许乱序/迟到
      event_type="click",
      source_id="s1",
      value=8)                  # 有符号整数
```

调用方负责去重与迟到准入：每个事件只交付一次，引擎不做撤回单个事件的
操作；"更正"通过迟到事件进入历史窗口、改变上游行的指标与可见性来传播。

## 查询定义

```python
QueryDef(
    name="per_minute",          # 查询名，全局唯一
    source=None,                # None=读取原始事件；否则为上游查询名
    window=60,                  # 60/300/900/3600 秒，Unix 零点对齐，左闭右开
    group_by=("event_type", "source_id"),  # 子集或空元组（不分组）
    threshold=10,               # 整数阈值：分组 sum >= threshold 才发布
    select=None,                # 读上游时必填："count"/"sum"/"min"/"max"
)
```

约束（加载时校验，违反即抛 `ValueError`）：

- 定义一次性加载（`load_queries` 只能调用一次），顺序任意，依赖必须无环；
- 下游窗口必须是上游窗口的整数倍（因此必然不短于上游）；
- 下游 `group_by` 必须是上游 `group_by` 的子集；
- 读原始事件的查询不得指定 `select`。

语义要点：

- 原始事件按自身时间戳归窗；上游物化行按其窗口起点归入下游窗口；
- 每个可见上游行只贡献一个输入（取 `select` 指定的指标），
  不按它包含的原始事件数展开；
- 没有输入的分组不发布零值行；隐藏分组继续累计，可再次越过阈值；
- 更正是替换而非累加：上游行出现、消失或指标变化都会传播到所有下游。

## 使用示例

```python
from cascade_agg import CascadeEngine, Event, QueryDef

eng = CascadeEngine()
eng.load_queries([
    QueryDef("per_minute", None, 60,
             ("event_type", "source_id"), threshold=10),
    QueryDef("dashboard", "per_minute", 300,
             ("event_type",), threshold=1, select="sum"),
])

eng.ingest([Event(10, "click", "s1", 8),
            Event(20, "click", "s1", 5)])
eng.visible_rows("per_minute")
# [Row(query='per_minute', window_start=0, group=('click', 's1'),
#      count=2, sum=13, min=5, max=8)]
eng.visible_rows("dashboard")
# [Row(query='dashboard', window_start=0, group=('click',),
#      count=1, sum=13, min=13, max=13)]

# 迟到事件使上游分组退出阈值筛选，下游行同步消失
eng.ingest([Event(30, "click", "s1", -6)])
eng.visible_rows("dashboard")   # []

# 再一个迟到事件使其重新越过阈值，多层结果同步恢复
eng.ingest([Event(40, "click", "s1", 4)])
eng.visible_rows("dashboard")
# [Row(query='dashboard', window_start=0, group=('click',),
#      count=1, sum=11, min=11, max=11)]
```

## API 一览

- `CascadeEngine()` — 创建引擎，全部状态保存在进程内存中；
- `load_queries(defs)` — 一次性加载查询定义（仅可调用一次）；
- `ingest(events)` — 分批接收事件，返回后全部查询已结算一致；
- `visible_rows(name)` — 读取某查询当前可见行，按
  `(window_start, group)` 排序；不可见行不返回。

## 实现原理

每个查询的每个 `(分组, 窗口)` 桶保存一份贡献字典（贡献键 → 数值）：
原始事件以事件序号为键（只增），下游以上游行标识
`(上游分组, 上游窗口起点)` 为键。每批事件按拓扑序逐查询结算被触动的
桶，把可见行的差分（出现 / 消失 / 指标变化）传播给下游——旧贡献被
原位替换或删除，因此不会重复累加，也不会在末个输入消失后留下残留。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

覆盖：阈值退出后重新出现的多层恢复、唯一最小值被更正（替换而非累加）、
重复最大值之一消失无残留、末个输入消失后三层全部清空、迟到事件更正
历史窗口、空分组不发布零值行、隐藏分组继续累计、共享上游、分组子集
投影、窗口对齐与各类定义校验；另有随机乱序事件的差分测试，逐批与
全量重算的参考实现对比。

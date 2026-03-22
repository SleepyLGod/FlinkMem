# Semantic Operators — 完整技术参考文档

> **适用版本**: V0.1 (Non-Stateful) + V0.2 (Stateful) + V0.2+ (Alignment)
> **生成日期**: 2026-03-17
> **代码路径**: `flink-python/pyflink/semantic_runtime/`

---

## 目录

1. [概述](#1-概述)
2. [V0.1 Non-Stateful Semantic Operators](#2-v01-non-stateful-semantic-operators)
   - 2.1 [公共基础设施 (`_common.py`)](#21-公共基础设施)
   - 2.2 [`sem_filter` — 语义过滤](#22-sem_filter--语义过滤)
   - 2.3 [`sem_map` — 语义映射/提取](#23-sem_map--语义映射提取)
   - 2.4 [`sem_lookup_join` — 检索增强语义 Lookup Join](#24-sem_lookup_join--检索增强语义-lookup-join)
   - 2.5 [`sem_local_topk` — 本地语义重排序](#25-sem_local_topk--本地语义重排序)
3. [V0.2 Stateful 基础模块](#3-v02-stateful-基础模块)
   - 3.1 [`event_model.py` — 事件模型与契约适配器](#31-event_modelpy--事件模型与契约适配器)
   - 3.2 [`state_descriptors.py` — 集中式状态描述符](#32-state_descriptorspy--集中式状态描述符)
   - 3.3 [`timer_policy.py` — 定时器策略](#33-timer_policypy--定时器策略)
   - 3.4 [`async_bridge.py` — 异步桥接模式](#34-async_bridgepy--异步桥接模式)
4. [V0.2 Stateful Semantic Operators](#4-v02-stateful-semantic-operators)
   - 4.1 [`sem_window` — 语义窗口](#41-sem_window--语义窗口)
   - 4.2 [`sem_groupby` — 动态语义分组](#42-sem_groupby--动态语义分组)
   - 4.3 [`sem_agg` — 语义聚合](#43-sem_agg--语义聚合)
   - 4.4 [`sem_search` / `sem_search` — 持续检索](#44-sem_search--持续检索)
   - 4.5 [`sem_topk` (Continuous) — 持续 Top-K](#45-sem_topk_continuous--持续-top-k)
5. [Continuous RAG Workflow](#5-continuous-rag-workflow)
6. [专有名词术语表](#6-专有名词术语表)
7. [Metrics — 指标体系](#7-metrics--指标体系)
8. [V0.2+ 补充 — API 对齐变更](#8-v02-补充--api-对齐变更)

---

## 1. 概述

本项目在 Apache Flink (PyFlink) 之上构建了一套**语义算子 (Semantic Operators)**，将 LLM 调用嵌入到流处理管道中。实现分两个阶段：

| 阶段           | 算子类型     | Flink 基类               | 状态管理                | LLM 交互                             |
| -------------- | ------------ | ------------------------ | ----------------------- | ------------------------------------ |
| **V0.1** | Non-Stateful | `AsyncFunction`        | 无 keyed state          | 每条记录直接调用 LLM                 |
| **V0.2** | Stateful     | `KeyedProcessFunction` | Flink keyed state + TTL | 通过 Async Bridge 侧输出异步调用 LLM |

**V0.1** 提供四个公开的 row-style 算子：`sem_filter`、`sem_map`、`sem_lookup_join`、`sem_local_topk`。
**V0.2** 提供四个公开的 stateful 算子：`sem_window`、`sem_groupby`、`sem_agg`、`sem_topk`。此外还包含内部 workflow helper `sem_search`；工作流编排与指标系统分别在第 5 节和第 7 节说明。

**V0.2+ 的 internal lowering 视角**：
- pointwise `sem_topk` 被看成内部 logical lowering：
  - 先生成 semantic score
  - 再接 classical Top-N 语义
- bounded/window-owned `sem_groupby` 被看成：
  - 先生成 semantic label
  - 再接 classical group-by 语义
- 未来的 `sem_join` 在逻辑层也按同样方式理解：
  - semantic match predicate / score
  - 再接 classical join/filter 语义
- `sem_agg` 是主要例外：
  - `summarize` 与 `compressive` 仍建模为 native semantic reduce
- internal lowering 过程中可能会经过 semantic score/label/match 这样的步骤，但这些不是 public 一等算子

---

## 1.1 当前目录结构

| 目录 | 作用 |
|------|------|
| `public_api.py` | 对外公开的 facade：user-facing request 与 business context |
| `operators/row/` | 低层 row operator kernel 与 expert/internal builder |
| `operators/stateful/` | 低层 stateful kernel 与 expert/internal builder |
| `runtime/` | 内部 runtime 基础设施、workflow 与 search helper |

## 1.2 Public 与 Internal API 边界

顶层包 `pyflink.semantic_runtime` 现在只暴露 public facade：

- `context(...)`
- `sem_map(...)`
- `sem_filter(...)`
- `sem_local_topk(...)`
- `sem_lookup_join(...)`
- `sem_window(...)`
- `sem_topk(...)`
- `sem_groupby(...)`
- `sem_agg(...)`

下面这些仍然存在于子模块中，但属于 internal / expert layer，不是面向普通
user 的 API：

- `RuntimeConfig`
- `QuerySpec`
- `ScopePolicy`
- `TriggerPolicy`
- `Sem*Config`
- low-level `Sem*Function`

## 2. V0.1 Non-Stateful Semantic Operators

V0.1 算子全部继承自 `AsyncFunction`，采用 Flink 的 `AsyncDataStream` 异步 I/O 模式。

**共同设计原则**：

- **`__init__` 只存可序列化配置**：不持有 LLM 连接或运行时对象
- **`open()` 创建 LLM 客户端**：通过 `create_llm_client(config)` 延迟初始化
- **严格 fail-fast**：模型输出非法、检索失败、Flink 超时都会直接抛异常
- **1:1 成功契约**：成功时每条输入恰好产生一条输出

### 2.1 公共基础设施

**文件**: `operators/row/_common.py`

| 函数                                 | 作用                                                                             |
| ------------------------------------ | -------------------------------------------------------------------------------- |
| `validate_schema(obj, schema)`     | 浅层类型检查：验证 dict 是否有指定 key 且类型匹配                                |
| `attach_metrics(parsed, metrics)`  | 将 LLM 调用指标（latency、tokens、attempts）附加到输出 dict                      |

### 2.2 `sem_filter` — 语义过滤

**文件**: `operators/row/sem_filter.py`
**低层 builder**: `build_sem_filter_operator(semantic, runtime_config)`

**用途**: 从语义 intent 构建 row-style semantic filter。backend 选择保持 internal。

| 项目               | 说明                                                                                                     |
| ------------------ | -------------------------------------------------------------------------------------------------------- |
| **Input**    | 任意字符串记录（Flink `Types.STRING()` 或 `PICKLED_BYTE_ARRAY`）                                     |
| **Output**   | JSON 字符串:`{"decision": bool, "confidence": float, "reason": str, "_input": ..., "_metrics": {...}}` |
| **Semantic intent** | `SemSpec.for_sem_filter(...)` 或 `SemSpec(..., output_mode="bool")` |
| **Failure**  | LLM 调用失败、JSON/schema 非法、Flink 超时时直接抛异常                                                  |

**实现思路**：

1. 低层 builder 校验 semantic intent，并绑定 internal runtime config
2. `async_invoke(value)` → 用 prompt 模板格式化输入 → 调用 LLM
2. 解析 LLM 返回的 JSON → 验证 `{decision, confidence, reason}` 三个 key 存在
3. 类型归一化：`decision→bool`，`confidence→float`，`reason→str`
4. 附加 `_metrics` 和 `_input` → 返回 JSON 字符串
5. 过滤本身**不在算子内完成**——下游用 `ds.filter(lambda x: json.loads(x)["decision"])` 做实际过滤

> ⚠️ 设计亮点：算子本身是 **1:1 映射**而非过滤器，保留了被拒绝记录的审计可追溯性。

### 2.3 `sem_map` — 语义映射/提取

**文件**: `operators/row/sem_map.py`
**低层 builder**: `build_sem_map_operator(semantic, runtime_config)`

**用途**: 从语义 intent 构建 row-style semantic map。backend 选择保持 internal。

| 项目               | 说明                                                                                 |
| ------------------ | ------------------------------------------------------------------------------------ |
| **Input**    | 任意字符串记录                                                                       |
| **Output**   | JSON 字符串，结构由 `output_schema` 定义，附加 `_metrics`                        |
| **Semantic intent** | `SemSpec.for_sem_map(...)` |
| **Schema**   | `SemSpec.schema` — 如 `{"sentiment": str, "confidence": float}` |
| **Failure**  | LLM 调用失败、JSON/schema 非法、Flink 超时时直接抛异常                              |

**实现思路**：

1. 低层 builder 校验 semantic intent，并绑定 internal runtime config
2. `async_invoke(value)` → 格式化 prompt → 调用 LLM
2. 解析 JSON → `validate_schema(parsed, output_schema)` 验证 key 和类型
3. 附加 `_metrics` → 返回 JSON 字符串
4. 解析失败或 schema 不匹配 → 直接抛异常

### 2.4 `sem_lookup_join` — 检索增强语义 Lookup Join

**文件**: `operators/row/sem_lookup_join.py`
**类名**: `SemLookupJoinFunction(AsyncFunction)`

**用途**: 对每条记录先检索外部候选集，再让 LLM 做语义匹配/连接。

| 项目 | 说明 |
| ---- | ---- |
| **Input** | 任意字符串记录（查询） |
| **Output** | JSON: `{"_input": ..., "join_result": <LLM解析结果>, "candidate_count": int, "truncated": bool, "_metrics": {...}}` |
| **Prompt** | `prompt_template.format(input=value, candidates=json.dumps(candidates))` |
| **Retriever** | `CandidateRetriever` 抽象接口；支持 `MockCandidateRetriever` 与 `CandidateRetrieverFromSearchBackend` |
| **Failure** | 检索超时、LLM 调用失败或结果解析失败时在 strict 模式下直接失败 |

**实现思路**：
1. `async_invoke(value)` → 调用 `CandidateRetriever.retrieve(query, max_candidates)` 获取候选
2. 用 `asyncio.wait_for(...)` 做严格超时控制
3. 候选数超限时做硬截断并标记 `truncated=True`
4. 将输入和候选集发送给 LLM 做语义匹配
5. 解析 LLM 输出并包装为 `join_result` 返回

**关键配置** (`SemLookupJoinConfig`)：

| 参数 | 默认值 | 含义 |
| ---- | ------ | ---- |
| `max_candidates_per_record` | 20 | 每条记录最大候选数 |
| `retrieve_timeout_ms` | 5000 | 检索超时（毫秒） |
| `search_backend` | None | 可选 `ExternalSearchBackend`；设置后通过共享 backend contract 做检索 |
| `mock_candidates` | None | 测试用固定候选集 |

### 2.5 `sem_local_topk` — 本地语义重排序

**文件**: `operators/row/sem_local_topk.py`
**低层 builder**: `build_sem_local_topk_operator(semantic, k, runtime_config)`

**用途**: 构建 row-style bounded semantic top-k。backend 选择保持 internal。

| 项目 | 说明 |
| ---- | ---- |
| **Input** | JSON 字符串，必须包含 `candidates_field` 指定的候选列表 |
| **Output** | JSON: `{"_input": ..., "top_k": [排序后列表], "k": int, "original_count": int, "_metrics": {...}}` |
| **Semantic intent** | `SemSpec.for_sem_topk(...)` |
| **Failure** | 输入解析失败或 LLM 返回非 list 时在 strict 模式下直接失败 |

**实现思路**：
1. 低层 builder 校验 semantic intent，并绑定 internal runtime config
2. `async_invoke(value)` → 解析输入 JSON → 提取 `candidates` 字段
2. 用 prompt 将完整记录和候选列表发送给 LLM
3. LLM 返回排序后的 JSON list → 截断到 top-k
4. 包装结果 → 返回

**V0.1 vs V0.2 TopK 对比**：

| 维度 | V0.1 `sem_local_topk` | V0.2 `sem_topk` |
| ---- | --------------------- | --------------- |
| 基类 | `AsyncFunction` | `KeyedProcessFunction` |
| 状态 | 无（每次重新排序） | 有（keyed `MapState` 维护候选池） |
| 触发 | 每条输入 | 增量更新 + 定时器重排 |
| LLM | 每次调用 | 当前实现不内置 LLM 调用；依赖上游提供 score，并通过定时器做本地重算 |

### 2.6 内部 Lowering Helper

internal lowering 过程中可能会物化 semantic score / label / match 这样的中间属性。这些 helper 只是实现细节，停留在 lowering/planning 背后，不属于 public operator surface。

## 3. V0.2 Stateful 基础模块

V0.2 引入了四个核心基础模块，所有有状态算子都依赖它们。工作流编排与指标系统单独放在后文。

### 3.1 `event_model.py` — 事件模型与契约适配器

**文件**: `runtime/event_model.py`

提供两个核心数据类和七个契约适配器函数。

**`SemEvent`** — V0.2 所有算子的标准输入格式：

| 字段               | 类型           | 必填 | 含义                                   |
| ------------------ | -------------- | ---- | -------------------------------------- |
| `key`            | `str`        | ✅   | 主分区键 (user_id / session_id)        |
| `payload`        | `str`        | ✅   | 文本内容                               |
| `seq_id`         | `int`        | ✅   | 单调递增序号（排序 + 去重）            |
| `event_time_ms`  | `int \| None` | ❌   | 事件时间戳（None = 仅处理时间）        |
| `proc_time_ms`   | `int`        | 自动 | 处理时间戳（创建时自动填充）           |
| `metadata`       | `dict`       | ❌   | 任意追踪元数据                         |
| `candidates`     | `list[dict]` | ❌   | 预检索候选记录                         |
| `boundary_flags` | `dict`       | ❌   | 语义边界信号 `{"topic_shift": True}` |

**`WindowSnapshot`** — `sem_window` 的输出，表示完整语义窗口：

| 字段                                 | 含义                                               |
| ------------------------------------ | -------------------------------------------------- |
| `key`                              | 分区键                                             |
| `window_id`                        | UUID-based 窗口标识                                |
| `events`                           | 窗口内所有事件的 dict 列表                         |
| `event_count`                      | 事件数量                                           |
| `open_time_ms` / `close_time_ms` | 窗口开启/关闭时间                                  |
| `trigger_reason`                   | `"count"` / `"time"` / `"semantic_boundary"` |

**契约适配器函数**：

| 函数                                               | 转换方向                                   | 用途                                                                         |
| -------------------------------------------------- | ------------------------------------------ | ---------------------------------------------------------------------------- |
| `is_window_snapshot(d)`                          | 类型检测                                   | 判断 dict 是否为 WindowSnapshot                                              |
| `window_snapshot_to_sem_events(snap)`       | WindowSnapshot → List[SemEvent dict] | **Subflow A 适配**：展开窗口为逐条事件，注入 `window_id` 到 metadata |
| `window_snapshot_to_summary_event(snap)`         | WindowSnapshot → 单条 SemEvent dict  | 将整窗口合并为一条摘要（payload = 所有事件 payload 拼接）                    |
| `group_assignment_to_sem_event(assignment)` | sem_groupby 输出 → SemEvent dict     | 将分组结果归一化为 `sem_agg` 可直接消费的事件 envelope                     |
| `retrieve_to_topk_items(output)`                 | SemSearch 输出 → List[候选 dict]         | **Subflow B 适配**：展开检索结果，确保每条有 `candidate_id`          |
| `retrieve_to_answer_context(output)`             | SemSearch 输出 → AnswerSynthesiser 输入  | 将检索输出直接归一化为 `{query, retrieved_context}`                        |
| `topk_to_answer_context(output)`                 | TopK 输出 → AnswerSynthesiser 输入        | **Subflow C 适配**：归一化为 `{query, retrieved_context}`            |

**键选择器**：

- `simple_key_selector(event_dict)`: 取 `event_dict["key"]`
- `composite_key_selector(*fields)`: 用 `|` 拼接多字段

### 3.2 `state_descriptors.py` — 集中式状态描述符

**文件**: `runtime/state_descriptors.py`

**设计目标**：所有算子的 Flink State Descriptor **集中声明在一个文件**，确保：

- 跨算子命名一致（不会意外重名导致 checkpoint 冲突）
- 统一 TTL 配置
- checkpoint 兼容性变更只需改一处

#### OverflowPolicy

当 state 容器达到硬上限时的处理策略：

| 策略            | 行为             |
| --------------- | ---------------- |
| `DROP_OLDEST` | 淘汰最旧条目     |
| `DROP_NEWEST` | 拒绝新条目       |

#### StateSafetyConfig

每个算子的状态安全配置：

| 参数                        | 默认值 | 含义                  |
| --------------------------- | ------ | --------------------- |
| `ttl_seconds`             | 3600   | State TTL（秒）       |
| `max_window_events`       | 500    | 单窗口最大事件数      |
| `max_groups_per_key`      | 50     | 单 key 最大分组数     |
| `max_candidates_per_key`  | 200    | 单 key 最大检索候选数 |
| `max_topk_candidates`     | 100    | TopK 候选池大小       |
| `max_pending_async_items` | 50     | 最大待处理异步项      |

#### TTL 配置

`build_ttl_config(ttl_seconds)` 构建标准 TTL 配置：

- **UpdateType**: `OnReadAndWrite`（读写都刷新 TTL）
- **StateVisibility**: `NeverReturnExpired`（永不返回过期值）

#### 描述符清单

| 描述符函数                             | State 类型     | 用于          |
| -------------------------------------- | -------------- | ------------- |
| `sem_window_event_buffer_descriptor` | `ListState`  | 窗口事件缓冲  |
| `sem_window_meta_descriptor`         | `ValueState` | 窗口元数据    |
| `sem_groupby_profiles_descriptor`    | `MapState`   | 分组 profile  |
| `sem_groupby_pending_events_descriptor` | `ListState` | 待异步分配的 groupby event chunk |
| `sem_search_cache_descriptor`      | `MapState`   | 检索缓存      |
| `sem_agg_buffer_descriptor`          | `ListState`  | 聚合事件缓冲  |
| `sem_agg_value_descriptor`           | `ValueState` | 聚合累积值    |
| `sem_agg_meta_descriptor`            | `ValueState` | 聚合元数据    |
| `sem_topk_candidates_descriptor`     | `MapState`   | TopK 候选池   |
| `sem_topk_snapshot_descriptor`       | `ValueState` | TopK 当前快照 |

### 3.3 `timer_policy.py` — 定时器策略

**文件**: `runtime/timer_policy.py`

**设计目标**：为所有 V0.2 有状态算子提供统一的定时器注册、分发和清理机制。

#### TimerCategory

三种标准化定时器类别：

| 类别          | 用途             | 典型使用者                                                   |
| ------------- | ---------------- | ------------------------------------------------------------ |
| `FLUSH`     | 超时刷新缓冲状态 | sem_window (窗口超时), sem_agg (聚合刷新)                    |
| `RECOMPUTE` | 周期性重计算     | sem_topk (周期重排)                                          |
| `EVICT`     | 清除过期状态     | sem_groupby、sem_search，以及其他开启淘汰扫描的 keyed 算子 |

#### TimerPolicy

每算子配置一个 `TimerPolicy` 实例：

| 参数                      | 默认值 | 含义                                    |
| ------------------------- | ------ | --------------------------------------- |
| `flush_interval_ms`     | 30,000 | FLUSH 定时器间隔（0 = 禁用）            |
| `recompute_interval_ms` | 0      | RECOMPUTE 定时器间隔（0 = 禁用）        |
| `evict_interval_ms`     | 60,000 | EVICT 定时器间隔（0 = 禁用）            |
| `use_event_time`        | False  | True = 事件时间定时器, False = 处理时间 |

#### 核心函数

| 函数                                                                  | 作用                                                 |
| --------------------------------------------------------------------- | ---------------------------------------------------- |
| `encode_timer_key(category)`                                        | 将 TimerCategory 编码为 state key:`"_timer_flush"` |
| `register_timer(timer_service, meta, category, fire_at_ms)`         | 注册定时器并在 meta dict 中记录                      |
| `resolve_timer_category(meta, fired_timestamp, tolerance_ms=200)`   | 根据触发时间戳反查是哪个 category 的定时器           |
| `clear_timer_registration(meta, category)`                          | 定时器触发后从 meta 中清除注册信息                   |
| `schedule_policy_timers(timer_service, meta, policy, base_time_ms)` | 批量注册 policy 中所有启用的定时器                   |

**定时器安全规则**：

- 定时器回调中允许：state 读写、排序、截断、淘汰、指标更新、emit 输出
- 定时器回调中禁止：阻塞/异步 LLM 调用
- 需要 LLM 的工作必须通过 **Side Output → Async Bridge** 处理

### 3.4 `async_bridge.py` — 异步桥接模式

**文件**: `runtime/async_bridge.py`

**设计目标**：解决 `KeyedProcessFunction` 无法直接做异步 LLM 调用的问题。

#### 拓扑模式

```
keyed_stream.process(StatefulOp)
    │                        │
    ├─ main output           └─ side output (OutputTag: "async_work_items")
    │                                  │
    │                           AsyncDataStream.unordered_wait(AsyncWorker)
    │                                  │
    └───── union ─────────────────────-┘
            │
      key_by(key_selector)
            │
      process(MergeFunction)  ← merges async results back
```

#### 核心数据类

**`AsyncWorkItem`** — 算子通过 side output 发出的工作项：

| 字段           | 含义                                                                             |
| -------------- | -------------------------------------------------------------------------------- |
| `key`        | 必须匹配上游 keying                                                              |
| `task_type`  | 工作类型：当前 V0.2 工作流使用 `"classify"` / `"summarize"` / `"retrieve"` |
| `payload`    | 算子自定义负载                                                                   |
| `request_id` | UUID 用于去重/关联                                                               |

**`AsyncResult`** — 异步工作完成后的返回：

| 字段          | 含义         |
| ------------- | ------------ |
| `key`       | 同上         |
| `task_type` | 同上         |
| `result`    | 异步计算结果 |
| `success`   | 是否成功     |
| `error`     | 错误信息     |

#### `build_async_bridge()` 函数

一键完成拓扑接线：

```python
merged = build_async_bridge(
    main_ds=operator_output,      # StatefulOp 的主输出
    async_fn=my_llm_worker,       # AsyncFunction 实例
    merge_fn=MyMergeFunction(),   # KeyedProcessFunction
    key_selector=simple_key_selector,
    timeout_ms=30_000,
    capacity=20,
)
```

内部步骤：

1. `main_ds.get_side_output(ASYNC_WORK_TAG)` → 获取侧输出流
2. `AsyncDataStream.unordered_wait(side_ds, async_fn, ...)` → 异步处理
3. `main_ds.union(async_result_ds)` → 合并
4. `unified_ds.key_by(...).process(merge_fn)` → 键控合并

---

## 4. V0.2 Stateful Semantic Operators

所有 V0.2 算子继承 `KeyedProcessFunction`，共享以下生命周期：

1. **`__init__`**: 仅存储 `Config` dataclass（可序列化）
2. **`open(runtime_context)`**: 从 `state_descriptors` 获取 state handle，初始化 `StatefulOperatorMetrics`
3. **`process_element(value, ctx)`**: 主处理入口，自动检测输入类型（单事件 / WindowSnapshot / async merge-back）
4. **`on_timer(timestamp, ctx)`**: 定时器回调，仅做本地 state 操作 + emit
5. **Side Output**: 需要 LLM 的工作通过 `ctx.output(ASYNC_WORK_TAG, work_item)` 发送

### 4.1 `sem_window` — 语义窗口

**文件**: `operators/stateful/sem_window.py`
**类名**: `SemWindowFunction(KeyedProcessFunction)`

**用途**: 按语义边界（而非固定时间/计数）将事件流切分为窗口。

| 项目             | 说明                                                      |
| ---------------- | --------------------------------------------------------- |
| **Input**  | `SemEvent` dict（keyed stream）                    |
| **Output** | `WindowSnapshot` dict（包含窗口内所有事件）             |
| **State**  | `ListState[event_buffer]` + `ValueState[window_meta]` |
| **Timer**  | FLUSH：窗口打开时注册，超时后强制 flush                   |

**配置** (`SemWindowConfig`):

| 参数                  | 默认值            | 含义           |
| --------------------- | ----------------- | -------------- |
| `max_window_events` | 50                | 计数触发阈值   |
| `window_timeout_ms` | 30,000            | 时间触发（ms） |
| `boundary_flag`     | `"topic_shift"` | 语义边界标志名 |
| `overflow_policy`   | `DROP_OLDEST`   | 超出时淘汰策略 |

**三种触发条件**：

| 触发类型           | 条件                                           | 来源                |
| ------------------ | ---------------------------------------------- | ------------------- |
| **Count**    | 事件数 ≥`max_window_events`                 | 本地计数            |
| **Time**     | 距窗口开启 ≥`window_timeout_ms`             | 处理时间定时器      |
| **Semantic** | 事件 `boundary_flags` 包含 `boundary_flag` | 上游 pre-classifier |

**处理流程**：

1. 收到事件 → 追加到 `ListState` 事件缓冲
2. 如果是首个事件 → 初始化 window_meta（`window_id`, `open_time_ms`），注册 FLUSH 定时器
3. 检查触发条件 → `_check_triggers()` 返回 trigger reason 或 None
4. 若触发 → 构造 `WindowSnapshot` 并 yield → 清空 buffer 和 meta
5. 若溢出（buffer 满）→ 应用 `overflow_policy` 淘汰旧事件

### 4.2 `sem_groupby` — 动态语义分组

**文件**:
- `operators/stateful/sem_groupby.py`
- `operators/stateful/sem_groupby_window.py`
- `operators/stateful/sem_groupby_pipeline.py`

**类名**:
- `SemGroupbyFunction(KeyedProcessFunction)` — operator-owned continuous 路径
- `WindowOwnedSemGroupbyFunction(KeyedProcessFunction)` — bounded/window-owned 路径

**用途**: 将事件动态分配到语义类别（group），支持新组创建和异步 LLM 分类。

| 项目                  | 说明                                                                                                                                    |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| **Input**       | `SemEvent` dict 或 `WindowSnapshot` dict（自动展开）                                                                           |
| **Output**      | 主路径输出 assignment envelope：`{key, group_id, confidence, source, event_seq_id, payload, event_time_ms, metadata, boundary_flags}` |
| **Side Output** | `AsyncWorkItem(task_type="classify")` — 仅在内部 assignment backend 为异步语义方法时发出                                           |
| **State**       | `MapState[group_id → group_profile]` + `ValueState[meta]`                                                                          |

**QuerySpec 接入**（`GroupbyQuerySpec`）:
- public query spec 只表达 grouping intent、scope 和 trigger
- `maintenance_trigger_policy` 已作为独立 maintenance/refinement trigger 进入 spec
- `scope_policy` 现在也支持可闭合的 operator-owned scope：
  - `window_kind = tumbling | semantic | session | sliding | None`
  - `window_size_ms`
  - `session_gap_ms`
  - `boundary_flag`
- `trigger_policy` 已进入 spec，但当前 runtime 支持刻意保持收敛：
  - `operator_owned`: 当前只支持 `on_event`
  - `window_owned`: 当前支持 bounded/window snapshot grouping
- 物理路径选择改为内部决策：
  - 默认：输入已是 `WindowSnapshot` 时走 `window_owned`
  - 否则：走 `operator_owned`

**当前 maintenance 支持**:
- `operator_owned`: 现在支持两类本地 maintenance/refinement：
  - `maintenance_trigger_policy.mode=\"periodic\"`：
    - 周期性 `RECOMPUTE` timer
    - 基于当前本地 scoring method 的高相似组贪心 merge
    - 可选的本地 survivor label 刷新（由内部 kernel config 控制）
    - metadata heartbeat（`last_refine_ms`、`refine_count`、`last_merge_count`）
  - `maintenance_trigger_policy.mode=\"on_scope_close\"`（仅限可闭合 scope）：
    - 当前支持 `session`、`tumbling`、`semantic`
    - scope close 时运行一次本地 refine
    - 然后 reset operator-owned group state，进入下一个 scope epoch
- `window_owned`: 支持 `maintenance_trigger_policy.mode=\"on_scope_close\"`：
  - 在 snapshot close 时运行一次 bounded 本地 refine
  - 本地贪心 merge 后重写 assignment rows
  - 如果内部 kernel config 开启，则会在最终输出前刷新合并后组的本地 label
  - 不跨 scope 保留组状态
- `sliding` / 纯 TTL 没有天然 operator-owned close 事件，因此继续不支持 `maintenance_trigger_policy.mode=\"on_scope_close\"`

**当前内部路径支持**:

| 路径 | 输入形状 | 状态 | 说明 |
|------|----------|------|------|
| `operator_owned` | flat event stream | 已实现 | 对 active groups 做 keyed-state continuous grouping；assignment trigger 当前为 `on_event` |
| `window_owned` | `WindowSnapshot` | 已实现 | 在单个 snapshot 内做 bounded grouping；不跨 scope 复用组状态 |

**配置** (`SemGroupbyConfig`):

| 参数                             | 默认值          | 含义                               |
| -------------------------------- | --------------- | ---------------------------------- |
| `max_groups_per_key`           | 50              | 单 key 最大分组数                  |
| `assignment_method`           | `rule`          | planner/runtime 使用的内部 assignment backend |
| `scope_chunk_size`            | `1`             | 内部 assignment 粒度（`1` = 逐条，`N` = chunk，`len(scope)` = 整个 scope 一次） |
| `confidence_threshold`         | 0.7             | 本地 assignment 方法的内部 reuse threshold |
| `new_group_creation_threshold` | 0.3             | maintenance merge threshold 的内部种子值 |
| `refresh_labels_during_maintenance` | `False`    | maintenance 时是否本地刷新 survivor label |
| `overflow_policy`              | `DROP_OLDEST` | 组数溢出时的淘汰策略               |

**分配流程**：
1. 路径解析：
   - `window_owned`：对一个 snapshot 做 bounded grouping
   - `operator_owned`：跨事件维护 keyed group state
2. 算子评估当前 `existing_groups`
3. 最终结果永远只有两种：
   - 分配到一个已有组
   - 创建一个新组
4. 本地方法（`rule`、`embedding`）同步决定 assignment，并使用内部 threshold
5. 异步语义方法（`llm`）发出 `AsyncWorkItem("classify")`，只在 async merge-back 后输出最终 assignment
6. internal chunking 由 planner/runtime 通过 `scope_chunk_size` 控制
   - `1` = 逐条 assignment
   - `N` = chunked assignment
   - `len(scope)` = 整个 scope 一次 assignment
7. `window_owned + llm` 当前会对整个 snapshot 发出一个 scope-level async request，并在 async worker 内按内部 chunk size 执行
8. `operator_owned + llm` 会先缓冲 pending events，凑满一个 chunk 再发出 async request；scope close 时会冲刷 remainder

**Group Profile 结构**：

```json
{"group_id": "abc123", "label": "技术讨论", "event_count": 42, "created_ms": ..., "last_update_ms": ..., "summary": "..."}
```

### 4.3 `sem_agg` — 语义聚合

**文件**:
- `operators/stateful/sem_agg.py`
- `operators/stateful/sem_agg_window.py`
- `operators/stateful/sem_agg_pipeline.py`

**类名**:
- `SemAggFunction(KeyedProcessFunction)` — operator-owned continuous 路径
- `WindowOwnedSemAggFunction(KeyedProcessFunction)` — bounded/window-owned 路径

**用途**: 对 keyed 流或 bounded snapshot 做语义聚合。

| 项目                  | 说明                                                                         |
| --------------------- | ---------------------------------------------------------------------------- |
| **Input**       | `SemEvent` dict；bounded/window-owned 路径也可接受 `WindowSnapshot` dict |
| **Output**      | 聚合结果 dict:`{key, aggregate, event_count, version, mode, timestamp_ms}` |
| **Side Output** | `AsyncWorkItem(task_type="summarize")` — summarize / compressive 模式下发出 |
| **State**       | `ListState[buffer]` + `ValueState[aggregate]` + `ValueState[meta]`     |

**QuerySpec 接入**（`AggQuerySpec`）:
- `agg_method = algebraic | summarize | compressive`
- `trigger_policy` 已进入 spec
- `scope_policy` 现在也支持可闭合的 operator-owned scope：
  - `window_kind = tumbling | semantic | session | sliding | None`
  - `window_size_ms`
  - `session_gap_ms`
  - `boundary_flag`
- 物理路径选择改为内部决策：
  - 默认：输入已是 `WindowSnapshot` 时走 `window_owned`
  - 否则：走 `operator_owned`

**配置** (`SemAggConfig`):

| 参数                  | 默认值          | 含义                               |
| --------------------- | --------------- | ---------------------------------- |
| `mode`              | `"algebraic"` | 基础 runtime mode；`AggQuerySpec` 可以解析到 `algebraic`、`summarize`、`compressive` |
| `max_buffer_events` | 100             | summarize 模式最大缓冲事件数       |
| `flush_interval_ms` | 30,000          | 定时器驱动的 summarize flush       |
| `reduce_fn`         | None            | algebraic 模式的二元归约函数       |
| `overflow_policy`   | `DROP_OLDEST` | 缓冲溢出策略                       |

当 `SemAggFunction` 在没有显式 `AggQuerySpec` 的情况下构造时，`SemAggConfig`
会先被归一成一个唯一的内部 query spec，然后再进入执行。operator core 里不再
保留第二套 legacy trigger 分支。

**当前内部路径支持**:

| 路径 | 输入形状 | 状态 | 说明 |
|------|----------|------|------|
| `operator_owned` | flat event stream | 已实现 | continuous keyed-state aggregation |
| `window_owned` | `WindowSnapshot` | 已实现 | 在单个 snapshot 内做 bounded aggregation；不跨 scope 保留 aggregate state |

**当前 trigger 支持**：

| 路径 | Trigger | 状态 | 说明 |
|------|---------|------|------|
| `window_owned` | 上游决定（`on_scope_close` / `on_event`） | 已实现 | runtime 只消费一个 bounded `WindowSnapshot`；真正的 trigger 语义由上游 window/snapshot 层负责 |
| `operator_owned` | `on_event` | 已实现 | `algebraic` 发 running aggregate；`summarize` / `compressive` 每次接受事件后发 summarize work |
| `operator_owned` | `periodic` | 已实现 | 定时器驱动 aggregate emit / summarize flush |
| `operator_owned` | `idle_flush` | 已实现 | idle 定时器驱动 aggregate emit / summarize flush |
| `operator_owned` | `count_threshold` | 已实现 | 每累计 N 个接受事件后 emit / summarize |
| `operator_owned` | `on_scope_close` | 已实现（仅限可闭合 scope） | 当前支持 `session`、`tumbling`、`semantic`；拒绝 `sliding` / 纯 TTL |

**Mode 1 — Algebraic（代数聚合）**：

- 用户提供 `reduce_fn(accumulator, new_event) → updated_accumulator`
- 每条事件到达 → 立即 reduce → emit 最新累积值
- **不需要 LLM**，纯本地计算
- 示例：求和 `lambda acc, evt: {"total": acc["total"] + evt["value"]}`

**Mode 2 — Summarize（摘要聚合）**：

- 事件缓冲在 `ListState` 中
- 现在由 `trigger_policy` 决定 summarize work 的发射时机：
  - `on_event`
  - `periodic`
  - `idle_flush`
  - `count_threshold`
  - close-capable scope 上的 `on_scope_close`
- `max_buffer_events` 现在是**硬缓冲上限**；当 live buffer 达到上限且当前没有
  summarize request in flight 时，runtime 会立即发 summarize work
- LLM 异步生成摘要 → merge-back 更新 `ValueState[aggregate]`
- summarize request 在飞行期间新进入的事件不会被清空；成功 merge-back 只删除这次已发出的 buffer 前缀
- 适用于需要理解语义的场景（如对话摘要、文档归纳）

**Mode 3 — Compressive（压缩聚合）**：
- 复用 summarize runtime path
- 在发 summarize request 前，先对 buffered events 做一次本地压缩，只保留较小的 suffix budget
- 当前只是本地 bounded compaction heuristic，不是独立的 async compressive worker

**`window_owned` 处理**：
- `algebraic` → 直接对 bounded snapshot 做 reduce，输出一个 final aggregate row
- `summarize` / `compressive` → 对 snapshot 发出一个 bounded summarize work item

**当前 `operator_owned` runtime 说明**：
- `AggQuerySpec` 已能覆盖 runtime mode / TTL / buffer / flush 配置
- `trigger_policy` 已真正进入 operator-owned runtime：
  - `algebraic`：`on_event`、`periodic`、`idle_flush`、`count_threshold`
  - `summarize` / `compressive`：`on_event`、`periodic`、`idle_flush`、`count_threshold`
- `operator_owned + on_scope_close` 现在支持可闭合 scope：
  - `session`：idle gap 关闭当前 scope
  - `tumbling`：bucket rollover 关闭当前 scope
  - `semantic`：boundary flag 在当前 boundary event 被吸收后关闭 scope
- `sliding` / 纯 TTL 仍然没有天然 close 事件，因此继续不支持 `on_scope_close`

### 4.4 `sem_search` — 内部持续检索 helper

**文件**: `runtime/steps/sem_search.py`
**类名**: `SemSearchFunction(KeyedProcessFunction)`

**用途**: 维护 per-key 检索缓存，本地缓存命中时直接返回，缓存未命中时通过 Async Bridge 调用外部检索服务。

| 项目                  | 说明                                                                                                          |
| --------------------- | ------------------------------------------------------------------------------------------------------------- |
| **Input**       | `SemEvent` dict（查询请求）                                                                            |
| **Output**      | 检索结果 envelope:`{key, query, query_seq_id, candidates, candidate_count, truncated, source, timestamp_ms}` |
| **Side Output** | `AsyncWorkItem(task_type="retrieve")` — 缓存未命中时发出                                                   |
| **State**       | `MapState[candidate_id → candidate_record]` + `ValueState[meta]`                                         |

**配置** (`SemSearchConfig`):

| 参数                           | 默认值        | 含义                |
| ------------------------------ | ------------- | ------------------- |
| `max_candidates_per_request` | 20            | 单次检索最大返回数  |
| `max_cache_entries_per_key`  | 200           | per-key 缓存上限    |
| `ttl_seconds`                | 1800          | 缓存 TTL（30 分钟） |
| `evict_interval_ms`          | 120,000       | 淘汰扫描间隔        |
| `cache_match_fn_name`        | `"keyword"` | 本地匹配策略：`keyword` 或轻量本地 `embedding` |
| `cache_embedding_dim`        | 128           | 本地 hashing encoder 维度 |
| `min_relevance_score`        | 0.0           | 最低相关性分数      |
| `search_backend`             | None          | 可选 `ExternalSearchBackend`；workflow 可自动包装成 `SearchBackendAsyncFn` |

**检索流程**：

1. 收到查询事件 → 本地缓存扫描（keyword / embedding match）
2. 只要本地有命中 → 立即 emit cache 结果，并按 `max_candidates_per_request` 截断
3. 本地完全未命中 → side output `AsyncWorkItem("retrieve")` 请求外部检索
4. Async merge-back: 收到外部检索结果 → 更新 `MapState` 缓存 → emit `source="async_store"` 结果
5. 当前实现不会在“部分命中”时同时发本地结果和异步补充；是否补外部仅由“是否完全 miss”决定

**External backend 当前状态**：
- `MockSearchBackend` 已实现，并用于测试与本地 workflow replay
- `FaissSearchBackend` 也已实现，但它只是一个**非常简单的可选 demo backend**
- `cache_match_fn_name="embedding"` 现在使用本地 `HashingTextEncoder`；它是轻量 hashing 向量，不是生产级语义 encoder
- FAISS backend 依赖本地安装 `faiss`，并复用同一套轻量 hashing encoder，不是生产级 embedding 检索
- 真正的生产后端（Milvus/Qdrant/Elasticsearch/Pinecone 等）仍然留在后续阶段

**与 V0.1 `sem_lookup_join` 的关系**：

- V0.1 是 stateless 的：每条请求独立检索，无缓存
- V0.2 `sem_search` 维护 per-key 缓存，连续查询同一 key 时缓存命中率提高

### 4.5 `sem_topk` — 持续 Top-K

**文件**: `operators/stateful/sem_topk.py`
**类名**: `SemTopKFunction(KeyedProcessFunction)`

**用途**: 维护 per-key 的候选池，持续更新 top-k 排名，只在排名变化时发出更新。

**V0.2+ kernel / builder 分层**：
- `SemTopKFunction` 现在被视为 **pure scored-item kernel**
- 打分编排已移到 `operators/stateful/sem_topk_pipeline.py`
- 当前支持的 scorer / rerank 路径有：
  - `external_score`
  - `embedding`（当前可用 `mock` / `local_hashing` 轻量本地 encoder）
  - `llm`
- `pairwise` / `listwise` 现在运行在 **bounded pool 或 operator-owned scope snapshot**
  上，并直接产出 top-k snapshot；它们不会回到 pointwise 的 pure kernel
- 当前 trigger 支持已经显式落地：
  - `on_event`
  - `on_scope_close`
  - `periodic`
  - `idle_flush`
  - `count_threshold`
- 物理路径选择改为内部决策：
  - bounded pool / window snapshot 走 bounded path
  - flat candidate stream 在 trigger 需要 keyed state 时走 operator-owned path

| 项目                  | 说明                                                                                                                                                      |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Input**       | 已经带分数的 flat candidate record dict |
| **Output**      | Top-K 快照 dict:`{key, topk, top_ids, query, query_seq_id, source, total_candidates, version, changed, emission_policy, error, timestamp_ms}` |
| **Side Output** | pure kernel 无 side output；异步打分由 top-k pipeline builder 负责 |
| **State**       | `MapState[candidate_id → candidate]` + `ValueState[snapshot]`                                                                                        |

**配置** (`SemTopKConfig`):

| 参数                      | 默认值      | 含义                          |
| ------------------------- | ----------- | ----------------------------- |
| `max_candidates`        | 100         | 候选池上限                    |
| `recompute_interval_ms` | 10,000      | RECOMPUTE 定时器间隔          |
| `emission_policy`       | `"delta"` | `"delta"` 或 `"snapshot"` |
| `score_field`           | `"score"` | 排序用的分数字段名            |

> 注意：
> - `k` 现在属于 `TopKQuerySpec`，不再属于 `SemTopKConfig`
> - `retrieve_to_topk_items()` 只负责展开 `candidates` 并补齐 `candidate_id`，不会自动重命名分数字段。如果上游检索输出使用的是 `_score` 或其他字段名，需要把 `SemTopKConfig.score_field` 配成对应值。

**两种发射策略**：

| 策略               | 行为                                                   |
| ------------------ | ------------------------------------------------------ |
| **delta**    | 仅当 top-k 列表与上次快照不同时才 emit（节省下游负载） |
| **snapshot** | 每次重排都 emit（适合需要完整状态的下游）              |

**处理流程**：

1. 收到新的 **scored** candidate → 写入 `MapState` 候选池
2. 若候选池超过 `max_candidates` → 应用 `overflow_policy` 淘汰低分项
3. 重排 top-k：按 `score_field` 降序排序 → 取前 k 个
4. 对比上次快照 → 若变化（或 snapshot 策略）→ emit 新快照
5. RECOMPUTE 定时器：周期性强制重排，基于当前候选池与已有 score 做本地重算

对于 retrieval-assisted workflow，当前路径已经变成：

- pointwise：`retrieval envelope -> expander -> optional pointwise scorer -> pure top-k kernel`
- pairwise/listwise：`bounded retrieval pool 或 scope snapshot -> contextual reranker -> top-k snapshot`

**当前内部 dispatch / trigger 边界**：

| 输入形状 | Trigger | 当前状态 | 说明 |
| -------- | ------- | -------- | ---- |
| bounded pool / closed snapshot | `on_scope_close` | 已实现 | 在 bounded pool 上直接产出 final top-k |
| bounded pool / early snapshot | `on_event` | 已实现 | 假设上游 window/pool 层会发 early snapshot |
| flat candidate stream | `on_event` | 已实现 | canonical continuous pointwise top-k 路径 |
| flat candidate stream | `periodic` | 已实现 | pointwise 从 pure kernel 发射；contextual rerank 从 operator-owned scope snapshot 发射 |
| flat candidate stream | `idle_flush` | 已实现 | pointwise 从 pure kernel 发射；contextual rerank 从 operator-owned scope snapshot 发射 |
| flat candidate stream | `count_threshold` | 已实现 | pointwise 从 pure kernel 发射；contextual rerank 从 operator-owned scope snapshot 发射 |
| flat candidate stream | `on_scope_close` | 已实现（仅限可闭合 scope） | 当前支持 `session`、`tumbling`、`semantic`；pointwise 拒绝 `sliding` / 纯 TTL |

也就是说，`sem_topk` 现在的第一阶段 trigger contract 已经明确：
- bounded 输入停留在 bounded path
- flat candidate stream 走 operator-owned path
- 不支持的组合会显式失败，不会静默改变语义

---

## 5. Continuous RAG Workflow

**文件**: `runtime/continuous_rag_workflow.py`

将所有 V0.2 算子编排为完整的 Continuous RAG 管道，分三条子流。

### 5.1 路由机制

输入事件通过 `stream_type` 字段路由：

| `stream_type` 值  | 目标子流                              |
| ------------------- | ------------------------------------- |
| `"memory_event"`  | Subflow A（记忆构建）                 |
| `"query_request"` | Subflow B（检索）→ Subflow C（合成） |

路由由轻量级 `_StreamRouter(KeyedProcessFunction)` 通过 `OutputTag` 侧输出实现。

### 5.2 Subflow A — 记忆构建

```
input events → key_by → sem_window → sem_groupby → sem_agg → memory sink
```

| 阶段 | 算子                   | 输入                      | 输出           |
| ---- | ---------------------- | ------------------------- | -------------- |
| 切窗 | `SemWindowFunction`  | SemEvent             | WindowSnapshot |
| 分组 | `SemGroupbyFunction` | WindowSnapshot (自动展开) | 分组结果       |
| 聚合 | `SemAggFunction`     | 分组结果                  | 聚合记忆条目   |

**Async Bridge 接入**：`sem_groupby` 的 classify 侧输出通过 `build_async_bridge` 接入 LLM 分类器。

### 5.3 Subflow B — 查询检索

```
query requests → key_by → sem_search → sem_topk → retrieved context
```

| 阶段 | 算子                    | 输入                                          | 输出          |
| ---- | ----------------------- | --------------------------------------------- | ------------- |
| 检索 | `SemSearchFunction` | SemEvent (query)                         | 检索 envelope |
| 重排 | `SemTopKFunction`     | 候选记录 (经 `retrieve_to_topk_items` 适配) | Top-K 快照    |

**契约适配**：`retrieve_to_topk_items()` 将检索输出的 `candidates` 列表展开为逐条候选记录，确保每条有 `candidate_id`。分数字段名不会被改写，需与 `SemTopKConfig.score_field` 保持一致。

### 5.4 Subflow C — 答案合成

```
(query ⊕ top-k context) → sem_map (V0.1 async) → answer with audit
```

`build_answer_subflow()` 当前由 `_AnswerSynthesiser` 生成标准化 answer request envelope，包含 `prompt`、`query`、`retrieved_ids`、`workflow_version`、`config_version` 等审计字段。是否继续调用真实 LLM 做最终答案生成，由下游测试 harness 或外部消费者决定；在 real 模式测试中，这一步由 DeepSeek 调用完成。

### 5.5 Stage-Aware Async Merge Functions

`continuous_rag_workflow.py` 中定义的是按阶段拆分的 merge-back 函数，而不是单一的通用 `_AsyncMergeFunction`：

| 类名                             | 作用                                             |
| -------------------------------- | ------------------------------------------------ |
| `_ClassifyAsyncMergeFunction`  | 将 classify 结果归一化为分组 assignment envelope |
| `_SummarizeAsyncMergeFunction` | 将 summarize 结果归一化为 `sem_agg` 风格输出   |
| `_RetrieveAsyncMergeFunction`  | 将 retrieve 结果归一化为 retrieval envelope      |

---

## 6. 专有名词术语表

| 术语                         | 含义                                                                                                                |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| **SemEvent**      | V0.2 标准输入事件，包含 key、payload、seq_id 等字段                                                                 |
| **WindowSnapshot**     | sem_window 输出的完整窗口快照，包含窗口内所有事件                                                                   |
| **Keyed State**        | Flink 按 key 分区的状态，每个 key 有独立的 state 实例                                                               |
| **ListState**          | 有序列表状态，用于事件缓冲（sem_window, sem_agg）                                                                   |
| **MapState**           | 键值映射状态，用于分组 profile、检索缓存、候选池                                                                    |
| **ValueState**         | 单值状态，用于存储元数据、累积值、快照                                                                              |
| **TTL (Time-To-Live)** | State 自动过期机制，防止状态无限增长                                                                                |
| **OverflowPolicy**     | 状态容器满时的处理策略：DROP_OLDEST / DROP_NEWEST                                                                   |
| **Side Output**        | Flink OutputTag 机制，将数据路由到主输出之外的侧流                                                                  |
| **Async Bridge**       | 异步桥接拓扑：side output → AsyncDataStream → union → merge                                                      |
| **AsyncWorkItem**      | 算子发出的异步工作请求（task_type + payload）                                                                       |
| **AsyncResult**        | 异步工作完成后的返回结果                                                                                            |
| **Merge-back**         | 异步结果通过 union 回到主流后由 MergeFunction 处理的过程                                                            |
| **TimerCategory**      | 定时器类型：FLUSH（刷新）、RECOMPUTE（重算）、EVICT（淘汰）                                                         |
| **TimerPolicy**        | 每算子的定时器间隔配置                                                                                              |
| **Boundary Flag**      | 事件中的语义边界标志（如 topic_shift），触发窗口关闭                                                                |
| **Delta Emission**     | 仅在 top-k 列表变化时发出更新（vs snapshot 每次都发）                                                               |
| **Contract Adapter**   | 算子间格式转换函数（如 window_snapshot_to_sem_events）                                                         |
| **Retrieval Envelope** | 检索结果的统一字典格式：`{key, query, query_seq_id, candidates, candidate_count, truncated, source, timestamp_ms}` |
| **Continuous RAG**     | 持续 RAG：记忆持续构建 + 查询持续检索的流式 RAG 模式                                                                |
| **Subflow**            | 拓扑中的子管道（A=记忆构建, B=检索, C=答案合成）                                                                    |

---

## 7. Metrics — 指标体系

**文件**: `runtime/stateful_metrics.py`

### 7.1 架构

每个 V0.2 算子在 `open()` 中调用 `StatefulOperatorMetrics.from_runtime_context(ctx, operator_name)` 创建指标实例。

**双轨模式**：

- **Flink MetricGroup 模式**：在真实 Flink 运行时中，注册到 `cp_stateful.<operator_name>` metric group
- **Local-only 模式**：Flink MetricGroup 不可用时（单元测试），自动降级为本地累加器

### 7.2 OperatorTag

每个算子实例附带不可变元数据标签：

| 字段                 | 含义                            |
| -------------------- | ------------------------------- |
| `operator_name`    | 算子名称（如 `"sem_window"`） |
| `operator_version` | 算子版本（`"v0.2.0"`）        |
| `workflow_version` | 工作流版本                      |
| `config_hash`      | 配置哈希（用于漂移检测）        |

标签嵌入到输出记录和指标标签中，支持审计和回放。

### 7.3 计数器 (Counters)

| 指标名                | 方法                          | 含义                                  |
| --------------------- | ----------------------------- | ------------------------------------- |
| `events_processed`  | `record_event_processed()`  | 处理的事件总数                        |
| `timer_fires`       | `record_timer_fire()`       | 定时器回调触发次数                    |
| `evictions`         | `record_eviction(count)`    | overflow policy 淘汰的条目数          |
| `overflows`         | `record_overflow()`         | 溢出事件次数（buffer/state 达到上限） |
| `stale_windows`     | `record_stale_window()`     | 超时关闭的窗口数                      |
| `boundary_triggers` | `record_boundary_trigger()` | 语义边界触发次数                      |
| `recomputes`        | `record_recompute()`        | top-k / 聚合重计算次数                |
| `async_emits`       | `record_async_emit()`       | 发出的异步工作项数                    |

### 7.4 仪表 (Gauges)

| 指标名                | 方法                                | 含义                    |
| --------------------- | ----------------------------------- | ----------------------- |
| `state_size`        | `update_state_size(size)`         | 当前 keyed state 条目数 |
| `async_queue_depth` | `update_async_queue_depth(depth)` | 待处理异步工作项数      |

### 7.5 指标快照

`metrics.snapshot()` 返回包含所有计数器和标签的 dict，可嵌入审计记录：

```python
{
    "operator_name": "sem_window",
    "operator_version": "v0.2.0",
    "events_processed": 1042,
    "timer_fires": 15,
    "evictions": 3,
    "overflows": 1,
    "stale_windows": 2,
    "boundary_triggers": 8,
    "recomputes": 0,
    "async_emits": 0,
    "state_size": 47,
    "async_queue_depth": 0,
}
```

### 7.6 各算子指标使用

| 算子             | 主要使用的指标                                                             |
| ---------------- | -------------------------------------------------------------------------- |
| `sem_window`   | events_processed, timer_fires, stale_windows, boundary_triggers, evictions |
| `sem_groupby`  | events_processed, async_emits (classify), overflows, evictions             |
| `sem_agg`      | events_processed, timer_fires (flush), async_emits (summarize), overflows  |
| `sem_search` | events_processed, async_emits (retrieve), evictions, state_size            |
| `sem_topk`     | events_processed, recomputes, evictions, state_size                        |

---

## 8. V0.2+ 补充 — API 对齐变更

### 8.1 命名清理

本节记录 V0.2+ 对齐阶段的命名清理。废弃的 public alias 将被移除，而不是无限期保留。

| 旧 public 名称 | 规范名称 | 模块 | 原因 |
| -------------- | -------- | ---- | ---- |
| `sem_join_retrieve` | `sem_lookup_join` | `operators/row/sem_lookup_join.py` | 与 Flink SQL `LOOKUP JOIN` 语义对齐 |
| `SemTopKFunction`（row-style 本地） | `SemLocalTopKFunction` | `operators/row/sem_local_topk.py` | 与有状态的 `SemTopKFunction` 区分 |

**导入示例：**

```python
from pyflink.semantic_runtime import (
    context,
    sem_agg,
    sem_filter,
    sem_groupby,
    sem_local_topk,
    sem_lookup_join,
    sem_map,
    sem_topk,
    sem_window,
)
```

### 8.2 `sem_map` 双模式

`sem_map` 现在支持两种模式，由 `SemSpec.schema` 和 `SemSpec.output_mode` 控制：

| 模式 | `output_schema` | `return_mode` | 输出 | 用途 |
| ---- | --------------- | ------------- | ---- | ---- |
| **结构化** | `dict`（必填） | `"json"`（默认） | 经过 schema 验证的 JSON | 提取、分类 |
| **自由文本** | `None` | `"text"`（隐式） | `{"input": ..., "text": ..., "_mode": "text"}` | 改写、摘要、答案合成 |

**关键规则：**

- 当 `output_schema` 为 `None` 时，`return_mode` 强制为 `"text"`，无论传入什么值。
- 自由文本模式下不对 LLM 响应进行 JSON 解析或 schema 验证。
- 输出 envelope 始终包含 `_mode` 和 `_latency_ms` 以便观测。

```python
# 结构化请求
req = sem_map(
    intent="提取情感: {input}",
    output_schema={"sentiment": str, "confidence": float},
)

# 自由文本请求
req = sem_map(intent="用正式英语改写: {input}")
```

### 8.3 `sem_topk` 排序语义

public `sem_topk` 现在表达为：

- `sem_topk(intent=..., k=..., context=...)`

内部 planner 再决定：

- ranking method
- scorer backend
- path selection
- chunking
- trigger plan

| 排序方法 | 当前状态 | 边界要求 |
| ---- | ---- | ---- |
| `"pointwise"` | 已实现 | 正常 active-scope 维护即可 |
| `"pairwise"` | 已实现 | 需要 bounded candidate pool 或 operator-owned scope snapshot |
| `"listwise"` | 已实现 | 需要 bounded candidate pool 或 operator-owned scope snapshot |

**当前边界必须说明清楚**：

- trigger 和 path 决策属于 internal。
- 当前 runtime 已真正落地这些语义：
  - bounded-pool final rerank（`window_owned + on_scope_close`）
  - bounded-pool early-snapshot rerank（`window_owned + on_event`）
  - operator-owned continuous pointwise top-k（`operator_owned + on_event`）
  - operator-owned timer-driven pointwise top-k（`operator_owned + periodic`）
  - operator-owned contextual rerank over scope snapshots
    - `periodic`
    - `idle_flush`
    - `count_threshold`
    - 自然 close scope 上的 `on_scope_close`
    - 对 `sliding` / 纯 `TTL` 等无天然 close scope 的 internal surrogate
- optimizer/CBO 自动决定 trigger 仍是 internal planner 的后续工作；它不是 public user API 的一部分。

**内部 contextual 执行计划**：

- `TopKContextualPlan` 是 internal-only；不进入 `TopKQuerySpec`
- 当前内部包含：
  - `context_chunk_size`
  - `merge_strategy`
  - `close_surrogate`
- 当前默认：
  - `pairwise` -> `context_chunk_size=2`，`merge_strategy="tournament"`
  - `listwise` -> full-pool context，`merge_strategy="global_rank"`
- 当前 operator-owned contextual rerank 的 surrogate 选择：
  - `sliding + on_scope_close` -> internal `epoch_close`
  - 纯 `TTL + on_scope_close` -> internal `periodic_snapshot`

### 8.4 `SemSpec` — 统一语义规约

**文件**: `sem_spec.py`
**类名**: `SemSpec`（dataclass）

一个最小化的规约，捕获算子的语义"做什么"，与状态管理和拓扑解耦。

| 字段 | 类型 | 默认值 | 描述 |
| ---- | ---- | ------ | ---- |
| `instruction` | `str` | `""` | Prompt 模板、谓词或评分标准 |
| `backend` | `str` | `"llm"` | `"llm"`, `"embedding"`, `"hybrid"`, `"rule"`, `"external_score"` 之一 |
| `output_mode` | `str` | `"json"` | `"bool"`, `"label"`, `"score"`, `"json"`, `"text"`, `"summary"` 之一 |
| `schema` | `dict \| None` | `None` | 当 `output_mode="json"` 时的 key→type 映射 |
| `threshold` | `float \| None` | `None` | 置信度/分数决策边界 |
| `examples` | `list` | `[]` | LLM 后端的 few-shot 示例 |
| `metadata` | `dict` | `{}` | 任意算子特定元数据 |

**便捷构造器：**

```python
# 用于 sem_map
spec = SemSpec.for_sem_map("提取: {input}",
                                 output_schema={"key": str},
                                 return_mode="json")

# 用于 sem_topk
spec = SemSpec.for_sem_topk(
    "按相关性排序",
    threshold=0.5,
)
```

支持 `to_dict()` / `from_dict()` 用于 JSON/YAML 序列化。

### 8.5 `RuntimeConfig` — Internal Typed 配置入口

**文件**: `runtime_config.py`
**类名**: `RuntimeConfig`（dataclass）

`RuntimeConfig` 是 internal assembly/configuration object。它服务于
planner/runtime，而不是普通 user-facing API。

它当前主要承担三件事：

- 持有后端配置与通用默认值
- 保存 operator-specific internal runtime config
- hydrate 出 typed query spec、typed kernel config 和 typed runtime bundle

| 区段 | 子配置 | 关键字段 |
| ---- | ------ | -------- |
| `defaults` | `DefaultsConfig` | `ttl_seconds`, `overflow_policy`, `async_timeout_ms`, `async_capacity`, `metrics_enabled` |
| `llm` | `LLMBackendConfig` | `backend`, `model`, `api_key`, `endpoint`, `temperature`, `max_tokens` |
| `embedding` | `EmbeddingBackendConfig` | `backend`, `model`, `endpoint`, `dimensions` |
| `operators` | `Dict[str, Dict]` | nested 算子配置区段。semantic operators 使用 `query_spec` + `kernel`；`sem_window` / `sem_search` 这类 runtime helper 只使用 `kernel` |
| `workflow` | `Dict` | 工作流级别设置 |

public user 应优先使用 facade：

```python
req = sem_topk(
    intent="rank weather days",
    k=5,
    context=context("window"),
)
```

之后再由 internal runtime assembly 用 `RuntimeConfig` 去 lower。

**Typed helper 方法**：

- `get_topk_query_spec()`
- `get_groupby_query_spec()`
- `get_agg_query_spec()`
- `get_join_query_spec()`
- `get_window_config()`
- `get_topk_kernel_config()`
- `get_groupby_kernel_config()`
- `get_agg_kernel_config()`
- `resolve_topk_runtime_bundle()`
- `resolve_groupby_runtime_bundle()`
- `resolve_agg_runtime_bundle()`
- `resolve_join_runtime_bundle()`

**runtime bundle 内容**：

- typed `QuerySpec`
- typed kernel config
- internal `SemLoweringPlan`

这层现在就是：

- public semantic config
- internal lowering 视角（`semantic attribute + classical operator`）
- native runtime path selection

之间的桥接层。

**布局规则**：

- user 的主入口是 facade 层（`sem_topk(...)`、`sem_groupby(...)`、`sem_agg(...)`、`sem_join(...)`）
- nested `query_spec` + `kernel` 是 semantic operators 的 internal/expert-layer runtime 布局
- nested `kernel` 是 runtime helpers（`sem_window`、`sem_search`）的 internal/expert-layer runtime 布局
- `ttl_seconds` 这类 defaults 会在 internal runtime layer 的 typed hydration 阶段自动注入

**工作流桥接**：

组合式 stateful workflow 现在也可以直接从同一个 typed 配置入口构造：

```python
workflow_cfg = ContinuousRAGConfig.from_runtime_config(cfg)
streams = build_continuous_rag_workflow_from_runtime_config(input_ds, cfg)
```

这样 workflow 的装配也和各个 operator builder 一样，统一走
`QuerySpec + kernel + lowering` 这条配置契约。

### 8.6 外部搜索后端接口

**文件**: `runtime/external_search_backend.py`
**状态**: 抽象接口 + demo 实现。

定义了 `sem_search`（`sem_search`）和 `sem_lookup_join` 使用的可插拔外部向量/搜索后端接口。

| 类 | 描述 |
| -- | ---- |
| `SearchResult` | 数据类：`candidate_id`, `text`, `score`, `metadata` |
| `ExternalSearchBackend` | 抽象基类，提供 `open()`, `close()`, `async search(query, top_k)` |

**当前已有实现**：

- `MockSearchBackend`
- `FaissSearchBackend`（仅 demo，不是 production-grade semantic retrieval）

**计划的 production-grade 实现**（后续阶段）：

- `MilvusSearchBackend`
- `QdrantSearchBackend`
- `ElasticsearchSearchBackend`
- `PineconeSearchBackend`

```python
class MilvusSearchBackend(ExternalSearchBackend):
    async def search(self, query: str, top_k: int = 10, **kwargs) -> list[SearchResult]:
        # 调用 milvus 客户端
        return [SearchResult(candidate_id="...", text="...", score=0.95)]
```

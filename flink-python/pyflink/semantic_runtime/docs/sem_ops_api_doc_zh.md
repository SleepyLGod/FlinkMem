# Semantic Operators — 完整技术参考文档

> **适用版本**: V0.1 (Non-Stateful) + V0.2 (Stateful)
> **生成日期**: 2026-03-17
> **代码路径**: `flink-python/pyflink/semantic_runtime/`

---

## 目录

1. [概述](#1-概述)
2. [V0.1 Non-Stateful Semantic Operators](#2-v01-non-stateful-semantic-operators)
   - 2.1 [公共基础设施 (`_common.py`)](#21-公共基础设施)
   - 2.2 [`sem_filter` — 语义过滤](#22-sem_filter--语义过滤)
   - 2.3 [`sem_map` — 语义映射/提取](#23-sem_map--语义映射提取)
   - 2.4 [`sem_join_retrieve` — 检索增强语义连接](#24-sem_join_retrieve--检索增强语义连接)
   - 2.5 [`sem_topk` — 本地语义重排序](#25-sem_topk--本地语义重排序)
3. [V0.2 Stateful 基础模块](#3-v02-stateful-基础模块)
   - 3.1 [`event_model.py` — 事件模型与契约适配器](#31-event_modelpy--事件模型与契约适配器)
   - 3.2 [`state_descriptors.py` — 集中式状态描述符](#32-state_descriptorspy--集中式状态描述符)
   - 3.3 [`timer_policy.py` — 定时器策略](#33-timer_policypy--定时器策略)
   - 3.4 [`async_bridge.py` — 异步桥接模式](#34-async_bridgepy--异步桥接模式)
4. [V0.2 Stateful Semantic Operators](#4-v02-stateful-semantic-operators)
   - 4.1 [`sem_window` — 语义窗口](#41-sem_window--语义窗口)
   - 4.2 [`sem_groupby` — 动态语义分组](#42-sem_groupby--动态语义分组)
   - 4.3 [`sem_agg` — 语义聚合](#43-sem_agg--语义聚合)
   - 4.4 [`cts_retrieve` — 持续检索](#44-cts_retrieve--持续检索)
   - 4.5 [`sem_topk_continuous` — 持续 Top-K](#45-sem_topk_continuous--持续-top-k)
5. [Continuous RAG Workflow](#5-continuous-rag-workflow)
6. [专有名词术语表](#6-专有名词术语表)
7. [Metrics — 指标体系](#7-metrics--指标体系)

---

## 1. 概述

本项目在 Apache Flink (PyFlink) 之上构建了一套**语义算子 (Semantic Operators)**，将 LLM 调用嵌入到流处理管道中。实现分两个阶段：

| 阶段           | 算子类型     | Flink 基类               | 状态管理                | LLM 交互                             |
| -------------- | ------------ | ------------------------ | ----------------------- | ------------------------------------ |
| **V0.1** | Non-Stateful | `AsyncFunction`        | 无 keyed state          | 每条记录直接调用 LLM                 |
| **V0.2** | Stateful     | `KeyedProcessFunction` | Flink keyed state + TTL | 通过 Async Bridge 侧输出异步调用 LLM |

**V0.1** 提供四个算子：`sem_filter`、`sem_map`、`sem_join_retrieve`、`sem_topk`。
**V0.2** 提供五个有状态算子：`sem_window`、`sem_groupby`、`sem_agg`、`cts_retrieve`、`sem_topk_continuous`，以及四个基础模块；工作流编排与指标系统分别在第 5 节和第 7 节说明。

---

## 2. V0.1 Non-Stateful Semantic Operators

V0.1 算子全部继承自 `AsyncFunction`，采用 Flink 的 `AsyncDataStream` 异步 I/O 模式。

**共同设计原则**：

- **`__init__` 只存可序列化配置**：不持有 LLM 连接或运行时对象
- **`open()` 创建 LLM 客户端**：通过 `create_llm_client(config)` 延迟初始化
- **`timeout()` 永不抛异常**：返回 degraded record
- **1:1 输出保证**：每条输入恰好产生一条输出（正常 or degraded）

### 2.1 公共基础设施

**文件**: `operators/_common.py`

| 函数                                 | 作用                                                                             |
| ------------------------------------ | -------------------------------------------------------------------------------- |
| `validate_schema(obj, schema)`     | 浅层类型检查：验证 dict 是否有指定 key 且类型匹配                                |
| `make_degraded(value, error)`      | 创建降级输出 envelope：`{"_input": value, "_error": error, "_degraded": True}` |
| `make_degraded_json(value, error)` | `make_degraded` 的 JSON 字符串版本                                             |
| `attach_metrics(parsed, metrics)`  | 将 LLM 调用指标（latency、tokens、attempts）附加到输出 dict                      |

### 2.2 `sem_filter` — 语义过滤

**文件**: `operators/sem_filter.py`
**类名**: `SemFilterFunction(AsyncFunction)`

**用途**: 对每条记录调用 LLM 判断是否应保留，输出 `{decision, confidence, reason}`。

| 项目               | 说明                                                                                                     |
| ------------------ | -------------------------------------------------------------------------------------------------------- |
| **Input**    | 任意字符串记录（Flink `Types.STRING()` 或 `PICKLED_BYTE_ARRAY`）                                     |
| **Output**   | JSON 字符串:`{"decision": bool, "confidence": float, "reason": str, "_input": ..., "_metrics": {...}}` |
| **Prompt**   | `prompt_template.format(input=value)`，需要 LLM 返回 `{decision, confidence, reason}` JSON           |
| **Degraded** | `{"_degraded": True, "decision": default_decision, "confidence": 0.0, "reason": error_msg}`            |

**实现思路**：

1. `async_invoke(value)` → 用 prompt 模板格式化输入 → 调用 LLM
2. 解析 LLM 返回的 JSON → 验证 `{decision, confidence, reason}` 三个 key 存在
3. 类型归一化：`decision→bool`，`confidence→float`，`reason→str`
4. 附加 `_metrics` 和 `_input` → 返回 JSON 字符串
5. 过滤本身**不在算子内完成**——下游用 `ds.filter(lambda x: json.loads(x)["decision"])` 做实际过滤

> ⚠️ 设计亮点：算子本身是 **1:1 映射**而非过滤器，保留了被拒绝记录的审计可追溯性。

### 2.3 `sem_map` — 语义映射/提取

**文件**: `operators/sem_map.py`
**类名**: `SemMapFunction(AsyncFunction)`

**用途**: 对每条记录调用 LLM，提取/转换为结构化 JSON 输出。

| 项目               | 说明                                                                                 |
| ------------------ | ------------------------------------------------------------------------------------ |
| **Input**    | 任意字符串记录                                                                       |
| **Output**   | JSON 字符串，结构由 `output_schema` 定义，附加 `_metrics`                        |
| **Prompt**   | `prompt_template.format(input=value)`                                              |
| **Schema**   | `output_schema: Dict[str, type]` — 如 `{"sentiment": str, "confidence": float}` |
| **Degraded** | `{"_input": value, "_error": ..., "_degraded": True}`                              |

**实现思路**：

1. `async_invoke(value)` → 格式化 prompt → 调用 LLM
2. 解析 JSON → `validate_schema(parsed, output_schema)` 验证 key 和类型
3. 附加 `_metrics` → 返回 JSON 字符串
4. 解析失败或 schema 不匹配 → 返回 degraded record

### 2.4 `sem_join_retrieve` — 检索增强语义连接

**文件**: `operators/sem_join_retrieve.py`
**类名**: `SemJoinRetrieveFunction(AsyncFunction)`

**用途**: 对每条记录先检索外部候选集，再让 LLM 做语义匹配/连接。

| 项目                | 说明                                                                                                                 |
| ------------------- | -------------------------------------------------------------------------------------------------------------------- |
| **Input**     | 任意字符串记录（查询）                                                                                               |
| **Output**    | JSON:`{"_input": ..., "join_result": <LLM解析结果>, "candidate_count": int, "truncated": bool, "_metrics": {...}}` |
| **Prompt**    | `prompt_template.format(input=value, candidates=json.dumps(candidates))`                                           |
| **Retriever** | `CandidateRetriever` 抽象接口 (V0.1 仅 `MockCandidateRetriever`)                                                 |
| **Degraded**  | `{"_input": ..., "_error": "retrieve_timeout"/"llm_call_error"/...}`                                               |

**实现思路**：

1. `async_invoke(value)` → 调用 `CandidateRetriever.retrieve(query, max_candidates)` 获取候选
2. 严格超时控制：`asyncio.wait_for(retrieve_call, timeout=retrieve_timeout_ms/1000)`
3. 硬上限截断：候选数 > `max_candidates_per_record` 时截断并标记 `truncated=True`
4. 将输入 + 候选集发送给 LLM 做语义匹配
5. 解析 LLM 输出 → 包装为 `join_result` → 返回

**关键配置** (`SemJoinRetrieveConfig`):

| 参数                          | 默认值 | 含义               |
| ----------------------------- | ------ | ------------------ |
| `max_candidates_per_record` | 20     | 每条记录最大候选数 |
| `retrieve_timeout_ms`       | 5000   | 检索超时（毫秒）   |
| `mock_candidates`           | None   | 测试用固定候选集   |

### 2.5 `sem_topk` — 本地语义重排序

**文件**: `operators/sem_topk.py`
**类名**: `SemTopKFunction(AsyncFunction)` *(注意与 V0.2 的同名类区分)*

**用途**: 对包含候选列表的记录，调用 LLM 重新排序并返回 top-k。

| 项目               | 说明                                                                                                |
| ------------------ | --------------------------------------------------------------------------------------------------- |
| **Input**    | JSON 字符串，必须包含 `candidates_field` 指定的候选列表                                           |
| **Output**   | JSON:`{"_input": ..., "top_k": [排序后列表], "k": int, "original_count": int, "_metrics": {...}}` |
| **Prompt**   | `prompt_template.format(input=json.dumps(record), candidates=json.dumps(candidates))`             |
| **Degraded** | input 解析失败 / LLM 返回非 list → degraded record                                                 |

**实现思路**：

1. `async_invoke(value)` → 解析输入 JSON → 提取 `candidates` 字段
2. 用 prompt 将完整记录和候选列表发送给 LLM
3. LLM 返回排序后的 JSON list → 截断到 top-k
4. 包装结果 → 返回

**V0.1 vs V0.2 TopK 对比**：

| 维度 | V0.1 `sem_topk`  | V0.2 `sem_topk_continuous`                                        |
| ---- | ------------------ | ------------------------------------------------------------------- |
| 基类 | `AsyncFunction`  | `KeyedProcessFunction`                                            |
| 状态 | 无（每次重新排序） | 有（keyed MapState 维护候选池）                                     |
| 触发 | 每条输入           | 增量更新 + 定时器重排                                               |
| LLM  | 每次调用           | 当前实现不内置 LLM 调用；依赖上游提供 score，并通过定时器做本地重算 |

---

## 3. V0.2 Stateful 基础模块

V0.2 引入了四个核心基础模块，所有有状态算子都依赖它们。工作流编排与指标系统单独放在后文。

### 3.1 `event_model.py` — 事件模型与契约适配器

**文件**: `stateful/event_model.py`

提供两个核心数据类和七个契约适配器函数。

**`SemanticEvent`** — V0.2 所有算子的标准输入格式：

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
| `window_snapshot_to_semantic_events(snap)`       | WindowSnapshot → List[SemanticEvent dict] | **Subflow A 适配**：展开窗口为逐条事件，注入 `window_id` 到 metadata |
| `window_snapshot_to_summary_event(snap)`         | WindowSnapshot → 单条 SemanticEvent dict  | 将整窗口合并为一条摘要（payload = 所有事件 payload 拼接）                    |
| `group_assignment_to_semantic_event(assignment)` | sem_groupby 输出 → SemanticEvent dict     | 将分组结果归一化为 `sem_agg` 可直接消费的事件 envelope                     |
| `retrieve_to_topk_items(output)`                 | CtsRetrieve 输出 → List[候选 dict]        | **Subflow B 适配**：展开检索结果，确保每条有 `candidate_id`          |
| `retrieve_to_answer_context(output)`             | CtsRetrieve 输出 → AnswerSynthesiser 输入 | 将检索输出直接归一化为 `{query, retrieved_context}`                        |
| `topk_to_answer_context(output)`                 | TopK 输出 → AnswerSynthesiser 输入        | **Subflow C 适配**：归一化为 `{query, retrieved_context}`            |

**键选择器**：

- `simple_key_selector(event_dict)`: 取 `event_dict["key"]`
- `composite_key_selector(*fields)`: 用 `|` 拼接多字段

### 3.2 `state_descriptors.py` — 集中式状态描述符

**文件**: `stateful/state_descriptors.py`

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
| `DEGRADE_TAG` | 接受但标记为降级 |

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
| `cts_retrieve_cache_descriptor`      | `MapState`   | 检索缓存      |
| `sem_agg_buffer_descriptor`          | `ListState`  | 聚合事件缓冲  |
| `sem_agg_value_descriptor`           | `ValueState` | 聚合累积值    |
| `sem_agg_meta_descriptor`            | `ValueState` | 聚合元数据    |
| `sem_topk_candidates_descriptor`     | `MapState`   | TopK 候选池   |
| `sem_topk_snapshot_descriptor`       | `ValueState` | TopK 当前快照 |

### 3.3 `timer_policy.py` — 定时器策略

**文件**: `stateful/timer_policy.py`

**设计目标**：为所有 V0.2 有状态算子提供统一的定时器注册、分发和清理机制。

#### TimerCategory

三种标准化定时器类别：

| 类别          | 用途             | 典型使用者                                                   |
| ------------- | ---------------- | ------------------------------------------------------------ |
| `FLUSH`     | 超时刷新缓冲状态 | sem_window (窗口超时), sem_agg (聚合刷新)                    |
| `RECOMPUTE` | 周期性重计算     | sem_topk (周期重排)                                          |
| `EVICT`     | 清除过期状态     | sem_groupby、cts_retrieve，以及其他开启淘汰扫描的 keyed 算子 |

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

**文件**: `stateful/async_bridge.py`

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

**文件**: `stateful/semantic_window.py`
**类名**: `SemWindowFunction(KeyedProcessFunction)`

**用途**: 按语义边界（而非固定时间/计数）将事件流切分为窗口。

| 项目             | 说明                                                      |
| ---------------- | --------------------------------------------------------- |
| **Input**  | `SemanticEvent` dict（keyed stream）                    |
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

**文件**: `stateful/sem_groupby_stateful.py`
**类名**: `SemGroupbyFunction(KeyedProcessFunction)`

**用途**: 将事件动态分配到语义类别（group），支持新组创建和异步 LLM 分类。

| 项目                  | 说明                                                                                                                                    |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| **Input**       | `SemanticEvent` dict 或 `WindowSnapshot` dict（自动展开）                                                                           |
| **Output**      | 主路径输出 assignment envelope：`{key, group_id, confidence, source, event_seq_id, payload, event_time_ms, metadata, boundary_flags}` |
| **Side Output** | `AsyncWorkItem(task_type="classify")` — 低置信度分配时发出                                                                           |
| **State**       | `MapState[group_id → group_profile]` + `ValueState[meta]`                                                                          |

**配置** (`SemGroupbyConfig`):

| 参数                             | 默认值          | 含义                               |
| -------------------------------- | --------------- | ---------------------------------- |
| `max_groups_per_key`           | 50              | 单 key 最大分组数                  |
| `confidence_threshold`         | 0.7             | 低于此值 → 发送异步分类           |
| `new_group_creation_threshold` | 0.3             | 与已有组相似度低于此值 → 创建新组 |
| `overflow_policy`              | `DROP_OLDEST` | 组数溢出时的淘汰策略               |

**分配流程**：

1. 输入检测：WindowSnapshot → `window_snapshot_to_semantic_events()` 展开 → 逐条处理
2. 本地候选匹配：遍历已有 group_profile，计算 keyword-based 相似度
3. 高置信度匹配（≥ threshold）→ 直接分配，更新 profile 计数器
4. 低置信度但有候选 → 临时分配 + side output `AsyncWorkItem("classify")` 异步确认
5. 无匹配候选 → 创建新 group → 分配
6. Async merge-back（算子直连模式）: 收到 `{task_type: "classify"}` → 更新 group profile 的 label / 时间戳，并输出紧凑确认结果 `{key, group_id, source, request_id}`

**Group Profile 结构**：

```json
{"group_id": "abc123", "label": "技术讨论", "event_count": 42, "created_ms": ..., "last_update_ms": ..., "summary": "..."}
```

### 4.3 `sem_agg` — 语义聚合

**文件**: `stateful/sem_agg_stateful.py`
**类名**: `SemAggFunction(KeyedProcessFunction)`

**用途**: 对 keyed 事件流做增量聚合，支持两种模式。

| 项目                  | 说明                                                                         |
| --------------------- | ---------------------------------------------------------------------------- |
| **Input**       | `SemanticEvent` dict 或 `WindowSnapshot` dict（自动展开）                |
| **Output**      | 聚合结果 dict:`{key, aggregate, event_count, version, mode, timestamp_ms}` |
| **Side Output** | `AsyncWorkItem(task_type="summarize")` — summarize 模式下发出             |
| **State**       | `ListState[buffer]` + `ValueState[aggregate]` + `ValueState[meta]`     |

**配置** (`SemAggConfig`):

| 参数                  | 默认值          | 含义                               |
| --------------------- | --------------- | ---------------------------------- |
| `mode`              | `"algebraic"` | `"algebraic"` 或 `"summarize"` |
| `max_buffer_events` | 100             | summarize 模式最大缓冲事件数       |
| `flush_interval_ms` | 30,000          | 定时器驱动的 summarize flush       |
| `reduce_fn`         | None            | algebraic 模式的二元归约函数       |
| `overflow_policy`   | `DROP_OLDEST` | 缓冲溢出策略                       |

**Mode 1 — Algebraic（代数聚合）**：

- 用户提供 `reduce_fn(accumulator, new_event) → updated_accumulator`
- 每条事件到达 → 立即 reduce → emit 最新累积值
- **不需要 LLM**，纯本地计算
- 示例：求和 `lambda acc, evt: {"total": acc["total"] + evt["value"]}`

**Mode 2 — Summarize（摘要聚合）**：

- 事件缓冲在 `ListState` 中
- 当 buffer 达到 `max_buffer_events` 或 FLUSH 定时器触发 → 发出 `AsyncWorkItem(task_type="summarize")`
- LLM 异步生成摘要 → merge-back 更新 `ValueState[aggregate]`
- 适用于需要理解语义的场景（如对话摘要、文档归纳）

**WindowSnapshot 处理**：与 `sem_groupby` 相同 — 调用 `window_snapshot_to_semantic_events()` 展开后逐条处理。

### 4.4 `cts_retrieve` — 持续检索

**文件**: `stateful/cts_retrieve.py`
**类名**: `CtsRetrieveFunction(KeyedProcessFunction)`

**用途**: 维护 per-key 检索缓存，本地缓存命中时直接返回，缓存未命中时通过 Async Bridge 调用外部检索服务。

| 项目                  | 说明                                                                                                          |
| --------------------- | ------------------------------------------------------------------------------------------------------------- |
| **Input**       | `SemanticEvent` dict（查询请求）                                                                            |
| **Output**      | 检索结果 envelope:`{key, query, query_seq_id, candidates, candidate_count, source, degraded, timestamp_ms}` |
| **Side Output** | `AsyncWorkItem(task_type="retrieve")` — 缓存未命中时发出                                                   |
| **State**       | `MapState[candidate_id → candidate_record]` + `ValueState[meta]`                                         |

**配置** (`CtsRetrieveConfig`):

| 参数                           | 默认值        | 含义                |
| ------------------------------ | ------------- | ------------------- |
| `max_candidates_per_request` | 20            | 单次检索最大返回数  |
| `max_cache_entries_per_key`  | 200           | per-key 缓存上限    |
| `ttl_seconds`                | 1800          | 缓存 TTL（30 分钟） |
| `evict_interval_ms`          | 120,000       | 淘汰扫描间隔        |
| `cache_match_fn_name`        | `"keyword"` | 本地匹配策略        |
| `min_relevance_score`        | 0.0           | 最低相关性分数      |

**检索流程**：

1. 收到查询事件 → 本地缓存扫描（keyword / embedding match）
2. 只要本地有命中 → 立即 emit cache 结果，并按 `max_candidates_per_request` 截断
3. 本地完全未命中 → side output `AsyncWorkItem("retrieve")` 请求外部检索
4. Async merge-back: 收到外部检索结果 → 更新 `MapState` 缓存 → emit `source="async_store"` 结果
5. 当前实现不会在“部分命中”时同时发本地结果和异步补充；是否补外部仅由“是否完全 miss”决定

**与 V0.1 `sem_join_retrieve` 的关系**：

- V0.1 是 stateless 的：每条请求独立检索，无缓存
- V0.2 `cts_retrieve` 维护 per-key 缓存，连续查询同一 key 时缓存命中率提高

### 4.5 `sem_topk_continuous` — 持续 Top-K

**文件**: `stateful/sem_topk_continuous.py`
**类名**: `SemTopKFunction(KeyedProcessFunction)`

**用途**: 维护 per-key 的候选池，持续更新 top-k 排名，只在排名变化时发出更新。

| 项目                  | 说明                                                                                                                                                      |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Input**       | 候选记录 dict，或 `cts_retrieve` 输出的 retrieval envelope（算子内部会自动展开 `candidates`）                                                         |
| **Output**      | Top-K 快照 dict:`{key, topk, top_ids, query, query_seq_id, source, total_candidates, version, changed, emission_policy, degraded, error, timestamp_ms}` |
| **Side Output** | 当前实现无独立 async rerank side output                                                                                                                   |
| **State**       | `MapState[candidate_id → candidate]` + `ValueState[snapshot]`                                                                                        |

**配置** (`SemTopKConfig`):

| 参数                      | 默认值      | 含义                          |
| ------------------------- | ----------- | ----------------------------- |
| `k`                     | 10          | 保留 top 数量                 |
| `max_candidates`        | 100         | 候选池上限                    |
| `recompute_interval_ms` | 10,000      | RECOMPUTE 定时器间隔          |
| `emission_policy`       | `"delta"` | `"delta"` 或 `"snapshot"` |
| `score_field`           | `"score"` | 排序用的分数字段名            |

> 注意：`retrieve_to_topk_items()` 只负责展开 `candidates` 并补齐 `candidate_id`，不会自动重命名分数字段。如果上游检索输出使用的是 `_score` 或其他字段名，需要把 `SemTopKConfig.score_field` 配成对应值。

**两种发射策略**：

| 策略               | 行为                                                   |
| ------------------ | ------------------------------------------------------ |
| **delta**    | 仅当 top-k 列表与上次快照不同时才 emit（节省下游负载） |
| **snapshot** | 每次重排都 emit（适合需要完整状态的下游）              |

**处理流程**：

1. 收到新候选 → 写入 `MapState` 候选池
2. 若候选池超过 `max_candidates` → 应用 `overflow_policy` 淘汰低分项
3. 重排 top-k：按 `score_field` 降序排序 → 取前 k 个
4. 对比上次快照 → 若变化（或 snapshot 策略）→ emit 新快照
5. RECOMPUTE 定时器：周期性强制重排，基于当前候选池与已有 score 做本地重算

---

## 5. Continuous RAG Workflow

**文件**: `stateful/continuous_rag_workflow.py`

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
| 切窗 | `SemWindowFunction`  | SemanticEvent             | WindowSnapshot |
| 分组 | `SemGroupbyFunction` | WindowSnapshot (自动展开) | 分组结果       |
| 聚合 | `SemAggFunction`     | 分组结果                  | 聚合记忆条目   |

**Async Bridge 接入**：`sem_groupby` 的 classify 侧输出通过 `build_async_bridge` 接入 LLM 分类器。

### 5.3 Subflow B — 查询检索

```
query requests → key_by → cts_retrieve → sem_topk_continuous → retrieved context
```

| 阶段 | 算子                    | 输入                                          | 输出          |
| ---- | ----------------------- | --------------------------------------------- | ------------- |
| 检索 | `CtsRetrieveFunction` | SemanticEvent (query)                         | 检索 envelope |
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
| **SemanticEvent**      | V0.2 标准输入事件，包含 key、payload、seq_id 等字段                                                                 |
| **WindowSnapshot**     | sem_window 输出的完整窗口快照，包含窗口内所有事件                                                                   |
| **Keyed State**        | Flink 按 key 分区的状态，每个 key 有独立的 state 实例                                                               |
| **ListState**          | 有序列表状态，用于事件缓冲（sem_window, sem_agg）                                                                   |
| **MapState**           | 键值映射状态，用于分组 profile、检索缓存、候选池                                                                    |
| **ValueState**         | 单值状态，用于存储元数据、累积值、快照                                                                              |
| **TTL (Time-To-Live)** | State 自动过期机制，防止状态无限增长                                                                                |
| **OverflowPolicy**     | 状态容器满时的处理策略：DROP_OLDEST / DROP_NEWEST / DEGRADE_TAG                                                     |
| **Side Output**        | Flink OutputTag 机制，将数据路由到主输出之外的侧流                                                                  |
| **Async Bridge**       | 异步桥接拓扑：side output → AsyncDataStream → union → merge                                                      |
| **AsyncWorkItem**      | 算子发出的异步工作请求（task_type + payload）                                                                       |
| **AsyncResult**        | 异步工作完成后的返回结果                                                                                            |
| **Merge-back**         | 异步结果通过 union 回到主流后由 MergeFunction 处理的过程                                                            |
| **TimerCategory**      | 定时器类型：FLUSH（刷新）、RECOMPUTE（重算）、EVICT（淘汰）                                                         |
| **TimerPolicy**        | 每算子的定时器间隔配置                                                                                              |
| **Boundary Flag**      | 事件中的语义边界标志（如 topic_shift），触发窗口关闭                                                                |
| **Delta Emission**     | 仅在 top-k 列表变化时发出更新（vs snapshot 每次都发）                                                               |
| **Degraded Record**    | LLM 调用失败时的降级输出，标记 `_degraded: True`                                                                  |
| **Contract Adapter**   | 算子间格式转换函数（如 window_snapshot_to_semantic_events）                                                         |
| **Retrieval Envelope** | 检索结果的统一字典格式：`{key, query, query_seq_id, candidates, candidate_count, source, degraded, timestamp_ms}` |
| **Continuous RAG**     | 持续 RAG：记忆持续构建 + 查询持续检索的流式 RAG 模式                                                                |
| **Subflow**            | 拓扑中的子管道（A=记忆构建, B=检索, C=答案合成）                                                                    |

---

## 7. Metrics — 指标体系

**文件**: `stateful/stateful_metrics.py`

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
| `cts_retrieve` | events_processed, async_emits (retrieve), evictions, state_size            |
| `sem_topk`     | events_processed, recomputes, evictions, state_size                        |

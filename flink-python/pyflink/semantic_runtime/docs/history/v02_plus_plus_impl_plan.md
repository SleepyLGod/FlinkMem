> Historical note: this document is retained as implementation history. It is not the current cleanup source of truth. The canonical cleanup status and active constraints live in `v02_semantic_runtime_cleanup_plan.md`.

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# V0.2++ Implementation Plan

Historical note:

- this file captures an earlier planning snapshot
- current cleanup decisions take precedence where this file still mentions
  public `execution_path` or `auto`
- current canonical cleanup document is
  `docs/updates/v02_semantic_runtime_cleanup_plan.md`

## 1. Goal

V0.2++ 的目标不是增加更多算子名字，而是把 stateful semantic operator 的两条实现路径都做干净：

- **A. Window-owned scope**
  - scope 由上游 window 层维护
  - operator 对 window snapshot / bounded pool 做语义计算
- **B. Operator-owned scope**
  - scope 由 operator 自己通过 keyed state + timer 维护
  - operator 自己就是 continuous query runtime

这两条路径都要保留，因为它们解决的是不同层次的问题：

- A 适合已有明确边界的 scope，例如 tumbling window、semantic window close、retrieval pool
- B 适合真正 continuously evolving active set，例如 sliding / TTL / long session

## 2. 非目标

V0.2++ 仍然不包含：

1. true two-input `sem_join` kernel
2. 生产级 external backend 选型与部署
3. CBO / optimizer 自动生成全局执行计划
4. engine-level vector acceleration

## 3. Core Principle

每个 stateful semantic operator 都要同时区分四层：

1. **SemanticSpec**
   - 语义标准本身
2. **QuerySpec**
   - operator-level continuous query semantics
   - 包括 scope / trigger / method / version
3. **Kernel**
   - 真正维护状态的执行核心
4. **Strategy / Helper**
   - LLM / embedding / retrieval / refinement / rerank 等执行手段

其中：

- **本质实现** = QuerySpec + Kernel
- **可选策略** = strategy / helper / optimization

## 4. Trigger Policy Rule

V0.2++ 统一采用以下规则：

1. `scope policy` 决定谁在 active set 里
2. `trigger policy` 决定什么时候基于当前 active set 计算 / 刷新 / 输出
3. 当前阶段优先由 **user 显式声明 trigger policy**
4. 后续可允许 optimizer 在语义不变前提下选择更优 trigger
5. 只有当 scope 自带封口边界时，`on_scope_close` 才是天然默认值
6. 对 continuously evolving active set，必须允许非 `on_scope_close` trigger

补充约束：

7. **A-path 也必须有 trigger**
   - 只是它的 trigger 通常实现在 window layer / bounded-pool adapter
   - 而不是 operator-owned kernel 里
8. 当前阶段 optimizer 只能在**不改变语义承诺**的前提下选择更优 trigger
   - 例如 batching / idle flush / periodic cadence 调整
   - 不能把 `final on_scope_close` 偷换成 `on_event` intermediate stream

## 5. Path Selection Rule

V0.2++ 采用统一 public API + 可选执行路径：

- `execution_path = "window_owned" | "operator_owned" | "auto"`

默认规则：

1. `auto` 优先选 A-path
2. 但当 operator 明显依赖 continuously evolving state 时，`auto` 必须切到 B-path

优先走 A-path 的典型情况：

- 输入已经是 bounded pool / closed window / closed snapshot
- 目标是 final result
- 方法天然需要 bounded pool，例如 bounded `pairwise` / `listwise`

必须走 B-path 的典型情况：

- scope 是 sliding / TTL / long session 的 active set
- trigger 不是天然 `on_scope_close`
- operator 需要复用历史状态，而不是只处理一包当前快照
- `sem_groupby` / `sem_join` 这类语义要求 groups / dual-side state 持续演化

## 6. Public I/O Rule

V0.2++ 的统一原则是：

1. **public API 尽量统一**
2. **public output envelope 尽量统一**
3. **internal kernel input 不强行统一**

原因：

- A-path 天然吃 bounded pool / window snapshot
- B-path 天然吃 continuous stream / active-set updates

强行让两者共享完全相同的 raw input，会让 A-path 失去清晰边界，也会让 B-path 失去 continuous runtime 的优势。

因此应统一的是：

- `QuerySpec`
- `Config`
- result envelope

不必强行统一的是：

- A-path executor 的内部输入
- B-path kernel 的内部输入

## 7. Work Packages

### WP1. AB Taxonomy Completion

把 A/B 两种实现路径正式写入文档和 API 语义：

- `sem_topk`
- `sem_agg`
- `sem_groupby`
- `sem_join`（先做设计，不做 true kernel）

验收标准：

- 文档明确区分 window-owned scope 与 operator-owned scope
- `scope` / `trigger` / `method` / `backend` 四者边界清楚

### WP2. `sem_topk` AB Completion

#### A-path: window-owned scope

支持把 bounded pool / closed window 作为 `sem_topk` 输入：

- tumbling window close
- semantic window close
- retrieval pool

方法：

- `pointwise`
- `pairwise`
- `listwise`

#### B-path: operator-owned scope

支持 `sem_topk` 自己维护：

- sliding scope
- TTL scope
- long session scope

方法：

- `pointwise` 先完整做完
- `pairwise/listwise` 先至少支持一种明确 trigger，例如 `periodic` 或 `idle_flush`

验收标准：

- A/B 两条路径都能跑
- `pointwise` 在 B-path 下是 canonical continuous top-k
- `pairwise/listwise` 在 A-path 下先稳定支持 bounded pool

### WP3. `sem_agg` AB Completion

#### A-path: window-owned scope

- closed window aggregate
- semantic-window final summary
- retrieval-bounded aggregation

#### B-path: operator-owned scope

- TTL / sliding / session aggregate state
- `on_event` / `count_threshold` / `periodic` / `idle_flush` / `on_scope_close`

方法：

- `algebraic`
- `summarize`
- `compressive`

验收标准：

- `sem_agg` 明确区分 kernel 与 strategy
- flush / finalize / emit 的 trigger contract 清楚

### WP4. `sem_groupby` AB Completion

#### A-path: window-owned scope

- window-bounded semantic grouping
- semantic-window-bounded grouping

#### B-path: operator-owned scope

- active group set
- evolving group lifecycle
- group assignment over continuous streams

方法：

- `rule`
- `embedding`
- `llm`
- `llm_refine`

额外原则：

- 最小本体先支持一个主 trigger（assignment）
- refinement / maintenance trigger 作为扩展层显式加入

验收标准：

- `sem_groupby` 支持 window-bounded grouping 和 continuous grouping
- assignment 与 maintenance 的边界清楚

### WP5. Unified Config + Public API Cleanup

完成以下收口：

1. `RuntimeConfig` 统一入口
2. `QuerySpec` 显式包含 `TriggerPolicy`
3. public docs 与代码一致
4. 命名与目录结构清理准备完毕

### WP6. V0.3 Join Preparation

V0.2++ 只做：

- `JoinQuerySpec`
- A/B 文档语义
- optimization layer contract
- test shell

不做：

- true two-input `sem_join` kernel

## 8. Execution Order

推荐执行顺序：

1. 文档与 QuerySpec 先统一
2. `sem_topk` A/B 完成
3. `sem_groupby` A/B 完成
4. `sem_agg` A/B 完成
5. `RuntimeConfig` 与 public API 收口
6. V0.3 join shell

## 9. Acceptance Criteria

V0.2++ 完成时，应满足：

1. `sem_topk` 同时支持 A-path 与 B-path
2. `sem_agg` 同时支持 A-path 与 B-path
3. `sem_groupby` 同时支持 A-path 与 B-path
4. `TriggerPolicy` 不再只是说明文字，而是明确进入 operator contract
5. `execution_path=auto` 的默认选择规则明确可验证
6. public API / output envelope 统一边界明确
7. 文档清楚区分：
   - 本质实现
   - strategy
   - optimization
8. true `sem_join` 仍然留给 V0.3，但语义设计已准备好

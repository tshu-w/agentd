# agentd implementation specification

兼容性：忽略向后兼容，直接实现目标架构

本文档期望实现 agentd 的自包含实现规格，目标从零重建整个系统，不需要阅读旧代码库或只做参考（优先采用业界主流方案/范式）。

### 全局约定

**ID 格式**：带类型前缀的随机 ID（Stripe 风格）。前缀用于区分实体类型，random 部分为 12 字符（如 `uuid4().hex[:12]`）。

| 实体 | 前缀 | 示例 |
|---|---|---|
| Actor | `act_` | `act_7a3f1b9e2d4c` |
| Turn | `turn_` | `turn_e5b8a1c3f290` |
| Message | `msg_` | `msg_d4e7f2a1b839` |
| Trigger | `trig_` | `trig_c1a9b3e5f472` |

Actor 参数解析规则：以已知前缀（`act_`）开头 → 按 id 匹配；否则 → 按 name 查找 root scope 内非终态 actor。Child actor 须通过 `actor_id` 引用。

**Wire format**：agentd 自有协议（RPC、响应、事件信封）统一使用 snake_case。

---
## 1. 项目背景

agentd（agent daemon）为 agent（主要）和人（次要）提供 CLI 来管理长期存在的 agent。

心智模型：**agent 进程的 supervisord + 结构化 mailbox**。借鉴 actor model 的设计思想（mailbox、状态机、监督树），专门适配 CLI agent 进程——涵盖 one-shot（如 `pi -p`、`claude`）和 long-running（如 Pi RPC）两种模式。不干预 agent 内部执行，只管生命周期、输入投递和会话连续性（checkpoint）。

### 核心定位

- **Agent-agnostic**：CLI 优先服务于不同 agent 的自编排；稳定、可组合、可被另一个 agent 驱动
- **Event-driven**：actor 通过 message 唤醒；mailbox 是统一输入面，接收各类 typed message（`env.*`）
- **薄框架**：框架尽量薄，让 agent 完成具体工作（如通过 SKILL 回复 Telegram、agent 自发注册 Webhook）

### v1 约束

单用户、单机、本地 daemon。Unix socket RPC，SQLite 持久化，PID-based 进程追踪。Backend 模式仅覆盖 one-shot（每 turn 一个进程）；long-running backend（进程跨 turn 存活）作为扩展点保留，不在 v1 实现。

### 典型场景

- 通过 IM 给电脑上的 agent 发消息并收到回复（类 OpenClaw 模式）
- Agent 自编排：一个 agent spawn/emit 其他 agent，形成协作

### 非目标

- **不暴露进程级 API**：对外抽象是 actor/turn，不是进程。Turn 完成由 `turn.end` 事件定义，不由进程退出定义。不提供查询 PID、等待进程退出等接口。
- **不内置特定平台 webhook 逻辑**：HTTP inbox 只接收通用 typed message。GitHub signature 验证、Telegram payload 解析等 provider 相关逻辑由 agent 自行处理。
- **v1 不做进程健康探测**：不主动检测 backend 进程是否 hang 住。依赖进程退出或 daemon 重启时的 reconciliation 兜底。

### 未来方向

Monorepo 内可探索：agent memory、多 agent 聊天室、长期对话 actor、更高层协作模式。Channel 是附加集成层，不是 core 模型的中心对象。

---
## 2. 核心模型

### Actor

Agent 长生命周期容器。一个 actor 可处理多个 turn。

| 类别 | 属性 |
|---|---|
| 身份 | actor_id, name, scope_id, parent_actor_id |
| 配置 | backend, backend_args, cwd, env |
| 运行时 | state, checkpoint, mailbox |

### Turn

Actor 的一次逻辑处理周期。属于且仅属于一个 actor。每个 actor 同时最多一个 active turn。

Turn 不是顶层命令对象；通过 actor 的 status/logs/results 暴露。

### Mailbox

Actor 的输入队列，装载 typed message：`{"type": "message", "payload": {"text": "hello"}}`

所有外部输入都以 message 形式进入系统。CLI `--message` 是 `type=message` 的语法糖。

本文档中 **message** 指发给 actor 的输入，**event** 指系统观测记录（§4 事件模型）。

### Backend

将 agentd turn 映射到具体 agent 执行的适配层。

进程模式：
- **One-shot**：每 turn 启动一个进程，进程退出即 turn 结束。如 `pi -p "<prompt>"`、`claude -p "<prompt>"`、`codex exec "<prompt>"`。
- **Long-running**：进程跨 turn 存活，通过进程内协议接收后续输入。如 Pi RPC。

每个 backend 声明自身 capability flags（如 `supports_steer`），agentd 据此决定投递策略。

### Checkpoint

跨 turn 的会话连续性。属于 actor 而非 turn。详细语义见 §9。

### 实体关系

```
Actor 1──N Turn
Actor 1──N Message (mailbox)
Actor 1──1 Backend (binding)
Actor 1──0..1 Checkpoint
Actor 0..1──N Actor (parent/child)
```

### 状态机

#### Actor states

| 状态 | 含义 |
|---|---|
| `idle` | 无 active turn；可接收输入 |
| `active` | 有一个 active turn（`pending` 或 `running`） |
| `closed` | 终态 |

迁移：`idle -> active`, `idle -> closed`, `active -> idle`, `active -> closed`

关键解释：`active` = 有 active turn，不是"有 live process （e.g., Pi RPC）"。

#### Turn states

| 状态 | 含义 |
|---|---|
| `pending` | turn 已创建，backend 未开始 |
| `running` | backend 已开始处理 |
| `ended` | turn 已结束 |

迁移：`pending -> running`, `pending -> ended`, `running -> ended`

#### Turn outcome

`succeeded` / `failed` / `canceled` / `interrupted`

### Scope 与树模型

#### Name

`name` optional。不提供则为 `null`，只能通过 `actor_id` 引用。

#### Scope 规则

- Root actor：`scope_id = "__root__"`
- Child actor：`scope_id = parent_actor_id`
- Actor name 唯一性在 scope_id 内强制（同 parent 下不重名；`name` 为 `null` 的 actor 不参与检查）

#### Parent/child

- parent/child 关系持久化在 `parent_actor_id` 字段
- Spawn 时通过 `parent_actor_id` 参数建立
- 深度限制：`max_depth`（config，默认 3）
- 每 parent 子 actor 数限制：`max_children_per_parent`（config，默认 8）

#### Close 子树

Close parent 时递归关闭整棵子树（所有 descendants，不只直接 children）。每个被关闭的 actor：
- 如有 active turn → outcome = `canceled`
- Actor → `closed`
- 删除其 triggers

### 关键不变量

- 每个 actor 同时最多一个 active turn
- 同 parent 下非终态 actor name 唯一（`null` name 不参与检查）
- `closed` 是终态，不可逆
- Checkpoint 生命周期长于 turn

---
## 3. 操作语义

### 公开操作

#### Spawn

创建新 actor。

- 无初始输入 → `idle` actor，不创建 turn
- 有初始输入 → 创建 actor + 第一个 turn
- 同 parent 下非终态 actor name 不可重复（`null` name 不参与检查）

#### Emit

向 actor 投递 typed message。

投递模式（deliver_as，默认 `auto`）：
- `auto`：根据 actor 状态、turn 状态和 backend 能力自动选择：
  - actor `idle` → `follow_up`
  - actor `active` + turn `running` + `supports_steer` → `steer`
  - actor `active` + turn `pending` → `follow_up`
  - actor `active` + 不支持 steer → `follow_up`
- `follow_up`：下一 turn 输入；不影响当前 turn
- `steer`：当前 turn 控制输入；仅 backend 支持时合法；不支持则直接报错；不静默降级

规则：
- 对 `closed` actor emit → 报错 `actor_closed`
- 对 `idle` actor emit → 唤醒（打开新 turn）
- 对 `active` actor emit → 由 `deliver_as` 决定作用范围
- `deliver_as=steer` 时禁止携带 `env`

#### Stop / Close

两种公开控制动作，都是 actor 命令：

| 动作 | 当前 turn outcome | actor 目标状态 | 子树 |
|---|---|---|---|
| soft stop | `interrupted` | `idle` | 不影响 |
| hard close | `canceled` | `closed` | 全部关闭 |

执行策略可因 backend 而异，但生命周期语义固定。

#### Wait

等待 actor 回到 `idle` 或 `closed`。actor-centric，不把 turn 变成顶层命令对象。

### 内部机制

#### Turn 形成

由 scheduler 负责。v1：一条 message 开启一个 turn。同一 actor 的 queued message 按 `(created_at, message_id)` FIFO 顺序 claim。

1. actor `idle` + 存在 queued message
2. scheduler claim 最早的 queued message（mailbox state: `queued → claimed`）→ 创建 `pending` turn → 发出 `turn.opened`（含 input snapshot）
3. actor `idle → active`
4. runtime 执行

步骤 2–3 必须原子（同一 DB 事务），防止"message 已 claim 但 turn 未创建"的不一致。

#### Completion 与 terminal intent

`turn.end` 是唯一的 turn completion 信号。Execution termination（如进程退出）不是 completion 定义，只是 cleanup/fallback 信号。

Runtime 追踪两类内部状态：
- **Terminal intent**：`none` | `stop` | `cancel`
- **In-turn controls**：如 `steer`

Outcome 归因：
- `turn.end` + intent `none` → `succeeded` 或 `failed`（由 backend 决定）
- `turn.end` + intent `stop` → `interrupted`
- `turn.end` + intent `cancel` → `canceled`
- 无 `turn.end` + execution termination → 结合 terminal intent 兜底归因

#### Turn-end 处理

Turn 结束后按以下步骤处理：

1. turn → `ended`，写入 outcome / result / error
2. ack mailbox input（mailbox state: `claimed → acked`）
3. actor → `idle`（或 `closed` 如需要）
4. 如有 queued message 且 actor 仍 open → 触发下一 turn

---
## 4. 事件模型

### 事件类型

| 事件 | payload 要点 | 含义 |
|---|---|---|
| `actor.spawned` | actor_id, name(nullable), backend | actor 已创建 |
| `turn.opened` | turn_id, input snapshot | scheduler 打开 turn；input snapshot 是开启输入的事实来源 |
| `turn.started` | turn_id, exec_pid | runtime 已开始处理 |
| `turn.progress` | 见下方 | 最小规范化进度 |
| `turn.result` | 结构化或文本结果 | turn 过程中产出（0~N 次，不是 completion 信号） |
| `turn.end` | outcome, result, error | 唯一的 turn completion 信号 |
| `actor.closed` | reason | actor 进入终态 |
| `actor.checkpoint.loaded` | | checkpoint 已加载 |
| `actor.checkpoint.saved` | | checkpoint 已保存 |
| `actor.checkpoint.missed` | | 无 checkpoint 可加载 |

可选（v1 可不实现）：
- `turn.control.accepted`：steer 投递被 backend 接受时发出
- `turn.execution.terminated`：execution 异常终止时发出（runtime 内部，不对 client 暴露）
- `trigger.fired`：cron trigger 触发时发出

#### `turn.progress` 子类型

每个 backend adapter 负责将 raw output 映射到以下三种子类型：

payload 以 `type` 字段区分子类型：

| type | 含义 | payload |
|---|---|---|
| `text` | agent 输出文本 | `{"type": "text", "content": "..."}` |
| `thinking` | agent 思考过程 | `{"type": "thinking", "content": "..."}` |
| `tool_call` | agent 工具调用 | `{"type": "tool_call", "name": "bash", "args": {...}, "status": "running\|completed\|failed"}` |

各 backend 映射：

| 子类型 | Pi (`--mode json`) | Claude (`stream-json`) | Codex (`--json`) |
|---|---|---|---|
| `text` | `text_delta` 逐条转发 | `assistant.content[type=text]` | `item.completed[type=agent_message]` |
| `thinking` | `thinking` 事件 | `assistant.content[type=thinking]` | 无 |
| `tool_call` | `toolcall_start/end` | `assistant.content[type=tool_use]` | `item.completed[type=tool_call]` |

### 日志规则

- 按序输出规范化 actor/turn events
- turn 边界事件（`turn.opened`、`turn.end`）必须对 client 可见
- client 不需要理解 backend 原始协议即可看懂生命周期

---
## 5. 外部接口

### 5.1 RPC 协议

传输：Unix socket, NDJSON。信封格式对齐 JSON-RPC 2.0，streaming 为自定义扩展。

```
请求：{"jsonrpc": "2.0", "id": "req-1", "method": "actor.spawn", "params": {...}}
成功：{"jsonrpc": "2.0", "id": "req-1", "result": {...}}
错误：{"jsonrpc": "2.0", "id": "req-1", "error": {"code": -32600, "message": "...", "data": {...}}}
流式：{"jsonrpc": "2.0", "id": "req-2", "event": {...}, "done": false}
      {"jsonrpc": "2.0", "id": "req-2", "result": {...}, "done": true}
```

错误码：协议级错误用 JSON-RPC 2.0 标准码（-32700 parse error, -32600 invalid request, -32601 method not found, -32602 invalid params, -32603 internal error）。业务错误统一用 -32000，具体类型放 `error.data.type`：`not_found` / `actor_closed` / `conflict` / `forbidden` / `backend_error` / `daemon_unavailable` / `timeout` / `slow_consumer`

交付语义：RPC 成功返回 = 相关 DB 事务已提交。客户端超时 = 结果未知，应通过查询确认。

`actor` 参数解析：见全局约定。

### 5.2 Actor methods

#### `actor.spawn`

参数：`name`(optional), `backend`, `parent_actor_id`, `backend_args`, `env`, `cwd`, `checkpoint`, message input(`message` 或 `type`+`payload`)

`cwd` 解析优先级：显式参数 > 配置文件目录 > daemon 工作目录。交互（TTY）模式下 CLI 省略 `--cwd` 时自动填充调用方 `$PWD`；非 TTY（如 channel 子进程调用）时留空，由 daemon 的配置文件目录 fallback 生效。

响应：
```json
// 无输入
{"actor_id": "act_7a3f1b9e2d4c", "state": "idle", "current_turn": null, "event_seq": 1}
// 有输入
{"actor_id": "act_7a3f1b9e2d4c", "state": "active", "current_turn": {"turn_id": "turn_e5b8a1c3f290", "state": "pending"}, "event_seq": 2}
```

#### `actor.emit`

参数：`actor`(required), message input(`message` 或 `type`+`payload`), `env`, `deliver_as`(`auto|steer|follow_up`)

响应：`{"actor_id": "act_7a3f1b9e2d4c", "delivery_mode": "follow_up", "woke": true, "event_seq": 42}`

#### `actor.wait`

参数：`actor`(required), `timeout`, `progress`, `since_seq`

流式响应（`progress=true` 时）：先回放最近最多 20 条历史事件（有限窗口），然后逐行推送 live 事件，最终以 `done: true` + actor 状态结束。非流式时阻塞直到 actor 回到 `idle` 或 `closed`。`wait` 是完成等待接口，不是完整日志审计接口；完整历史补流请使用 `actor.logs --follow`。

```json
// progress 流式事件
{"jsonrpc": "2.0", "id": "req-3", "event": {"event_type": "turn.progress", ...}, "done": false}
// 最终结果
{"jsonrpc": "2.0", "id": "req-3", "result": {"actor": {...}, "result": "..."}, "done": true}
```

超时返回错误（`data.type=timeout`），不改变 actor 状态。

#### `actor.stop`

参数：`actor`(required)

响应：`{"actor_id": "act_7a3f1b9e2d4c", "state": "idle", "changed_count": 1}`

边界：actor `idle` → 幂等返回当前状态（`changed_count: 0`）；actor `closed` → 报错 `actor_closed`。

#### `actor.close`

参数：`actor`(required)

响应：`{"actor_id": "act_7a3f1b9e2d4c", "state": "closed", "changed_count": 3}`

关闭 actor 及其整棵子树。边界：actor `idle` → 直接关闭；actor `closed` → 幂等返回（`changed_count: 0`）。

#### `actor.list`

参数：`include_terminal`, `watch`, `limit`。watch 模式流式推送快照。

#### `actor.logs`

参数：`actor`, `since_seq`, `follow`, `limit`。

权威事件流接口。非 follow 模式返回历史事件快照；follow 模式先回放历史（受 `limit` 控制），然后持续推送 live 事件。这是"完整历史 + future"的主要入口。

#### 慢消费者（Slow Consumer）

所有流式接口（`logs --follow`、`wait --progress`、`ps --watch`）均使用有界 per-subscriber 队列。当客户端消费速度跟不上事件产出速度时，服务端返回 `slow_consumer` 错误并附带 `resume_seq`：

```json
{"jsonrpc": "2.0", "id": "req-1", "error": {"code": -32000, "message": "slow consumer", "data": {"type": "slow_consumer", "resume_seq": 142}}}
```

对于事件流（`logs --follow`、`wait --progress`），客户端应使用 `resume_seq` 作为下次请求的 `since_seq` 进行重连。注意 `resume_seq` 指向当前全局尾部，上次消费位置与 `resume_seq` 之间的事件可能被跳过。对于快照流（`ps --watch`），客户端重新发起 watch 请求即可获取最新快照。

#### `actor.status`

参数：`actor`, `include_events`, `include_result`, `since_seq`, `limit`

响应：
```json
{
  "actor": {"actor_id": "...", "name": "..." | null, "state": "active", ...},
  "current_turn": {"turn_id": "...", "state": "running", ...},
  "last_turn": {"turn_id": "...", "state": "ended", "outcome": "succeeded", "result": "...", "error": null},
  "events": [...],
  "next_seq": 100
}
```

### 5.3 Daemon methods

- `daemon.status`：daemon 健康/配置快照
- `daemon.doctor`：健康检查 + 可选自动修复

### 5.4 Trigger methods

- `trigger.add`：参数 `actor`, `schedule`, `type`, `payload`
- `trigger.ls`：可选按 actor 过滤
- `trigger.rm`：按 trigger_id 删除

Cron 格式：标准 5-field（minute hour day month weekday），不支持秒级和 year 字段。时区语义：schedule 按 daemon 进程本地时区解释（与系统 cron 行为一致）；内部存储的 `next_fire_at` 为 UTC。

### 5.5 HTTP Inbox Bridge

可选 HTTP ingress：`POST /v1/actors/{actor_id}/inbox`

请求体：`{"type": "env.webhook.github.push", "payload": {...}}`

Provider-agnostic，内部转发到 `actor.emit`。不负责 provider-specific 语义。支持可选 `Idempotency-Key` 请求头，用于 best-effort 内存级 webhook 重试去重。

### 5.6 CLI

CLI 是 actor-first 的，turn 只作为附属信息出现。

| 命令 | 说明 |
|---|---|
| `spawn` | 创建 actor（无输入→idle，有输入→active+turn） |
| `emit` | 向 actor 发送 typed message |
| `wait` | 等待 actor 回到 idle/closed；`--timeout`、`--progress` |
| `stop` | soft stop（默认）或 `--close` hard close；分别映射到 `actor.stop` / `actor.close` RPC |
| `ps` | 列出 actors |
| `logs` | 查看/follow actor logs |
| `status` | daemon 或 actor 状态快照（actor 输出含 current/last turn） |
| `trigger add\|ls\|rm` | 管理 triggers |
| `serve` | 前台运行 daemon |
| `init` | 初始化 `~/.config/agentd/` 并安装系统服务；config/AGENTS.md/.env 仅在不存在时创建，skills 始终覆盖（升级同步） |
| `doctor` | 诊断/修复 |
| `service install\|uninstall` | 管理系统服务 |

`service install` 生成平台相关的服务定义（macOS 上为 launchd plist，Linux 上为 systemd user unit）并启用。系统环境变量（`PATH`、`HOME`、`XDG_*`、`PI_CODING_AGENT_DIR` 等）会快照到服务定义中。密钥（API token 等）应放在 `~/.config/agentd/.env` 中——daemon 启动时会在解析 config 中的 `${VAR}` 引用之前加载此文件。Shell 环境变量优先于 `.env` 中的值。

可选 backend 快捷命令（`agentd pi/claude/codex`）：spawn + wait + 输出结果的语法糖。

#### 输出格式

- TTY：人类可读格式
- Non-TTY（agent 调用），非流式命令输出 JSON envelope：成功 `{"ok": true, ...result_fields}`，失败 `{"ok": false, "error": {"code": "...", "message": "..."}}`
- 流式命令逐行输出 JSON。有限流（如 `wait --progress`）最后一行为 result envelope；开放流（如 `logs --follow`、`ps --watch`）持续输出直到客户端取消

---
## 6. 配置模型

### 解析优先级

CLI flag → 环境变量 → 配置文件 → 内置默认值

### 配置文件解析

1. `-c` / `--config <path>`
2. `AGENTD_CONFIG` 环境变量
3. `${XDG_CONFIG_HOME}/agentd/config.yaml`
4. `~/.config/agentd/config.yaml`
5. `~/.agentd.yaml`

未找到配置文件 = 使用内置默认值。

### Workspace

持有 socket、数据库、pid 文件、日志文件。

解析：`AGENTD_WORKSPACE` → config `workspace` → `${XDG_STATE_HOME:-~/.local/state}/agentd`

### 关键配置项

```yaml
default_backend: pi

limits:
  max_depth: 3
  max_children_per_parent: 8
  max_total_workers: 64

channels:
  telegram:                                # 内置，无需 command
    spawn:                                 # 可选：该 channel 创建 actor 的默认值
      cwd: ~/.config/agentd/agents/telegram
    env:
      TELEGRAM_BOT_TOKEN: ${TELEGRAM_BOT_TOKEN}
      TELEGRAM_ALLOWED_USERS: "123456"

inbox_gateway:
  enabled: false
  host: 127.0.0.1
  port: 8765
  public_base_url: null
```

- `default_backend`：`--backend` 省略时使用，默认 `pi`
- `limits`：并发和深度限制
- `channels`：由 daemon 托管的 channel adapter。内置 channel（`telegram`、`cli`）只需配 `env`；自定义 channel 需指定 `command`（字符串列表）。每个 channel 可包含 `spawn:` 块，设置该 channel 创建 actor 时的 `backend`、`cwd`、`args` 默认值。环境变量值支持 `${VAR}` 引用，在 daemon 启动时从宿主环境解析。`enabled: false` 的 channel 被跳过。内置 channel 依赖通过 extras 安装：`pip install agentd[telegram]`。
- `inbox_gateway`：HTTP inbox bridge 配置；`public_base_url` 用于反向代理场景

### Actor 注入环境变量

每个 backend 子进程自动注入：

| 变量 | 值 |
|---|---|
| `AGENTD_ACTOR_ID` | actor 自身 ID |
| `AGENTD_INBOX_URL` | 可选；actor inbox HTTP 端点 |

### 环境变量模型

分两层：

- **Actor-level env**：`spawn.env` 设定，运行时内存持有（不持久化），作为该 actor 所有 turn 的默认执行环境。Daemon 重启后丢失
- **Turn-level env overlay**：`emit.env` 设定，仅对由该 message 触发的 turn 生效，持久化在 `turn.opened` input snapshot 中，不回写 actor 默认 env

`deliver_as=steer` 时禁止携带 `env`（steer 不开启新 turn，无处应用 env overlay）。

实际执行环境合成优先级（高 → 低）：turn env overlay > actor env > 注入变量（`AGENTD_ACTOR_ID` 等）> daemon 继承环境。
环境变量在进程启动时注入，v1 one-shot 模式下每 turn 起新进程，overlay 自然生效。
Long-running backend 不支持 turn-level env overlay（进程已运行，无注入点）。


---
## 7. 系统组件

### CLI

命令入口。解析命令，规范化输入，通过 RPC 与 daemon 通信。

### Daemon API server

Unix socket 上的 JSON-RPC 2.0 daemon。校验请求，解析 actor 引用，持久化变更，调用 scheduler，流式输出 logs/ps。可选 HTTP inbox bridge（用于 agent 自注册 Webhook）。

### Scheduler

编排层。做决策，不做执行。

- 负责：turn opening/ending、mailbox claiming、actor/turn 状态迁移、`follow_up`/`steer` 投递策略、wakeup、close 子树、trigger 投递
- 不负责：直接执行 backend process

### Runtime

执行层。做执行，不做决策。

- 负责：执行已形成的 turn、加载 checkpoint、传递 turn input、接收 steer、发出 `turn.started`/`turn.end`、上报 execution termination、更新 checkpoint
- 不负责：选择 mailbox input、定义 wakeup 策略

### Store

SQLite 持久化层。持久化 actors、turns、mailbox、events、triggers。强制不变量，提供查询。单写者模型。

### Backend adapters

每个 backend 的集成模块。将 turn input 映射为 backend invocation，将 backend 输出映射为 canonical signals。负责 checkpoint 逻辑。暴露 capability flags（如 `supports_steer`）。

内建：`pi`、`claude`、`codex`。

Backend adapter 通用契约和各 adapter 实现见 §10。

### 模块结构

```
src/agentd/
├── cli/           → CLI 入口、参数解析
├── api/           → Daemon API server（RPC 处理）
├── scheduler/     → Scheduler（状态机、turn 管理、EventBus）
├── runtime/       → Runtime（进程执行、backend adapters）
│   └── backends/
├── store/         → Store（SQLite 持久化）
├── config.py      → 配置解析
└── protocol.py    → 共享类型定义（RPC 信封、错误码）
```

### 流式订阅背压

`logs --follow`、`ps --watch`、`wait --progress` 等流式订阅使用 per-subscriber queue，有固定上限。溢出时断开该 subscriber（返回 `slow_consumer` 错误）。对于事件流（`logs --follow`、`wait --progress`），客户端通过 `since_seq` 从最新位置恢复订阅，中间事件可能被跳过。对于快照流（`ps --watch`），客户端重新发起 watch 请求即可获取最新快照。

### 本地安全模型

- Unix socket 文件权限 `0600`（仅 owner 可访问）
- HTTP inbox bridge 默认关闭；开启后默认仅监听 `127.0.0.1`
- 公网暴露须经反向代理和外部认证层；agentd 不内置 provider-specific auth

### Graceful shutdown

SIGTERM / SIGINT 均触发以下序列：

1. 停止接受新 RPC 请求
2. 停止 cron scheduler
3. 对所有 running turns 发 stop
4. 带超时等待所有 turns 结束
5. 超时后强制 terminate 残留进程
6. 关闭 EventBus、Store

### Startup reconciliation

Daemon 启动时对残留状态做确定性收敛：

1. 扫描 `running` turns 的 `exec_pid`，对仍存活的 orphan 进程发 SIGTERM
2. 对每个 active actor 的 `running` turn：合成 `turn.end(outcome=failed, error="daemon restarted")`，actor → `idle`
3. 对每个 active actor 的 `pending` turn：重新调度执行
4. 对每个 `idle` actor 且有 queued message：触发 wakeup

### 并发限制

`max_total_workers` 上限。当 running turns 达到上限时，新 turn 保持 `pending` 排队，待有空位时再调度。

### 已知限制（v1）

- 无进程 heartbeat/liveness probe：如果 backend 进程 hang 住（不退出也不输出），无法主动感知。依赖 PID-based 事后检测。

---
## 8. 持久化模型

模型级不变量见 §2。本节定义表结构、持久化层约束与索引。

### Actors

| 字段 | 说明 |
|---|---|
| `actor_id` | 主键 |
| `name` | nullable；人类可见绑定键 |
| `scope_id` | 唯一性域 |
| `parent_actor_id` | nullable |
| `backend` | adapter 名称 |
| `backend_args` | JSON；命令行参数列表 |
| `cwd` | 工作目录路径 |
| `state` | `idle\|active\|closed` |
| `checkpoint` | nullable；JSON；`null` = 禁用，非 null = 启用（spawn 时 `checkpoint=true` 初始化为 `{}`，turn 结束后写入实际数据如 `{"session_id": "..."}` ） |
| `created_at`, `updated_at`, `closed_at` | |

### Turns

| 字段 | 说明 |
|---|---|
| `turn_id` | 主键 |
| `actor_id` | FK |
| `state` | `pending\|running\|ended` |
| `exec_pid` | nullable；执行进程 PID |
| `result` | nullable；TEXT；turn 产出 |
| `outcome` | nullable；终止分类：`succeeded\|failed\|canceled\|interrupted` |
| `error` | nullable；TEXT；失败原因（failed 时有值） |
| `created_at`, `started_at`, `ended_at` | nullable where applicable |

### Mailbox

| 字段 | 说明 |
|---|---|
| `message_id` | 主键 |
| `actor_id` | FK |
| `message_type` | |
| `payload` | JSON |
| `state` | `queued\|claimed\|acked` |
| `created_at`, `acked_at` | nullable where applicable |

- `queued`：等待被 turn claim
- `claimed`：已绑定到某个已打开的 turn，等待该 turn 完成
- `acked`：已被完成的 turn 消费

### Events

Append-only 日志。`seq` 全局单调递增，提供跨 actor 的全局排序。

| 字段 | 说明 |
|---|---|
| `seq` | 主键；全局递增 |
| `actor_id` | |
| `turn_id` | nullable |
| `event_type` | |
| `payload` | JSON |
| `created_at` | |

### Triggers

| 字段 | 说明 |
|---|---|
| `trigger_id` | 主键 |
| `target_actor_id` | FK |
| `kind` | v1 为 `cron` |
| `spec` | JSON；trigger 规格（如 cron 表达式） |
| `message_type`, `payload` | 触发时生成的 message |
| `next_fire_at` | |
| `created_at` | |

Actor close 时删除其 triggers。Trigger firing = 系统生成的 `actor.emit`。

### 持久化层约束

- Turn 开启输入可从 event log 重建（`turn.opened` 事件含 input snapshot）

### 索引

- `idle|active` actor 的 `(scope_id, name)` 唯一索引
- `pending|running` turn 的每 actor 唯一索引
- events 按 actor + seq 索引
- events 按 turn_id + seq 索引（turn 级事件查询）
- mailbox 按 actor + queued state 索引
- actors 按 parent_actor_id 索引（close 子树查找 children）
- triggers 按 target_actor_id 索引（close actor 时删除关联 triggers）
- triggers 按 next_fire_at 索引（cron 调度查找到期 triggers）

---
## 9. Checkpoint 语义

### 归属

属于 actor，不属于 turn。单字段 `checkpoint`（actors 表）：`null` = 禁用，非 null JSON = 启用。Spawn 时 `checkpoint=true` 初始化为 `{}`，`checkpoint=false` 保持 `null`。Turn 结束后 backend adapter 将实际数据（如 session_id）写入该字段。

### 默认值

| 类型 | 默认值 | 理由 |
|---|---|---|
| Root actor | `true` | 长期存在，多 turn 需要上下文延续 |
| Child actor | `false` | 通常是临时任务，每 turn 独立 |

Spawn 时可显式覆盖。

- `checkpoint=true`：backend 保存和恢复 session（`--session <file>` / `--resume <id>`）
- `checkpoint=false`：backend 无 session 模式运行（`--no-session`），不保存、不恢复

### 每 turn 流程

1. **加载**（`checkpoint=true` 时）：runtime 从 actor 记录读取 checkpoint → 传递给 backend adapter → 发出 `actor.checkpoint.loaded`（或 `actor.checkpoint.missed`）
2. **跳过**（`checkpoint=false` 时）：不加载，backend 以无 session 模式启动
3. **执行**：backend 处理 turn input
4. **保存**（`checkpoint=true` 时）：turn 结束后，backend adapter 提取新的 checkpoint 数据 → 持久化回 actor 记录 → 发出 `actor.checkpoint.saved`

### 失败策略（v1）

- `checkpoint=true`，非首次 turn，checkpoint 存在但加载失败 → turn outcome = `failed`，不静默降级
- 首次 turn，无 checkpoint 可加载 → 正常，`actor.checkpoint.missed` 是信息性事件

### 公开语义

不是独立命令 surface。通过 `actor.checkpoint.*` 事件体现，client 可从 logs 观察 checkpoint 状态。

各 backend 的 checkpoint 具体实现见 §10。

---
## 10. Backend adapter 实现细节

### 通用契约

每个 adapter 必须将 backend 行为规范化为 canonical signals：`turn.started` / `turn.progress` / `turn.result` / `turn.end` / checkpoint 更新。

Completion：`turn.end` 是唯一的 turn completion 信号。`turn.result` 是过程产出，不表示 completion。兜底：仅在 `turn.end` 缺失时，根据 execution termination + terminal intent 合成 `turn.end`。

Steering：`supports_steer=false` → `deliver_as=steer` 快速失败；`=true` → 投递给当前 turn。

进程终止策略：v1 one-shot backend 统一采用 `SIGTERM → 等待超时 → SIGKILL`。无 stdin pipe，无法 cooperative shutdown，SIGTERM 是最软的可用信号。stop 与 close 在进程终止手段上相同，区别仅在语义层（stop → turn `interrupted` + actor `idle`；close → turn `canceled` + actor/subtree `closed`，见 §3）。

Message → prompt 渲染规则：daemon 将 mailbox 队列中的 message 渲染为文本后传给 backend CLI。规则：

- `type=message`：直接取 `payload.text` 作为 prompt 文本
- 其他 typed event：序列化为 `[{type}]\n` + payload key-value 文本块（嵌套值 JSON 序列化）。渲染时去掉 `env.` 前缀

Channel 特有的格式化（如 Telegram message 结构展开）由 channel adapter 在调用 `actor.emit` **之前**完成，daemon runtime 不感知具体 channel 格式。

### pi

- 命令：`pi --mode json -p "<prompt>" [--session <file>] <backend_args>`
- 输出格式：NDJSON（`--mode json`）
- Prompt 传入：`-p` flag，值为 turn input text
- Completion 检测：进程退出。Pi 在每个 assistant turn 结束后都会输出 `turn_end`（tool-use turn、reply turn……），单个 `turn_end` **不代表**对话结束。Adapter 将每个 `turn_end` 映射为 `turn.result`（提取最新 assistant 文本），让进程继续运行至 EOF。由 runner 通用的"无显式 `turn.end` → 进程退出时兜底合成"逻辑处理 completion。
- Progress 映射：pi 主要流式事件是 `message_update`（包装格式），adapter 从 `assistantMessageEvent.type` 提取（`thinking_start`/`thinking_delta` → `thinking`，`text_delta` → `text`，`tool_use_start`/`tool_use_end` → `tool_call`）。独立的 `tool_execution_start`/`tool_execution_end` 事件也映射为 `tool_call`（使用 `toolName`/`args` 字段）。
- Checkpoint：`{session_id, session_cwd, session_timestamp}` → 构造精确文件路径 `<session_dir>/--<cwd_slug>--/<ts_slug>_<session_id>.jsonl` → `--session <file>`。Checkpoint 存在但文件找不到时报错（不 glob）。
- Capability：`supports_steer=false`

### claude

- 命令：`claude -p "<prompt>" --output-format stream-json --verbose --permission-mode auto [--resume <session_id>] <backend_args>`
- 输出格式：stream-json NDJSON
- Prompt 传入：`-p` flag，值为 turn input text
- Completion 检测：`type=result` 事件 → `turn.end`；assistant message 含 text → `turn.end`（兜底）
- Checkpoint：session_id → `--resume <session_id>`
- Capability：`supports_steer=false`

### codex

- 命令：`codex exec "<prompt>" --json --full-auto [resume <thread_id>] <backend_args>`
- 输出格式：JSONL（`--json`）
- Prompt 传入：位置参数（第一个非 flag 参数）
- Completion 检测：`type=item.completed` + `item.type=agent_message` → `turn.end`；`type=turn.completed` → `turn.end`（兜底）
- Checkpoint：thread_id → `codex exec resume <thread_id>`
- Capability：`supports_steer=false`

---
## 11. 错误处理策略

### RPC 层

| 情况 | 处理 |
|---|---|
| 请求校验失败 | JSON-RPC -32602（invalid params），`data.type=invalid_params` |
| Actor 不存在 | -32000，`data.type=not_found` |
| 状态冲突（如 emit closed actor） | -32000，`data.type=conflict` |
| Backend 启动失败 | -32000，`data.type=backend_error` |
| 流式订阅消费者过慢被断开 | -32000，`data.type=slow_consumer` |

### Scheduler 层

| 情况 | 处理 |
|---|---|
| Turn 开启失败 | `turn.end(outcome=failed)`，actor 回 `idle` |
| 状态迁移违反不变量 | 记录错误日志，拒绝操作，不静默吞掉 |
| 并发上限已满 | 新 turn 保持 `pending` 排队 |

### Runtime 层

| 情况 | 处理 |
|---|---|
| Backend 进程异常退出 | 合成 `turn.end(outcome=failed, error=exit code+stderr 摘要)` |
| Backend 输出解析错误 | 记录警告，跳过该行，不终止 turn |
| Steer 投递失败 | 返回错误给调用者 |
| Checkpoint 加载失败（非首次） | `turn.end(outcome=failed, error=原因)` |

### Store 层

| 情况 | 处理 |
|---|---|
| SQLite 写入失败 | 向上传播，不重试 |
| Schema 不匹配 | daemon 拒绝启动 |

---
## 12. Channel 集成层

附加集成层，不是 core 模型的一部分。每个 channel 是独立脚本，通过 CLI 与 daemon 交互。

### 架构

```
IM 平台 ←→ Transport ←→ Channel adapter（独立脚本）←→ agentd CLI
```

### 生命周期

Channel 可独立运行，也可通过 `channels` 配置段（§6）由 daemon 托管。托管模式下：

- `agentd serve` 将每个启用的 channel 作为子进程启动
- 崩溃后自动重启，指数退避（1s → 2s → … → 最大 60s）
- 健康运行 30s 后重置退避
- `agentd` 关闭时终止所有 channel 进程（SIGTERM → 5s → SIGKILL）

### Adapter 契约

每个 adapter 做两件事：

- **入站**：平台消息 → `agentd emit <actor> --type env.<platform> --payload '{...}'`（adapter 层逻辑：先 emit，actor 不存在时 fallback 到 spawn；这是 adapter 的 notify 模式，不是 daemon 行为——daemon 的 emit 对不存在的 actor 返回 `not_found`）
- **出站**：`agentd logs --follow` 监听 actor 进度 → 推送回平台（typing indicator、文本、结果）

Adapter 负责平台特有逻辑（消息格式、长度限制、Markdown 兼容、权限校验）。agentd 不感知平台细节。

### Transport 模式

两种 transport 覆盖所有主流 IM：

| Transport | 原理 | 适用平台 |
|---|---|---|
| **WebSocket** | Adapter 主动建立出站 WebSocket 连接到平台 | 飞书、钉钉、Slack (Socket Mode)、Discord (Gateway) |
| **Long-polling** | Adapter 轮询平台 HTTP API 拉取新消息 | Telegram (`getUpdates`)、Matrix (Sync API) |

两种 transport 均无需公网 IP。

### 平台 Transport 对照

| 平台 | Transport | 无需公网 |
|---|---|---|
| Telegram | Long-polling | ✅ |
| 飞书 | WebSocket | ✅ |
| 钉钉 | WebSocket (Stream) | ✅ |
| Slack | WebSocket (Socket Mode) | ✅ |
| Discord | WebSocket (Gateway) | ✅ |
| Matrix | Long-polling (Sync) | ✅ |

### Telegram adapter（参考实现）

v1 交付物之一。内置 adapter（`agentd.channels.telegram`），使用 long-polling transport。通过 `pip install agentd[telegram]` 安装。

#### 核心流程

1. 长轮询 `getUpdates` 获取消息
2. 按 `chat_id` 序列化处理（同一 chat 不并发）
3. 消息 → notify 模式：先 `agentd emit`，actor 不存在时 fallback 到 `agentd spawn`
4. `agentd wait <actor> --progress` 流式接收进度事件，实时推送到 Telegram
5. Turn 结束后将 result 发回 Telegram

#### 进度展示

进度管线分两层：

**ProgressState（channel 无关，`lib.py`）**：解析 `turn.progress` 事件流（§4 规范格式），维护当前 phase 和 tool step 计数，输出人类可读的进度文本。

| `turn.progress.payload.type` | Phase |
|---|---|
| `thinking` | Thinking |
| `tool_call`（`status=running`） | Running tool |
| `tool_call`（`status=failed`） | Tool failed |
| `text` | Generating reply |

`tool_call` 的 `status=completed` 静默忽略——工具完成后下一个事件（thinking、另一个 tool、或 text）自然接管。

Tool 摘要从 `turn.progress` payload 的 `name` 和 `args` 提取操作细节（如 `name=read, args.path=src/main.py` → `Reading src/main.py`；`name=bash, args.command="npm test"` → `$ npm test`）。敏感信息（token、API key）自动脱敏。

输出格式示例：
```
✨ Running tool…
Step 3: Reading src/main.py
```

**ProgressReporter（Telegram 特有，`telegram.py`）**：管理一条可编辑的 Telegram 消息。

- **延迟创建**：等待 1s 后才发送，避免快速任务的消息闪烁
- **编辑合并**：通过 lock 合并高频更新，避免 Telegram API 限流
- **结束删除**：turn 结束后删除进度消息，保持聊天整洁

ProgressState 可被其他 channel adapter 复用，只需替换渲染层。

#### Actor 命名

`telegram:<chat_id>`，每个 Telegram chat 对应一个 root actor。

#### 自动新 session

空闲超过阈值（默认 2 小时）的 actor，下次收到消息时 close + 重新 spawn（新 checkpoint）。也支持 `/new` 命令手动触发。

#### 平台特有逻辑

- Markdown 渲染失败时 fallback 到纯文本
- Typing indicator 每 4.5s 刷新
- `/ping`、`/help`、`/status`、`/logs`、`/stop`、`/new` 等管理命令
- `TELEGRAM_ALLOWED_USERS` 白名单鉴权

#### 环境变量注入

每个 actor turn 注入 `TELEGRAM_BOT_TOKEN`、`TELEGRAM_DEFAULT_CHAT_ID`、`TELEGRAM_REPLY_TO_MESSAGE_ID`，供 agent 通过 `telegram` skill 主动回复。

#### SKILL 文件

Adapter 为 actor 加载 `skills/telegram/SKILL.md`，告诉 agent 它在 Telegram 环境中运行、消息 payload 格式、如何回复。

---
## 13. Agent Skill 定义

交付物之一：`skills/agentd/SKILL.md`。供运行在 agentd 上的 agent 阅读，告诉它如何与 daemon 交互。

Skill 格式参考各 backend 的 skill 规范：
- Codex: https://developers.openai.com/codex/concepts/customization#skills
- Claude Code: https://code.claude.com/docs/en/skills

### 内容要求

#### 可用命令（精简版，agent 视角）

| 命令 | 用途 |
|---|---|
| `agentd spawn [--name <name>] --message "..."` | 创建 actor 并发消息 |
| `agentd emit <actor> --message "..."` | 向已有 actor 发消息 |
| `agentd emit <actor> --type <type> --payload '{...}'` | 发送 typed message |
| `agentd stop <actor>` | 停止当前 turn |
| `agentd stop <actor> --close` | 关闭 actor 及子树 |
| `agentd wait <actor>` | 等待 actor 回到 idle/closed |
| `agentd status <actor>` | 查看 actor 状态和 last turn 结果 |
| `agentd ps` | 列出所有 actor |

#### 环境变量

Agent 进程内可用的注入变量：

| 变量 | 用途 |
|---|---|
| `AGENTD_ACTOR_ID` | 自身 actor ID，用于 spawn 子 actor 时传 `--parent-actor-id` |
| `AGENTD_INBOX_URL` | 自身 inbox HTTP 端点（如需注册 webhook） |

#### 常用模式

- **委派子任务**：`child=$(agentd spawn --name reviewer --message "review PR #42" --parent-actor-id $AGENTD_ACTOR_ID | jq -r '.actor_id')` → `agentd wait "$child"` → `agentd status "$child"` 获取结果
- **向已有 agent 发消息**：`agentd emit bob --message "help me review this"` → `agentd wait bob`
- **查看结果**：`agentd status <actor>` 返回 JSON，含 `last_turn.result`
- **子 actor 命名**：`name` 可选；提供时同 parent 下唯一；不提供则通过 `actor_id` 引用

#### 输出格式

Non-TTY 环境（agent 调用）下所有命令输出 JSON envelope：`{"ok": true, ...}`。Agent 应解析 JSON 获取结果。

#### 配置预设

Agent 通过 CLI flag（`--backend`、`--cwd`、`--args`）显式选择 backend、工作目录和参数。Channel adapter 可通过 channel 级 spawn 默认值配置（§6）。

---
## 14. 验收清单

1. `spawn` 无输入 → `idle` actor，无 turn
2. `name` optional；同 parent 下非终态 actor name 唯一（`null` 不参与检查）
3. 每 actor 最多一个 active turn
4. `emit` 持久化 typed message 到 mailbox
5. `follow_up` = next-turn input
6. `steer` = current-turn control input；不支持则报错；turn `pending` 时 auto 退回 `follow_up`
7. Mailbox claim 按 `(created_at, message_id)` FIFO
8. 显式 `turn.end` 是唯一 completion 信号；缺失时兜底合成
9. Soft stop → `interrupted` + actor `idle`
10. Hard close → `canceled` + actor/subtree `closed`；递归关闭所有 descendants
11. Checkpoint 归 actor；root 默认启用，child 默认禁用；加载失败 → fail turn
12. 环境变量两层合成：actor env + turn overlay；steer 禁止携带 env
13. RPC 对齐 JSON-RPC 2.0；`actor.stop` / `actor.close` 分离
14. CLI actor-first；status/logs 以 turn-attached 方式暴露
15. Startup reconciliation：daemon 重启后清理 orphan 进程、收敛残留状态
16. Graceful shutdown：SIGTERM/SIGINT 触发有序关闭
17. Telegram adapter：long-polling 收消息 → emit/spawn → progress 实时推送 → result 回复；ProgressState channel 无关可复用

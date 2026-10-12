# a2a-t-sample — 示例用例集

基于 **a2a-t-sdk** 的示例用例，以用例独立目录组织。

## 目录结构

```
a2a-t-sample/
├── env.example              # 共享环境配置模板
├── requirements.txt         # 共享依赖
├── ruff.toml                # 共享 lint 配置
├── README.md / README-zh.md # 本文档
├── subscribe-incident/      # 用例：故障订阅
│   ├── src/                 #   client / server / registry / common 模块
│   ├── test/                #   单元测试
│   └── resources/           #   用例独有 mock LLM 响应数据（zh-CN / en-US）
├── negotiation/             # 用例：协商闭环（离线）
│   ├── src/negotiation_demo/ #  演示入口 / client & server 运行时 / 生成策略
│   ├── test/                #  闭环测试（mock LLM，双语言）
│   └── resources/           #  场景数据 + 脚本化 mock LLM 响应（zh-CN / en-US）
└── observability-sample/    # 用例：可观测——端到端查看智能体被追踪的全过程
    ├── src/obs_sample/       #  server / client 运行时 + Langfuse 后端辅助
    └── README.md / README-zh.md
```

共享配置（`env.example`、`requirements.txt`、`ruff.toml`）位于容器层，各用例可复用。每个用例携带自己的 `src/`、`test/` 和 `resources/`。

## 已有用例

| 目录 | 说明 |
|------|------|
| [subscribe-incident/](subscribe-incident/) | 故障订阅用例 — 流式推送 Incident artifact（含注册中心） |
| [negotiation/](negotiation/) | 协商闭环用例 — 离线 propose -> accept 往返（脚本化 mock LLM） |
| [observability-sample/](observability-sample/) | 可观测样例 — 所有场景的 trace / 指标 / 日志，可选 Langfuse 后端 |

新用例直接在 `a2a-t-sample/` 下以 `subscribe-incident/` 的同级目录添加。

## 快速开始（共享）

```bash
cd a2a-t-sample
cp env.example .env
uv pip install -r requirements.txt
```

> 如果 `A2AT_LLM_API_KEY` 留空，sample 自动使用 `resources/mock_responses/` 下的 mock LLM 响应，无需真实 API 即可跑通完整流程。

## 用例：subscribe-incident（故障订阅）

最小端到端用例，演示故障订阅场景：客户端生成 prompt → 服务端校验 → 流式推送 Incident artifact。流程包含客户端 prompt 生成、服务端校验、流式 artifact 推送，并保留 LLM mock 能力。

### 启动服务（三个终端）

> 模块位于 `subscribe-incident/src/`，而 `.env` 位于 `a2a-t-sample/`，因此需要**在 `a2a-t-sample` 目录下设置 `PYTHONPATH` 指向 `subscribe-incident/src`**。

```powershell
# 进入 sample 目录（.env 所在位置）
cd a2a-t-sample

# 设置模块搜索路径（每个终端都要执行）
$env:PYTHONPATH = "$pwd\subscribe-incident\src"
```

```bash
# 终端1：启动注册中心（端口 5001）
uv run python -m agentcard_example.registry_main

# 终端2：启动服务端（端口 8000）
uv run python -m server_example.server_main

# 终端3：启动客户端（持续接收 artifact，Ctrl+C 停止）
uv run python -m client_example.client_main
```

客户端会持续接收 artifact，按 `Ctrl+C` 停止。

### 限制接收数量（可选）

```powershell
$env:A2AT_SAMPLE_MAX_ARTIFACTS = "5"
uv run python -m client_example.client_main
```

### 流程说明

| 阶段 | 谁调 SDK | SDK 做什么 | LLM 调用次数 |
|------|---------|-----------|-------------|
| 启动 | client + server | A2ATClient / A2ATServer 初始化 | 0 |
| Prompt 生成 | 客户端 | 场景识别 + slot 提取 + 模板渲染 | 2 |
| Prompt 校验 | 服务端 | 场景识别 → slot 提取 → 语义校验 | 3 |
| 流式推送 | 客户端 | normalize_event 归一化 stream 事件 | 0 |

### 消息体约定

客户端发送的 A2A 请求遵循以下约定：

| 位置 | 内容 |
|------|------|
| text part | scenario 名（`"create incident subscription"`） |
| `metadata[Notification-T/NL/v1]` | 生成的 promptText |
| header `A2A-Extensions` | `https://projects.tmforum.org/a2aproject/telecommunication/extensions/Notification-T/NL/v1` |

- Prompt 生成输入为**硬编码自然语言**，按照 `A2AT_LANGUAGE` 自动选择：
  - `zh-CN`：`"请生成一个Incident事件订阅任务：通知主题为Incident，订阅条件为订阅级别为critical的ETH-LOS的故障，上报通知数据格式为DataPart"`
  - `en-US`：`"Generate an Incident event subscription task: notification topic is Incident, subscription condition is a critical ETH-LOS fault, and the notification data format is DataPart"`

### 服务端校验流程

`execute_server_flow` 的状态机：

1. **校验 `A2A-Extensions` header**：必须包含 Notification-T/NL 扩展 URI，否则抛 `ValueError("a2a client extensions is not exist.")`
2. **从 `metadata[Notification-T/NL/v1]` 提取 promptText**（不再从 `parts[0].text` 读取）
3. **`SUBMITTED`** → 调用 `A2ATServer.check_task_prompt` 校验
   - 校验失败 → 发 **`REJECTED`** 状态（不抛异常）
   - 校验通过 → 发 **`WORKING`** 状态
4. **循环推送 Incident artifact**（每 `ARTIFACT_SEND_INTERVAL_SECONDS = 5.0s` 一次，默认无限推，可被 `max_artifacts` 截断）
5. 推送过程异常 → 发 **`FAILED`** 状态

### AgentCard 数据

- name：`SPN Domain Agent`，provider：`Huawei`
- 仅声明 `Notification-T/NL/v1` 扩展（subscribe 用例不涉及 Task-T）

### 关键点

- **SDK 是中间层**：client/server 不直接调 LLM，通过 A2ATClient/A2ATServer 间接调用
- **LLM 只在 prompt 阶段用**：推送 artifact 时不调 LLM
- **无协商**：客户端一次性提交完整输入，服务端校验通过直接推送
- **三层解耦**：client（发现 + 消费）→ server（注册 + 推送）→ registry（注册中心）
- **mock 能力保留**：`common/mock_llm.py` + `resources/mock_responses/` 在 key 为空时自动启用，完整流程无需真实 API
- **如何区分 mock**：每次 mock LLM 响应前都会输出一行独立日志 `[llm] llm-mock: using canned mock LLM response`；使用真实 LLM 时没有这行日志

## 运行测试

```bash
# 运行全部用例测试（从 a2a-t-sample 目录）
uv run pytest subscribe-incident/test/ -v
uv run pytest negotiation/test/ -v
```

## 用例：negotiation

**离线**协商闭环演示（Java `a2a-t-sample` NegotiationDemoApp 的 Python 对应物，内嵌 HTTP
服务器替换为进程内运行时调用）。它驱动 4 消息流程：

1. client -> 以**缺失**参数生成 Task-T prompt（`generate_task_prompt_from_data_with_schema`）；
2. server -> `validate_task_prompt_and_data_filling` 校验拒绝（参数缺失）-> 生成 Negotiation-T
   信息**提议（propose）** -> `INPUT_REQUIRED`；
3. client -> **补全**参数的 Task-T prompt + Negotiation-T **接受（accept）**；
4. server -> 校验通过 -> 诊断结果 -> `COMPLETED`。

### 运行（离线，无需 API key）

```bash
# 从 a2a-t-sample 目录执行；PYTHONPATH 指向用例 src
$env:PYTHONPATH = "$pwd\negotiation\src"     # PowerShell；bash 下用 export PYTHONPATH=...

uv run python -m negotiation_demo                 # fromData 策略（协商消息确定性生成）
uv run python -m negotiation_demo --fromText      # fromText 策略（每条协商消息一次 LLM 抽取）
uv run python -m negotiation_demo --language zh-CN
```

`.env` 中没有 `A2AT_LLM_API_KEY` 时，脚本化 mock LLM
（`negotiation/src/negotiation_demo/shared/mock_llm.py`）从
`negotiation/resources/mock_responses/` 应答槽位抽取、内容校验与协商抽取调用，整个往返
完全离线运行。配置了真实 API key 时同一流程调用真实 LLM（fromData 策略的协商消息依然
零 LLM 调用 — SDK 直接按模板渲染类型化内容）。

### 流程

| 阶段 | 谁调 SDK | SDK 做什么 | LLM 调用（fromData） |
|------|----------|-----------|---------------------|
| 消息 1 | client | Task-T 槽位抽取 + 模板渲染（`fromDataWithSchema`） | 1 |
| 消息 2 | server | `validate_task_prompt_and_data_filling` + 协商 propose 生成 | 1（fromData +0） |
| 消息 3 | client | Task-T 生成 + 协商 accept 生成 | 1（fromData +0） |
| 消息 4 | server | `validate_task_prompt_and_data_filling` + 诊断渲染 | 1 |

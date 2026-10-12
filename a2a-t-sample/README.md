# a2a-t-sample — Sample Use Cases

Sample use cases built on **a2a-t-sdk**, organized as sibling use-case directories.

## Directory Structure

```
a2a-t-sample/
├── env.example              # shared environment config template
├── requirements.txt         # shared dependencies
├── ruff.toml                # shared lint config
├── README.md / README-zh.md # this document
├── subscribe-incident/      # use case: fault (incident) subscription
│   ├── src/                 #   client / server / registry / common modules
│   ├── test/                #   unit tests
│   └── resources/           #   use-case mock LLM response data (zh-CN / en-US)
├── negotiation/             # use case: negotiation closed loop (offline)
│   ├── src/negotiation_demo/ #  demo app / client & server runtimes / strategies
│   ├── test/                 #  closed-loop tests (mock LLM, both languages)
│   └── resources/            #  scenario data + scripted mock LLM responses (zh-CN / en-US)
└── observability-sample/    # use case: observability — watch agents traced end to end
    ├── src/obs_sample/       #  server / client runtimes + Langfuse backend helper
    └── README.md / README-zh.md
```

Shared configuration (`env.example`, `requirements.txt`, `ruff.toml`) lives at the container level so every use case can reuse it. Each use case carries its own `src/`, `test/`, and `resources/`.

## Available Use Cases

| Directory | Description |
|-----------|-------------|
| [subscribe-incident/](subscribe-incident/) | Fault (incident) subscription — streaming Incident artifact push with registry center |
| [negotiation/](negotiation/) | Negotiation closed loop — offline propose -> accept round trip with a scripted mock LLM |
| [observability-sample/](observability-sample/) | Observability showcase — traces, metrics and logs for every scenario, with an optional Langfuse backend |

New use cases are added as sibling directories of `subscribe-incident/`.

## Quick Start (shared)

```bash
cd a2a-t-sample
cp env.example .env
uv pip install -r requirements.txt
```

> If `A2AT_LLM_API_KEY` is left empty in `.env`, the samples automatically use mock LLM responses from `resources/mock_responses/`. No real API key is needed to run the full flow.

## Use Case: subscribe-incident

A minimal end-to-end use case demonstrating the **subscribe-incident (fault subscription)** scenario: client generates prompt -> server validates -> streaming Incident artifact push. The flow includes client-side prompt generation, server-side validation, and streaming artifact push, while retaining the LLM mock capability.

### Start services (three terminals)

> Modules live under `subscribe-incident/src/`, while the `.env` file is at `a2a-t-sample/`. Therefore you must set `PYTHONPATH` to point to `subscribe-incident/src` **from the `a2a-t-sample` directory**.

```powershell
# Change to the sample directory (where .env lives)
cd a2a-t-sample

# Set module search path (every terminal needs this)
$env:PYTHONPATH = "$pwd\subscribe-incident\src"
```

```bash
# Terminal 1: start registry (port 5001)
uv run python -m agentcard_example.registry_main

# Terminal 2: start server (port 8000)
uv run python -m server_example.server_main

# Terminal 3: start client (receives artifacts continuously; Ctrl+C to stop)
uv run python -m client_example.client_main
```

The client will continuously receive artifacts. Press `Ctrl+C` to stop.

### Limit received count (optional)

```powershell
$env:A2AT_SAMPLE_MAX_ARTIFACTS = "5"
uv run python -m client_example.client_main
```

### Flow

| Stage | Who calls SDK | What SDK does | LLM calls |
|-------|--------------|---------------|-----------|
| Startup | client + server | A2ATClient / A2ATServer init | 0 |
| Prompt generation | client | scenario recognition + slot extraction + template render | 2 |
| Prompt validation | server | scenario recognition -> slot extraction -> semantic validation | 3 |
| Streaming push | client | normalize_event for stream events | 0 |

### Message Body Convention

The A2A request sent by the client follows this convention:

| Field | Content |
|-------|---------|
| text part | scenario name (`"create incident subscription"`) |
| `metadata[Notification-T/NL/v1]` | generated promptText |
| header `A2A-Extensions` | `https://projects.tmforum.org/a2aproject/telecommunication/extensions/Notification-T/NL/v1` |

- Prompt generation input is a **hard-coded natural language string**, selected by `A2AT_LANGUAGE`:
  - `zh-CN`: `"请生成一个Incident事件订阅任务：通知主题为Incident，订阅条件为订阅级别为critical的ETH-LOS的故障，上报通知数据格式为DataPart"`
  - `en-US`: `"Generate an Incident event subscription task: notification topic is Incident, subscription condition is a critical ETH-LOS fault, and the notification data format is DataPart"`

### Server Validation Flow

The `execute_server_flow` state machine:

1. **Validate `A2A-Extensions` header** — must contain the Notification-T/NL extension URI; otherwise throws `ValueError("a2a client extensions is not exist.")`
2. **Extract promptText from `metadata[Notification-T/NL/v1]`** (no longer reads from `parts[0].text`)
3. **`SUBMITTED`** → call `A2ATServer.check_task_prompt` for validation
   - Validation fails → emit **`REJECTED`** status (not an exception)
   - Validation passes → emit **`WORKING`** status
4. **Loop pushing Incident artifacts** (every `ARTIFACT_SEND_INTERVAL_SECONDS = 5.0s`, infinite by default, can be capped via `max_artifacts`)
5. Exception during push → emit **`FAILED`** status

### AgentCard Data

- name: `SPN Domain Agent`, provider: `Huawei`
- Declares only the `Notification-T/NL/v1` extension (subscribe use case does not involve Task-T)

### Key Points

- **SDK as middleware**: client/server never call LLM directly; they go through A2ATClient/A2ATServer
- **LLM only for prompt phase**: no LLM calls during artifact push
- **No negotiation**: client submits complete input; server validates and pushes directly
- **Three-layer decoupling**: client (discover + consume) -> server (register + push) -> registry (registry center)
- **Mock capability preserved**: `common/mock_llm.py` + `resources/mock_responses/` auto-activate when the API key is empty; the full flow runs without a real API
- **How to tell it's mock**: before each mock LLM response, a standalone log line `[llm] llm-mock: using canned mock LLM response` is printed; this line is absent when using a real LLM

## Run Tests

```bash
# Run all use case tests (from the a2a-t-sample directory)
uv run pytest subscribe-incident/test/ -v
uv run pytest negotiation/test/ -v
```

## Use Case: negotiation

An **offline** negotiation closed-loop demo (the Python counterpart of the Java
`a2a-t-sample` NegotiationDemoApp, with the embedded HTTP server replaced by an in-process
runtime call). It drives the 4-message flow:

1. client -> Task-T prompt generated with **missing** params (`generate_task_prompt_from_data_with_schema`);
2. server -> `validate_task_prompt_and_data_filling` rejects (params missing) -> Negotiation-T
   information **propose** -> `INPUT_REQUIRED`;
3. client -> Task-T prompt with **filled** params + Negotiation-T **accept**;
4. server -> validation passes -> diagnosis result -> `COMPLETED`.

### Run (offline, no API key needed)

```bash
# From the a2a-t-sample directory; PYTHONPATH points at the use case src
$env:PYTHONPATH = "$pwd\negotiation\src"     # PowerShell; export PYTHONPATH=... on bash

uv run python -m negotiation_demo                 # fromData strategy (deterministic negotiation messages)
uv run python -m negotiation_demo --fromText      # fromText strategy (one LLM extraction per negotiation message)
uv run python -m negotiation_demo --language zh-CN
```

Without `A2AT_LLM_API_KEY` in `.env`, the scripted mock LLM (`negotiation/src/negotiation_demo/shared/mock_llm.py`)
serves the slot-extraction, content-validation and negotiation-extraction calls from
`negotiation/resources/mock_responses/`, so the whole round trip runs offline. With a real API
key the same flow calls the real LLM (the negotiation messages of the fromData strategy still
make no LLM call — the SDK renders the typed content directly from the template).

### Flow

| Stage | Who calls SDK | What SDK does | LLM calls (fromData) |
|-------|--------------|---------------|----------------------|
| Message 1 | client | Task-T slot extraction + template render (`fromDataWithSchema`) | 1 |
| Message 2 | server | `validate_task_prompt_and_data_filling` + negotiation propose generation | 1 (+0 fromData) |
| Message 3 | client | Task-T generation + negotiation accept generation | 1 (+0 fromData) |
| Message 4 | server | `validate_task_prompt_and_data_filling` + diagnosis rendering | 1 |

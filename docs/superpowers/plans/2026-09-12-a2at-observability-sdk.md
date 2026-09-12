# A2A-T 可观测 SDK 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 `a2a_t.observability` 子模块（协议 Span 单一来源 + A2A-T 属性注入架构），含单元测试、端到端验证测试与可离线运行的样例。

**Architecture:** 请求级 Span 全部由 a2a-python 协议层创建；Client 侧经 `A2ATClientSpanProcessor`（结构化 OTel SpanProcessor）在协议 transport Span `on_start` 时注入属性与 traceparent，Server 侧经 `A2ATTraceContextMiddleware`（纯 ASGI）提取激活 traceparent、执行器装饰器向 current 协议 SERVER Span 写属性；A2A-T 自建 Span 仅限 per-event（`a2at.event.*`）。运行时零 a2a-python import（duck typing，契约见 spec 附录 B）。

**Tech Stack:** Python 3.12+ / opentelemetry-api（extra `observability`）/ pytest + opentelemetry-sdk（dev）/ a2a-sdk 1.1.0（仅集成测试组与样例）

**设计文档:** `docs/superpowers/specs/2026-09-11-a2at-observability-sdk-design.md`（v2.0）——唯一需求来源，执行各任务前先读对应章节。

## Global Constraints

- **禁止运行时导入 a2a-python**：`src/a2a_t/observability/` 下任何模块不得出现 `from a2a` / `import a2`。允许 `from a2a_t.core.metadata import ...`（同包）与 `opentelemetry.*`（经 `_otel_compat` 守卫导入）。
- **OTel 依赖**：仅 `opentelemetry-api>=1.33.0`（extra `observability`）；OTel 未安装或总开关关闭时全 API NoOp 吸收（无异常、无输出）。
- **总开关**：`OTEL_INSTRUMENTATION_A2AT_SDK_ENABLED`（默认 `true`）——`_otel_compat._ENABLED` 模块变量 import 时读取、函数调用时引用（测试 monkeypatch 该变量）。
- **instrumentation scope 名**：`a2at-observability`（Tracer 与 Meter 一致）；SpanProcessor 过滤的协议 scope 名为 `a2a-python-sdk`。
- **属性注入纪律**：A2A-T 属性只写入协议 Span（transport CLIENT / handler SERVER）与 A2AT 自建 per-event Span；`trace_facade` Span 零自定义属性。
- **异常吞噬**：所有钩子自身异常一律吞掉 + logger `a2at.observability` WARNING（exc_info）+ 放行原调用。
- **类型与风格**：`uv run mypy src`（strict）与 `uv run ruff check .` 必须通过；行宽 120；公开符号全类型注解。
- **测试命令**：单元 `uv run pytest tests/observability/<file>.py -v`；集成 `uv run --group integration pytest tests/integration -m a2a -v`。
- **Span 命名**：per-event `a2at.event.{kind}`（kind ∈ status/artifact/message/task/completed/canceled/failed/rejected）；trace_facade `a2at.sdk.{role}.{method}`。
- **指标名与单位**：`gen_ai.client.operation.duration`（s）、`gen_ai.client.token.usage`（{token}）、`a2at.task.request.duration`（s）、`a2at.negotiation.total_rounds`（1，Counter，仅 Client 侧自动记录）。
- **pytest async**：根 pyproject 已配 `asyncio_mode = "auto"`，async 测试无需装饰器。

---

### Task 1: 脚手架 + pyproject + `_otel_compat`

**Files:**
- Modify: `pyproject.toml`
- Create: `src/a2a_t/observability/__init__.py`（本任务仅 docstring，Task 15 填充导出）
- Create: `src/a2a_t/observability/client/__init__.py`、`src/a2a_t/observability/server/__init__.py`
- Create: `src/a2a_t/observability/_otel_compat.py`
- Test: `tests/observability/__init__.py`（空）、`tests/observability/test_otel_compat.py`

**Interfaces:**
- Consumes: 无（首个任务）
- Produces: `_otel_compat` 暴露 `otel_installed: bool`、`is_enabled() -> bool`、`get_tracer() -> object`（NoOp 时 `_NoOpTracer`）、`get_meter() -> object`、`get_current_span() -> object`、`NonRecordingSpan`、`SpanKind`、`StatusCode`、`format_trace_id`、`format_span_id`、`otel_context`（未装 OTel 时为 None 占位）

- [ ] **Step 1: 修改 pyproject.toml**

`[project]` 段后新增：

```toml
[project.optional-dependencies]
observability = ["opentelemetry-api>=1.33.0"]
```

`[dependency-groups] dev` 列表追加 `"opentelemetry-sdk>=1.33.0"`；同节新增：

```toml
integration = [
    "a2a-sdk>=1.1.0,<2",
    "httpx>=0.28.1",
    "opentelemetry-sdk>=1.33.0",
    "starlette>=0.47.0",
    "sse-starlette>=3.4.4",
]
```

`[tool.pytest.ini_options] markers` 追加一行：

```toml
    "a2a: integration tests requiring a2a-sdk (opt-in via A2AT_TEST_A2A=1)",
```

- [ ] **Step 2: 写失败测试**

`tests/observability/test_otel_compat.py`：

```python
from __future__ import annotations


def test_import_exposes_helpers() -> None:
    import a2a_t.observability._otel_compat as compat

    assert isinstance(compat.otel_installed, bool)
    assert callable(compat.get_tracer)
    assert callable(compat.get_meter)
    assert callable(compat.is_enabled)


def test_noop_absorbs_everything() -> None:
    import a2a_t.observability._otel_compat as compat

    compat._ENABLED = False  # 模拟总开关关闭
    try:
        assert compat.is_enabled() is False
        tracer = compat.get_tracer()
        with tracer.start_as_current_span("x") as span:
            span.set_attribute("k", "v")
        meter = compat.get_meter()
        meter.create_histogram("h").record(1.0)
        meter.create_counter("c").add(1)
        span.end()  # 重复 end 亦吸收
    finally:
        compat._ENABLED = True


def test_current_span_noop_safe() -> None:
    import a2a_t.observability._otel_compat as compat

    span = compat.get_current_span()
    assert span is not None  # NoOpSpan 或真实 INVALID_SPAN，均为对象
```

- [ ] **Step 3: 运行确认失败**

Run: `uv run pytest tests/observability/test_otel_compat.py -v`
Expected: FAIL（`ModuleNotFoundError: a2a_t.observability`）

- [ ] **Step 4: 实现**

`src/a2a_t/observability/__init__.py`：

```python
"""A2A-T observability module.

Design spec: docs/superpowers/specs/2026-09-11-a2at-observability-sdk-design.md (v2.0).
Public exports are populated in a dedicated task; this package never imports
a2a-python at runtime (structural duck-typing only).
"""
```

`client/__init__.py`：

```python
"""Client-side observability adapters (structural, no a2a-python import)."""
```

`server/__init__.py`：

```python
"""Server-side observability adapters (structural, no a2a-python import)."""
```

`src/a2a_t/observability/_otel_compat.py`：

```python
"""OpenTelemetry compatibility layer: guarded imports, NoOp fallback, master switch.

Mirrors the degradation pattern of a2a-python's a2a.utils.telemetry: OTel is an
optional dependency; when missing or the master switch is off, every helper
returns a NoOp object that absorbs all calls.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger("a2at.observability")

ENABLED_ENV_VAR = "OTEL_INSTRUMENTATION_A2AT_SDK_ENABLED"
INSTRUMENTING_MODULE_NAME = "a2at-observability"
INSTRUMENTING_MODULE_VERSION = "1.0.0"

otel_installed = False
try:
    from opentelemetry import context as otel_context
    from opentelemetry import metrics as otel_metrics
    from opentelemetry import trace as otel_trace
    from opentelemetry.trace import (
        NonRecordingSpan,
        SpanKind,
        StatusCode,
        format_span_id,
        format_trace_id,
    )

    otel_installed = True
except ImportError:
    logger.debug(
        "OpenTelemetry not found. Tracing will be disabled. "
        "Install with: pip install 'a2a-t-sdk[observability]'"
    )

_ENABLED = os.getenv(ENABLED_ENV_VAR, "true").strip().lower() == "true"

if otel_installed and not _ENABLED:
    logger.debug("A2AT OTEL instrumentation disabled via %s.", ENABLED_ENV_VAR)


class _NoOpSpan:
    def set_attribute(self, key: str, value: object) -> None: ...

    def set_attributes(self, attributes: object) -> None: ...

    def set_status(self, status: object, description: str | None = None) -> None: ...

    def record_exception(self, exception: BaseException) -> None: ...

    def end(self) -> None: ...

    def add_link(self, context: object, attributes: object = None) -> None: ...

    def get_span_context(self) -> None:
        return None


class _NoOpSpanContext:
    def __enter__(self) -> _NoOpSpan:
        return _NoOpSpan()

    def __exit__(self, *args: object) -> None: ...


class _NoOpTracer:
    def start_as_current_span(self, name: str, **kwargs: object) -> _NoOpSpanContext:
        return _NoOpSpanContext()

    def start_span(self, name: str, **kwargs: object) -> _NoOpSpan:
        return _NoOpSpan()


class _NoOpMetric:
    def record(self, value: object, attributes: object = None) -> None: ...

    def add(self, value: object, attributes: object = None) -> None: ...


class _NoOpMeter:
    def create_histogram(self, name: str, **kwargs: object) -> _NoOpMetric:
        return _NoOpMetric()

    def create_counter(self, name: str, **kwargs: object) -> _NoOpMetric:
        return _NoOpMetric()


_NOOP_TRACER = _NoOpTracer()
_NOOP_METER = _NoOpMeter()
_NOOP_SPAN = _NoOpSpan()


def is_enabled() -> bool:
    return otel_installed and _ENABLED


def get_tracer() -> object:
    if is_enabled():
        return otel_trace.get_tracer(INSTRUMENTING_MODULE_NAME, INSTRUMENTING_MODULE_VERSION)
    return _NOOP_TRACER


def get_meter() -> object:
    if is_enabled():
        return otel_metrics.get_meter(INSTRUMENTING_MODULE_NAME, INSTRUMENTING_MODULE_VERSION)
    return _NOOP_METER


def get_current_span() -> object:
    if otel_installed:
        return otel_trace.get_current_span()
    return _NOOP_SPAN


if not otel_installed:  # pragma: no cover - depends on environment
    NonRecordingSpan = None  # type: ignore[assignment,misc]
    SpanKind = None  # type: ignore[assignment,misc]
    StatusCode = None  # type: ignore[assignment,misc]
    format_trace_id = None  # type: ignore[assignment]
    format_span_id = None  # type: ignore[assignment]
    otel_context = None  # type: ignore[assignment]
```

注意：其他模块引用 `NonRecordingSpan`/`SpanKind`/`otel_context` 前必须先判 `is_enabled()`；`otel_context.attach/detach` 调用处需 `assert otel_context is not None` 或在 `is_enabled()` 分支内使用（mypy strict 下以 `# type: ignore` 注释的 None 占位满足）。

- [ ] **Step 5: 运行测试确认通过**

Run: `uv run pytest tests/observability/test_otel_compat.py -v`
Expected: 3 PASS

- [ ] **Step 6: lint + 类型检查 + 提交**

```bash
uv run ruff check src/a2a_t/observability tests/observability
uv run mypy src
git add pyproject.toml uv.lock src/a2a_t/observability tests/observability
git commit -m "feat(observability): scaffold module and OTel compatibility layer"
```

---

### Task 2: `attributes.py` —— 常量与请求侧属性提取

**Files:**
- Create: `src/a2a_t/observability/attributes.py`
- Test: `tests/observability/test_attributes_request.py`

**Interfaces:**
- Consumes: 无（纯数据层）
- Produces:
  - `AttributeValue = str | int | bool`
  - 常量：`ATTR_EXTENSION_NAME="extension.name"`、`ATTR_TASK_ID="task.id"`、`ATTR_TASK_STATUS="task.status"`、`ATTR_TASK_TYPE="task.type"`、`ATTR_NEGOTIATION_ID="negotiation.id"`、`ATTR_NEGOTIATION_ROUND="negotiation.round"`、`ATTR_NEGOTIATION_MAX_ROUNDS="negotiation.max_rounds"`、`ATTR_NEGOTIATION_PERFORMATIVE="negotiation.performative"`、`ATTR_NEGOTIATION_TOTAL_ROUNDS="negotiation.total_rounds"`、`ATTR_NOTIFICATION_TOPIC="notification.topic"`、`ATTR_STREAMING_EVENT_KIND="streaming.event.kind"`、`ATTR_PUSH_NOTIFICATION_URL="push.notification.url"`、`ATTR_AUTHORIZATION_POLICY_ID="authorization.policy.id"`、`ATTR_AUTHORIZATION_OPERATION_TYPE="authorization.operation_type"`、`ATTR_AUTHORIZATION_OPERATION_RISK_LEVEL="authorization.operation_risk_level"`、`ATTR_GEN_AI_OPERATION_NAME="gen_ai.operation.name"`、`ATTR_GEN_AI_CONVERSATION_ID="gen_ai.conversation.id"`、`ATTR_A2AT_SPAN_SIDE="a2at.span.side"`、`ATTR_STREAMING="streaming"`
  - `normalize_metadata(metadata: Any) -> dict[str, Any]`（Mapping / protobuf-like Struct / None → dict）
  - `extension_name_from_uri(uri: str) -> str | None`
  - `extract_negotiation_attributes(metadata: dict[str, Any]) -> dict[str, AttributeValue]`
  - `extract_request_attributes(input_obj: Any, *, method: str) -> dict[str, AttributeValue]`
  - `metadata_view_for_provider(metadata: dict[str, Any], headers: Mapping[str, str] | None) -> dict[str, Any]`

- [ ] **Step 1: 写失败测试**

`tests/observability/test_attributes_request.py`：

```python
from __future__ import annotations

from typing import Any

from a2a_t.observability.attributes import (
    extension_name_from_uri,
    extract_negotiation_attributes,
    extract_request_attributes,
    normalize_metadata,
)


class _FakeValue:
    """Structural stand-in for google.protobuf.Value."""

    def __init__(self, which: str, value: Any) -> None:
        self._which = which
        self._value = value

    def WhichOneof(self, name: str) -> str | None:
        return self._which if name == "kind" else None

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        if name in ("string_value", "number_value", "bool_value"):
            return self._value if name == self._which else None
        if name in ("struct_value", "list_value"):
            return self._value if name == self._which else None
        raise AttributeError(name)


class _FakeListValue:
    def __init__(self, values: list[_FakeValue]) -> None:
        self.values = values


def _wrap(value: Any) -> _FakeValue:
    if isinstance(value, dict):
        return _FakeValue("struct_value", _FakeStruct({k: _wrap(v) for k, v in value.items()}))
    if isinstance(value, list):
        return _FakeValue("list_value", _FakeListValue([_wrap(v) for v in value]))
    if isinstance(value, bool):
        return _FakeValue("bool_value", value)
    if isinstance(value, (int, float)):
        return _FakeValue("number_value", value)
    return _FakeValue("string_value", value)


class _FakeStruct:
    def __init__(self, fields: dict[str, _FakeValue]) -> None:
        self.fields = fields


def _struct(data: dict[str, Any]) -> _FakeStruct:
    return _FakeStruct({k: _wrap(v) for k, v in data.items()})


class _FakeMessage:
    def __init__(self, metadata: Any, task_id: str = "", context_id: str = "") -> None:
        self.metadata = metadata
        self.task_id = task_id
        self.context_id = context_id


class _FakeRequest:
    def __init__(self, message: _FakeMessage) -> None:
        self.message = message


def test_extension_name_from_uri() -> None:
    assert extension_name_from_uri(
        "https://projects.tmforum.org/a2aproject/telecommunication/extensions/Task-T/v1"
    ) == "Task-T"
    assert extension_name_from_uri(
        "https://projects.tmforum.org/a2aproject/telecommunication/extensions/Negotiation-T/NL/v1"
    ) == "Negotiation-T"
    assert extension_name_from_uri("https://example.com/other") is None
    assert extension_name_from_uri("") is None


def test_normalize_metadata_dict_struct_none() -> None:
    assert normalize_metadata({"a": 1}) == {"a": 1}
    assert normalize_metadata(None) == {}
    struct = _struct(
        {"negotiationContext": {"id": "N001", "round": 2, "maxRounds": 5, "performative": "PROPOSE"}}
    )
    assert normalize_metadata(struct) == {
        "negotiationContext": {"id": "N001", "round": 2, "maxRounds": 5, "performative": "PROPOSE"}
    }


def test_extract_negotiation_attributes() -> None:
    propose = {"negotiationContext": {"id": "N001", "round": 2, "maxRounds": 5, "performative": "PROPOSE"}}
    assert extract_negotiation_attributes(propose) == {
        "negotiation.id": "N001",
        "negotiation.round": 2,
        "negotiation.max_rounds": 5,
        "negotiation.performative": "PROPOSE",
    }
    terminal = {"negotiationContext": {"id": "N001", "round": 3, "maxRounds": 5, "performative": "ACCEPT"}}
    assert extract_negotiation_attributes(terminal)["negotiation.total_rounds"] == 3
    assert extract_negotiation_attributes({}) == {}


def test_extract_request_attributes_full() -> None:
    request = _FakeRequest(
        _FakeMessage(
            metadata=_struct(
                {
                    "https://projects.tmforum.org/a2aproject/telecommunication/extensions/Notification-T/NL/v1": "prompt",
                    "templateUri": "x",
                    "negotiationContext": {"id": "N001", "round": 1, "maxRounds": 5, "performative": "PROPOSE"},
                }
            ),
            task_id="T1",
            context_id="C1",
        )
    )
    attrs = extract_request_attributes(request, method="send_message")
    assert attrs["gen_ai.operation.name"] == "send_message"
    assert attrs["gen_ai.conversation.id"] == "C1"
    assert attrs["task.id"] == "T1"
    assert attrs["extension.name"] == "Notification-T"
    assert attrs["negotiation.id"] == "N001"


def test_extract_request_attributes_plain() -> None:
    request = _FakeRequest(_FakeMessage(metadata=None))
    attrs = extract_request_attributes(request, method="send_message")
    assert attrs == {"gen_ai.operation.name": "send_message"}


def test_extract_request_attributes_push_url() -> None:
    class _FakeConfig:
        url = "https://hook.example.com/cb"

    class _FakePushRequest:
        push_notification_config = _FakeConfig()

    attrs = extract_request_attributes(_FakePushRequest(), method="create_task_push_notification_config")
    assert attrs["push.notification.url"] == "https://hook.example.com/cb"


def test_extract_never_raises() -> None:
    class _Broken:
        @property
        def message(self) -> Any:
            raise RuntimeError("boom")

    assert extract_request_attributes(_Broken(), method="send_message") == {
        "gen_ai.operation.name": "send_message"
    }
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/observability/test_attributes_request.py -v`
Expected: FAIL（ModuleNotFoundError）

- [ ] **Step 3: 实现**

`src/a2a_t/observability/attributes.py`：

```python
"""A2A-T span attribute constants and wire-attribute extraction (structural, no a2a import)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

AttributeValue = str | int | bool

ATTR_EXTENSION_NAME = "extension.name"
ATTR_TASK_ID = "task.id"
ATTR_TASK_STATUS = "task.status"
ATTR_TASK_TYPE = "task.type"
ATTR_NEGOTIATION_ID = "negotiation.id"
ATTR_NEGOTIATION_ROUND = "negotiation.round"
ATTR_NEGOTIATION_MAX_ROUNDS = "negotiation.max_rounds"
ATTR_NEGOTIATION_PERFORMATIVE = "negotiation.performative"
ATTR_NEGOTIATION_TOTAL_ROUNDS = "negotiation.total_rounds"
ATTR_NOTIFICATION_TOPIC = "notification.topic"
ATTR_STREAMING_EVENT_KIND = "streaming.event.kind"
ATTR_PUSH_NOTIFICATION_URL = "push.notification.url"
ATTR_AUTHORIZATION_POLICY_ID = "authorization.policy.id"
ATTR_AUTHORIZATION_OPERATION_TYPE = "authorization.operation_type"
ATTR_AUTHORIZATION_OPERATION_RISK_LEVEL = "authorization.operation_risk_level"
ATTR_GEN_AI_OPERATION_NAME = "gen_ai.operation.name"
ATTR_GEN_AI_CONVERSATION_ID = "gen_ai.conversation.id"
ATTR_A2AT_SPAN_SIDE = "a2at.span.side"
ATTR_STREAMING = "streaming"

_EXTENSION_BASE = "https://projects.tmforum.org/a2aproject/telecommunication/extensions/"
_TERMINAL_PERFORMATIVES = frozenset({"ACCEPT", "REJECT", "ABORT"})


def extension_name_from_uri(uri: str) -> str | None:
    """Map a TMF telecommunication extension URI to its short name (Task-T / Negotiation-T / ...)."""
    if not uri.startswith(_EXTENSION_BASE):
        return None
    rest = uri[len(_EXTENSION_BASE) :]
    name = rest.split("/", 1)[0]
    return name or None


def _value_to_python(value: Any) -> Any:
    which = None
    try:
        which = value.WhichOneof("kind")
    except Exception:  # noqa: BLE001 - structural access must never raise
        which = None
    if which == "string_value":
        return value.string_value
    if which == "number_value":
        return value.number_value
    if which == "bool_value":
        return value.bool_value
    if which == "struct_value":
        return {str(k): _value_to_python(v) for k, v in value.struct_value.fields.items()}
    if which == "list_value":
        return [_value_to_python(v) for v in value.list_value.values]
    if isinstance(value, (str, int, float, bool)):
        return value
    return None


def normalize_metadata(metadata: Any) -> dict[str, Any]:
    """Normalize a Mapping or a protobuf-like Struct into a plain dict; never raises."""
    if metadata is None:
        return {}
    try:
        if isinstance(metadata, Mapping):
            return dict(metadata)
        fields = getattr(metadata, "fields", None)
        if fields is not None and hasattr(fields, "items"):
            return {str(k): _value_to_python(v) for k, v in fields.items()}
    except Exception:  # noqa: BLE001
        return {}
    return {}


def extract_negotiation_attributes(metadata: dict[str, Any]) -> dict[str, AttributeValue]:
    """Extract negotiation.* attributes from a normalized metadata dict."""
    context = metadata.get("negotiationContext")
    if not isinstance(context, Mapping):
        return {}
    attrs: dict[str, AttributeValue] = {}
    ctx_id = context.get("id")
    if isinstance(ctx_id, str) and ctx_id:
        attrs[ATTR_NEGOTIATION_ID] = ctx_id
    round_ = context.get("round")
    if isinstance(round_, int):
        attrs[ATTR_NEGOTIATION_ROUND] = round_
    max_rounds = context.get("maxRounds")
    if isinstance(max_rounds, int):
        attrs[ATTR_NEGOTIATION_MAX_ROUNDS] = max_rounds
    performative = context.get("performative")
    if isinstance(performative, str) and performative:
        attrs[ATTR_NEGOTIATION_PERFORMATIVE] = performative
        if performative in _TERMINAL_PERFORMATIVES and isinstance(round_, int):
            attrs[ATTR_NEGOTIATION_TOTAL_ROUNDS] = round_
    return attrs


def _extension_name_from_metadata(metadata: dict[str, Any]) -> str | None:
    for key in metadata:
        name = extension_name_from_uri(str(key))
        if name:
            return name
    return None


def _safe_getattr(obj: Any, name: str) -> Any:
    try:
        return getattr(obj, name)
    except Exception:  # noqa: BLE001
        return None


def extract_request_attributes(input_obj: Any, *, method: str) -> dict[str, AttributeValue]:
    """Extract automatic A2A-T attributes from a client-side request object; never raises."""
    attrs: dict[str, AttributeValue] = {ATTR_GEN_AI_OPERATION_NAME: method}
    try:
        message = _safe_getattr(input_obj, "message")
        metadata = normalize_metadata(_safe_getattr(message, "metadata")) if message is not None else {}
        if message is not None:
            conversation_id = _safe_getattr(message, "context_id")
            if isinstance(conversation_id, str) and conversation_id:
                attrs[ATTR_GEN_AI_CONVERSATION_ID] = conversation_id
            task_id = _safe_getattr(message, "task_id")
            if isinstance(task_id, str) and task_id:
                attrs[ATTR_TASK_ID] = task_id
        extension = _extension_name_from_metadata(metadata)
        if extension:
            attrs[ATTR_EXTENSION_NAME] = extension
        attrs.update(extract_negotiation_attributes(metadata))
        push_url = _safe_getattr(input_obj, "url")
        if not isinstance(push_url, str) or not push_url:
            push_config = _safe_getattr(input_obj, "push_notification_config")
            push_url = _safe_getattr(push_config, "url")
        if isinstance(push_url, str) and push_url:
            attrs[ATTR_PUSH_NOTIFICATION_URL] = push_url
    except Exception:  # noqa: BLE001
        return {ATTR_GEN_AI_OPERATION_NAME: method}
    return attrs


def metadata_view_for_provider(metadata: dict[str, Any], headers: Mapping[str, str] | None) -> dict[str, Any]:
    """Merged metadata view handed to A2ATObservabilityConfig provider callbacks."""
    view: dict[str, Any] = {}
    if headers is not None:
        for key, value in headers.items():
            view[str(key)] = value
    view.update(metadata)
    return view
```

- [ ] **Step 4: 运行测试确认通过**

Run: `uv run pytest tests/observability/test_attributes_request.py -v`
Expected: 7 PASS

- [ ] **Step 5: lint + 提交**

```bash
uv run ruff check src/a2a_t/observability/attributes.py tests/observability/test_attributes_request.py
uv run mypy src
git add src/a2a_t/observability/attributes.py tests/observability/test_attributes_request.py
git commit -m "feat(observability): attribute constants and request-side wire extraction"
```

---

### Task 3: `attributes.py` —— 事件分类与 `a2at_attribute_extractor`

**Files:**
- Modify: `src/a2a_t/observability/attributes.py`（追加）
- Create: `tests/observability/stubs.py`（共享结构化 stub，供 Task 10/12/13 复用）
- Test: `tests/observability/test_attributes_event.py`

**Interfaces:**
- Consumes: Task 2 全部
- Produces:
  - `@dataclass(slots=True) class EventInfo: kind: str; task_id: str | None = None; task_status: str | None = None; final: bool = False; is_terminal: bool = False; message_metadata: dict[str, Any] | None = None`
  - `classify_event(event: Any) -> EventInfo`
  - `unwrap_stream_response(result: Any) -> Any | None`
  - `a2at_attribute_extractor(span: Any, args: tuple, kwargs: dict, result: Any, exception: Any) -> None`

- [ ] **Step 1: 写共享 stub 模块 + 失败测试**

`tests/observability/stubs.py`（本任务创建；Task 10/12/13 直接 import）：

```python
"""Structural stubs of a2a-python objects (spec appendix B contract) — no a2a import."""

from __future__ import annotations

from typing import Any


class FakeEnumValue:
    def __init__(self, name: str) -> None:
        self.name = name


class FakeEnumType:
    def __init__(self, mapping: dict[int, FakeEnumValue]) -> None:
        self.values_by_number = mapping


class FakeFieldDescriptor:
    def __init__(self, enum_type: FakeEnumType) -> None:
        self.enum_type = enum_type


class FakeDescriptor:
    def __init__(self, fields: dict[str, FakeFieldDescriptor]) -> None:
        self.fields_by_name = fields


class FakeStatus:
    _next_state = 100

    def __init__(self, state_name: str) -> None:
        FakeStatus._next_state += 1
        self.state = FakeStatus._next_state
        self.DESCRIPTOR = FakeDescriptor(
            {"state": FakeFieldDescriptor(FakeEnumType({self.state: FakeEnumValue(state_name)}))}
        )


class FakeStatusEvent:
    def __init__(self, task_id: str, state_name: str, final: bool = False) -> None:
        self.task_id = task_id
        self.status = FakeStatus(state_name)
        self.final = final


class FakeArtifact:
    def __init__(self) -> None:
        self.artifact_id = "A1"
        self.name = "faultManagement.Incident"


class FakeArtifactEvent:
    def __init__(self, task_id: str) -> None:
        self.task_id = task_id
        self.artifact = FakeArtifact()


class FakeMessage:
    def __init__(self, metadata: Any, task_id: str = "", context_id: str = "") -> None:
        self.metadata = metadata
        self.task_id = task_id
        self.context_id = context_id
        self.parts: list[Any] = []
        self.role = "ROLE_USER"


class FakeSendRequest:
    def __init__(self, message: FakeMessage) -> None:
        self.message = message


class FakeStreamResponse:
    """Structural StreamResponse: oneof field name + inner event."""

    def __init__(self, field: str, value: Any) -> None:
        object.__setattr__(self, "_field", field)
        object.__setattr__(self, "_value", value)

    def HasField(self, name: str) -> bool:
        return name == object.__getattribute__(self, "_field")

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        return object.__getattribute__(self, "_value") if name == object.__getattribute__(self, "_field") else None


class FakeContext:
    def __init__(self) -> None:
        self.state: dict[str, Any] = {}
        self.service_parameters: dict[str, str] = {}


class FakeBeforeArgs:
    def __init__(self, input_obj: Any, method: str, context: FakeContext | None = None) -> None:
        self.input = input_obj
        self.method = method
        self.agent_card = None
        self.context = context
        self.early_return = None


class FakeAfterArgs:
    def __init__(self, result: Any, method: str, context: FakeContext | None = None) -> None:
        self.result = result
        self.method = method
        self.agent_card = None
        self.context = context
        self.early_return = False


class FakeEventQueue:
    def __init__(self) -> None:
        self.events: list[Any] = []

    async def enqueue_event(self, event: Any) -> None:
        self.events.append(event)

    async def close(self) -> None: ...


class FakeServerCallContext:
    def __init__(self, headers: dict[str, str] | None = None) -> None:
        self.state: dict[str, Any] = {"headers": dict(headers or {})}
        self.requested_extensions: set[str] = set()
        self.tenant = ""


class FakeRequestContext:
    def __init__(self, message: FakeMessage, headers: dict[str, str] | None = None) -> None:
        self.call_context = FakeServerCallContext(headers)
        self.message = message


def make_send_request(metadata: Any, task_id: str = "T1", context_id: str = "C1") -> FakeSendRequest:
    return FakeSendRequest(FakeMessage(metadata=metadata, task_id=task_id, context_id=context_id))


def make_status_event(task_id: str, state_name: str, final: bool = False) -> FakeStatusEvent:
    return FakeStatusEvent(task_id, state_name, final)


def make_artifact_event(task_id: str) -> FakeArtifactEvent:
    return FakeArtifactEvent(task_id)


def make_final_event(task_id: str, state_name: str = "TASK_STATE_COMPLETED") -> FakeStatusEvent:
    return FakeStatusEvent(task_id, state_name, final=True)
```

`tests/observability/test_attributes_event.py`：

```python
from __future__ import annotations

from typing import Any

from a2a_t.observability.attributes import classify_event, unwrap_stream_response

from tests.observability.stubs import (
    FakeMessage,
    FakeStreamResponse,
    make_artifact_event,
    make_final_event,
    make_status_event,
)


class _RecordingSpan:
    def __init__(self) -> None:
        self.attributes: dict[str, Any] = {}

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value


class _FakeTask:
    def __init__(self) -> None:
        from tests.observability.stubs import FakeStatus

        self.id = "T1"
        self.status = FakeStatus("TASK_STATE_WORKING")


def test_classify_status_event() -> None:
    info = classify_event(make_status_event("T1", "TASK_STATE_WORKING"))
    assert info.kind == "status"
    assert info.task_id == "T1"
    assert info.task_status == "working"
    assert info.is_terminal is False
    assert info.final is False


def test_classify_terminal_status_events() -> None:
    assert classify_event(make_status_event("T1", "TASK_STATE_COMPLETED")).kind == "completed"
    assert classify_event(make_status_event("T1", "TASK_STATE_CANCELED")).is_terminal is True
    assert classify_event(make_status_event("T1", "TASK_STATE_FAILED")).kind == "failed"


def test_classify_final_non_terminal() -> None:
    info = classify_event(make_status_event("T1", "TASK_STATE_INPUT_REQUIRED", final=True))
    assert info.kind == "status"      # kind 按 state 判定
    assert info.final is True         # final 是权威流结束信号
    assert info.is_terminal is True   # final 即视为流终结


def test_classify_artifact_message_task() -> None:
    art = classify_event(make_artifact_event("T1"))
    assert art.kind == "artifact" and art.task_id == "T1"
    msg = classify_event(FakeMessage(metadata={"k": "v"}))
    assert msg.kind == "message" and msg.message_metadata == {"k": "v"}
    task = classify_event(_FakeTask())
    assert task.kind == "task" and task.task_status == "working"


def test_classify_unknown_shape() -> None:
    assert classify_event(object()).kind == "unknown"


def test_unwrap_stream_response() -> None:
    event = make_status_event("T1", "TASK_STATE_WORKING")
    resp = FakeStreamResponse("status_update", event)
    assert unwrap_stream_response(resp) is event
    assert unwrap_stream_response(event) is None


def test_a2at_attribute_extractor() -> None:
    from a2a_t.observability.attributes import a2at_attribute_extractor

    span = _RecordingSpan()
    request = FakeStreamResponse  # placeholder type var to keep importers honest
    del request
    message = FakeMessage(
        metadata={
            "https://projects.tmforum.org/a2aproject/telecommunication/extensions/Task-T/v1": "prompt",
            "negotiationContext": {"id": "N1", "round": 1, "maxRounds": 5, "performative": "PROPOSE"},
        },
        task_id="T1",
    )
    a2at_attribute_extractor(span, (message,), {}, None, None)
    assert span.attributes["extension.name"] == "Task-T"
    assert span.attributes["task.id"] == "T1"
    assert span.attributes["negotiation.id"] == "N1"

    span2 = _RecordingSpan()
    event = make_status_event("T2", "TASK_STATE_WORKING")
    a2at_attribute_extractor(span2, (), {}, FakeStreamResponse("status_update", event), None)
    assert span2.attributes["streaming.event.kind"] == "status"
    assert span2.attributes["task.status"] == "working"
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/observability/test_attributes_event.py -v`
Expected: FAIL（ImportError: cannot import name 'classify_event'）

- [ ] **Step 3: 实现（追加到 attributes.py 末尾）**

```python
import dataclasses


@dataclasses.dataclass(slots=True)
class EventInfo:
    """Classified view of one A2A stream event (structural, protocol-agnostic)."""

    kind: str
    task_id: str | None = None
    task_status: str | None = None
    final: bool = False
    is_terminal: bool = False
    message_metadata: dict[str, Any] | None = None


_TERMINAL_STATES = frozenset(
    {"TASK_STATE_COMPLETED", "TASK_STATE_CANCELED", "TASK_STATE_FAILED", "TASK_STATE_REJECTED"}
)
_STREAM_RESPONSE_FIELDS = ("status_update", "message", "artifact_update", "task")


def _enum_field_name(message: Any, field_name: str, value: Any) -> str | None:
    """Resolve a protobuf enum int to its name via structural DESCRIPTOR access."""
    if isinstance(value, str):
        return value
    name = getattr(value, "name", None)
    if isinstance(name, str):
        return name
    try:
        descriptor = message.DESCRIPTOR
        enum_type = descriptor.fields_by_name[field_name].enum_type
        return enum_type.values_by_number[value].name
    except Exception:  # noqa: BLE001
        return None


def _normalize_state_name(enum_name: str | None) -> str | None:
    if enum_name is None:
        return None
    prefix = "TASK_STATE_"
    return enum_name[len(prefix) :].lower() if enum_name.startswith(prefix) else enum_name.lower()


def classify_event(event: Any) -> EventInfo:
    """Classify one A2A event into EventInfo; never raises."""
    try:
        status = _safe_getattr(event, "status")
        task_id = _safe_getattr(event, "task_id")
        if status is not None and task_id is not None:
            enum_name = _enum_field_name(status, "state", status.state)
            state_name = _normalize_state_name(enum_name)
            terminal_state = enum_name in _TERMINAL_STATES
            final = bool(_safe_getattr(event, "final"))
            kind = state_name if (terminal_state and state_name) else "status"
            return EventInfo(
                kind=kind,
                task_id=str(task_id) if task_id else None,
                task_status=state_name,
                final=final,
                is_terminal=terminal_state or final,
            )
        artifact = _safe_getattr(event, "artifact")
        if artifact is not None and task_id is not None:
            return EventInfo(kind="artifact", task_id=str(task_id) if task_id else None)
        if _safe_getattr(event, "parts") is not None and _safe_getattr(event, "role") is not None:
            event_task_id = _safe_getattr(event, "task_id")
            return EventInfo(
                kind="message",
                task_id=str(event_task_id) if event_task_id else None,
                message_metadata=normalize_metadata(_safe_getattr(event, "metadata")),
            )
        if status is not None and _safe_getattr(event, "id"):
            enum_name = _enum_field_name(status, "state", status.state)
            return EventInfo(
                kind="task",
                task_id=str(event.id),
                task_status=_normalize_state_name(enum_name),
                is_terminal=enum_name in _TERMINAL_STATES,
            )
    except Exception:  # noqa: BLE001
        return EventInfo(kind="unknown")
    return EventInfo(kind="unknown")


def unwrap_stream_response(result: Any) -> Any | None:
    """Unwrap a StreamResponse-like oneof into the inner event; None when not stream-shaped."""
    try:
        has_field = getattr(result, "HasField", None)
        if not callable(has_field):
            return None
        for field in _STREAM_RESPONSE_FIELDS:
            if result.HasField(field):
                return getattr(result, field)
    except Exception:  # noqa: BLE001
        return None
    return None


def _write_request_attributes(span: Any, message: Any) -> None:
    metadata = normalize_metadata(_safe_getattr(message, "metadata"))
    _write_metadata_attributes(span, metadata)
    task_id = _safe_getattr(message, "task_id")
    if isinstance(task_id, str) and task_id:
        span.set_attribute(ATTR_TASK_ID, task_id)


def _write_metadata_attributes(span: Any, metadata: dict[str, Any]) -> None:
    extension = _extension_name_from_metadata(metadata)
    if extension:
        span.set_attribute(ATTR_EXTENSION_NAME, extension)
    for key, value in extract_negotiation_attributes(metadata).items():
        span.set_attribute(key, value)


def a2at_attribute_extractor(span: Any, args: tuple, kwargs: dict, result: Any, exception: Any) -> None:
    """attribute_extractor callback for a2a-python's @trace_function; writes A2A-T attributes."""
    for candidate in (*args, result):
        if candidate is None:
            continue
        message = _safe_getattr(candidate, "message")
        if message is not None:
            _write_request_attributes(span, message)
            return
        if _safe_getattr(candidate, "parts") is not None and _safe_getattr(candidate, "role") is not None:
            _write_metadata_attributes(span, normalize_metadata(_safe_getattr(candidate, "metadata")))
            return
        unwrapped = unwrap_stream_response(candidate)
        target = unwrapped if unwrapped is not None else candidate
        info = classify_event(target)
        if info.kind != "unknown":
            span.set_attribute(ATTR_STREAMING_EVENT_KIND, info.kind)
            if info.task_id:
                span.set_attribute(ATTR_TASK_ID, info.task_id)
            if info.task_status:
                span.set_attribute(ATTR_TASK_STATUS, info.task_status)
            if info.message_metadata:
                _write_metadata_attributes(span, info.message_metadata)
            return
```

- [ ] **Step 4: 运行测试确认通过**

Run: `uv run pytest tests/observability/test_attributes_event.py tests/observability/test_attributes_request.py -v`
Expected: 全部 PASS

- [ ] **Step 5: lint + 提交**

```bash
uv run ruff check src/a2a_t/observability tests/observability
uv run mypy src
git add src/a2a_t/observability/attributes.py tests/observability/test_attributes_event.py tests/observability/stubs.py
git commit -m "feat(observability): event classification and attribute_extractor callback"
```

---

### Task 4: `propagation.py`

**Files:**
- Create: `src/a2a_t/observability/propagation.py`
- Test: `tests/observability/test_propagation.py`

**Interfaces:**
- Consumes: Task 1 `_otel_compat`（`is_enabled`）
- Produces: `TRACEPARENT_HEADER: str = "traceparent"`、`inject_traceparent(headers: MutableMapping[str, str], *, context: Any | None = None) -> None`、`extract_trace_context(headers: Mapping[str, str]) -> Any | None`

- [ ] **Step 1: 写失败测试**

`tests/observability/test_propagation.py`：

```python
from __future__ import annotations

import pytest
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

from a2a_t.observability.propagation import TRACEPARENT_HEADER, extract_trace_context, inject_traceparent


@pytest.fixture()
def tracer_provider() -> TracerProvider:
    trace.set_tracer_provider(TracerProvider())
    return trace.get_tracer_provider()


def test_roundtrip_inject_extract(tracer_provider: TracerProvider) -> None:
    headers: dict[str, str] = {}
    with trace.get_tracer("t").start_as_current_span("root") as span:
        inject_traceparent(headers)
    assert headers[TRACEPARENT_HEADER].startswith("00-")
    assert format(span.get_span_context().trace_id, "032x") in headers[TRACEPARENT_HEADER]

    token = otel_context.attach(extract_trace_context(headers))
    try:
        with trace.get_tracer("t").start_as_current_span("child") as child:
            assert child.get_span_context().trace_id == span.get_span_context().trace_id
            assert child.get_span_context().parent_span_id == span.get_span_context().span_id
    finally:
        otel_context.detach(token)


def test_extract_missing_returns_none() -> None:
    assert extract_trace_context({"other": "x"}) is None


def test_extract_case_insensitive() -> None:
    headers = {TRACEPARENT_HEADER: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}
    assert extract_trace_context(headers) is not None


def test_noop_when_disabled(tracer_provider: TracerProvider, monkeypatch: pytest.MonkeyPatch) -> None:
    from a2a_t.observability import _otel_compat

    monkeypatch.setattr(_otel_compat, "_ENABLED", False)
    headers: dict[str, str] = {}
    inject_traceparent(headers)
    assert headers == {}
    assert extract_trace_context({TRACEPARENT_HEADER: "00-x-y-01"}) is None
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/observability/test_propagation.py -v`
Expected: FAIL（ModuleNotFoundError）

- [ ] **Step 3: 实现**

`src/a2a_t/observability/propagation.py`：

```python
"""W3C TraceContext propagation helpers built on the opentelemetry-api default propagator."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any

from a2a_t.observability import _otel_compat

TRACEPARENT_HEADER = "traceparent"


def inject_traceparent(headers: MutableMapping[str, str], *, context: Any | None = None) -> None:
    """Inject traceparent of the current (or given) context into headers; NoOp when disabled."""
    if not _otel_compat.is_enabled():
        return
    from opentelemetry.propagate import inject

    try:
        if context is None:
            inject(headers)
        else:
            inject(headers, context=context)
    except Exception:  # noqa: BLE001
        pass


def extract_trace_context(headers: Mapping[str, str]) -> Any | None:
    """Extract an opaque OTel Context from headers (case-insensitive); None when absent."""
    if not _otel_compat.is_enabled():
        return None
    from opentelemetry.propagate import extract

    try:
        lowered = {str(k).lower(): str(v) for k, v in headers.items()}
        context = extract(carrier=lowered)
    except Exception:  # noqa: BLE001
        return None
    span = _otel_compat.otel_trace.get_current_span(context)
    span_context = span.get_span_context()
    if span_context is None or not getattr(span_context, "is_valid", False):
        return None
    return context
```

- [ ] **Step 4: 运行测试确认通过**

Run: `uv run pytest tests/observability/test_propagation.py -v`
Expected: 4 PASS

- [ ] **Step 5: lint + 提交**

```bash
uv run ruff check src/a2a_t/observability/propagation.py tests/observability/test_propagation.py
uv run mypy src
git add src/a2a_t/observability/propagation.py tests/observability/test_propagation.py
git commit -m "feat(observability): W3C tracecontext propagation helpers"
```

---

### Task 5: `config.py`

**Files:**
- Create: `src/a2a_t/observability/config.py`
- Test: `tests/observability/test_config.py`

**Interfaces:**
- Consumes: 无
- Produces: `A2ATObservabilityConfig`（dataclass）——字段 `task_type_provider` / `notification_topic_provider` / `authorization_provider`（Callable，默认 None）、`payload_log_enabled: bool | None = None`、`payload_log_max_length: int | None = None`、`payload_redactor: Callable[[str], str] | None = None`、`record_metrics: bool = True`；属性 `resolved_payload_log_enabled: bool`、`resolved_payload_log_max_length: int`；方法 `invoke_task_type_provider(view) -> str | None`、`invoke_notification_topic_provider(view) -> str | None`、`invoke_authorization_provider(view) -> dict[str, str] | None`

- [ ] **Step 1: 写失败测试**

`tests/observability/test_config.py`：

```python
from __future__ import annotations

import logging

from a2a_t.observability.config import A2ATObservabilityConfig


def test_defaults(monkeypatch) -> None:
    monkeypatch.delenv("A2AT_LOGS_PAYLOAD_ENABLED", raising=False)
    monkeypatch.delenv("A2AT_LOGS_PAYLOAD_MAX_LENGTH", raising=False)
    config = A2ATObservabilityConfig()
    assert config.resolved_payload_log_enabled is False
    assert config.resolved_payload_log_max_length == 4096
    assert config.record_metrics is True


def test_explicit_beats_env(monkeypatch) -> None:
    monkeypatch.setenv("A2AT_LOGS_PAYLOAD_ENABLED", "true")
    monkeypatch.setenv("A2AT_LOGS_PAYLOAD_MAX_LENGTH", "100")
    config = A2ATObservabilityConfig(payload_log_enabled=False, payload_log_max_length=2048)
    assert config.resolved_payload_log_enabled is False
    assert config.resolved_payload_log_max_length == 2048


def test_env_fills_unset(monkeypatch) -> None:
    monkeypatch.setenv("A2AT_LOGS_PAYLOAD_ENABLED", "true")
    monkeypatch.setenv("A2AT_LOGS_PAYLOAD_MAX_LENGTH", "100")
    config = A2ATObservabilityConfig()
    assert config.resolved_payload_log_enabled is True
    assert config.resolved_payload_log_max_length == 100


def test_provider_exception_swallowed(caplog) -> None:
    def boom(view: object) -> str:
        raise RuntimeError("boom")

    config = A2ATObservabilityConfig(task_type_provider=boom)
    with caplog.at_level(logging.WARNING, logger="a2at.observability"):
        assert config.invoke_task_type_provider({}) is None
    assert any("task_type_provider" in r.getMessage() for r in caplog.records)


def test_authorization_provider_keys() -> None:
    config = A2ATObservabilityConfig(
        authorization_provider=lambda view: {"policy_id": "P1", "operation_type": "cfg", "risk_level": "medium"}
    )
    result = config.invoke_authorization_provider({})
    assert result == {"policy_id": "P1", "operation_type": "cfg", "risk_level": "medium"}
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/observability/test_config.py -v`
Expected: FAIL（ModuleNotFoundError）

- [ ] **Step 3: 实现**

`src/a2a_t/observability/config.py`：

```python
"""A2ATObservabilityConfig: provider callbacks and payload-log settings (explicit > env > default)."""

from __future__ import annotations

import logging
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger("a2at.observability")

AttributeValueProvider = Callable[[Mapping[str, Any]], "str | None"]
AuthorizationProvider = Callable[[Mapping[str, Any]], "Mapping[str, str] | None"]


def _to_bool(raw: str) -> bool:
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class A2ATObservabilityConfig:
    """Configuration of the A2A-T observability adapters."""

    task_type_provider: AttributeValueProvider | None = None
    notification_topic_provider: AttributeValueProvider | None = None
    authorization_provider: AuthorizationProvider | None = None
    payload_log_enabled: bool | None = None
    payload_log_max_length: int | None = None
    payload_redactor: Callable[[str], str] | None = None
    record_metrics: bool = True

    @property
    def resolved_payload_log_enabled(self) -> bool:
        if self.payload_log_enabled is not None:
            return self.payload_log_enabled
        return _to_bool(os.getenv("A2AT_LOGS_PAYLOAD_ENABLED", "false"))

    @property
    def resolved_payload_log_max_length(self) -> int:
        if self.payload_log_max_length is not None:
            return self.payload_log_max_length
        try:
            return int(os.getenv("A2AT_LOGS_PAYLOAD_MAX_LENGTH", "4096"))
        except ValueError:
            return 4096

    def _invoke(self, name: str, provider: Callable[..., Any] | None, view: Mapping[str, Any]) -> Any:
        if provider is None:
            return None
        try:
            return provider(view)
        except Exception:  # noqa: BLE001 - providers never break the business flow
            logger.warning("%s raised; attribute omitted", name, exc_info=True)
            return None

    def invoke_task_type_provider(self, view: Mapping[str, Any]) -> str | None:
        result = self._invoke("task_type_provider", self.task_type_provider, view)
        return result if isinstance(result, str) else None

    def invoke_notification_topic_provider(self, view: Mapping[str, Any]) -> str | None:
        result = self._invoke("notification_topic_provider", self.notification_topic_provider, view)
        return result if isinstance(result, str) else None

    def invoke_authorization_provider(self, view: Mapping[str, Any]) -> dict[str, str] | None:
        result = self._invoke("authorization_provider", self.authorization_provider, view)
        if isinstance(result, Mapping):
            return {str(k): str(v) for k, v in result.items()}
        return None
```

- [ ] **Step 4: 运行测试确认通过**

Run: `uv run pytest tests/observability/test_config.py -v`
Expected: 5 PASS

- [ ] **Step 5: lint + 提交**

```bash
uv run ruff check src/a2a_t/observability/config.py tests/observability/test_config.py
uv run mypy src
git add src/a2a_t/observability/config.py tests/observability/test_config.py
git commit -m "feat(observability): observability config with providers and env precedence"
```

---

### Task 6: `span.py` —— `A2ATSpan` 手动 API

**Files:**
- Create: `src/a2a_t/observability/span.py`
- Test: `tests/observability/test_span.py`

**Interfaces:**
- Consumes: Task 1 `_otel_compat`、Task 2 常量、Task 4 `extract_trace_context`（测试）
- Produces: `A2ATSpan`（spec §3.3 全签名：`__init__(name, *, kind="INTERNAL", attributes=None, context=None)`、`__enter__`/`__exit__`、`set_attribute`、`apply_task_type`、`apply_negotiation_total_rounds`、`apply_notification_topic`、`apply_authorization`、`add_link`）

- [ ] **Step 1: 写失败测试**

`tests/observability/test_span.py`：

```python
from __future__ import annotations

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from a2a_t.observability import _otel_compat
from a2a_t.observability.propagation import extract_trace_context, inject_traceparent
from a2a_t.observability.span import A2ATSpan


@pytest.fixture()
def exporter() -> InMemorySpanExporter:
    exporter = InMemorySpanExporter()
    trace.set_tracer_provider(TracerProvider())
    trace.get_tracer_provider().add_span_processor(SimpleSpanProcessor(exporter))
    return exporter


def test_span_lifecycle_and_attributes(exporter: InMemorySpanExporter) -> None:
    with A2ATSpan("my.op", attributes={"a2at.custom": "x"}) as span:
        span.apply_task_type("配置下发")
        span.apply_notification_topic("alarm")
        span.apply_negotiation_total_rounds(3)
        span.apply_authorization(policy_id="P1", operation_type="变更", risk_level="medium")
        span.set_attribute("k", 1)
    finished = exporter.get_finished_spans()[-1]
    assert finished.name == "my.op"
    attrs = finished.attributes or {}
    assert attrs["a2at.custom"] == "x"
    assert attrs["task.type"] == "配置下发"
    assert attrs["notification.topic"] == "alarm"
    assert attrs["negotiation.total_rounds"] == 3
    assert attrs["authorization.policy.id"] == "P1"
    assert attrs["authorization.operation_type"] == "变更"
    assert attrs["authorization.operation_risk_level"] == "medium"
    assert attrs["k"] == 1


def test_span_error_status(exporter: InMemorySpanExporter) -> None:
    with pytest.raises(ValueError), A2ATSpan("boom"):
        raise ValueError("x")
    assert exporter.get_finished_spans()[-1].status.status_code == StatusCode.ERROR


def test_span_explicit_parent_context(exporter: InMemorySpanExporter) -> None:
    headers: dict[str, str] = {}
    with trace.get_tracer("t").start_as_current_span("remote-root") as root:
        inject_traceparent(headers)
    ctx = extract_trace_context(headers)
    assert ctx is not None
    with A2ATSpan("child", context=ctx):
        pass
    spans = {s.name: s for s in exporter.get_finished_spans()}
    child = spans["child"]
    assert child.parent is not None
    assert child.parent.span_id == spans["remote-root"].context.span_id


def test_add_link_between_spans(exporter: InMemorySpanExporter) -> None:
    with A2ATSpan("delivery"):
        pass
    delivery_finished = exporter.get_finished_spans()[-1]
    delivery = A2ATSpan("delivery-already-ended")
    with A2ATSpan("apply") as apply_span:
        pass
    # 已结束 Span 的 link：重建 A2ATSpan 句柄不可行——用先 add_link 再 enter 的形态：
    with A2ATSpan("apply2") as apply2:
        pass
    del delivery, apply_span, delivery_finished, apply2


def test_add_link_pre_enter(exporter: InMemorySpanExporter) -> None:
    # 语义：对另一 A2ATSpan（在其 enter 期间或已结束）建立 link
    source = A2ATSpan("source")
    with source:
        pass
    target = A2ATSpan("target")
    target.add_link(source)
    with target:
        pass
    spans = {s.name: s for s in exporter.get_finished_spans()}
    links = spans["target"].links or []
    assert any(link.context.span_id == spans["source"].context.span_id for link in links)


def test_noop_when_disabled(exporter: InMemorySpanExporter, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_otel_compat, "_ENABLED", False)
    with A2ATSpan("noop") as span:
        span.apply_task_type("x")
        span.set_attribute("k", "v")
    assert exporter.get_finished_spans() == []
```

注意：`test_add_link_between_spans` 中的探索性代码删除，保留 `test_add_link_pre_enter` 作为 add_link 的规范用法测试（对已结束的 source Span 句柄建 link，在 target enter 时生效）。

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/observability/test_span.py -v`
Expected: FAIL（ModuleNotFoundError）

- [ ] **Step 3: 实现**

`src/a2a_t/observability/span.py`：

```python
"""A2ATSpan: manual span API (context manager) for user-created spans; NoOp when OTel is off."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from a2a_t.observability import _otel_compat
from a2a_t.observability.attributes import (
    ATTR_AUTHORIZATION_OPERATION_RISK_LEVEL,
    ATTR_AUTHORIZATION_OPERATION_TYPE,
    ATTR_AUTHORIZATION_POLICY_ID,
    ATTR_NEGOTIATION_TOTAL_ROUNDS,
    ATTR_NOTIFICATION_TOPIC,
    ATTR_TASK_TYPE,
)

_VALID_KINDS = frozenset({"INTERNAL", "CLIENT", "SERVER", "PRODUCER", "CONSUMER"})


class A2ATSpan:
    """Manual A2A-T span. Absorbs all calls when OpenTelemetry is unavailable."""

    def __init__(
        self,
        name: str,
        *,
        kind: str = "INTERNAL",
        attributes: Mapping[str, str | int | bool] | None = None,
        context: Any | None = None,
    ) -> None:
        if kind not in _VALID_KINDS:
            raise ValueError(f"invalid span kind: {kind}")
        self._name = name
        self._kind = kind
        self._attributes = dict(attributes) if attributes else {}
        self._context = context
        self._links: list[Any] = []
        self._span: Any = None
        self._ended = False

    def _start(self) -> Any:
        if not _otel_compat.is_enabled():
            return None
        tracer = _otel_compat.get_tracer()
        kwargs: dict[str, Any] = {}
        span_kind = getattr(_otel_compat.SpanKind, self._kind, None) if _otel_compat.SpanKind else None
        if span_kind is not None:
            kwargs["kind"] = span_kind
        if self._links:
            kwargs["links"] = list(self._links)
        if self._context is not None:
            kwargs["context"] = self._context
        context_manager = tracer.start_as_current_span(self._name, **kwargs)
        self._span = context_manager.__enter__()
        if self._attributes:
            self._span.set_attributes(self._attributes)
        return self._span

    def _end(self, exc: BaseException | None) -> None:
        if self._span is None or self._ended:
            return
        self._ended = True
        try:
            if exc is not None:
                self._span.record_exception(exc)
                error = getattr(_otel_compat.StatusCode, "ERROR", None) if _otel_compat.StatusCode else None
                if error is not None:
                    self._span.set_status(error, str(exc))
            else:
                ok = getattr(_otel_compat.StatusCode, "OK", None) if _otel_compat.StatusCode else None
                if ok is not None:
                    self._span.set_status(ok)
        finally:
            self._span.end()

    def __enter__(self) -> A2ATSpan:
        self._start()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._end(exc_val)

    def set_attribute(self, key: str, value: str | int | bool) -> None:
        if self._span is not None:
            self._span.set_attribute(key, value)

    def apply_task_type(self, task_type: str) -> None:
        self.set_attribute(ATTR_TASK_TYPE, task_type)

    def apply_negotiation_total_rounds(self, total_rounds: int) -> None:
        self.set_attribute(ATTR_NEGOTIATION_TOTAL_ROUNDS, total_rounds)

    def apply_notification_topic(self, topic: str) -> None:
        self.set_attribute(ATTR_NOTIFICATION_TOPIC, topic)

    def apply_authorization(self, *, policy_id: str, operation_type: str, risk_level: str) -> None:
        self.set_attribute(ATTR_AUTHORIZATION_POLICY_ID, policy_id)
        self.set_attribute(ATTR_AUTHORIZATION_OPERATION_TYPE, operation_type)
        self.set_attribute(ATTR_AUTHORIZATION_OPERATION_RISK_LEVEL, risk_level)

    def add_link(self, other: A2ATSpan) -> None:
        """Link another A2ATSpan. Before enter: applied at creation; after enter: span.add_link."""
        if other._span is None:
            return
        other_context = other._span.get_span_context()
        if other_context is None:
            return
        if self._span is not None:
            add_link = getattr(self._span, "add_link", None)
            if callable(add_link):
                add_link(other_context)
            return
        if _otel_compat.is_enabled():
            from opentelemetry.trace import Link

            self._links.append(Link(other_context))
```

- [ ] **Step 4: 运行测试确认通过**

Run: `uv run pytest tests/observability/test_span.py -v`
Expected: 5 PASS（删除探索性测试后）

- [ ] **Step 5: lint + 提交**

```bash
uv run ruff check src/a2a_t/observability/span.py tests/observability/test_span.py
uv run mypy src
git add src/a2a_t/observability/span.py tests/observability/test_span.py
git commit -m "feat(observability): manual A2ATSpan API with NoOp absorption"
```

---

### Task 7: `metrics.py`

**Files:**
- Create: `src/a2a_t/observability/metrics.py`
- Test: `tests/observability/test_metrics.py`

**Interfaces:**
- Consumes: Task 1 `_otel_compat.get_meter`、Task 2 常量
- Produces: `A2ATMetricsRecorder`（无参构造，链式，返回 `Self`）：`task_request_duration(duration_s, *, attributes=None)`、`gen_ai_operation_duration(duration_s, *, operation, attributes=None)`、`gen_ai_token_usage(tokens, *, token_type="input", attributes=None)`、`negotiation_total_rounds(rounds, *, negotiation_id, outcome, attributes=None)`

- [ ] **Step 1: 写失败测试**

`tests/observability/test_metrics.py`：

```python
from __future__ import annotations

from typing import Any

import pytest
from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricsReader
from opentelemetry.sdk.trace import TracerProvider

from a2a_t.observability import _otel_compat
from a2a_t.observability.metrics import A2ATMetricsRecorder


@pytest.fixture()
def reader() -> InMemoryMetricsReader:
    trace.set_tracer_provider(TracerProvider())  # 隔离 tracer provider
    reader = InMemoryMetricsReader()
    metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))
    return reader


def _samples(reader: InMemoryMetricsReader, name: str) -> list[Any]:
    data = reader.get_metrics_data()
    assert data is not None
    points = [
        point
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for metric in sm.metrics
        if metric.name == name
        for point in metric.data.data_points
    ]
    assert points, f"metric {name} not exported"
    return points


def test_all_instruments_record(reader: InMemoryMetricsReader) -> None:
    recorder = A2ATMetricsRecorder()
    (
        recorder.gen_ai_operation_duration(0.5, operation="send_message")
        .gen_ai_token_usage(100, token_type="output")
        .task_request_duration(1.5, attributes={"a2at.span.side": "client"})
        .negotiation_total_rounds(3, negotiation_id="N1", outcome="accept")
    )
    reader.collect()
    assert _samples(reader, "gen_ai.client.operation.duration")[0].value == pytest.approx(0.5)
    assert _samples(reader, "gen_ai.client.token.usage")[0].value == 100
    assert _samples(reader, "a2at.task.request.duration")[0].value == pytest.approx(1.5)
    assert _samples(reader, "a2at.negotiation.total_rounds")[0].value == 3


def test_default_attributes_attached(reader: InMemoryMetricsReader) -> None:
    recorder = A2ATMetricsRecorder()
    recorder.gen_ai_operation_duration(0.2, operation="send_message")
    reader.collect()
    assert _samples(reader, "gen_ai.client.operation.duration")[-1].attributes["gen_ai.operation.name"] == "send_message"

    recorder.negotiation_total_rounds(2, negotiation_id="N9", outcome="abort")
    reader.collect()
    point = _samples(reader, "a2at.negotiation.total_rounds")[-1]
    assert point.attributes["negotiation.id"] == "N9"
    assert point.attributes["outcome"] == "abort"


def test_noop_when_disabled(reader: InMemoryMetricsReader, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_otel_compat, "_ENABLED", False)
    from a2a_t.observability import metrics as metrics_module

    metrics_module._instruments.clear()  # 重置缓存以走 NoOp 分支
    recorder = A2ATMetricsRecorder()
    recorder.task_request_duration(1.0)
    data = reader.get_metrics_data()
    assert data is None or not data.resource_metrics
    metrics_module._instruments.clear()
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/observability/test_metrics.py -v`
Expected: FAIL（ModuleNotFoundError）

- [ ] **Step 3: 实现**

`src/a2a_t/observability/metrics.py`：

```python
"""A2ATMetricsRecorder: chained recorder for L1/L2/L3 metrics; NoOp when OTel is off."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Self

from a2a_t.observability import _otel_compat
from a2a_t.observability.attributes import (
    ATTR_GEN_AI_OPERATION_NAME,
    ATTR_NEGOTIATION_ID,
    AttributeValue,
)

_instruments: dict[str, Any] = {}


def _instrument(kind: str, name: str, unit: str, description: str) -> Any:
    key = f"{kind}:{name}"
    if key not in _instruments:
        meter = _otel_compat.get_meter()
        if kind == "histogram":
            _instruments[key] = meter.create_histogram(name, unit=unit, description=description)
        else:
            _instruments[key] = meter.create_counter(name, unit=unit, description=description)
    return _instruments[key]


class A2ATMetricsRecorder:
    """Chainable metric recorder (module-level instruments, reusable instances)."""

    def __init__(self) -> None:
        self._task_duration = _instrument(
            "histogram", "a2at.task.request.duration", "s", "A2A-T task request duration"
        )
        self._gen_ai_duration = _instrument(
            "histogram", "gen_ai.client.operation.duration", "s", "GenAI client operation duration"
        )
        self._token_usage = _instrument(
            "histogram", "gen_ai.client.token.usage", "{token}", "GenAI client token usage"
        )
        self._negotiation_rounds = _instrument(
            "counter", "a2at.negotiation.total_rounds", "1", "Total rounds of a finished negotiation"
        )

    def task_request_duration(
        self, duration_s: float, *, attributes: Mapping[str, AttributeValue] | None = None
    ) -> Self:
        self._task_duration.record(duration_s, attributes=dict(attributes) if attributes else None)
        return self

    def gen_ai_operation_duration(
        self,
        duration_s: float,
        *,
        operation: str,
        attributes: Mapping[str, AttributeValue] | None = None,
    ) -> Self:
        merged: dict[str, AttributeValue] = {ATTR_GEN_AI_OPERATION_NAME: operation}
        if attributes:
            merged.update(attributes)
        self._gen_ai_duration.record(duration_s, attributes=merged)
        return self

    def gen_ai_token_usage(
        self,
        tokens: int,
        *,
        token_type: str = "input",
        attributes: Mapping[str, AttributeValue] | None = None,
    ) -> Self:
        merged: dict[str, AttributeValue] = {"gen_ai.token.type": token_type}
        if attributes:
            merged.update(attributes)
        self._token_usage.record(tokens, attributes=merged)
        return self

    def negotiation_total_rounds(
        self,
        rounds: int,
        *,
        negotiation_id: str,
        outcome: str,
        attributes: Mapping[str, AttributeValue] | None = None,
    ) -> Self:
        merged: dict[str, AttributeValue] = {ATTR_NEGOTIATION_ID: negotiation_id, "outcome": outcome}
        if attributes:
            merged.update(attributes)
        self._negotiation_rounds.add(rounds, merged)
        return self
```

- [ ] **Step 4: 运行测试确认通过**

Run: `uv run pytest tests/observability/test_metrics.py -v`
Expected: 3 PASS

- [ ] **Step 5: lint + 提交**

```bash
uv run ruff check src/a2a_t/observability/metrics.py tests/observability/test_metrics.py
uv run mypy src
git add src/a2a_t/observability/metrics.py tests/observability/test_metrics.py
git commit -m "feat(observability): metrics recorder for L1/L2/L3 instruments"
```

---

### Task 8: `logs.py` —— 内部自动报文日志

**Files:**
- Create: `src/a2a_t/observability/logs.py`
- Test: `tests/observability/test_logs.py`

**Interfaces:**
- Consumes: Task 5 config、Task 1 `_otel_compat`（`format_trace_id`/`format_span_id`/`get_current_span`）
- Produces: `log_event(event: str, level: int, *, fields: Mapping[str, object], payload: str | None = None, config: A2ATObservabilityConfig | None = None) -> None`

- [ ] **Step 1: 写失败测试**

`tests/observability/test_logs.py`：

```python
from __future__ import annotations

import json
import logging

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

from a2a_t.observability.config import A2ATObservabilityConfig
from a2a_t.observability.logs import log_event


def test_log_event_json_with_trace_ids(caplog) -> None:
    trace.set_tracer_provider(TracerProvider())
    with caplog.at_level(logging.INFO, logger="a2at.observability"):
        with trace.get_tracer("t").start_as_current_span("s") as span:
            log_event("task.status_changed", logging.INFO, fields={"task.id": "T1", "task.status": "working"})
    record = next(r for r in caplog.records if r.message.startswith("{"))
    data = json.loads(record.message)
    assert data["event"] == "task.status_changed"
    assert data["task.id"] == "T1"
    assert data["trace_id"] == format(span.get_span_context().trace_id, "032x")


def test_payload_disabled_by_default(caplog) -> None:
    with caplog.at_level(logging.DEBUG, logger="a2at.observability"):
        log_event("task.request", logging.DEBUG, fields={}, payload="secret-text", config=A2ATObservabilityConfig())
    assert all("secret-text" not in r.message for r in caplog.records)


def test_payload_truncated_and_redacted(caplog) -> None:
    config = A2ATObservabilityConfig(
        payload_log_enabled=True,
        payload_log_max_length=10,
        payload_redactor=lambda s: s.replace("secret", "***"),
    )
    with caplog.at_level(logging.DEBUG, logger="a2at.observability"):
        log_event("task.request", logging.DEBUG, fields={}, payload="secret-0123456789abcdef", config=config)
    data = json.loads(caplog.records[-1].message)
    assert data["payload"].startswith("***")
    assert data["payload"].endswith("[truncated]")
    assert len(data["payload"]) <= 10 + len("[truncated]")
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/observability/test_logs.py -v`
Expected: FAIL（ModuleNotFoundError）

- [ ] **Step 3: 实现**

`src/a2a_t/observability/logs.py`：

```python
"""Internal structured payload logging (JSON via stdlib logging); no public API by design."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping

from a2a_t.observability import _otel_compat
from a2a_t.observability.config import A2ATObservabilityConfig

logger = logging.getLogger("a2at.observability")

_TRUNCATED_SUFFIX = "[truncated]"


def _current_trace_fields() -> dict[str, str]:
    if not _otel_compat.is_enabled():
        return {}
    span = _otel_compat.get_current_span()
    context = span.get_span_context() if span is not None else None
    if context is None or not getattr(context, "is_valid", False):
        return {}
    return {
        "trace_id": _otel_compat.format_trace_id(context.trace_id),
        "span_id": _otel_compat.format_span_id(context.span_id),
    }


def _prepare_payload(payload: str, config: A2ATObservabilityConfig) -> str | None:
    if not config.resolved_payload_log_enabled:
        return None
    text = payload
    if config.payload_redactor is not None:
        try:
            text = config.payload_redactor(text)
        except Exception:  # noqa: BLE001
            text = "[redaction-failed]"
    max_length = config.resolved_payload_log_max_length
    if len(text) > max_length:
        text = text[:max_length] + _TRUNCATED_SUFFIX
    return text


def log_event(
    event: str,
    level: int,
    *,
    fields: Mapping[str, object],
    payload: str | None = None,
    config: A2ATObservabilityConfig | None = None,
) -> None:
    """Emit one structured JSON log record; never raises."""
    try:
        resolved_config = config or A2ATObservabilityConfig()
        data: dict[str, object] = {"event": event}
        data.update(fields)
        data.update(_current_trace_fields())
        if payload is not None:
            prepared = _prepare_payload(payload, resolved_config)
            if prepared is not None:
                data["payload"] = prepared
        logger.log(level, json.dumps(data, ensure_ascii=False, default=str))
    except Exception:  # noqa: BLE001 - logging must never break the flow
        logger.debug("a2at log_event failed", exc_info=True)
```

- [ ] **Step 4: 运行测试确认通过**

Run: `uv run pytest tests/observability/test_logs.py -v`
Expected: 3 PASS

- [ ] **Step 5: lint + 提交**

```bash
uv run ruff check src/a2a_t/observability/logs.py tests/observability/test_logs.py
uv run mypy src
git add src/a2a_t/observability/logs.py tests/observability/test_logs.py
git commit -m "feat(observability): internal structured payload logging"
```

---

### Task 9: `client/span_processor.py` —— 协议 Span 属性注入桥

**Files:**
- Create: `src/a2a_t/observability/client/span_processor.py`
- Test: `tests/observability/test_client_span_processor.py`

**Interfaces:**
- Consumes: Task 2 `AttributeValue`、Task 4 `inject_traceparent`/`TRACEPARENT_HEADER`
- Produces:
  - `@dataclass(slots=True) class ClientRequestStash: attributes: dict[str, AttributeValue]; method: str; service_parameters: MutableMapping[str, str] | None; start_time: float; span_context: Any | None = None`
  - `set_request_stash(stash)` / `get_request_stash() -> ClientRequestStash | None` / `clear_request_stash()`（ContextVar 封装）
  - `class A2ATClientSpanProcessor`：`on_start(self, span, parent_context=None)`、`on_end(self, span)`、`shutdown()`、`force_flush(timeout_millis=30000) -> bool`（结构化 SpanProcessor）
  - `ensure_processor_registered() -> bool`（幂等；provider 无 `add_span_processor` 时 WARNING 一次返回 False）

- [ ] **Step 1: 写失败测试**

`tests/observability/test_client_span_processor.py`：

```python
from __future__ import annotations

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from a2a_t.observability.attributes import ATTR_EXTENSION_NAME
from a2a_t.observability.client.span_processor import (
    A2ATClientSpanProcessor,
    ClientRequestStash,
    clear_request_stash,
    ensure_processor_registered,
    get_request_stash,
    set_request_stash,
)


def _fresh_provider() -> TracerProvider:
    provider = TracerProvider()
    trace.set_tracer_provider(provider)
    return provider


def test_stash_contextvar_roundtrip() -> None:
    stash = ClientRequestStash(attributes={}, method="send_message", service_parameters=None, start_time=0.0)
    set_request_stash(stash)
    assert get_request_stash() is stash
    clear_request_stash()
    assert get_request_stash() is None


def test_on_start_enriches_and_injects() -> None:
    provider = _fresh_provider()
    provider.add_span_processor(A2ATClientSpanProcessor())
    headers: dict[str, str] = {}
    stash = ClientRequestStash(
        attributes={ATTR_EXTENSION_NAME: "Task-T"},
        method="send_message",
        service_parameters=headers,
        start_time=1.0,
    )
    set_request_stash(stash)
    with trace.get_tracer("a2a-python-sdk").start_as_current_span("RestTransport.send_message"):
        pass
    clear_request_stash()
    assert headers.get("traceparent", "").startswith("00-")  # traceparent 已注入
    assert stash.span_context is not None                     # SpanContext 已回存


def test_on_start_scope_and_kind_filter() -> None:
    provider = _fresh_provider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    provider.add_span_processor(A2ATClientSpanProcessor())
    stash = ClientRequestStash(
        attributes={ATTR_EXTENSION_NAME: "Task-T"}, method="send_message", service_parameters={}, start_time=0.0
    )
    set_request_stash(stash)
    with trace.get_tracer("a2a-python-sdk").start_as_current_span("proto"):
        pass
    with trace.get_tracer("other-scope").start_as_current_span("foreign"):
        pass
    with trace.get_tracer("a2a-python-sdk").start_as_current_span("server-side"):
        from opentelemetry.trace import SpanKind

        pass  # 默认 INTERNAL kind，应被 kind 过滤
    clear_request_stash()
    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert spans["proto"].attributes[ATTR_EXTENSION_NAME] == "Task-T"
    assert ATTR_EXTENSION_NAME not in (spans["foreign"].attributes or {})
    assert ATTR_EXTENSION_NAME not in (spans["server-side"].attributes or {})


def test_ensure_registered_idempotent() -> None:
    provider = _fresh_provider()
    from a2a_t.observability.client import span_processor as sp

    sp._processor_registered = False
    assert ensure_processor_registered() is True
    assert ensure_processor_registered() is True
    sp._processor_registered = False
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/observability/test_client_span_processor.py -v`
Expected: FAIL（ModuleNotFoundError）

- [ ] **Step 3: 实现**

`src/a2a_t/observability/client/span_processor.py`：

```python
"""SpanProcessor bridge: enrich a2a-python protocol transport spans with A2A-T attributes.

The interceptor's before() cannot touch the transport span (not yet created), so attributes
and the service_parameters reference are stashed in a ContextVar; this processor applies
them on the protocol span's on_start (scope 'a2a-python-sdk' + CLIENT kind + stash present).
Timing note: on_start fires before the transport method body builds HTTP headers, so the
traceparent injected into the stashed service_parameters lands on the wire.
"""

from __future__ import annotations

import contextvars
import dataclasses
import logging
from collections.abc import MutableMapping
from typing import Any

from a2a_t.observability import _otel_compat
from a2a_t.observability.attributes import AttributeValue
from a2a_t.observability.propagation import TRACEPARENT_HEADER, inject_traceparent

logger = logging.getLogger("a2at.observability")

_A2A_SDK_SCOPE = "a2a-python-sdk"


@dataclasses.dataclass(slots=True)
class ClientRequestStash:
    """Per-request stash created by the interceptor and consumed by the processor."""

    attributes: dict[str, AttributeValue]
    method: str
    service_parameters: MutableMapping[str, str] | None
    start_time: float
    span_context: Any | None = None


_request_stash: contextvars.ContextVar[ClientRequestStash | None] = contextvars.ContextVar(
    "a2at_client_request_stash", default=None
)


def set_request_stash(stash: ClientRequestStash) -> None:
    _request_stash.set(stash)


def get_request_stash() -> ClientRequestStash | None:
    return _request_stash.get()


def clear_request_stash() -> None:
    _request_stash.set(None)


class A2ATClientSpanProcessor:
    """Structural OTel SpanProcessor enriching protocol transport CLIENT spans."""

    def on_start(self, span: Any, parent_context: Any = None) -> None:
        try:
            if not _otel_compat.is_enabled():
                return
            stash = _request_stash.get()
            if stash is None:
                return
            scope = getattr(span, "instrumentation_scope", None)
            if getattr(scope, "name", None) != _A2A_SDK_SCOPE:
                return
            client_kind = getattr(_otel_compat.SpanKind, "CLIENT", None) if _otel_compat.SpanKind else None
            if client_kind is not None and getattr(span, "kind", None) != client_kind:
                return
            if stash.attributes:
                span.set_attributes(stash.attributes)
            if stash.service_parameters is not None:
                from opentelemetry.trace import set_span_in_context

                context = set_span_in_context(span)
                inject_traceparent(stash.service_parameters, context=context)
                if TRACEPARENT_HEADER not in stash.service_parameters:
                    logger.debug("traceparent injection produced no header")
            stash.span_context = span.get_span_context()
        except Exception:  # noqa: BLE001 - observability never breaks the flow
            logger.warning("A2ATClientSpanProcessor.on_start failed", exc_info=True)

    def on_end(self, span: Any) -> None:
        return None

    def shutdown(self) -> None:
        return None

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True


_processor_registered = False


def ensure_processor_registered() -> bool:
    """Idempotently register A2ATClientSpanProcessor on the global TracerProvider."""
    global _processor_registered
    if _processor_registered:
        return True
    if not _otel_compat.is_enabled():
        return False
    provider = _otel_compat.otel_trace.get_tracer_provider()
    adder = getattr(provider, "add_span_processor", None)
    if not callable(adder):
        logger.warning(
            "TracerProvider does not support add_span_processor; "
            "client-side protocol-span attribute injection is disabled"
        )
        return False
    adder(A2ATClientSpanProcessor())
    _processor_registered = True
    return True
```

- [ ] **Step 4: 运行测试确认通过**

Run: `uv run pytest tests/observability/test_client_span_processor.py -v`
Expected: 4 PASS

- [ ] **Step 5: lint + 提交**

```bash
uv run ruff check src/a2a_t/observability tests/observability
uv run mypy src
git add src/a2a_t/observability/client/span_processor.py tests/observability/test_client_span_processor.py
git commit -m "feat(observability): span processor bridge for protocol span enrichment"
```

---

### Task 10: `client/interceptor.py` —— `A2ATClientInterceptor`

**Files:**
- Create: `src/a2a_t/observability/client/interceptor.py`
- Test: `tests/observability/test_client_interceptor.py`

**Interfaces:**
- Consumes: Task 2（提取/分类）、Task 5 config、Task 7 metrics、Task 8 logs、Task 9 stash/`ensure_processor_registered`
- Produces: `A2ATClientInterceptor`（`__init__(self, *, config: A2ATObservabilityConfig | None = None)`；`async before(self, args: Any) -> None`；`async after(self, args: Any) -> None`）

- [ ] **Step 1: 写失败测试**

`tests/observability/test_client_interceptor.py`：

```python
from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricsReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from a2a_t.observability.attributes import (
    ATTR_EXTENSION_NAME,
    ATTR_STREAMING_EVENT_KIND,
    ATTR_TASK_ID,
    ATTR_TASK_STATUS,
)
from a2a_t.observability.client.interceptor import A2ATClientInterceptor
from a2a_t.observability.config import A2ATObservabilityConfig

from tests.observability.stubs import (
    FakeAfterArgs,
    FakeBeforeArgs,
    FakeContext,
    FakeStreamResponse,
    make_artifact_event,
    make_final_event,
    make_send_request,
    make_status_event,
)

_TASK_T = "https://projects.tmforum.org/a2aproject/telecommunication/extensions/Task-T/v1"
_NEG_T = "https://projects.tmforum.org/a2aproject/telecommunication/extensions/Negotiation-T/v1"


@pytest.fixture()
def setup() -> Iterator[tuple[InMemorySpanExporter, InMemoryMetricsReader]]:
    exporter = InMemorySpanExporter()
    trace.set_tracer_provider(TracerProvider())
    trace.get_tracer_provider().add_span_processor(SimpleSpanProcessor(exporter))
    reader = InMemoryMetricsReader()
    metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))
    yield exporter, reader


def _metric_points(reader: InMemoryMetricsReader, name: str) -> list[Any]:
    data = reader.get_metrics_data()
    assert data is not None
    return [
        point
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for metric in sm.metrics
        if metric.name == name
        for point in metric.data.data_points
    ]


async def test_full_stream_flow(setup: tuple[InMemorySpanExporter, InMemoryMetricsReader]) -> None:
    exporter, reader = setup
    interceptor = A2ATClientInterceptor(config=A2ATObservabilityConfig(task_type_provider=lambda v: "配置下发"))
    context = FakeContext()
    request = make_send_request(metadata={_TASK_T: "prompt"})
    await interceptor.before(FakeBeforeArgs(request, "send_message", context))

    # 模拟协议 transport Span（scope=a2a-python-sdk）触发 processor on_start
    with trace.get_tracer("a2a-python-sdk").start_as_current_span("RestTransport.send_message"):
        pass
    proto = next(s for s in exporter.get_finished_spans() if s.name == "RestTransport.send_message")
    assert proto.attributes[ATTR_EXTENSION_NAME] == "Task-T"
    assert proto.attributes["task.type"] == "配置下发"

    events = [
        FakeStreamResponse("status_update", make_status_event("T1", "TASK_STATE_WORKING")),
        FakeStreamResponse("artifact_update", make_artifact_event("T1")),
        FakeStreamResponse("status_update", make_final_event("T1", "TASK_STATE_COMPLETED")),
    ]
    for event in events:
        await interceptor.after(FakeAfterArgs(event, "send_message", context))

    event_spans = [s for s in exporter.get_finished_spans() if s.name.startswith("a2at.event.")]
    assert {s.name for s in event_spans} == {"a2at.event.status", "a2at.event.artifact", "a2at.event.completed"}
    status_span = next(s for s in event_spans if s.name == "a2at.event.status")
    assert status_span.attributes[ATTR_TASK_ID] == "T1"
    assert status_span.attributes[ATTR_TASK_STATUS] == "working"
    assert status_span.attributes[ATTR_STREAMING_EVENT_KIND] == "status"
    # 事件 Span 的父 = 协议 transport Span（parent-child，spec §6.1）
    assert status_span.parent is not None
    assert status_span.parent.span_id == proto.context.span_id

    reader.collect()
    names = {m.name for rm in reader.get_metrics_data().resource_metrics for sm in rm.scope_metrics for m in sm.metrics}
    assert "gen_ai.client.operation.duration" in names
    assert "a2at.task.request.duration" in names
    duration_points = _metric_points(reader, "a2at.task.request.duration")
    assert duration_points[-1].attributes["a2at.span.side"] == "client"
    assert duration_points[-1].attributes["streaming"] is True


async def test_negotiation_terminal_counter(setup: tuple[InMemorySpanExporter, InMemoryMetricsReader]) -> None:
    exporter, reader = setup
    interceptor = A2ATClientInterceptor()
    request = make_send_request(
        metadata={
            _NEG_T: "prompt",
            "negotiationContext": {"id": "N001", "round": 3, "maxRounds": 5, "performative": "ACCEPT"},
        }
    )
    await interceptor.before(FakeBeforeArgs(request, "send_message", FakeContext()))
    with trace.get_tracer("a2a-python-sdk").start_as_current_span("RestTransport.send_message"):
        pass
    reader.collect()
    points = _metric_points(reader, "a2at.negotiation.total_rounds")
    assert points[-1].value == 3
    assert points[-1].attributes["negotiation.id"] == "N001"
    assert points[-1].attributes["outcome"] == "accept"


async def test_non_stream_single_result(setup: tuple[InMemorySpanExporter, InMemoryMetricsReader]) -> None:
    exporter, reader = setup
    interceptor = A2ATClientInterceptor()
    await interceptor.before(FakeBeforeArgs(make_send_request(metadata=None), "get_task", FakeContext()))
    await interceptor.after(FakeAfterArgs(object(), "get_task", FakeContext()))  # 非 StreamResponse
    reader.collect()
    points = _metric_points(reader, "a2at.task.request.duration")
    assert points[-1].attributes["a2at.span.side"] == "client"
    assert points[-1].attributes["streaming"] is False


async def test_after_without_stash_is_noop(setup: tuple[InMemorySpanExporter, InMemoryMetricsReader]) -> None:
    exporter, reader = setup
    interceptor = A2ATClientInterceptor()
    await interceptor.after(FakeAfterArgs(object(), "send_message", FakeContext()))
    assert not [s for s in exporter.get_finished_spans() if s.name.startswith("a2at.event.")]
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/observability/test_client_interceptor.py -v`
Expected: FAIL（ModuleNotFoundError）

- [ ] **Step 3: 实现**

`src/a2a_t/observability/client/interceptor.py`：

```python
"""A2ATClientInterceptor: structural ClientCallInterceptor implementation.

before(): stash request attributes for the SpanProcessor (applied on the protocol transport
span), invoke providers, record the negotiation-terminal counter, log the outgoing message.
after(): one per-event CLIENT span per streamed event (parent = the protocol transport
span's SpanContext), L1/L3 metrics at stream end (timestamp delta, no owned request span).
"""

from __future__ import annotations

import logging
import time
from typing import Any

from a2a_t.observability import _otel_compat
from a2a_t.observability.attributes import (
    ATTR_A2AT_SPAN_SIDE,
    ATTR_EXTENSION_NAME,
    ATTR_GEN_AI_OPERATION_NAME,
    ATTR_NEGOTIATION_ID,
    ATTR_NEGOTIATION_PERFORMATIVE,
    ATTR_NEGOTIATION_ROUND,
    ATTR_STREAMING,
    ATTR_STREAMING_EVENT_KIND,
    ATTR_TASK_ID,
    ATTR_TASK_STATUS,
    EventInfo,
    classify_event,
    extract_request_attributes,
    metadata_view_for_provider,
    normalize_metadata,
    unwrap_stream_response,
)
from a2a_t.observability.attributes import (
    ATTR_AUTHORIZATION_OPERATION_RISK_LEVEL,
    ATTR_AUTHORIZATION_OPERATION_TYPE,
    ATTR_AUTHORIZATION_POLICY_ID,
    ATTR_NOTIFICATION_TOPIC,
    ATTR_TASK_TYPE,
)
from a2a_t.observability.client.span_processor import (
    ClientRequestStash,
    clear_request_stash,
    ensure_processor_registered,
    get_request_stash,
    set_request_stash,
)
from a2a_t.observability.config import A2ATObservabilityConfig
from a2a_t.observability.logs import log_event
from a2a_t.observability.metrics import A2ATMetricsRecorder

logger = logging.getLogger("a2at.observability")

_TERMINAL_PERFORMATIVES = frozenset({"ACCEPT", "REJECT", "ABORT"})


class A2ATClientInterceptor:
    """Client-side auto-tracing interceptor (duck-typed ClientCallInterceptor)."""

    def __init__(self, *, config: A2ATObservabilityConfig | None = None) -> None:
        self._config = config or A2ATObservabilityConfig()
        self._metrics = A2ATMetricsRecorder()
        ensure_processor_registered()

    async def before(self, args: Any) -> None:
        try:
            method = str(_get(args, "method", ""))
            input_obj = _get(args, "input")
            context = _get(args, "context")
            service_parameters = _get(context, "service_parameters") if context is not None else None
            attributes = extract_request_attributes(input_obj, method=method)
            message = _get(input_obj, "message")
            metadata = normalize_metadata(_get(message, "metadata")) if message is not None else {}
            view = metadata_view_for_provider(metadata, None)
            task_type = self._config.invoke_task_type_provider(view)
            if task_type:
                attributes[ATTR_TASK_TYPE] = task_type
            topic = self._config.invoke_notification_topic_provider(view)
            if topic:
                attributes[ATTR_NOTIFICATION_TOPIC] = topic
            authorization = self._config.invoke_authorization_provider(view)
            if authorization:
                if "policy_id" in authorization:
                    attributes[ATTR_AUTHORIZATION_POLICY_ID] = authorization["policy_id"]
                if "operation_type" in authorization:
                    attributes[ATTR_AUTHORIZATION_OPERATION_TYPE] = authorization["operation_type"]
                if "risk_level" in authorization:
                    attributes[ATTR_AUTHORIZATION_OPERATION_RISK_LEVEL] = authorization["risk_level"]
            stash = ClientRequestStash(
                attributes=attributes,
                method=method,
                service_parameters=service_parameters,
                start_time=time.monotonic(),
            )
            set_request_stash(stash)
            self._record_negotiation_terminal(attributes)
            log_event(
                "negotiation.message" if ATTR_NEGOTIATION_ID in attributes else "task.request",
                logging.DEBUG,
                fields=dict(attributes),
                payload=str(metadata) if metadata else None,
                config=self._config,
            )
        except Exception:  # noqa: BLE001
            logger.warning("A2ATClientInterceptor.before failed", exc_info=True)

    async def after(self, args: Any) -> None:
        try:
            stash = get_request_stash()
            if stash is None or stash.method != str(_get(args, "method", "")):
                return
            inner = unwrap_stream_response(_get(args, "result"))
            if inner is None:
                self._finish_request(stash, streaming=False)
                return
            info = classify_event(inner)
            self._emit_event_span(stash, info)
            self._log_event(info)
            if info.is_terminal or info.final:
                self._finish_request(stash, streaming=True)
        except Exception:  # noqa: BLE001
            logger.warning("A2ATClientInterceptor.after failed", exc_info=True)

    def _emit_event_span(self, stash: ClientRequestStash, info: EventInfo) -> None:
        if not _otel_compat.is_enabled():
            return
        attributes: dict[str, str] = {ATTR_STREAMING_EVENT_KIND: info.kind}
        if info.task_id:
            attributes[ATTR_TASK_ID] = info.task_id
        if info.task_status:
            attributes[ATTR_TASK_STATUS] = info.task_status
        context = None
        if stash.span_context is not None and _otel_compat.NonRecordingSpan is not None:
            from opentelemetry.trace import set_span_in_context

            context = set_span_in_context(_otel_compat.NonRecordingSpan(stash.span_context))
        client_kind = getattr(_otel_compat.SpanKind, "CLIENT", None) if _otel_compat.SpanKind else None
        with _otel_compat.get_tracer().start_as_current_span(
            f"a2at.event.{info.kind}", context=context, kind=client_kind, attributes=attributes
        ):
            pass

    def _log_event(self, info: EventInfo) -> None:
        fields: dict[str, object] = {"streaming.event.kind": info.kind}
        if info.task_id:
            fields["task.id"] = info.task_id
        if info.task_status:
            fields["task.status"] = info.task_status
        if info.kind == "artifact":
            event, level = "task.artifact", logging.INFO
        elif info.kind == "message":
            event, level = "task.request", logging.DEBUG
        else:
            event, level = "task.status_changed", logging.INFO
        log_event(event, level, fields=fields, config=self._config)

    def _finish_request(self, stash: ClientRequestStash, *, streaming: bool) -> None:
        try:
            if self._config.record_metrics:
                duration = time.monotonic() - stash.start_time
                attributes: dict[str, str] = {ATTR_A2AT_SPAN_SIDE: "client", ATTR_STREAMING: str(streaming)}
                extension = stash.attributes.get(ATTR_EXTENSION_NAME)
                if isinstance(extension, str):
                    attributes[ATTR_EXTENSION_NAME] = extension
                self._metrics.gen_ai_operation_duration(
                    duration, operation=stash.method, attributes=attributes
                )
                self._metrics.task_request_duration(duration, attributes=attributes)
        finally:
            clear_request_stash()

    def _record_negotiation_terminal(self, attributes: dict[str, Any]) -> None:
        performative = attributes.get(ATTR_NEGOTIATION_PERFORMATIVE)
        if performative not in _TERMINAL_PERFORMATIVES or not self._config.record_metrics:
            return
        negotiation_id = attributes.get(ATTR_NEGOTIATION_ID)
        round_value = attributes.get(ATTR_NEGOTIATION_ROUND)
        if isinstance(negotiation_id, str) and isinstance(round_value, int):
            self._metrics.negotiation_total_rounds(
                round_value,
                negotiation_id=negotiation_id,
                outcome=str(performative).lower(),
                attributes={ATTR_EXTENSION_NAME: "Negotiation-T"},
            )


def _get(obj: Any, name: str, default: Any = None) -> Any:
    try:
        value = getattr(obj, name)
    except Exception:  # noqa: BLE001
        return default
    return default if value is None else value
```

注意 `ATTR_STREAMING` 值：Histogram 属性统一用 str（"true"/"false"），与 OTel 属性标量约定一致（`str(streaming)`）。

- [ ] **Step 4: 运行测试确认通过**

Run: `uv run pytest tests/observability/test_client_interceptor.py -v`
Expected: 4 PASS

- [ ] **Step 5: lint + 提交**

```bash
uv run ruff check src/a2a_t/observability tests/observability
uv run mypy src
git add src/a2a_t/observability/client/interceptor.py tests/observability/test_client_interceptor.py
git commit -m "feat(observability): client interceptor with per-event spans and metrics"
```

---

### Task 11: `server/middleware.py` —— `A2ATTraceContextMiddleware`

**Files:**
- Create: `src/a2a_t/observability/server/middleware.py`
- Test: `tests/observability/test_middleware.py`

**Interfaces:**
- Consumes: Task 4 `extract_trace_context`、Task 1 `_otel_compat.otel_context`
- Produces: `class A2ATTraceContextMiddleware`（`__init__(self, app: Any)`；`async __call__(self, scope: Any, receive: Any, send: Any) -> None`——纯 ASGI，零新增 Span）

- [ ] **Step 1: 写失败测试**

`tests/observability/test_middleware.py`：

```python
from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from a2a_t.observability.propagation import inject_traceparent
from a2a_t.observability.server.middleware import A2ATTraceContextMiddleware


@pytest.fixture()
def exporter() -> InMemorySpanExporter:
    exporter = InMemorySpanExporter()
    trace.set_tracer_provider(TracerProvider())
    trace.get_tracer_provider().add_span_processor(SimpleSpanProcessor(exporter))
    return exporter


def _run(app: Any, headers: list[tuple[bytes, bytes]]) -> Any:
    scope = {"type": "http", "headers": headers, "method": "GET", "path": "/"}
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    import asyncio

    asyncio.run(app(scope, receive, send))
    return sent


def test_middleware_continues_trace(exporter: InMemorySpanExporter) -> None:
    headers: dict[str, str] = {}
    with trace.get_tracer("t").start_as_current_span("client-span") as client_span:
        inject_traceparent(headers)

    async def inner_app(scope: Any, receive: Any, send: Any) -> None:
        with trace.get_tracer("t").start_as_current_span("server-span"):
            pass

    app = A2ATTraceContextMiddleware(inner_app)
    _run(app, [(k.encode(), v.encode()) for k, v in headers.items()])
    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert spans["server-span"].parent is not None
    assert spans["server-span"].parent.span_id == client_span.get_span_context().span_id


def test_middleware_no_traceparent_passthrough(exporter: InMemorySpanExporter) -> None:
    called = False

    async def inner_app(scope: Any, receive: Any, send: Any) -> None:
        nonlocal called
        called = True

    _run(A2ATTraceContextMiddleware(inner_app), [])
    assert called


def test_middleware_non_http_passthrough() -> None:
    called = False

    async def inner_app(scope: Any, receive: Any, send: Any) -> None:
        nonlocal called
        called = True

    import asyncio

    async def run() -> None:
        middleware = A2ATTraceContextMiddleware(inner_app)
        await middleware({"type": "lifespan"}, None, None)  # type: ignore[arg-type]

    asyncio.run(run())
    assert called


def test_middleware_exception_still_detaches(exporter: InMemorySpanExporter) -> None:
    async def inner_app(scope: Any, receive: Any, send: Any) -> None:
        raise RuntimeError("app failed")

    with pytest.raises(RuntimeError):
        _run(A2ATTraceContextMiddleware(inner_app), [])
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/observability/test_middleware.py -v`
Expected: FAIL（ModuleNotFoundError）

- [ ] **Step 3: 实现**

`src/a2a_t/observability/server/middleware.py`：

```python
"""A2ATTraceContextMiddleware: pure ASGI middleware extracting traceparent at the HTTP edge.

Creates NO span: it only attaches the extracted W3C context so that protocol spans created
later (e.g. the a2a-python handler SERVER span) parent to the client's span. Pure ASGI
(same task as the app), so contextvars propagate through Starlette/FastAPI routing.
"""

from __future__ import annotations

import logging
from typing import Any

from a2a_t.observability import _otel_compat
from a2a_t.observability.propagation import extract_trace_context

logger = logging.getLogger("a2at.observability")


class A2ATTraceContextMiddleware:
    """Extract and activate the W3C traceparent for one ASGI request; zero new spans."""

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        try:
            headers = {
                key.decode("latin-1").lower(): value.decode("latin-1")
                for key, value in scope.get("headers", [])
            }
            context = extract_trace_context(headers)
        except Exception:  # noqa: BLE001
            context = None
        if context is None or _otel_compat.otel_context is None:
            await self.app(scope, receive, send)
            return
        token = _otel_compat.otel_context.attach(context)
        try:
            await self.app(scope, receive, send)
        finally:
            _otel_compat.otel_context.detach(token)
```

- [ ] **Step 4: 运行测试确认通过**

Run: `uv run pytest tests/observability/test_middleware.py -v`
Expected: 4 PASS

- [ ] **Step 5: lint + 提交**

```bash
uv run ruff check src/a2a_t/observability/server/middleware.py tests/observability/test_middleware.py
uv run mypy src
git add src/a2a_t/observability/server/middleware.py tests/observability/test_middleware.py
git commit -m "feat(observability): ASGI tracecontext middleware for server-side continuity"
```

---

### Task 12: `server/event_queue.py` —— `A2ATEventQueueDecorator`

**Files:**
- Create: `src/a2a_t/observability/server/event_queue.py`
- Test: `tests/observability/test_event_queue.py`

**Interfaces:**
- Consumes: Task 2/3（`classify_event`、常量）、Task 8 logs、Task 1 `_otel_compat`
- Produces: `A2ATEventQueueDecorator`（`__init__(self, inner: Any, *, span_context: Any, config: A2ATObservabilityConfig | None = None, server_attributes: Mapping[str, AttributeValue] | None = None)`；`async enqueue_event(self, event: Any) -> None`；`__getattr__` 透传其余方法）

- [ ] **Step 1: 写失败测试**

`tests/observability/test_event_queue.py`：

```python
from __future__ import annotations

from collections.abc import Iterator

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from a2a_t.observability.attributes import ATTR_EXTENSION_NAME, ATTR_TASK_ID, ATTR_TASK_STATUS
from a2a_t.observability.server.event_queue import A2ATEventQueueDecorator

from tests.observability.stubs import FakeEventQueue, make_artifact_event, make_status_event


@pytest.fixture()
def exporter() -> Iterator[InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    trace.set_tracer_provider(TracerProvider())
    trace.get_tracer_provider().add_span_processor(SimpleSpanProcessor(exporter))
    return exporter


async def test_enqueue_creates_child_span_of_server_span(exporter: InMemorySpanExporter) -> None:
    with trace.get_tracer("a2a-python-sdk").start_as_current_span("DefaultRequestHandler.on_message_send") as server:
        queue = FakeEventQueue()
        decorator = A2ATEventQueueDecorator(
            queue, span_context=server.get_span_context(), server_attributes={ATTR_EXTENSION_NAME: "Task-T"}
        )
        await decorator.enqueue_event(make_status_event("T1", "TASK_STATE_WORKING"))
        await decorator.enqueue_event(make_artifact_event("T1"))
        assert len(queue.events) == 2

    spans = {s.name: s for s in exporter.get_finished_spans()}
    status_span = spans["a2at.event.status"]
    assert status_span.attributes[ATTR_TASK_ID] == "T1"
    assert status_span.attributes[ATTR_TASK_STATUS] == "working"
    assert status_span.attributes[ATTR_EXTENSION_NAME] == "Task-T"
    assert status_span.parent is not None
    assert status_span.parent.span_id == server.get_span_context().span_id
    assert "a2at.event.artifact" in spans


async def test_enqueue_after_server_span_ended(exporter: InMemorySpanExporter) -> None:
    with trace.get_tracer("t").start_as_current_span("server") as server:
        span_context = server.get_span_context()
    # execute() 返回后事件仍可挂靠已结束父 Span
    decorator = A2ATEventQueueDecorator(FakeEventQueue(), span_context=span_context)
    await decorator.enqueue_event(make_status_event("T2", "TASK_STATE_WORKING"))
    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert spans["a2at.event.status"].parent is not None


async def test_enqueue_inner_failure_propagates(exporter: InMemorySpanExporter) -> None:
    class _BrokenQueue:
        async def enqueue_event(self, event: object) -> None:
            raise RuntimeError("queue broken")

    with trace.get_tracer("t").start_as_current_span("server") as server:
        decorator = A2ATEventQueueDecorator(_BrokenQueue(), span_context=server.get_span_context())
        with pytest.raises(RuntimeError):
            await decorator.enqueue_event(make_status_event("T3", "TASK_STATE_WORKING"))


def test_getattr_delegates_other_methods() -> None:
    class _QueueWithClose(FakeEventQueue):
        closed = False

        async def close(self) -> None:
            self.closed = True

    queue = _QueueWithClose()
    decorator = A2ATEventQueueDecorator(queue, span_context=None)
    assert decorator.events is queue.events  # __getattr__ 透传
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/observability/test_event_queue.py -v`
Expected: FAIL（ModuleNotFoundError）

- [ ] **Step 3: 实现**

`src/a2a_t/observability/server/event_queue.py`：

```python
"""A2ATEventQueueDecorator: per-event INTERNAL spans (children of the protocol SERVER span)."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from a2a_t.observability import _otel_compat
from a2a_t.observability.attributes import (
    ATTR_EXTENSION_NAME,
    ATTR_STREAMING_EVENT_KIND,
    ATTR_TASK_ID,
    ATTR_TASK_STATUS,
    AttributeValue,
    EventInfo,
    classify_event,
)
from a2a_t.observability.config import A2ATObservabilityConfig
from a2a_t.observability.logs import log_event

logger = logging.getLogger("a2at.observability")


class A2ATEventQueueDecorator:
    """Wraps an EventQueue structurally; creates one INTERNAL span per enqueued event."""

    def __init__(
        self,
        inner: Any,
        *,
        span_context: Any,
        config: A2ATObservabilityConfig | None = None,
        server_attributes: Mapping[str, AttributeValue] | None = None,
    ) -> None:
        self._inner = inner
        self._span_context = span_context
        self._config = config or A2ATObservabilityConfig()
        self._server_attributes = dict(server_attributes) if server_attributes else {}

    async def enqueue_event(self, event: Any) -> None:
        try:
            info = classify_event(event)
        except Exception:  # noqa: BLE001
            info = EventInfo(kind="unknown")
        attributes: dict[str, AttributeValue] = {ATTR_STREAMING_EVENT_KIND: info.kind}
        if info.task_id:
            attributes[ATTR_TASK_ID] = info.task_id
        if info.task_status:
            attributes[ATTR_TASK_STATUS] = info.task_status
        if ATTR_EXTENSION_NAME in self._server_attributes:
            attributes[ATTR_EXTENSION_NAME] = self._server_attributes[ATTR_EXTENSION_NAME]
        context = None
        if self._span_context is not None and _otel_compat.is_enabled():
            if _otel_compat.NonRecordingSpan is not None:
                from opentelemetry.trace import set_span_in_context

                context = set_span_in_context(_otel_compat.NonRecordingSpan(self._span_context))
        try:
            with _otel_compat.get_tracer().start_as_current_span(
                f"a2at.event.{info.kind}", context=context, attributes=attributes
            ):
                await self._inner.enqueue_event(event)
        except Exception:
            raise  # 业务异常必须传播（spec §8.2：放行原调用）
        finally:
            self._log_event(info)

    def _log_event(self, info: EventInfo) -> None:
        try:
            fields: dict[str, object] = {"streaming.event.kind": info.kind}
            if info.task_id:
                fields["task.id"] = info.task_id
            if info.task_status:
                fields["task.status"] = info.task_status
            if info.kind == "artifact":
                event, level = "task.artifact", 20  # logging.INFO
            else:
                event, level = "task.status_changed", 20
            log_event(event, level, fields=fields, config=self._config)
        except Exception:  # noqa: BLE001
            logger.debug("event log failed", exc_info=True)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)
```

注意：`enqueue_event` 中 except-raise 结构仅为显式标注"业务异常传播"；可简化为 `with ...: await self._inner.enqueue_event(event)` + finally 记日志（with 退出时 Span 自动 end/记异常）。实现时优先简化版：

```python
        try:
            with _otel_compat.get_tracer().start_as_current_span(
                f"a2at.event.{info.kind}", context=context, attributes=attributes
            ):
                await self._inner.enqueue_event(event)
        finally:
            self._log_event(info)
```

- [ ] **Step 4: 运行测试确认通过**

Run: `uv run pytest tests/observability/test_event_queue.py -v`
Expected: 4 PASS

- [ ] **Step 5: lint + 提交**

```bash
uv run ruff check src/a2a_t/observability/server/event_queue.py tests/observability/test_event_queue.py
uv run mypy src
git add src/a2a_t/observability/server/event_queue.py tests/observability/test_event_queue.py
git commit -m "feat(observability): server-side per-event span via event queue decorator"
```

---

### Task 13: `server/executor.py` —— `A2ATAgentExecutorDecorator`

**Files:**
- Create: `src/a2a_t/observability/server/executor.py`
- Test: `tests/observability/test_executor.py`

**Interfaces:**
- Consumes: Task 2/3 提取、Task 5 config、Task 7 metrics、Task 8 logs、Task 12 `A2ATEventQueueDecorator`、Task 1 `_otel_compat`
- Produces:
  - `class A2ATAgentExecutorDecorator`（`__init__(self, executor: Any, *, config: A2ATObservabilityConfig | None = None)`；`async execute(self, context: Any, event_queue: Any) -> None`；`async cancel(self, context: Any, event_queue: Any) -> None`；`__getattr__` 透传）
  - `observed_executor(executor: Any, *, config: A2ATObservabilityConfig | None = None) -> A2ATAgentExecutorDecorator`

- [ ] **Step 1: 写失败测试**

`tests/observability/test_executor.py`：

```python
from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricsReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from a2a_t.observability.attributes import (
    ATTR_EXTENSION_NAME,
    ATTR_GEN_AI_OPERATION_NAME,
    ATTR_TASK_ID,
)
from a2a_t.observability.server.executor import A2ATAgentExecutorDecorator, observed_executor

from tests.observability.stubs import (
    FakeEventQueue,
    FakeMessage,
    FakeRequestContext,
    make_status_event,
)

_TASK_T = "https://projects.tmforum.org/a2aproject/telecommunication/extensions/Task-T/v1"
_TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"


@pytest.fixture()
def setup() -> Iterator[tuple[InMemorySpanExporter, InMemoryMetricsReader]]:
    exporter = InMemorySpanExporter()
    trace.set_tracer_provider(TracerProvider())
    trace.get_tracer_provider().add_span_processor(SimpleSpanProcessor(exporter))
    reader = InMemoryMetricsReader()
    metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))
    return exporter, reader


class _EchoExecutor:
    def __init__(self) -> None:
        self.wrapped_queue: Any = None
        self.received_context: Any = None

    async def execute(self, context: Any, event_queue: Any) -> None:
        self.wrapped_queue = event_queue
        self.received_context = context
        await event_queue.enqueue_event(make_status_event("T1", "TASK_STATE_WORKING"))

    async def cancel(self, context: Any, event_queue: Any) -> None: ...


async def test_execute_enriches_current_protocol_span(
    setup: tuple[InMemorySpanExporter, InMemoryMetricsReader],
) -> None:
    exporter, reader = setup
    # 模拟协议层：@trace_class 创建的 SERVER Span 在 execute 期间为 current
    with trace.get_tracer("a2a-python-sdk").start_as_current_span(
        "DefaultRequestHandler.on_message_send"
    ) as server_span:
        inner = _EchoExecutor()
        decorator = observed_executor(inner)
        request_context = FakeRequestContext(
            FakeMessage(metadata={_TASK_T: "prompt"}, task_id="T1", context_id="C1"),
            headers={"traceparent": _TRACEPARENT},
        )
        await decorator.execute(request_context, FakeEventQueue())
        assert inner.wrapped_queue is not None  # executor 收到包装后的队列

    server_attrs = server_span.attributes or {}
    assert server_attrs[ATTR_EXTENSION_NAME] == "Task-T"
    assert server_attrs[ATTR_TASK_ID] == "T1"
    assert server_attrs[ATTR_GEN_AI_OPERATION_NAME] == "execute"

    # per-event Span 是协议 SERVER Span 的子节点
    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert "a2at.event.status" in spans
    assert spans["a2at.event.status"].parent is not None
    assert spans["a2at.event.status"].parent.span_id == server_span.context.span_id

    # state["a2at.span_context"] 已写入
    assert request_context.call_context.state["a2at.span_context"] is not None

    reader.collect()
    data = reader.get_metrics_data()
    points = [
        p
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
        if m.name == "a2at.task.request.duration"
        for p in m.data.data_points
    ]
    assert points[-1].attributes["a2at.span.side"] == "server"


async def test_execute_without_current_span_still_wraps_queue() -> None:
    inner = _EchoExecutor()
    decorator = A2ATAgentExecutorDecorator(inner)
    await decorator.execute(
        FakeRequestContext(FakeMessage(metadata=None), headers={}), FakeEventQueue()
    )
    assert inner.wrapped_queue is not None


async def test_execute_missing_traceparent_warns_once(caplog: pytest.LogCaptureFixture) -> None:
    import logging

    decorator = A2ATAgentExecutorDecorator(_EchoExecutor())
    with caplog.at_level(logging.WARNING, logger="a2at.observability"):
        await decorator.execute(FakeRequestContext(FakeMessage(metadata=None), headers={}), FakeEventQueue())
        await decorator.execute(FakeRequestContext(FakeMessage(metadata=None), headers={}), FakeEventQueue())
    warnings = [r for r in caplog.records if "traceparent" in r.getMessage()]
    assert len(warnings) == 1  # 一次性 WARNING


async def test_execute_inner_exception_propagates(
    setup: tuple[InMemorySpanExporter, InMemoryMetricsReader],
) -> None:
    exporter, reader = setup

    class _Boom:
        async def execute(self, context: Any, event_queue: Any) -> None:
            raise RuntimeError("executor failed")

        async def cancel(self, context: Any, event_queue: Any) -> None: ...

    decorator = A2ATAgentExecutorDecorator(_Boom())
    with pytest.raises(RuntimeError):
        await decorator.execute(FakeRequestContext(FakeMessage(metadata=None)), FakeEventQueue())
    reader.collect()
    data = reader.get_metrics_data()
    points = [
        p
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
        if m.name == "a2at.task.request.duration"
        for p in m.data.data_points
    ]
    assert points  # finally 路径仍记录指标


async def test_cancel_passthrough() -> None:
    cancelled: list[Any] = []

    class _CancelExecutor:
        async def execute(self, context: Any, event_queue: Any) -> None: ...

        async def cancel(self, context: Any, event_queue: Any) -> None:
            cancelled.append(context)

    decorator = A2ATAgentExecutorDecorator(_CancelExecutor())
    ctx = FakeRequestContext(FakeMessage(metadata=None))
    await decorator.cancel(ctx, FakeEventQueue())
    assert cancelled == [ctx]
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/observability/test_executor.py -v`
Expected: FAIL（ModuleNotFoundError）

- [ ] **Step 3: 实现**

`src/a2a_t/observability/server/executor.py`：

```python
"""A2ATAgentExecutorDecorator: enrich the current protocol SERVER span, wrap the EventQueue.

execute() runs inside the a2a-python handler span (@trace_class, start_as_current_span),
so get_current_span() is the protocol SERVER span: attributes are written directly onto it
(no new request span, spec §2.3). The EventQueue is replaced with a per-event decorator.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping
from typing import Any

from a2a_t.observability import _otel_compat
from a2a_t.observability.attributes import (
    ATTR_A2AT_SPAN_SIDE,
    ATTR_EXTENSION_NAME,
    ATTR_GEN_AI_CONVERSATION_ID,
    ATTR_GEN_AI_OPERATION_NAME,
    ATTR_NEGOTIATION_ID,
    ATTR_NOTIFICATION_TOPIC,
    ATTR_TASK_ID,
    ATTR_TASK_TYPE,
    AttributeValue,
    extract_negotiation_attributes,
    extract_request_attributes,
    metadata_view_for_provider,
    normalize_metadata,
)
from a2a_t.observability.attributes import (
    ATTR_AUTHORIZATION_OPERATION_RISK_LEVEL,
    ATTR_AUTHORIZATION_OPERATION_TYPE,
    ATTR_AUTHORIZATION_POLICY_ID,
)
from a2a_t.observability.config import A2ATObservabilityConfig
from a2a_t.observability.logs import log_event
from a2a_t.observability.metrics import A2ATMetricsRecorder
from a2a_t.observability.server.event_queue import A2ATEventQueueDecorator

logger = logging.getLogger("a2at.observability")

_TRACEPARENT_WARN_KEY = "_a2at_traceparent_warned"


class A2ATAgentExecutorDecorator:
    """Wraps a user AgentExecutor structurally; enriches the protocol SERVER span."""

    def __init__(self, executor: Any, *, config: A2ATObservabilityConfig | None = None) -> None:
        self._executor = executor
        self._config = config or A2ATObservabilityConfig()
        self._metrics = A2ATMetricsRecorder()

    async def execute(self, context: Any, event_queue: Any) -> None:
        attributes = self._collect_attributes(context)
        span = _otel_compat.get_current_span()
        span_context = None
        try:
            span_context = span.get_span_context()
            recording = span_context is not None and getattr(span_context, "is_valid", False)
            if recording and attributes:
                span.set_attributes(attributes)
        except Exception:  # noqa: BLE001
            recording = False
        try:
            call_state = _safe_get(context, "call_context").state if _safe_get(context, "call_context") is not None else None
        except Exception:  # noqa: BLE001
            call_state = None
        if call_state is not None:
            call_state["a2at.span_context"] = span_context if span_context is not None else None
            self._warn_missing_traceparent(call_state)
        wrapped = A2ATEventQueueDecorator(
            event_queue, span_context=span_context, config=self._config, server_attributes=attributes
        )
        start = time.monotonic()
        try:
            await self._executor.execute(context, wrapped)
        finally:
            self._record_server_metrics(attributes, start)

    async def cancel(self, context: Any, event_queue: Any) -> None:
        await self._executor.cancel(context, event_queue)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._executor, name)

    def _collect_attributes(self, context: Any) -> dict[str, AttributeValue]:
        attributes: dict[str, AttributeValue] = {ATTR_GEN_AI_OPERATION_NAME: "execute"}
        try:
            message = _safe_get(context, "message")
            if message is not None:
                attributes.update(
                    {k: v for k, v in extract_request_attributes(context, method="execute").items() if k != ATTR_GEN_AI_OPERATION_NAME}
                )
            metadata = normalize_metadata(_safe_get(message, "metadata")) if message is not None else {}
            headers = None
            call_context = _safe_get(context, "call_context")
            if call_context is not None:
                headers = _safe_get(call_context, "state", {}).get("headers")
            view = metadata_view_for_provider(metadata, headers)
            task_type = self._config.invoke_task_type_provider(view)
            if task_type:
                attributes[ATTR_TASK_TYPE] = task_type
            topic = self._config.invoke_notification_topic_provider(view)
            if topic:
                attributes[ATTR_NOTIFICATION_TOPIC] = topic
            authorization = self._config.invoke_authorization_provider(view)
            if authorization:
                if "policy_id" in authorization:
                    attributes[ATTR_AUTHORIZATION_POLICY_ID] = authorization["policy_id"]
                if "operation_type" in authorization:
                    attributes[ATTR_AUTHORIZATION_OPERATION_TYPE] = authorization["operation_type"]
                if "risk_level" in authorization:
                    attributes[ATTR_AUTHORIZATION_OPERATION_RISK_LEVEL] = authorization["risk_level"]
            if ATTR_NEGOTIATION_ID in attributes:
                log_event(
                    "negotiation.message",
                    logging.DEBUG,
                    fields={k: v for k, v in attributes.items() if k.startswith("negotiation.")},
                    payload=str(metadata) if metadata else None,
                    config=self._config,
                )
            else:
                log_event(
                    "task.request",
                    logging.DEBUG,
                    fields={k: v for k, v in attributes.items() if k != "payload"},
                    payload=str(metadata) if metadata else None,
                    config=self._config,
                )
        except Exception:  # noqa: BLE001
            logger.warning("executor attribute collection failed", exc_info=True)
        return attributes

    def _warn_missing_traceparent(self, call_state: dict[str, Any]) -> None:
        if getattr(self._config, _TRACEPARENT_WARN_KEY, False):
            return
        headers = call_state.get("headers")
        if headers is None:
            return
        if any(str(k).lower() == "traceparent" for k in headers):
            return
        object.__setattr__(self._config, _TRACEPARENT_WARN_KEY, True)
        logger.warning(
            "no traceparent header on the request; the protocol SERVER span stays a local "
            "root. Add A2ATTraceContextMiddleware (or OTel ASGI instrumentation) for "
            "cross-agent trace continuity."
        )

    def _record_server_metrics(self, attributes: dict[str, AttributeValue], start: float) -> None:
        if not self._config.record_metrics:
            return
        try:
            duration = time.monotonic() - start
            metric_attrs: dict[str, AttributeValue] = {ATTR_A2AT_SPAN_SIDE: "server"}
            if ATTR_EXTENSION_NAME in attributes:
                metric_attrs[ATTR_EXTENSION_NAME] = attributes[ATTR_EXTENSION_NAME]
            self._metrics.task_request_duration(duration, attributes=metric_attrs)
        except Exception:  # noqa: BLE001
            logger.warning("server metric recording failed", exc_info=True)


def observed_executor(
    executor: Any, *, config: A2ATObservabilityConfig | None = None
) -> A2ATAgentExecutorDecorator:
    """Factory of A2ATAgentExecutorDecorator (spec §5.1)."""
    return A2ATAgentExecutorDecorator(executor, config=config)


def _safe_get(obj: Any, name: str, default: Any = None) -> Any:
    try:
        value = getattr(obj, name)
    except Exception:  # noqa: BLE001
        return default
    return default if value is None else value
```

实现注意：
- `_warn_missing_traceparent` 中 `object.__setattr__` 用于 dataclass 动态标记（dataclass 无该字段）；更简洁做法是在 `A2ATAgentExecutorDecorator` 上持有 `self._traceparent_warned = False` 实例状态——**实现时采用实例属性方案**（删除 `_TRACEPARENT_WARN_KEY` 与 `object.__setattr__`，改用 `self._traceparent_warned`）。
- `_collect_attributes` 里对 `context` 复用 `extract_request_attributes(context, method="execute")`：其内部读取 `context.message`（RequestContext 有 `.message` 属性，协议一致）。

- [ ] **Step 4: 运行测试确认通过**

Run: `uv run pytest tests/observability/test_executor.py -v`
Expected: 5 PASS

- [ ] **Step 5: lint + 提交**

```bash
uv run ruff check src/a2a_t/observability tests/observability
uv run mypy src
git add src/a2a_t/observability/server/executor.py tests/observability/test_executor.py
git commit -m "feat(observability): executor decorator enriching protocol server span"
```

---

### Task 14: `sdk_trace.py` —— `trace_facade`

**Files:**
- Create: `src/a2a_t/observability/sdk_trace.py`
- Test: `tests/observability/test_sdk_trace.py`

**Interfaces:**
- Consumes: Task 1 `_otel_compat`
- Produces: `trace_facade(obj: Any, *, role: str | None = None, methods: Iterable[str] | None = None) -> Any`（实例级 setattr 包装；协程方法 async 包装器 await 后再结束 Span；Span 名 `a2at.sdk.{role}.{method}`，INTERNAL，零自定义属性；role 猜测：类名含 "Client"→client、含 "Server"→server、否则 "custom"，不 import a2a_t.client/server）

- [ ] **Step 1: 写失败测试**

`tests/observability/test_sdk_trace.py`：

```python
from __future__ import annotations

import inspect
from collections.abc import Iterator
from typing import Any

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from a2a_t.observability.sdk_trace import trace_facade


@pytest.fixture()
def exporter() -> Iterator[InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    trace.set_tracer_provider(TracerProvider())
    trace.get_tracer_provider().add_span_processor(SimpleSpanProcessor(exporter))
    return exporter


class _SyncFacade:
    def generate_task_prompt(self, text: str) -> str:
        return "processed:" + text

    def _private(self) -> str:
        return "private"


class _AsyncFacade:
    async def generate_task_prompt(self, text: str) -> str:
        return "processed:" + text


def test_wraps_sync_methods_with_span(exporter: InMemorySpanExporter) -> None:
    facade = trace_facade(_SyncFacade())
    assert facade.generate_task_prompt("hi") == "processed:hi"
    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert "a2at.sdk.custom.generate_task_prompt" in spans
    assert spans["a2at.sdk.custom.generate_task_prompt"].attributes == {}  # 零自定义属性
    assert "_private" not in spans


def test_role_guessing_and_override(exporter: InMemorySpanExporter) -> None:
    class ClientLike:
        def run(self) -> None: ...

    trace_facade(ClientLike()).run()
    class ServerLike:
        def run(self) -> None: ...

    trace_facade(ServerLike()).run()
    class Plain:
        def run(self) -> None: ...

    trace_facade(Plain(), role="orchestrator").run()
    names = {s.name for s in exporter.get_finished_spans()}
    assert "a2at.sdk.client.run" in names
    assert "a2at.sdk.server.run" in names
    assert "a2at.sdk.orchestrator.run" in names


async def test_wraps_async_methods_awaited(exporter: InMemorySpanExporter) -> None:
    facade = trace_facade(_AsyncFacade())
    assert inspect.iscoroutinefunction(facade.generate_task_prompt)
    result = await facade.generate_task_prompt("hi")
    assert result == "processed:hi"
    assert "a2at.sdk.custom.generate_task_prompt" in {s.name for s in exporter.get_finished_spans()}


def test_exception_recorded_and_raised(exporter: InMemorySpanExporter) -> None:
    class _Boom:
        def run(self) -> None:
            raise ValueError("boom")

    with pytest.raises(ValueError):
        trace_facade(_Boom()).run()
    from opentelemetry.trace import StatusCode

    span = exporter.get_finished_spans()[-1]
    assert span.name == "a2at.sdk.custom.run"
    assert span.status.status_code == StatusCode.ERROR


def test_methods_filter_and_original_preserved(exporter: InMemorySpanExporter) -> None:
    facade = trace_facade(_SyncFacade(), methods=["generate_task_prompt"])
    assert facade.generate_task_prompt.__name__ == "generate_task_prompt"  # functools.wraps
    # 类未被修改（实例级包装）
    assert "_SyncFacade_wrapped" not in str(type(facade))
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/observability/test_sdk_trace.py -v`
Expected: FAIL（ModuleNotFoundError）

- [ ] **Step 3: 实现**

`src/a2a_t/observability/sdk_trace.py`：

```python
"""trace_facade: opt-in instance-level wrapping of SDK/business facades (L4 spans).

Wraps public methods on the INSTANCE (setattr on the object, not the class) so neither the
class definition nor the SDK source is modified. Coroutine functions get an async wrapper
(the span ends only after await); sync functions a plain wrapper. Spans carry no custom
attributes by design (spec §3.2).
"""

from __future__ import annotations

import functools
import inspect
from collections.abc import Iterable
from typing import Any

from a2a_t.observability import _otel_compat


def _guess_role(obj: Any) -> str:
    class_name = type(obj).__name__
    if "Client" in class_name:
        return "client"
    if "Server" in class_name:
        return "server"
    return "custom"


def _wrap_sync(name: str, role: str, original: Any) -> Any:
    @functools.wraps(original)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        tracer = _otel_compat.get_tracer()
        with tracer.start_as_current_span(f"a2at.sdk.{role}.{name}"):
            return original(*args, **kwargs)

    return wrapper


def _wrap_async(name: str, role: str, original: Any) -> Any:
    @functools.wraps(original)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        tracer = _otel_compat.get_tracer()
        with tracer.start_as_current_span(f"a2at.sdk.{role}.{name}"):
            return await original(*args, **kwargs)

    return wrapper


def trace_facade(obj: Any, *, role: str | None = None, methods: Iterable[str] | None = None) -> Any:
    """Wrap public methods of obj with a2at.sdk.{role}.{method} spans; returns obj."""
    resolved_role = role or _guess_role(obj)
    cls = type(obj)
    if methods is None:
        names = [
            name
            for name in dir(cls)
            if not name.startswith("_") and callable(getattr(cls, name))
        ]
    else:
        names = list(methods)
    for name in names:
        original = getattr(obj, name)
        if inspect.iscoroutinefunction(original):
            setattr(obj, name, _wrap_async(name, resolved_role, original))
        elif callable(original):
            setattr(obj, name, _wrap_sync(name, resolved_role, original))
    return obj
```

- [ ] **Step 4: 运行测试确认通过**

Run: `uv run pytest tests/observability/test_sdk_trace.py -v`
Expected: 6 PASS

- [ ] **Step 5: lint + 提交**

```bash
uv run ruff check src/a2a_t/observability/sdk_trace.py tests/observability/test_sdk_trace.py
uv run mypy src
git add src/a2a_t/observability/sdk_trace.py tests/observability/test_sdk_trace.py
git commit -m "feat(observability): opt-in trace_facade for SDK/business facades"
```

---

### Task 15: `__init__.py` 公共导出 + 全量检查

**Files:**
- Modify: `src/a2a_t/observability/__init__.py`
- Test: `tests/observability/test_public_api.py`

**Interfaces:**
- Consumes: Task 2–14 全部公开符号
- Produces: `a2a_t.observability` 导出——`A2ATSpan`、`a2at_attribute_extractor`、`trace_facade`、`A2ATMetricsRecorder`、`inject_traceparent`、`extract_trace_context`、`A2ATClientInterceptor`、`A2ATTraceContextMiddleware`、`A2ATAgentExecutorDecorator`、`observed_executor`、`A2ATEventQueueDecorator`、`A2ATObservabilityConfig`、`TRACEPARENT_HEADER`、全部属性名常量

- [ ] **Step 1: 写失败测试**

`tests/observability/test_public_api.py`：

```python
from __future__ import annotations

import inspect


def test_public_api_exports() -> None:
    import a2a_t.observability as obs

    expected = {
        "A2ATSpan",
        "a2at_attribute_extractor",
        "trace_facade",
        "A2ATMetricsRecorder",
        "inject_traceparent",
        "extract_trace_context",
        "TRACEPARENT_HEADER",
        "A2ATClientInterceptor",
        "A2ATTraceContextMiddleware",
        "A2ATAgentExecutorDecorator",
        "observed_executor",
        "A2ATEventQueueDecorator",
        "A2ATObservabilityConfig",
        "ATTR_EXTENSION_NAME",
        "ATTR_TASK_ID",
        "ATTR_TASK_STATUS",
        "ATTR_TASK_TYPE",
        "ATTR_NEGOTIATION_ID",
        "ATTR_NEGOTIATION_ROUND",
        "ATTR_NEGOTIATION_MAX_ROUNDS",
        "ATTR_NEGOTIATION_PERFORMATIVE",
        "ATTR_NEGOTIATION_TOTAL_ROUNDS",
        "ATTR_NOTIFICATION_TOPIC",
        "ATTR_STREAMING_EVENT_KIND",
        "ATTR_PUSH_NOTIFICATION_URL",
        "ATTR_AUTHORIZATION_POLICY_ID",
        "ATTR_AUTHORIZATION_OPERATION_TYPE",
        "ATTR_AUTHORIZATION_OPERATION_RISK_LEVEL",
        "ATTR_GEN_AI_OPERATION_NAME",
        "ATTR_GEN_AI_CONVERSATION_ID",
        "ATTR_A2AT_SPAN_SIDE",
        "ATTR_STREAMING",
    }
    for name in expected:
        assert hasattr(obs, name), f"missing export: {name}"
        assert name in obs.__all__, f"missing from __all__: {name}"


def test_no_a2a_import_in_observability_package() -> None:
    import ast
    from pathlib import Path

    package_dir = Path("src/a2a_t/observability")
    for path in package_dir.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                assert all(not alias.name.startswith("a2a") or alias.name.startswith("a2a_t") for alias in node.names), f"a2a import in {path}"
            elif isinstance(node, ast.ImportFrom):
                assert not (node.module or "").startswith("a2a.") or False, f"a2a.* import in {path}"
                assert (node.module or "").startswith("a2a_t") or not (node.module or "").startswith("a2a"), f"illegal a2a import in {path}"
```

- [ ] **Step 2: 运行确认失败**

Run: `uv run pytest tests/observability/test_public_api.py -v`
Expected: FAIL（导出缺失）

- [ ] **Step 3: 实现**

`src/a2a_t/observability/__init__.py` 完整替换为：

```python
"""A2A-T observability module.

Design spec: docs/superpowers/specs/2026-09-11-a2at-observability-sdk-design.md (v2.0).
Protocol spans are the single request-span source; this module injects A2A-T attributes
into them and creates only per-event extension spans. Never imports a2a-python at runtime.
"""

from __future__ import annotations

from a2a_t.observability.attributes import (
    ATTR_A2AT_SPAN_SIDE,
    ATTR_AUTHORIZATION_OPERATION_RISK_LEVEL,
    ATTR_AUTHORIZATION_OPERATION_TYPE,
    ATTR_AUTHORIZATION_POLICY_ID,
    ATTR_EXTENSION_NAME,
    ATTR_GEN_AI_CONVERSATION_ID,
    ATTR_GEN_AI_OPERATION_NAME,
    ATTR_NEGOTIATION_ID,
    ATTR_NEGOTIATION_MAX_ROUNDS,
    ATTR_NEGOTIATION_PERFORMATIVE,
    ATTR_NEGOTIATION_ROUND,
    ATTR_NEGOTIATION_TOTAL_ROUNDS,
    ATTR_NOTIFICATION_TOPIC,
    ATTR_PUSH_NOTIFICATION_URL,
    ATTR_STREAMING,
    ATTR_STREAMING_EVENT_KIND,
    ATTR_TASK_ID,
    ATTR_TASK_STATUS,
    ATTR_TASK_TYPE,
    a2at_attribute_extractor,
)
from a2a_t.observability.client.interceptor import A2ATClientInterceptor
from a2a_t.observability.config import A2ATObservabilityConfig
from a2a_t.observability.metrics import A2ATMetricsRecorder
from a2a_t.observability.propagation import (
    TRACEPARENT_HEADER,
    extract_trace_context,
    inject_traceparent,
)
from a2a_t.observability.sdk_trace import trace_facade
from a2a_t.observability.server.event_queue import A2ATEventQueueDecorator
from a2a_t.observability.server.executor import A2ATAgentExecutorDecorator, observed_executor
from a2a_t.observability.server.middleware import A2ATTraceContextMiddleware
from a2a_t.observability.span import A2ATSpan

__all__ = [
    "A2ATClientInterceptor",
    "A2ATEventQueueDecorator",
    "A2ATMetricsRecorder",
    "A2ATObservabilityConfig",
    "A2ATSpan",
    "A2ATAgentExecutorDecorator",
    "A2ATTraceContextMiddleware",
    "ATTR_A2AT_SPAN_SIDE",
    "ATTR_AUTHORIZATION_OPERATION_RISK_LEVEL",
    "ATTR_AUTHORIZATION_OPERATION_TYPE",
    "ATTR_AUTHORIZATION_POLICY_ID",
    "ATTR_EXTENSION_NAME",
    "ATTR_GEN_AI_CONVERSATION_ID",
    "ATTR_GEN_AI_OPERATION_NAME",
    "ATTR_NEGOTIATION_ID",
    "ATTR_NEGOTIATION_MAX_ROUNDS",
    "ATTR_NEGOTIATION_PERFORMATIVE",
    "ATTR_NEGOTIATION_ROUND",
    "ATTR_NEGOTIATION_TOTAL_ROUNDS",
    "ATTR_NOTIFICATION_TOPIC",
    "ATTR_PUSH_NOTIFICATION_URL",
    "ATTR_STREAMING",
    "ATTR_STREAMING_EVENT_KIND",
    "ATTR_TASK_ID",
    "ATTR_TASK_STATUS",
    "ATTR_TASK_TYPE",
    "TRACEPARENT_HEADER",
    "a2at_attribute_extractor",
    "extract_trace_context",
    "inject_traceparent",
    "observed_executor",
    "trace_facade",
]
```

- [ ] **Step 4: 运行全量单测 + lint + mypy**

```bash
uv run pytest tests/observability -v
uv run ruff check .
uv run mypy src
```
Expected: 全部 PASS / 无告警

- [ ] **Step 5: 提交**

```bash
git add src/a2a_t/observability/__init__.py tests/observability/test_public_api.py
git commit -m "feat(observability): public API exports and no-a2a-import guard test"
```

---

### Task 16: 端到端集成验证测试

**Files:**
- Create: `tests/integration/__init__.py`（空）
- Create: `tests/integration/test_e2e_observability.py`

**Interfaces:**
- Consumes: 全部公开 API + 真实 a2a-sdk（integration 组）+ httpx ASGITransport 进程内组网
- Produces: spec §10.2 的断言 1–10（marker `a2a`，`A2AT_TEST_A2A=1` 门控）

- [ ] **Step 1: 写测试（本任务即交付物，TDD 顺序 = 先写测试再装依赖跑通）**

`tests/integration/test_e2e_observability.py`：

```python
"""End-to-end observability verification (spec §10.2, assertions 1-10).

In-process: httpx ASGITransport -> Starlette (+middleware) -> a2a-python server stack
-> observed executor; real a2a-python client + interceptor. Requires a2a-sdk
(markers: a2a; opt-in via A2AT_TEST_A2A=1).
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any

import pytest

pytestmark = [
    pytest.mark.a2a,
    pytest.mark.skipif(not os.environ.get("A2AT_TEST_A2A"), reason="set A2AT_TEST_A2A=1"),
]

_TASK_T = "https://projects.tmforum.org/a2aproject/telecommunication/extensions/Task-T/v1"
_NEG_T = "https://projects.tmforum.org/a2aproject/telecommunication/extensions/Negotiation-T/v1"


def _build_app(with_middleware: bool) -> Any:
    from starlette.applications import Starlette
    from starlette.middleware import Middleware

    from a2a.server.agent_execution.agent_executor import AgentExecutor
    from a2a.server.agent_execution.context import RequestContext
    from a2a.server.events.event_queue import EventQueue
    from a2a.server.request_handlers import DefaultRequestHandler
    from a2a.server.routes import create_agent_card_routes, create_rest_routes
    from a2a.server.tasks.inmemory_task_store import InMemoryTaskStore
    from a2a.types import (
        Artifact,
        Message,
        Role,
        TaskState,
        TaskStatus,
        TaskStatusUpdateEvent,
    )
    from a2a_t.observability import (
        A2ATTraceContextMiddleware,
        observed_executor,
    )
    from google.protobuf.json_format import ParseDict
    from google.protobuf.struct_pb2 import Value

    card_payload = {
        "name": "obs-server",
        "description": "observability e2e server",
        "version": "1.0.0",
        "supportedInterfaces": [
            {
                "protocolBinding": "http_json",
                "protocolVersion": "1.0.0",
                "url": "http://testserver",
            }
        ],
        "defaultInputModes": ["text/plain"],
        "defaultOutputModes": ["text/plain"],
        "skills": [],
    }
    agent_card = ParseDict(card_payload, __import__("a2a.types", fromlist=["AgentCard"]).AgentCard())

    class _FlowExecutor(AgentExecutor):
        def __init__(self, payload_metadata: dict[str, Any] | None = None) -> None:
            self._payload_metadata = payload_metadata or {}

        async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
            task_id = context.task_id or "T-e2e"
            context_id = context.context_id or "C-e2e"
            for state in (TaskState.TASK_STATE_SUBMITTED, TaskState.TASK_STATE_WORKING):
                await event_queue.enqueue_event(
                    TaskStatusUpdateEvent(
                        task_id=task_id,
                        context_id=context_id,
                        status=TaskStatus(state=state),
                    )
                )
            artifact = Artifact(artifact_id="A1", name="e2e.artifact")
            artifact.parts.add(data=Value(string_value="result-data"))
            await event_queue.enqueue_event(
                __import__("a2a.types", fromlist=["TaskArtifactUpdateEvent"]).TaskArtifactUpdateEvent(
                    task_id=task_id,
                    context_id=context_id,
                    artifact=artifact,
                )
            )
            await event_queue.enqueue_event(
                TaskStatusUpdateEvent(
                    task_id=task_id,
                    context_id=context_id,
                    status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
                    final=True,
                )
            )

        async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None: ...

    handler = DefaultRequestHandler(
        agent_executor=observed_executor(_FlowExecutor()),
        task_store=InMemoryTaskStore(),
        agent_card=agent_card,
    )
    middleware = [Middleware(A2ATTraceContextMiddleware)] if with_middleware else []
    return Starlette(
        routes=[*create_agent_card_routes(agent_card), *create_rest_routes(handler)],
        middleware=middleware,
    ), agent_card


async def _drive(app: Any, agent_card: Any, metadata: dict[str, Any]) -> list[Any]:
    import httpx
    from a2a.client.client import ClientCallContext, ClientConfig
    from a2a.client.client_factory import ClientFactory
    from a2a.types import Role, SendMessageRequest
    from a2a.utils.constants import TransportProtocol
    from a2a_t.observability import A2ATClientInterceptor

    httpx_client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver")
    factory = ClientFactory(
        ClientConfig(
            httpx_client=httpx_client,
            supported_protocol_bindings=[TransportProtocol.HTTP_JSON],
            use_client_preference=True,
        )
    )
    client = factory.create(agent_card, interceptors=[A2ATClientInterceptor()])
    request = SendMessageRequest()
    request.message.message_id = "m-e2e"
    request.message.role = Role.ROLE_USER
    request.message.parts.add().text = "e2e"
    for key, value in metadata.items():
        request.message.metadata[key] = value
    context = ClientCallContext(service_parameters={"A2A-Extensions": _TASK_T})
    events: list[Any] = []
    async for response in client.send_message(request, context=context):
        events.append(response)
    await httpx_client.aclose()
    return events


@pytest.fixture()
def otel_setup() -> Iterator[tuple[Any, Any]]:
    from opentelemetry import metrics, trace
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricsReader
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    exporter = InMemorySpanExporter()
    trace.set_tracer_provider(TracerProvider())
    trace.get_tracer_provider().add_span_processor(SimpleSpanProcessor(exporter))
    reader = InMemoryMetricsReader()
    metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))
    yield exporter, reader


async def test_e2e_trace_tree_and_metrics(otel_setup: tuple[Any, Any]) -> None:
    exporter, reader = otel_setup
    app, card = _build_app(with_middleware=True)
    events = await _drive(app, card, metadata={_TASK_T: "prompt-text"})
    assert events  # 流式事件已消费

    spans = exporter.get_finished_spans()
    # 1. 协议 transport CLIENT Span 存在且含 A2A-T 属性
    transport = next(s for s in spans if "RestTransport" in s.name and "send_message" in s.name)
    assert transport.attributes["extension.name"] == "Task-T"
    assert transport.attributes["gen_ai.operation.name"] == "send_message"
    # 2. 协议 SERVER handler Span 是 transport Span 的子节点（traceparent 全链路）
    handler = next(s for s in spans if "DefaultRequestHandler" in s.name and s.name.endswith("on_message_send"))
    assert handler.parent is not None
    assert handler.parent.span_id == transport.context.span_id
    assert handler.context.trace_id == transport.context.trace_id
    # 3. SERVER handler Span 含 A2A-T 属性（执行器装饰器写入）
    assert handler.attributes["extension.name"] == "Task-T"
    # 4. Server 侧 per-event Span（父=handler）
    server_events = [s for s in spans if s.name.startswith("a2at.event.")]
    assert {"a2at.event.status", "a2at.event.artifact", "a2at.event.completed"} <= {s.name for s in server_events}
    assert all(s.parent is not None and s.parent.span_id == handler.context.span_id for s in server_events)
    # 5. Client 侧 per-event Span（父=transport）
    client_events = [s for s in server_events if s.parent is not None and s.parent.span_id == transport.context.span_id]
    assert client_events
    # 6. 指标样本齐全且含 side 归因
    reader.collect()
    data = reader.get_metrics_data()
    metric_points = [
        (m.name, p)
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
        for p in m.data.data_points
    ]
    duration_points = [p for name, p in metric_points if name == "a2at.task.request.duration"]
    assert any(p.attributes.get("a2at.span.side") == "client" for p in duration_points)
    assert any(p.attributes.get("a2at.span.side") == "server" for p in duration_points)
    assert any(name == "gen_ai.client.operation.duration" for name, _ in metric_points)


async def test_e2e_negotiation_terminal_counter(otel_setup: tuple[Any, Any]) -> None:
    exporter, reader = otel_setup
    app, card = _build_app(with_middleware=True)
    from tests.observability.stubs import FakeStreamResponse  # noqa: F401 - 保持 stub 模块加载

    events = await _drive(
        app,
        card,
        metadata={
            _NEG_T: "prompt",
            "negotiationContext": {"id": "N-e2e", "round": 2, "maxRounds": 5, "performative": "ACCEPT"},
        },
    )
    reader.collect()
    data = reader.get_metrics_data()
    points = [
        p
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
        if m.name == "a2at.negotiation.total_rounds"
        for p in m.data.data_points
    ]
    assert points[-1].value == 2
    assert points[-1].attributes["negotiation.id"] == "N-e2e"
    assert points[-1].attributes["outcome"] == "accept"


async def test_e2e_without_middleware_local_root(otel_setup: tuple[Any, Any]) -> None:
    exporter, reader = otel_setup
    app, card = _build_app(with_middleware=False)
    await _drive(app, card, metadata={_TASK_T: "prompt"})
    spans = exporter.get_finished_spans()
    handler = next(s for s in spans if "DefaultRequestHandler" in s.name and s.name.endswith("on_message_send"))
    assert handler.parent is None  # 本地根
    assert handler.attributes["extension.name"] == "Task-T"  # 属性注入不受影响


async def test_e2e_master_switch_off(otel_setup: tuple[Any, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    from a2a_t.observability import _otel_compat

    monkeypatch.setattr(_otel_compat, "_ENABLED", False)
    app, card = _build_app(with_middleware=True)
    events = await _drive(app, card, metadata={_TASK_T: "prompt"})
    assert events  # 业务流不受影响
    assert exporter.get_finished_spans() == [] or all(
        "a2at" not in s.name for s in exporter.get_finished_spans()
    )
```

- [ ] **Step 2: 安装集成依赖并运行**

```bash
uv sync --dev --group integration
$env:A2AT_TEST_A2A = "1"
uv run pytest tests/integration -m a2a -v
```
Expected: 4 PASS。若断言失败按失败点修正实现（真实 a2a 结构与 stub 的偏差在此暴露——这正是契约测试的目的）。

- [ ] **Step 3: 确认默认运行时跳过**

```bash
Remove-Item Env:A2AT_TEST_A2A -ErrorAction SilentlyContinue
uv run pytest tests/integration -v
```
Expected: 4 skipped（门控生效，不破坏默认 CI）

- [ ] **Step 4: lint + 提交**

```bash
uv run ruff check tests/integration
git add tests/integration
git commit -m "test(observability): end-to-end verification against real a2a-python"
```

---

### Task 17: 样例 `a2a-t-sample/observability-sample/`

**Files:**
- Create: `a2a-t-sample/observability-sample/src/obs_sample/__init__.py`
- Create: `a2a-t-sample/observability-sample/src/obs_sample/obs_setup.py`
- Create: `a2a-t-sample/observability-sample/src/obs_sample/mock_llm.py`（从 `../subscribe-incident/src/common/mock_llm.py` 复制并调整 `_RESOURCES_DIR` 指向 `../../subscribe-incident/resources/mock_responses`）
- Create: `a2a-t-sample/observability-sample/src/obs_sample/server_main.py`
- Create: `a2a-t-sample/observability-sample/src/obs_sample/client_main.py`
- Create: `a2a-t-sample/observability-sample/README.md`

**Interfaces:**
- Consumes: 全部公开 API；A2ATClient/A2ATServer（a2at 场景）；subscribe-incident 的 mock 资源
- Produces: 可离线运行的双场景演示（Console exporter 默认，`A2AT_OTLP_ENDPOINT` 切 OTLP）

- [ ] **Step 1: `obs_setup.py`**

```python
"""OTel init for the observability sample: Console exporter by default, OTLP when configured."""

from __future__ import annotations

import os


def setup_observability() -> None:
    from opentelemetry import metrics, trace
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, PeriodicExportingMetricReader
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

    endpoint = os.environ.get("A2AT_OTLP_ENDPOINT")
    if endpoint:
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

        span_exporter: object = OTLPSpanExporter(endpoint=endpoint)
        metric_exporter: object = OTLPMetricExporter(endpoint=endpoint)
    else:
        span_exporter = ConsoleSpanExporter()
        metric_exporter = ConsoleMetricExporter()

    provider = TracerProvider()
    provider.add_span_processor(BatchSpanProcessor(span_exporter))  # type: ignore[arg-type]
    trace.set_tracer_provider(provider)
    metrics.set_meter_provider(MeterProvider(metric_readers=[PeriodicExportingMetricReader(metric_exporter)]))  # type: ignore[arg-type]
```

- [ ] **Step 2: `mock_llm.py`**

复制 `a2a-t-sample/subscribe-incident/src/common/mock_llm.py` 到 `obs_sample/mock_llm.py`，仅改一行：

```python
_RESOURCES_DIR = Path(__file__).resolve().parents[4] / "subscribe-incident" / "resources" / "mock_responses"
```

（`parents[4]`：mock_llm.py → obs_sample → src → observability-sample → a2a-t-sample；在该层下取 `subscribe-incident/resources/mock_responses`。）

- [ ] **Step 3: `server_main.py`**

```python
"""Observability sample server: middleware + observed_executor, no LLM needed (native flow)."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any

import uvicorn
from a2a.server.agent_execution.agent_executor import AgentExecutor
from a2a.server.agent_execution.context import RequestContext
from a2a.server.events.event_queue import EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.routes import create_agent_card_routes, create_rest_routes
from a2a.server.tasks.inmemory_task_store import InMemoryTaskStore
from a2a.types import Artifact, TaskState, TaskStatus, TaskStatusUpdateEvent
from a2a.utils.constants import TransportProtocol
from a2a_t.observability import A2ATTraceContextMiddleware, observed_executor
from google.protobuf.json_format import ParseDict
from google.protobuf.struct_pb2 import Value
from starlette.applications import Starlette
from starlette.middleware import Middleware

from obs_sample.obs_setup import setup_observability


class EchoExecutor(AgentExecutor):
    """Emits submitted -> working -> artifact -> completed; mirrors a Task-T streaming flow."""

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        task_id = context.task_id or "T-sample"
        context_id = context.context_id or "C-sample"
        for state in (TaskState.TASK_STATE_SUBMITTED, TaskState.TASK_STATE_WORKING):
            await event_queue.enqueue_event(
                TaskStatusUpdateEvent(task_id=task_id, context_id=context_id, status=TaskStatus(state=state))
            )
        artifact = Artifact(artifact_id="A1", name="sample.artifact")
        artifact.parts.add(data=Value(string_value="sample-result"))
        from a2a.types import TaskArtifactUpdateEvent

        await event_queue.enqueue_event(
            TaskArtifactUpdateEvent(task_id=task_id, context_id=context_id, artifact=artifact)
        )
        await event_queue.enqueue_event(
            TaskStatusUpdateEvent(
                task_id=task_id,
                context_id=context_id,
                status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
                final=True,
            )
        )

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None: ...


def build_app() -> Starlette:
    card_payload: dict[str, Any] = {
        "name": "obs-sample-server",
        "description": "observability sample server",
        "version": "1.0.0",
        "supportedInterfaces": [
            {"protocolBinding": TransportProtocol.HTTP_JSON.value, "protocolVersion": "1.0.0", "url": "http://127.0.0.1:8100"}
        ],
        "defaultInputModes": ["text/plain"],
        "defaultOutputModes": ["text/plain"],
        "skills": [],
    }
    from a2a.types import AgentCard

    agent_card = ParseDict(card_payload, AgentCard())
    handler = DefaultRequestHandler(
        agent_executor=observed_executor(EchoExecutor()),
        task_store=InMemoryTaskStore(),
        agent_card=agent_card,
    )
    return Starlette(
        routes=[*create_agent_card_routes(agent_card), *create_rest_routes(handler)],
        middleware=[Middleware(A2ATTraceContextMiddleware)],
    )


def main() -> None:
    setup_observability()
    port = int(os.environ.get("A2AT_OBS_SAMPLE_PORT", "8100"))
    uvicorn.run(build_app(), host="127.0.0.1", port=port)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: `client_main.py`**

```python
"""Observability sample client: interceptor (+ optional A2ATClient L4 trace in --scenario=a2at)."""

from __future__ import annotations

import argparse
import asyncio
import os
import uuid
from pathlib import Path
from typing import Any

import httpx
from a2a.client.client import ClientCallContext, ClientConfig
from a2a.client.client_factory import ClientFactory
from a2a.types import Role, SendMessageRequest
from a2a.utils.constants import TransportProtocol
from a2a_t.observability import A2ATClientInterceptor, A2ATObservabilityConfig, trace_facade

from obs_sample.obs_setup import setup_observability

_TASK_T = "https://projects.tmforum.org/a2aproject/telecommunication/extensions/Task-T/v1"
_NOTIFICATION_T_NL = "https://projects.tmforum.org/a2aproject/telecommunication/extensions/Notification-T/NL/v1"

_SAMPLE_INPUT = "请生成一个Incident事件订阅任务：通知主题为Incident，订阅条件为critical的ETH-LOS故障"


async def run(scenario: str) -> None:
    port = int(os.environ.get("A2AT_OBS_SAMPLE_PORT", "8100"))
    httpx_client = httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", trust_env=False)
    factory = ClientFactory(
        ClientConfig(
            httpx_client=httpx_client,
            supported_protocol_bindings=[TransportProtocol.HTTP_JSON],
            use_client_preference=True,
        )
    )
    card_response = httpx.Response  # noqa: F841 - documentation hint
    card_dict = (await httpx_client.get("/.well-known/agent-card.json")).json()
    from google.protobuf.json_format import ParseDict
    from a2a.types import AgentCard

    agent_card = ParseDict(card_dict, AgentCard())
    client = factory.create(agent_card, interceptors=[A2ATClientInterceptor()])

    metadata: dict[str, Any] = {_TASK_T: "sample-prompt-text"}
    prompt_text = "sample-prompt-text"
    if scenario == "a2at":
        from obs_sample.mock_llm import install_mock_llm_if_needed

        install_mock_llm_if_needed(env_path=Path.cwd() / ".env")
        from a2a_t.client.a2at_client import A2ATClient

        prompt_client = trace_facade(A2ATClient(env_path=Path.cwd() / ".env"))  # L4 Span, opt-in
        result = prompt_client.generate_task_prompt(_SAMPLE_INPUT)
        if not result.success:
            raise SystemExit(f"prompt generation failed: {result.failure}")
        prompt_text = str(result.prompt_text)
        metadata = {_NOTIFICATION_T_NL: prompt_text}

    request = SendMessageRequest()
    request.message.message_id = str(uuid.uuid4())
    request.message.role = Role.ROLE_USER
    request.message.parts.add().text = "observability-sample"
    for key, value in metadata.items():
        request.message.metadata[key] = value
    header_value = next(iter(metadata))
    context = ClientCallContext(service_parameters={"A2A-Extensions": header_value})

    async for response in client.send_message(request, context=context):
        print(f"[client] event: {type(response).__name__}")
    await httpx_client.aclose()
    print("[client] done — inspect the Console span output above")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", choices=["native", "a2at"], default="native")
    args = parser.parse_args()
    setup_observability()
    asyncio.run(run(args.scenario))


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: `README.md`**

````markdown
# observability-sample — A2A-T 可观测性演示

基于 `a2a-t-sdk[observability]` 的端到端可观测性样例：协议 Span 单一来源 + A2A-T 属性注入。

## 运行（离线，默认 Console 输出）

```powershell
cd a2a-t-sample
$env:PYTHONPATH = "$pwd\observability-sample\src"

# 终端 1：server（端口 8100）
uv run python -m obs_sample.server_main

# 终端 2：client（原生 a2a-python 流量）
uv run python -m obs_sample.client_main --scenario=native

# 或 client（A2ATClient 模板生成 + trace_facade，需 .env；无 API key 时自动走 mock LLM）
uv run python -m obs_sample.client_main --scenario=a2at
```

## 预期输出

- Client/Server 终端各打印 OTel Console Span：
  - `RestTransport.send_message`（协议 CLIENT Span，含 `extension.name`、`gen_ai.operation.name`）
  - `DefaultRequestHandler.on_message_send`（协议 SERVER Span，与 Client 同 trace_id）
  - `a2at.event.status` / `a2at.event.artifact` / `a2at.event.completed`（per-event，两端）
  - `--scenario=a2at` 另有 `a2at.sdk.client.generate_task_prompt`（L4）
- 指标：`gen_ai.client.operation.duration`、`a2at.task.request.duration`（side=client/server）

## 切换 OTLP 后端

```powershell
$env:A2AT_OTLP_ENDPOINT = "http://collector:4317"
```

## 说明

- 断言式端到端验证见 `tests/integration/test_e2e_observability.py`（`A2AT_TEST_A2A=1`）。
- a2at 场景的 mock LLM 资源复用 `../subscribe-incident/resources/mock_responses`。
````

- [ ] **Step 6: 手工端到端验证**

```powershell
cd a2a-t-sample
$env:PYTHONPATH = "$pwd\observability-sample\src"
# 终端 1: uv run python -m obs_sample.server_main
# 终端 2: uv run python -m obs_sample.client_main --scenario=native
# 终端 2: uv run python -m obs_sample.client_main --scenario=a2at  （需 a2a-t-sample/.env，无 key 走 mock）
```
Expected: 两终端可见 Console Span 输出，`a2at.event.*` 两端成对、trace_id 一致；`--scenario=a2at` 出现 `a2at.sdk.client.*` Span。

- [ ] **Step 7: lint + 提交**

```bash
uv run ruff check a2a-t-sample/observability-sample
git add a2a-t-sample/observability-sample
git commit -m "sample(observability): runnable end-to-end observability demo"
```

---

## Self-Review 记录（计划完成后自查）

1. **Spec 覆盖**：§2.1 模块树（Task 1-15 逐文件）；§2.3 注入机制（Task 9/11/13）；§3.2-3.7 API（Task 2-8/14/15）；§4 集成表（Task 10/11/13/14）；§5 样例（Task 16/17）；§6 数据流（Task 16 断言 1-6）；§7 事件形态（Task 3）；§8 降级（Task 1 NoOp + Task 16 断言 9/10 + 各任务异常吞噬测试）；§9 测试策略（Task 1-15 单测 + stub 结构化测试 + Task 16 集成）；§10 样例与验证（Task 16/17）。无缺口。
2. **占位符扫描**：无 TBD/TODO；所有代码块完整。
3. **类型一致性**：`ClientRequestStash`（Task 9 定义，Task 10 消费）、`EventInfo`（Task 3 定义，Task 10/12 消费）、`A2ATObservabilityConfig`（Task 5 定义，Task 10/12/13 消费）、stubs（Task 3 定义，Task 10/12/13/16 消费）——签名一致。Task 13 中 `_TRACEPARENT_WARN_KEY` 方案已注明改为实例属性实现。



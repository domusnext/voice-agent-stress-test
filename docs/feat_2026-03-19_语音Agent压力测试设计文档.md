# 语音 Agent 压力测试设计文档

## 一、测试目标

1. 以 **concurrency=3** 为基准，建立性能 baseline
2. 分别以 **30 / 50 / 100** 并发度施压，观察各延迟指标的变化趋势
3. 通过 CloudWatch Logs Insights 查询服务端指标，定位瓶颈组件（STT / LLM / TTS）
4. 确认外部服务（Deepgram / Anthropic / ElevenLabs / Cartesia）的限流阈值

## 二、整体架构

```
┌──────────────────────────────────────────────────────────────────┐
│                     压测控制器 (run.py)                            │
│                                                                  │
│  config.yaml ──→ transport: "daily" | "grpc"                     │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐    │
│  │  ProcessPoolExecutor + Semaphore 令牌池                   │    │
│  │                                                          │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │    │
│  │  │  Session 1   │  │  Session 2   │  │  Session N   │   │    │
│  │  │ (subprocess) │  │ (subprocess) │  │ (subprocess) │   │    │
│  │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘   │    │
│  └─────────┼─────────────────┼─────────────────┼────────────┘    │
│            │                 │                 │                 │
│            ▼                 ▼                 ▼                 │
│  ┌──────────────────────────────────────────────────────────┐    │
│  │             Transport 抽象层 (transport/__init__.py)      │    │
│  │                                                          │    │
│  │  ┌─────────────────────┐  ┌────────────────────────┐     │    │
│  │  │   DailyTransport    │  │    GrpcTransport       │     │    │
│  │  │ (transport/daily.py)│  │  (transport/grpc.py)   │     │    │
│  │  └──────────┬──────────┘  └───────────┬────────────┘     │    │
│  └─────────────┼─────────────────────────┼──────────────────┘    │
└────────────────┼─────────────────────────┼───────────────────────┘
                 │                         │
                 ▼                         ▼
        ┌────────────────┐       ┌──────────────────┐
        │  Daily.co SFU  │       │  gRPC Server     │
        │  (WebRTC)      │       │  (:8085)         │
        └────────┬───────┘       └────────┬─────────┘
                 │                         │
                 └────────────┬────────────┘
                              ▼
                 ┌──────────────────────┐
                 │ Voice Agent Pipeline │
                 │  STT → LLM → TTS     │
                 └──────────┬───────────┘
                            │ [voice metric]
                            ▼
                 ┌──────────────────────┐
                 │  CloudWatch Logs     │
                 │  ← collector.py      │
                 │  → reporter.py       │
                 └──────────────────────┘
```

### 设计原则

1. **Transport 抽象**：通过 ABC 定义统一接口，Daily 和 gRPC 各自实现
2. **配置驱动**：`config.yaml` 中 `transport` 字段选择传输模式
3. **持续并发**：Semaphore 令牌池维持恒定并发度，单轮模型（1 session = 1 问题）
4. **指标一致**：两种模式产出相同的 `SessionResult` 结构，reporter/collector 无需区分
5. **端到端自动化**：`run.py --report` 一键完成压测→采集→报告

### 设计演进

当前架构经历了两次重大重构：

**重构一：主流程重构（2026-03-12）**

原始方案存在两个核心问题：

1. **Bot 回复结束判断不可靠**：原实现使用静音检测（连续 1.5s 无非静音音频帧 = bot 回复结束），但 LLM 在生成回复时可能出现中间停顿（思考时间），导致 1.5s 的静音被误判为"bot 已说完"
2. **并发模型不真实**：原实现采用批量提交模型（一次性提交 N 个 session，等全部完成），无法模拟持续负载，且慢 session 会拖累整批

解决方案：
- Bot 结束判断改为 `on_participant_left` 事件（bot 配置为回复完主动离开房间）
- 采用单轮模型（1 session = 1 问题），不再需要多轮对话支持
- 并发模型改为 Semaphore 令牌池，持续维持恒定并发度

`client_e2e_ms` 语义因此发生变化：

| | 重构前 | 重构后 |
|---|---|---|
| 含义 | 停止说话 → bot 最后一帧音频 | 停止说话 → bot 离开房间 / 收到 turn-done |
| 判断方式 | 静音 1.5s | `on_participant_left` / `turn-done` 事件 |
| 准确性 | 可能误判（LLM 思考停顿） | 精确（应用层明确信号） |

> 注意：重构后的 E2E 时间会比重构前稍长（bot 离开房间的时间点通常在最后一帧音频之后），但这个值更可靠。

**重构二：gRPC 传输适配（2026-03-18）**

domi-voice-agent 从 Daily.co WebRTC 迁移到自研 gRPC 双向流方案（详见 `domi-voice-agent/docs/analysis_2026-03-12_Daily到gRPC架构迁移分析.md`）。压测工具引入 Transport 抽象层，将 Daily 特定逻辑从 session.py 中提取，同时新增 gRPC 实现。

### 两种传输模式对比

| 维度 | Daily 模式 | gRPC 模式 |
|------|-----------|-----------|
| 连接方式 | POST `/rtvi/start` → room_url + token → WebRTC | gRPC 双向流直连（支持 TLS / 非 TLS） |
| 音频发送 | Virtual Microphone → Daily SDK | `StreamMessage{type="audio"}` 20ms 帧 |
| 音频接收 | Virtual Speaker → 读取帧 | 接收 `StreamMessage{type="audio"}` |
| Bot 结束判断 | `on_participant_left` 事件 | 收到 `turn-done` server-message |
| 认证 | HTTP Headers（经 gateway） | gRPC metadata（直连，服务端主动验证） |
| SDK 依赖 | `daily-python`（需进程隔离） | `grpcio`（纯 Python，无特殊限制） |
| 进程模型 | 必须 ProcessPoolExecutor（Daily SDK 限制） | 可用 ThreadPoolExecutor 或 asyncio |
| 房间概念 | 有（Room 生命周期管理） | 无（流即连接） |

## 三、前置准备

### 3.1 环境要求

| 项目 | 要求 |
|------|------|
| Python | >= 3.13 |
| 运行环境 | macOS (arm64) / Linux (x86_64/arm64) |
| ffmpeg | 系统安装（音频生成需要） |

安装依赖：

```bash
uv sync
```

### 3.2 Room Pool 配置（Daily 模式）

Daily 模式下，压测前需根据最大并发数调整 Room Pool 的高水位线：

| 并发度 | ROOM_POOL_HIGH_WATERMARK | 说明 |
|--------|-------------------------|------|
| 3 (baseline) | 15 (默认) | 无需调整 |
| 30 | 40 | 预留 buffer |
| 50 | 60 | 预留 buffer |
| 100 | 120 | 预留 buffer |

通过环境变量设置：

```bash
# ECS Task Definition 或 .env
ROOM_POOL_HIGH_WATERMARK=120
ROOM_POOL_FLIP_INTERVAL=1200   # 延长翻转周期，避免压测期间翻转
```

> Room Pool 预填充需要时间（每个 room 需调用 Daily REST API 创建）。建议压测前等待 pool 填满（观察 `/metrics/room-pool` 端点）。

gRPC 模式无此限制（无 Room 概念）。

### 3.3 外部服务 API 配额

在执行大规模压测前，确认以下服务的并发/速率限制：

| 服务 | 用途 | 理论并发上限 | 检查方式 |
|------|------|-------------|---------|
| Daily.co | RTC | 并发房间数上限（待确认） | Dashboard / 联系 Daily |
| Deepgram | STT | 50 | API Dashboard |
| AssemblyAI | STT | 610 | API Dashboard |
| OpenRouter | LLM | 100 | OpenRouter Dashboard |
| ElevenLabs | TTS | 30 | API Dashboard |
| Cartesia | TTS | 15 | API Dashboard |
| Deepgram | TTS | 15 | API Dashboard |

> 以上并发上限均为理论值（来自各服务官方文档/控制台），实际可承受并发可能受网络、账户层级、突发限流等因素影响，需通过压测验证。

### 3.4 数据隔离

Daily 模式下，请求体中传入 `require_new_conversation: true`，使每个压测会话获得独立的 `conversation_id`：

```json
{
  "source": "stress_test",
  "require_new_conversation": true
}
```

`require_new_conversation=true` 会跳过 Redis 缓存查找，直接为每个会话生成新的 UUID 作为 `conversation_id`，避免多个并发会话因共享同一个 `family_id + user_id` 而命中同一个 Redis key 导致数据污染。

gRPC 模式下每个流天然独立，无此问题。

## 四、项目结构

```
voice-agent-stress-test/
├── src/
│   ├── run.py                # 压测主入口（编排并发、汇总结果）
│   ├── session.py            # 单会话逻辑（Transport 抽象驱动）
│   ├── transport/            # 传输层
│   │   ├── __init__.py       #   ABC + 工厂函数 + TransportResult
│   │   ├── daily.py          #   Daily.co WebRTC 实现
│   │   └── grpc.py           #   gRPC 双向流实现
│   ├── collector.py          # CloudWatch Logs Insights 指标采集
│   ├── reporter.py           # 结构化报告生成
│   ├── gen_audio.py          # 测试音频生成（edge-tts）
│   └── proto_generated/      # protoc 生成的 gRPC 代码
│       ├── __init__.py
│       ├── voice_agent_transport_pb2.py
│       └── voice_agent_transport_pb2_grpc.py
├── proto/
│   ├── voice_agent_transport.proto
│   └── generate.sh           # proto 代码生成脚本
├── audio/                    # 测试音频文件
├── reports/                  # 输出报告和指标数据
├── config.yaml               # 测试配置
├── config.example.yaml
└── pyproject.toml
```

### 涉及文件

| 文件 | 说明 |
|------|------|
| `src/transport/__init__.py` | Transport ABC 定义 + 工厂函数 |
| `src/transport/daily.py` | Daily 传输实现 |
| `src/transport/grpc.py` | gRPC 传输实现 |
| `src/session.py` | 单会话逻辑，通过 Transport 抽象驱动 |
| `src/run.py` | 压测主入口，Semaphore 令牌池并发模型 |
| `src/collector.py` | CloudWatch Logs Insights 指标采集 |
| `src/reporter.py` | 结构化 Markdown 报告生成 |
| `src/gen_audio.py` | edge-tts 测试音频生成 |
| `proto/` | gRPC proto 定义 + 代码生成脚本 |
| `config.yaml` / `config.example.yaml` | 测试配置 |
| `pyproject.toml` | 项目依赖 |

## 五、详细设计

### 4.1 Transport 抽象层 (`transport/__init__.py`)

Transport 抽象层定义了统一的传输接口，使 session 层与具体传输实现解耦。

#### 核心定义

```python
@dataclass
class TransportResult:
    """Transport 层执行结果，用于向 Session 传递延迟指标。"""
    connect_ms: float = 0.0           # 连接建立耗时
    client_ttfa_ms: float = 0.0       # 停止说话 → 首帧 bot 音频
    client_e2e_ms: float = 0.0        # 停止说话 → bot 结束回复
    success: bool = True
    error: str = ""


class BaseTransport(ABC):
    """
    生命周期：connect() → send_audio() → wait_for_completion() → close()
    """
    result: TransportResult

    @abstractmethod
    def connect(self) -> None:
        """建立连接。应在内部记录耗时到 self.result.connect_ms。"""

    @abstractmethod
    def send_audio(self, audio_pcm: bytes, sample_rate: int) -> None:
        """按实时速率分片发送音频，发送完毕后记录 stop_speaking 时间戳。"""

    @abstractmethod
    def wait_for_completion(self, timeout: float) -> TransportResult:
        """等待 bot 回复结束。Daily: on_participant_left；gRPC: turn-done。"""

    @abstractmethod
    def close(self) -> None:
        """释放资源。"""
```

#### 工厂函数

```python
def create_transport(transport_type: str, **kwargs) -> BaseTransport:
    if transport_type == "daily":
        from transport.daily import DailyTransport
        return DailyTransport(**kwargs)
    elif transport_type == "grpc":
        from transport.grpc import GrpcTransport
        return GrpcTransport(**kwargs)
    else:
        raise ValueError(f"不支持的传输类型: {transport_type}")
```

使用延迟导入：Daily 模式不会导入 grpcio，gRPC 模式不会导入 daily-python。

### 4.2 Daily Transport (`transport/daily.py`)

通过 Daily.co WebRTC 平台连接。

#### 连接流程

1. POST `/rtvi/start` → 获取 `room_url` + `token`
2. 创建虚拟麦克风/扬声器设备
3. `CallClient.join(room_url, meeting_token=token)`
4. 等待 `on_joined` + `on_participant_joined`（bot 加入）

#### 音频发送

通过虚拟麦克风按 1 秒分片写入 PCM 数据：

```python
chunk_bytes = sample_rate * 2  # 1 秒 16-bit mono
for offset in range(0, len(audio_pcm), chunk_bytes):
    self._mic_device.write_frames(audio_pcm[offset:offset + chunk_bytes])
    time.sleep(1.0)
```

#### TTFA 检测

启动独立监听线程，从虚拟扬声器读取音频帧，检测首个非静音帧（RMS > 200）：

```python
@staticmethod
def _monitor_for_first_audio(spk, stop_event, result_holder):
    CHUNK_FRAMES = 1600  # 100ms at 16kHz
    while not stop_event.is_set():
        raw = spk.read_frames(CHUNK_FRAMES)
        if raw and _is_non_silent(raw):
            result_holder["first_bot_audio_at"] = time.perf_counter()
            return
        time.sleep(0.1)
```

#### Bot 结束判断

通过 `on_participant_left` 事件判断 bot 回复结束（bot 配置为回复完主动离开房间）：

```python
class DailyEventHandler(daily.EventHandler):
    def on_participant_left(self, participant, reason=None):
        if not participant.get("local", False):
            self.bot_left_at = time.perf_counter()
            self.bot_left.set()
```

#### 认证

通过 HTTP Headers 传递：

```python
headers = {
    "Authorization": f"Bearer {auth_token}",
    "X-Family-ID": family_id,
    "X-User-ID": user_id,
    "X-Timezone": timezone,
}
```

### 4.3 gRPC Transport (`transport/grpc.py`)

通过 gRPC 双向流直连服务端。

#### 4.3.1 gRPC 消息协议

```protobuf
service VoiceAgentTransport {
  rpc Stream(stream StreamMessage) returns (stream StreamMessage);
}

message StreamMessage {
  string                 type = 1;  // "audio" | "rtvi_message" | "echo"
  bytes                  raw  = 2;  // 音频二进制数据
  google.protobuf.Struct data = 3;  // 元信息
}
```

发送音频帧：

```python
StreamMessage(
    type="audio",
    raw=pcm_bytes,          # 20ms PCM 帧 (640 bytes at 16kHz/16bit/mono)
    data=Struct(fields={
        "sample_rate": Value(number_value=16000),
        "channels": Value(number_value=1),
    }),
)
```

#### 4.3.2 连接流程

1. 根据 `tls` 配置创建 `grpc.aio.secure_channel(host, ssl_credentials)` 或 `grpc.aio.insecure_channel(host)`
2. 创建 `VoiceAgentTransportStub(channel)`
3. 准备 metadata（认证信息）
4. 建立双向流：`stub.Stream(request_iter(), metadata=metadata)`
5. 启动接收循环

#### 4.3.3 音频发送

将 PCM 切分为 20ms 帧（与服务端 `CustomInputTransport` 对齐），按实时速率发送：

- 16kHz / 16bit / mono → 每帧 320 samples = 640 bytes
- 帧间隔 20ms

```python
frame_duration_ms = 20
samples_per_frame = sample_rate * frame_duration_ms // 1000
bytes_per_frame = samples_per_frame * 2  # 16-bit mono

for offset in range(0, len(audio_pcm), bytes_per_frame):
    frame = audio_pcm[offset:offset + bytes_per_frame]
    asyncio.run_coroutine_threadsafe(
        self._send_queue.put(frame), self._loop
    ).result(timeout=5)
    time.sleep(frame_duration_ms / 1000)
```

#### 4.3.4 接收 bot 消息

服务端推送的消息分为两大类：

**1. 音频帧**：`response.type == "audio"` → bot 回复音频（用于 TTFA 检测）

**2. RTVI 消息**：`response.type == "rtvi_message"`，存在两种子格式：

| 格式 | 示例 | 来源 |
|------|------|------|
| **server-message 包装** | `{"type": "server-message", "data": {"type": "turn-done"}}` | `UIController.send_server_message()` 发出的生命周期事件 |
| **直接 RTVI 事件** | `{"type": "bot-tts-stopped", "label": "rtvi-ai"}` | pipecat 框架自动推送的状态事件 |

生命周期事件时序：

```
inited → agent-start → turn-start → [tool-call → tool-result]*
  → agent-done → [bot-tts-started → 音频帧 → bot-tts-stopped]
    → (客户端回传 bot-stopped-speaking) → turn-done → conversation-end
```

TTFA 检测：收到首个 `type="audio"` 的 StreamMessage 即为 TTFA 时间点（gRPC 消息边界清晰，无需静音检测）。

#### 4.3.5 Bot 结束判断与 `bot-stopped-speaking` 确认机制

服务端开启了 `use_client_bot_stopped_speaking_event=True`，pipeline 在 TTS 音频输出完成后**不会自动触发 `turn-done`**，而是等待客户端回传 `bot-stopped-speaking` 确认音频播放完毕后才推进到 `turn-done`。

**完整流程：**

```
服务端发送 bot-tts-stopped（TTS 输出完成）
    ↓
客户端检测到 → 自动回传 bot-stopped-speaking 确认消息
    ↓
服务端收到 → CustomBotStoppedSpeakingFrame → BotStoppedSpeakingFrame
    ↓
TurnAssistantProcessor._turn_done()
    ↓
服务端发送 turn-done（server-message 包装格式）
    ↓
客户端检测到 → 设置 _conversation_end Event → 关闭流
```

**客户端回传的 `bot-stopped-speaking` 消息格式：**

```python
StreamMessage(
    type="rtvi_message",
    data=Struct({
        "type": "client-message",
        "data": {"t": "bot_stopped_speaking", "d": None},
        "id": "<uuid>",
        "label": "rtvi-ai",
    }),
)
```

**RTVI 消息类型统一提取函数**（同时处理两种子格式）：

```python
def _extract_rtvi_type(data_struct) -> Optional[str]:
    """
    server-message 包装: {"type": "server-message", "data": {"type": "turn-done"}} → "turn-done"
    直接 RTVI 事件:      {"type": "bot-tts-stopped", "label": "rtvi-ai"}           → "bot-tts-stopped"
    """
    fields = data_struct.fields
    msg_type = fields.get("type")
    if not msg_type:
        return None
    type_str = msg_type.string_value
    if type_str == "server-message":
        inner_data = fields.get("data")
        if inner_data and inner_data.struct_value:
            inner_type = inner_data.struct_value.fields.get("type")
            if inner_type:
                return inner_type.string_value
        return None
    return type_str
```

相比"等待 gRPC 流结束"，使用 `turn-done` 消息的优势：

| 对比项 | 等待流结束 | 监听 turn-done |
|--------|-----------|----------------------|
| 语义 | 流关闭 = TCP 层断连 | 应用层明确通知"对话完成" |
| 时间精度 | 包含流关闭的传输开销 | 精确对应 bot pipeline 完成时间点 |
| 可靠性 | 依赖 gRPC 框架正常关闭流 | 即使流延迟关闭也不影响 |
| 额外收益 | 无 | 可同时采集 turn-start/turn-done 等生命周期事件 |

#### 4.3.6 流的生命周期管理

```
客户端发送音频帧 ──→ 音频完毕 ──→ request_iter 保持活跃等待后续指令
                                    │
服务端处理 ──→ STT → LLM → TTS ──→ 推送音频 + RTVI 消息
                                    │
服务端发送 bot-tts-stopped ──→ 客户端检测到
                                    │
                         客户端通过 send_queue 回传 bot-stopped-speaking
                                    │
                         request_iter 将其 yield 给服务端
                                    │
服务端收到 bot-stopped-speaking ──→ 触发 turn-done
                                    │
客户端接收 turn-done ──→ 设置 _conversation_end Event
                         + send_queue 放入 None
                                    │
                         request_iter 收到 None → return → 发送侧关闭
                                    │
                         gRPC 流完整结束
```

关键要点：
1. 客户端发送完音频后，**不能关闭发送侧**——`request_iter()` 继续从 `send_queue` 读取待发消息
2. `request_iter()` 支持发送两种消息：`bytes` 自动包装为 audio StreamMessage，`StreamMessage` 对象直接 yield
3. 接收循环检测到 `bot-tts-stopped` 后，通过 `send_queue` 投递 `bot-stopped-speaking` 消息，由 `request_iter()` yield 给服务端
4. 接收循环检测到 `turn-done` 后设置 `_conversation_end` 并投递 `None` 给 `send_queue`，使 `request_iter()` return 关闭发送流
5. 兜底：如果 gRPC 流异常结束但没收到完成信号，接收循环结束后也会 set event

#### 4.3.7 同步/异步桥接

gRPC Transport 内部使用 `asyncio.new_event_loop()` 在独立守护线程中驱动异步 gRPC 调用。外部接口（`connect` / `send_audio` / `wait_for_completion` / `close`）保持同步，对 session.py 透明。

线程间协调：
- `threading.Event`：`_connected`、`_send_done`、`_conversation_end`
- `asyncio.run_coroutine_threadsafe`：主线程向 asyncio queue 投递音频帧

Event loop 关闭策略：
1. `close()` 先设置 `_conversation_end`，再向 `send_queue` 投递 `None` 使 `request_iter()` 正常退出
2. 等待 event loop 线程自然结束（join timeout=5s）
3. 如果仍未退出，强制 `loop.stop()` 后再 join
4. `_run_event_loop()` 捕获 `RuntimeError`（event loop 外部停止时的预期异常），避免抛出到线程顶层

#### 4.3.8 认证

gRPC 模式下认证信息通过 metadata 传递：

```python
metadata = [
    ("authorization", f"Bearer {auth_token}"),
    ("x-device-id", device_id),
    ("x-family-id", family_id),
    ("source", "stress_test"),
    ("x-audio-encoding", "pcm"),
    ("x-mode", "standard"),
]
```

与 Daily 模式不同，gRPC 模式下服务端会主动调用 gateway 的 `/v1/families/{family_id}/my_role` 接口验证 token。

#### 4.3.9 TTFA 检测方式差异

| | Daily 模式 | gRPC 模式 |
|---|---|---|
| 检测方式 | Virtual Speaker 读帧 + 静音判断（RMS > 200） | 收到首个 `type="audio"` 的 StreamMessage |
| 准确性 | 依赖静音阈值，可能受 WebRTC 编解码影响 | 精确——gRPC 消息边界清晰 |
| 实现复杂度 | 需要额外监听线程 | 在接收循环中直接判断 |

### 4.4 会话管理 (`session.py`)

每个会话采用单轮模型（1 session = 1 个问题），通过 Transport 抽象驱动。

#### SessionResult

```python
@dataclass
class SessionResult:
    session_id: int
    transport_type: str = ""
    connect_ms: float = 0.0           # 连接建立耗时
    client_ttfa_ms: float = 0.0       # 停止说话 → 首帧 bot 音频
    client_e2e_ms: float = 0.0        # 停止说话 → bot 回复结束
    total_duration_ms: float = 0.0
    success: bool = True
    error: str = ""
```

#### 会话执行流程

```python
class StressTestSession:
    def run(self) -> SessionResult:
        transport = create_transport(self.transport_type, **self.transport_kwargs)
        try:
            audio_pcm = load_wav_pcm(self.audio_file, self.sample_rate)
            transport.connect()           # 1. 建立连接
            transport.send_audio(...)     # 2. 发送音频
            tr = transport.wait_for_completion(self.max_wait)  # 3. 等待 bot 回复
            # 4. 收集指标
        finally:
            transport.close()             # 5. 释放资源
```

#### 音频加载

`load_wav_pcm()` 加载 WAV 文件并返回 16-bit PCM bytes。如果采样率不匹配目标（16kHz），会进行线性插值重采样。

### 4.5 压测编排 (`run.py`)

#### 4.5.1 并发模型：Semaphore 令牌池

采用持续并发模型，始终维持 N 个并发 session：

```python
class AtomicCounter:
    """线程安全的自增计数器，用于在持续提交 session 时分配递增 ID。"""
    def __init__(self, start=0):
        self._value = start
        self._lock = threading.Lock()
    def next(self):
        with self._lock:
            val = self._value
            self._value += 1
            return val

sem = threading.Semaphore(concurrency)
counter = AtomicCounter()
deadline = time.monotonic() + duration_secs

with ProcessPoolExecutor(max_workers=max_workers) as executor:
    while time.monotonic() < deadline:
        sem.acquire()           # 阻塞，直到有空位
        kwargs = make_session_kwargs()  # 动态生成，ID 自增
        future = executor.submit(run_single_session, kwargs)
        future.add_done_callback(on_session_done)  # 回调中 sem.release()

    # Drain：获取全部 N 个令牌 = 所有 in-flight 完成
    for _ in range(concurrency):
        sem.acquire()
```

Semaphore 天然实现"令牌池"语义：
- 初始化为 N（并发度）
- 每提交一个 session，acquire 一个令牌
- session 完成后 release 一个令牌
- 主线程 acquire 时自动阻塞，直到有空位

Drain 机制：duration 到期后，连续 acquire N 个令牌，等价于等待所有 in-flight session 完成。

#### 4.5.2 Transport 参数构造

根据 `config.transport` 构造不同的 `transport_kwargs`：

```python
def make_session_kwargs():
    base = {
        "session_id": counter.next(),
        "transport_type": transport_type,
        "audio_file": test_cfg["audio_file"],
        "max_wait": test_cfg["max_wait_response"],
        "sample_rate": test_cfg.get("sample_rate", 16000),
    }

    if transport_type == "daily":
        base["transport_kwargs"] = {
            "server_url": config["server"]["url"],
            "auth_token": auth_cfg["token"],
            "family_id": auth_cfg["family_id"],
            "user_id": auth_cfg["user_id"],
            "timezone": auth_cfg.get("timezone", "Asia/Shanghai"),
            "sample_rate": test_cfg.get("sample_rate", 16000),
        }
    elif transport_type == "grpc":
        grpc_cfg = config["grpc"]
        base["transport_kwargs"] = {
            "grpc_host": grpc_cfg["host"],
            "auth_token": auth_cfg["token"],
            "device_id": grpc_cfg.get("device_id", f"stress-test-{sid}"),
            "family_id": auth_cfg["family_id"],
            "sample_rate": test_cfg.get("sample_rate", 16000),
            "audio_encoding": grpc_cfg.get("audio_encoding", "pcm"),
            "tls": grpc_cfg.get("tls", False),
        }
    return base
```

#### 4.5.3 Ramp-up

仅对初始 N 个 session 生效，之后的 session 按需补位，无 ramp-up 延迟。

#### 4.5.4 端到端自动化

`--report` 标志启用时，压测完成后自动：
1. 等待 15s CloudWatch 日志摄入延迟
2. 对每个 level 调用 `collector.collect_metrics()` 采集指标
3. 调用 `reporter.generate_report()` 生成最终 Markdown 报告

#### 4.5.5 CLI 接口

```bash
# 执行所有并发级别
uv run python src/run.py --config config.yaml

# 指定单个级别
uv run python src/run.py --config config.yaml --level baseline

# 自定义并发度和持续时间
uv run python src/run.py --config config.yaml --concurrency 10 --duration 60

# 压测完成后自动采集 CloudWatch 指标并生成报告
uv run python src/run.py --config config.yaml --report
```

### 4.6 CloudWatch 指标采集 (`collector.py`)

#### 4.6.1 查询模式

使用 boto3 的异步查询模式：并发启动 11 个 CloudWatch Logs Insights 查询，然后统一轮询结果，总耗时约 2~5 秒。

```python
def collect_metrics(logs_client, log_group, start, end) -> dict:
    # Phase 1：并发启动全部查询
    pending = {}
    for key, qdef in METRIC_QUERIES.items():
        resp = logs_client.start_query(
            logGroupName=log_group, startTime=start, endTime=end,
            queryString=qdef["expression"],
        )
        pending[key] = resp["queryId"]

    # Phase 2：轮询直到全部完成（最多 60 秒）
    while pending and time.time() < deadline:
        for key in list(pending.keys()):
            result = logs_client.get_query_results(queryId=pending[key])
            if result["status"] == "Complete":
                collected[key] = parse_cloudwatch_result(result)
                del pending[key]
        if pending:
            time.sleep(1)
```

#### 4.6.2 查询指标

| Key | 查询目标 |
|-----|---------|
| `ttfa` | TTFA 首音延迟（P50/P90/P99/Avg/Count） |
| `e2e` | 端到端延迟（P50/P90/P99/Avg/Count） |
| `stt` | STT 耗时（P50/P90/P99/Avg） |
| `llm_ttft` | LLM TTFT（P50/P90/P99/Avg） |
| `llm_call` | LLM 调用耗时 + 成功率 |
| `tts_ttfb` | TTS TTFB（P50/P90/P99/Avg） |
| `tts` | TTS 合成耗时（P50/P90/P99/Avg） |
| `ttfa_breakdown` | TTFA 组件占比（STT/LLM/TTS 各自均值） |
| `errors` | STT/TTS 错误（按 event_type 分组计数） |
| `sessions` | 会话统计（started/stopped 计数） |
| `interrupts` | 打断率 |

#### 4.6.3 AWS 权限要求

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["logs:StartQuery", "logs:GetQueryResults", "logs:StopQuery"],
    "Resource": "arn:aws:logs:us-east-1:*:log-group:/ecs/domus/dev/voice-agent:*"
  }]
}
```

凭证配置方式（任选其一）：
1. 环境变量：`AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`
2. AWS CLI profile：`config.yaml` 中 `cloudwatch.profile` 字段
3. IAM Role（EC2 上运行时自动获取）

#### 4.6.4 CLI 接口

```bash
uv run python src/collector.py --config config.yaml \
    --start '2026-03-19T08:00:00+00:00' \
    --end '2026-03-19T08:10:00+00:00' \
    --label 'baseline'
```

时间窗口从 `run.py` 输出的 `test_start_utc` / `test_end_utc` 获取。输出：`reports/metrics_<label>_<idx>.json`（`idx` 为时间戳 `HHMMSS`）。

采集完成后会自动打印生成报告的命令提示：

```
指标已保存: reports/metrics_baseline_141811.json

生成报告命令:
  uv run python src/reporter.py --data <stress_test_*.json> --metrics-dir reports/ --config config.yaml --output reports/report_baseline_141811.md
```

### 4.7 报告生成 (`reporter.py`)

合并客户端数据和 CloudWatch 指标，生成五章结构化 Markdown 报告。

#### 报告结构

| 章节 | 内容 |
|------|------|
| 一、测试概览 | 目标服务、测试级别、音频文件、总持续时间 |
| 二、客户端侧延迟对比 | 并发度 × TTFA/E2E P50/P90/P99 + 吞吐量 |
| 三、服务端指标 | 延迟对比、组件耗时、TTFA 占比（含 ASCII 条形图）、错误统计、会话统计 |
| 四、瓶颈分析 | 自动判定瓶颈组件 + 建议 |
| 五、客户端 vs 服务端差异 | TTFA P50 对比，差值即网络开销 |

#### 报告输出示例

```markdown
# 语音 Agent 压力测试报告

生成时间: 2026-03-19 16:00:00

## 一、测试概览

| 项目 | 值 |
|------|-----|
| 目标服务 | https://dev-voice-agent.example.com |
| 测试级别 | baseline(120s), medium(300s), high(300s), peak(300s) |
| 音频文件 | question_greeting.wav |
| 总持续时间 | 15 min 32 sec |

## 二、客户端侧延迟对比

| 并发度 | 持续时间 | 总会话数 | 会话成功 | 吞吐量 | TTFA P50 | TTFA P90 | TTFA P99 | E2E P50 | E2E P90 | E2E P99 |
|--------|---------|---------|---------|--------|----------|----------|----------|---------|---------|---------|
| 3      | 120s    | 36      | 36/36   | 0.30   | 1150ms   | 1320ms   | 1400ms   | 2250ms  | 2600ms  | 2800ms  |
| 30     | 300s    | 280     | 275/280 | 0.93   | 1200ms   | 1500ms   | 2100ms   | 2400ms  | 3200ms  | 4500ms  |

## 三、服务端指标（CloudWatch）

### 3.1 延迟指标对比

| 指标 | 并发度 | P50 | P90 | P99 | Avg | 样本数 |
|------|--------|-----|-----|-----|-----|--------|
| TTFA | 3 | 1020ms | 1180ms | 1250ms | 1050ms | 27 |
| TTFA | 30 | 1080ms | 1350ms | 1900ms | 1120ms | 88 |
| E2E | 3 | 2100ms | 2450ms | 2600ms | 2150ms | 27 |

### 3.2 组件耗时对比

| 组件 | 并发度 | P50 | P90 | P99 | Avg |
|------|--------|-----|-----|-----|-----|
| STT | 3 | 350ms | 420ms | 480ms | 370ms |
| LLM TTFT | 3 | 380ms | 450ms | 520ms | 400ms |
| TTS TTFB | 3 | 250ms | 310ms | 350ms | 265ms |

### 3.3 TTFA 组件占比分析

| 并发度 | STT 占比 | LLM 占比 | TTS 占比 | 其他 |
|--------|----------|----------|----------|------|
| 3 | 35% ███████░░░░░░░░░░░░░ | 40% ████████░░░░░░░░░░░░ | 25% █████░░░░░░░░░░░░░░░ | 0% |
| 30 | 30% ██████░░░░░░░░░░░░░░ | 50% ██████████░░░░░░░░░░ | 20% ████░░░░░░░░░░░░░░░░ | 0% |

### 3.4 错误统计

| 并发度 | STT 错误 | TTS 错误 | LLM 成功率 | 打断率 |
|--------|----------|----------|-----------|--------|
| 3 | 0 | 0 | 100% | 5.2% |
| 30 | 0 | 2 | 99.8% | 8.1% |

### 3.5 会话统计

| 并发度 | 服务端 Started | 服务端 Stopped | 丢失会话 |
|--------|--------------|---------------|---------|
| 3 | 3 | 3 | 0 |
| 50 | 50 | 48 | 2 |

## 四、瓶颈分析

| 并发度 | 瓶颈组件 | 表现 | 建议 |
|--------|---------|------|------|
| 3 (baseline) | — | 各指标稳定 | 作为基准线 |
| 30 | LLM | TTFA P99 上升 50%，LLM 占比从 40%→50% | 检查 LLM RPM 限制 |
| 50 | LLM + TTS | TTS 错误 5 次，TTFA P99 突破 3s | 升级 TTS API 计划 |
| 100 | 全链路 | 会话丢失 15%，大量限流错误 | 多实例部署 + 服务扩容 |

## 五、客户端 vs 服务端指标差异

| 并发度 | 客户端 TTFA P50 | 服务端 TTFA P50 | 差值(网络开销) |
|--------|----------------|----------------|---------------|
| 3 | 1150ms | 1020ms | +130ms |
| 30 | 1200ms | 1080ms | +120ms |
```

#### 自动瓶颈判定规则引擎

`analyze_bottleneck()` 以第一个 level（baseline）为基准，对比各并发级别的指标：

```
规则 1：TTFA P99 劣化判定
  IF level.ttfa.p99 > baseline.ttfa.p99 * 1.5  →  标记 TTFA 劣化

规则 2：瓶颈组件定位（基于 TTFA 组件占比变化）
  IF level.llm_pct - baseline.llm_pct > 10%  →  瓶颈 = LLM
  IF level.stt_pct - baseline.stt_pct > 10%  →  瓶颈 = STT
  IF level.tts_pct - baseline.tts_pct > 10%  →  瓶颈 = TTS
  IF 多个组件均上升  →  瓶颈 = 全链路

规则 3：错误率判定
  IF stt_error_count > 0 OR tts_error_count > 0  →  追加错误告警
  IF llm_success_rate < 99%  →  追加 LLM 告警

规则 4：会话丢失判定
  IF sessions.started - sessions.stopped > started * 0.05  →  追加会话丢失告警

规则 5：建议生成
  LLM →  "检查 LLM API 的 RPM/TPM 限制"
  STT →  "检查 Deepgram 并发 WebSocket 连接数限制"
  TTS →  "检查 ElevenLabs/Cartesia 并发限制"
  全链路 →  "系统整体过载，建议多实例部署 + 外部服务扩容"
```

#### CLI 接口

```bash
uv run python src/reporter.py \
    --data reports/stress_test_20260319_160000.json \
    --metrics-dir reports/ \
    --config config.yaml \
    --output reports/report_baseline_141811.md
```

### 4.8 测试音频生成 (`gen_audio.py`)

使用 edge-tts 将文本转为 WAV 音频文件。

```python
QUESTIONS = [
    ("question_greeting.wav", "Hello, how's the weather today?"),
]
```

流程：edge-tts 生成 MP3 → ffmpeg 转换为 16kHz 16-bit mono WAV。

修改 `QUESTIONS` 列表可自定义测试语句。需要系统安装 `ffmpeg`。

```bash
uv run python src/gen_audio.py
```

### 4.9 Proto 文件管理

`voice_agent_transport.proto` 从 domi-voice-agent 复制，定义了 gRPC 双向流接口。

代码生成脚本 `proto/generate.sh`：

```bash
#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
OUT_DIR="$PROJECT_DIR/src/proto_generated"

mkdir -p "$OUT_DIR"
touch "$OUT_DIR/__init__.py"

python -m grpc_tools.protoc \
    -I "$PROTO_DIR" \
    --python_out="$OUT_DIR" \
    --grpc_python_out="$OUT_DIR" \
    "$PROTO_DIR/voice_agent_transport.proto"

# 修复 grpc 生成代码的 import 路径（bare import → package import）
sed -i '' 's/^import voice_agent_transport_pb2/import proto_generated.voice_agent_transport_pb2/' \
    "$OUT_DIR/voice_agent_transport_pb2_grpc.py"
```

生成的代码在 `src/proto_generated/`，`sed` 命令自动修复 import 路径。

## 六、配置说明

```yaml
# ---- 传输模式 ----
transport: "grpc"                # "daily" | "grpc"

# ---- 目标服务（Daily 模式）----
server:
  url: "https://dev-voice-agent.example.com"

# ---- gRPC 配置（gRPC 模式）----
grpc:
  host: "localhost:8085"                # gRPC server 地址（host:port 格式，不带 scheme）
  tls: false                            # 远程 TLS 连接设为 true，本地测试设为 false
  device_id: "stress-test-device"       # 可选，默认自动生成
  audio_encoding: "pcm"                 # "pcm" | "opus"

# ---- 认证（两种模式共用）----
auth:
  token: ""              # Supabase JWT（必填）
  family_id: ""          # 测试 family UUID（必填）
  user_id: ""            # 测试 user UUID（Daily 模式使用）
  timezone: "Asia/Shanghai"

# ---- 测试参数 ----
test:
  audio_file: "audio/question_greeting.wav"
  max_wait_response: 30.0                      # 等待 bot 回复结束的超时（秒）
  ramp_up_secs: 10.0                           # 全部 worker 启动所需时间
  cooldown_between_levels: 60                  # 不同并发度之间的冷却（秒）
  sample_rate: 16000

# ---- 并发度梯度 ----
levels:
  - name: "baseline"
    concurrency: 3
    duration_secs: 120
  - name: "medium"
    concurrency: 30
    duration_secs: 300
  - name: "high"
    concurrency: 50
    duration_secs: 300
  - name: "peak"
    concurrency: 100
    duration_secs: 300

# ---- CloudWatch 指标采集 ----
cloudwatch:
  region: "us-east-1"
  log_group: "/ecs/domus/dev/voice-agent"
  profile: ""                                   # 可选：AWS CLI profile
```

### 认证差异

| 场景 | Daily 模式 | gRPC 模式 |
|------|-----------|-----------|
| 本地免认证 | N/A | 服务端配置 `USE_MOCK_AUTH=true` + `ENVIRONMENT=local` |
| TLS | N/A（WebRTC 内置加密） | 配置 `tls: true`（远程）/ `tls: false`（本地） |
| 需要 user_id | 是（HTTP Header） | 否（服务端从 token 解析） |
| 需要 device_id | 否 | 是（gRPC metadata 必填） |
| 需要 timezone | 是（HTTP Header） | 否 |

## 七、指标语义对齐

两种模式产出相同的 `SessionResult` 结构，reporter/collector 无需区分。

| 指标 | Daily 模式 | gRPC 模式 | 备注 |
|------|-----------|-----------|------|
| `connect_ms` | POST /rtvi/start + join room + bot 加入 | channel 建立 + stream 握手 | gRPC 更快（无 Room 创建开销） |
| `client_ttfa_ms` | 停说话 → Virtual Speaker 收到首帧非静音音频 | 停说话 → 首个 `type="audio"` 响应 | gRPC 更精确 |
| `client_e2e_ms` | 停说话 → `on_participant_left` | 停说话 → 收到 `turn-done` 消息 | 语义等价：均为"对话完成"信号 |

gRPC 模式额外可采集的指标（来自 server-message 生命周期事件，可在后续版本中扩展）：

| 事件 | 可衍生指标 | 含义 |
|------|-----------|------|
| `agent-start` | client_agent_start_ms | 停说话 → Agent 开始处理（≈ STT 完成） |
| `turn-start` | client_turn_start_ms | 停说话 → AI 开始回复 |
| `turn-done` | client_turn_done_ms | 停说话 → 一轮对话完成 |

## 八、数据流

```
[gen_audio.py]
  ↓ 生成 audio/question_greeting.wav

[run.py 执行压测]
  ↓ 对每个 level：Semaphore 令牌池 × ProcessPoolExecutor
  ↓ 每个 session：Transport.connect → send_audio → wait_for_completion → close
  ↓ 输出 reports/stress_test_<timestamp>.json
  ↓ 每个 level 含 test_start_utc / test_end_utc

[collector.py × N 次]
  ↓ 对每个 level：
  ↓   boto3.start_query(log_group, start, end, expression × 11)
  ↓   boto3.get_query_results(query_id × 11)
  ↓ 输出 reports/metrics_<label>_<idx>.json
  ↓ 打印生成报告的命令提示

[reporter.py]
  ↓ 读取 stress_test_*.json（客户端数据）
  ↓ 读取 metrics_*.json（服务端数据）
  ↓ 合并 + 对比 + 自动瓶颈分析
  ↓ 输出 reports/report_<label>_<idx>.md
```

## 九、并发级别

| 级别 | 并发度 | 持续时间 | 用途 | 预计会话数 |
|------|--------|----------|------|-----------|
| baseline | 3 | 120s | 性能基准线 | ~36 |
| medium | 30 | 300s | 中等负载 | ~280 |
| high | 50 | 300s | 高负载 | ~400 |
| peak | 100 | 300s | 峰值负载 | ~600 |

> 预计会话数基于单会话约 10s（音频播放 + bot 回复 + 连接开销）估算，实际取决于 bot 回复时长。
> 级别间冷却 60s，确保 Room Pool 回填、外部服务限流窗口重置。
> 预计单次全量测试（4 个 level + 冷却）约 **20-25 分钟**。

可在 `config.yaml` 的 `levels` 中自定义。

## 十、依赖

```toml
[project]
dependencies = [
    "daily-python>=0.23.0",     # Daily 模式
    "httpx>=0.28.1",            # Daily 模式 HTTP 请求
    "pyyaml>=6.0",              # 配置文件解析
    "edge-tts>=6.1.0",          # 测试音频生成
    "loguru>=0.7.3",            # 日志
    "boto3>=1.35.0",            # CloudWatch Logs Insights
    "grpcio>=1.70.0",           # gRPC 模式
    "grpcio-tools>=1.70.0",     # proto 代码生成
    "protobuf>=5.29.0",         # protobuf 运行时
]
```

系统依赖：`ffmpeg`（音频生成时需要）。

## 十一、CloudWatch Logs Insights 限制须知

| 限制 | 值 | 影响 |
|------|-----|------|
| 并发查询数 | 30/account（可申请提升） | 11 个查询并发启动不会超限 |
| 查询时间范围 | 最大 24 小时 | 单次压测不会超过 |
| 查询超时 | 60 分钟 | stats 查询通常 2-5 秒完成 |
| 结果行数 | 最大 10,000 行 | stats 聚合查询通常只返回 1-2 行 |
| 扫描数据量 | 按扫描量计费 ($0.005/GB) | 单次全量采集约扫描 10-50 MB，成本可忽略 |

## 十二、运维与注意事项

### 12.1 资源消耗估算

| 维度 | concurrency=3 | concurrency=30 | concurrency=100 |
|------|--------------|----------------|-----------------|
| 进程数 | 3 | ~30 | ~100 |
| 内存（估算） | ~200 MB | ~1.5 GB | ~4 GB |
| Daily 房间（Daily 模式） | 3 | 30 | 100 |
| 网络连接 | 3 WebRTC/gRPC | 30 WebRTC/gRPC | 100 WebRTC/gRPC |

- 100 并发建议在 **4C8G+ 的 EC2 实例**上运行
- 30 以内可以在本地 Mac 上运行

### 12.2 Daily.co 计费（Daily 模式）

每个房间会消耗 Daily.co 的使用额度。以 100 并发 × 单轮 × ~15s/轮估算，单次 level 约消耗 **25 分钟**的 participant-minutes。全量测试（4 个 level）约 **60-80 分钟**。请提前确认 Daily.co 计划的额度。

gRPC 模式无此开销。

### 12.3 外部 API 限流预期

| 服务 | 典型限制 | 100 并发时的请求速率 | 风险 |
|------|---------|-------------------|------|
| Deepgram (STT) | ~50 并发 WebSocket | ~100 并发连接 | 可能触及上限 |
| AssemblyAI (STT) | ~610 并发 | ~100 并发连接 | 低风险 |
| OpenRouter (LLM) | ~100 RPM | ~100 RPM | 可能触及上限 |
| ElevenLabs (TTS) | ~30 并发 WebSocket | ~100 并发连接 | 大概率触发限流 |
| Cartesia (TTS) | ~15 并发 | ~100 并发连接 | 大概率触发限流 |

> 建议：先从小并发度开始（3 → 30），观察是否有限流，再决定是否升级 API 计划后测试更高并发度。

### 12.4 测试环境隔离

- 压测应在 **dev 环境**执行，避免影响 prod
- 压测期间 dev 环境不应有其他用户活动，避免指标污染
- 如果需要在 prod 测试，应选择低峰时段并提前通知

### 12.5 压测期间实时监控

在压测运行期间，可通过 AWS Console 的 CloudWatch Logs Insights 实时查询以下指标：

1. **sessions 查询**：确认并发度是否达到预期（started 数）
2. **e2e 查询**：观察 Turn 样本数，判断是否有 turn 积压
3. **ttfa_breakdown 查询**：观察 STT/LLM/TTS 各组件耗时占比变化
4. **errors 查询**：是否出现 STT/TTS 限流错误
5. **ttfa 查询**：P99 延迟是否超出预期阈值

Daily 模式下还可通过 Room Pool 端点监控房间状态：

```bash
curl -s <server_url>/metrics/room-pool | jq .
```

## 十三、相关文档

| 文档 | 说明 |
|------|------|
| `docs/feat_2026-02-27_语音 Agent 压力测试方案.md` | 初始压测方案（Daily-only，多轮模型，批量提交） |
| `docs/feat_2026-03-06_基于 CloudWatch 的压测报告生成方案.md` | CloudWatch 采集 + 报告生成的详细设计 |
| `docs/feat_2026-03-12_压测主流程重构记录.md` | 主流程重构记录（单轮模型 + Semaphore + on_participant_left） |
| `docs/feat_2026-03-18_压力测试gRPC传输适配方案.md` | gRPC Transport 适配的详细设计 |
| `domi-voice-agent/docs/analysis_2026-03-12_Daily到gRPC架构迁移分析.md` | 服务端 Daily→gRPC 架构迁移分析 |
| `domi-voice-agent/docs/analysis_2026-03-16_gRPC服务端到客户端消息类型分析.md` | gRPC 服务端推送消息类型分析 |

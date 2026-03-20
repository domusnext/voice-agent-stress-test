# 压测 TTFA 指标对齐分析与方案

## 一、问题背景

压测报告第五章"客户端 vs 服务端指标差异"中，将 `客户端 TTFA P50 - 服务端 TTFA P50` 的差值解读为"网络开销"。在本地测试（`localhost:8083`）中，该差值高达 **1781ms**（4120ms - 2339ms），明显不合理——本地通信的网络延迟应在个位数毫秒级别。

| 并发度 | 客户端 TTFA P50 | 服务端 TTFA P50 | 差值（报告标注为"网络开销"） |
|--------|----------------|----------------|---------------------------|
| baseline | 4120ms | 2339ms | +1781ms |

**结论：两侧 TTFA 的测量起点不同，服务端 TTFA 系统性偏小，差值不是网络开销，而是指标起点的不对齐。**

## 二、原因分析

### 2.1 起点不对齐（主因，贡献 ~800-1000ms+）

```
客户端起点                   服务端起点
    │                           │
    ▼                           ▼
发完最后一帧音频    ──VAD 静音检测──▶   UserStoppedSpeakingFrame
(grpc.py:150)       (~800-1000ms)     (monitor_for_grafana.py:92)
```

| | 客户端 | 服务端 |
|---|---|---|
| **起点** | 最后一帧音频**发完** | `UserStoppedSpeakingFrame` 到达 |
| **代码** | `grpc.py:150` `self._stop_speaking_at = time.perf_counter()` | `monitor_for_grafana.py:92` `user_stopped_at = time.perf_counter()` |
| **语义** | 客户端"我说完了" | VAD 检测到足够静音**之后**才触发 |

客户端在发完最后一帧音频后立即打点。服务端需要 VAD 检测到持续静音才触发 `UserStoppedSpeakingFrame`，延迟取决于配置：

| VAD 配置 | 延迟 | 代码位置 |
|----------|------|---------|
| SileroVAD `stop_secs=0.8` | ~800ms | `vad_analyzer_factory.py:14` |
| Deepgram `utterance_end_ms=1000` | ~1000ms | `stt_service_factory.py:52` |
| Deepgram Flux | Flux 内部控制 | `stt_service_factory.py:37` |

> 报告中 STT = 0ms（`transcription_at ≈ user_stopped_at`）佐证了这一点：streaming STT 实时处理，最终转写在 VAD 触发时已就绪。

### 2.2 终点不对齐（次要，贡献 output pipeline 延迟）

| | 客户端 | 服务端 |
|---|---|---|
| **终点** | 首帧 bot 音频**到达客户端** | `BotStartedSpeakingFrame` 进入 pipeline |
| **代码** | `grpc.py:313` | `monitor_for_grafana.py:165` |

服务端终点比客户端终点早——音频还需经过 output transport 编码、gRPC send_queue、网络传输后才到达客户端。本地场景下该延迟较小，暂不处理。

### 2.3 差值分解

```
客户端 TTFA = VAD 静音检测延迟 + 服务端 TTFA(pipeline 内部) + output 链路延迟
  4120ms    ≈    ~800-1000ms+    +        2339ms            +    剩余
```

## 三、方案：对齐 `event=ttfa` 起点

### 3.1 核心思路

在 `CustomInputTransport.input_audio()` 中，每帧音频到达时记录 `last_audio_received_at`。当 `UserStoppedSpeakingFrame` 触发时，取此值作为 TTFA 起点（`start_at`），替代原来的 `user_stopped_at`。

**这是精确值**——`input_audio()` 在每帧到达时调用 `time.perf_counter()`，`UserStoppedSpeakingFrame` 触发时读取的就是最后一帧音频到达的精确时间戳。不依赖 VAD 配置，不是估算。

对齐后：
- `ttfa_ms = bot_started_at - start_at`（包含 VAD 延迟）
- 新增 `vad_ms = user_stopped_at - start_at` 字段，显式暴露 VAD 延迟

终点暂不处理（`bot_started_at` 保持不变），残余差异为 output pipeline 延迟 + gRPC 传输。

### 3.2 涉及文件

**domi-voice-agent（服务端）**：

| 文件 | 改动 |
|------|------|
| `src/bot/processor/monitor_for_grafana.py` | `VoiceMetricContext` 新增 `last_audio_received_at`；`_start_new_turn()` 用 `last_audio_received_at` 赋值 `start_at`；`_log_ttfa()` 用 `start_at` 算 `ttfa_ms`，新增 `vad_ms` 字段；`VoiceMetricProcessor` 公开 `metric_context` |
| `src/bot/transport/custom_transport.py` | `CustomInputTransport` 接收 `metric_context`，`input_audio()` 每帧更新 `last_audio_received_at`；`CustomTransport` 新增 `metric_context` 属性，透传给 input |
| `src/bot/core/pipeline.py` | 将 `VoiceMetricProcessor.metric_context` 共享给 `CustomTransport` |

**voice-agent-stress-test（客户端）**：

| 文件 | 改动 |
|------|------|
| `src/reporter.py` | 第五章表头从"网络开销"改为"传输开销" |

> `collector.py` 无需改动——现有查询 `parse msg /ttfa_ms=(?<ttfa>[\d.]+)/` 自动读取修改后的 `ttfa_ms` 值。

### 3.3 详细改动

#### 3.3.1 `monitor_for_grafana.py`

**VoiceMetricContext 新增字段**：

```python
@dataclass
class VoiceMetricContext:
    turn: Optional[VoiceTurnLatency] = None
    last_audio_received_at: Optional[float] = None  # 新增：input transport 每帧更新
```

**`_start_new_turn()` 使用实测起点**：

```python
def _start_new_turn(self):
    now = time.perf_counter()
    last_audio = self._metric_context.last_audio_received_at
    turn = VoiceTurnLatency(
        start_at=last_audio if last_audio else now,
        user_stopped_at=now,
    )
    self._metric_context.turn = turn
```

**`_log_ttfa()` 改用 `start_at` 并新增 `vad_ms`**：

```python
def _log_ttfa(self):
    turn = self._metric_context.turn
    if (turn is None or turn.reported_ttfa
        or turn.bot_started_at is None or turn.user_stopped_at is None):
        return

    # 改：用 start_at（= last_audio_received_at）替代 user_stopped_at
    ttfa_ms = round((turn.bot_started_at - turn.start_at) * 1000, 2)

    # 新增：VAD 延迟 = UserStoppedSpeaking 时间 - 最后音频帧到达时间
    vad_ms = 0.0
    if turn.start_at and turn.user_stopped_at:
        vad_ms = round((turn.user_stopped_at - turn.start_at) * 1000, 2)

    # stt_ms、llm_ttft_ms 计算不变（仍基于 user_stopped_at）
    stt_ms = 0.0
    if turn.transcription_at and turn.user_stopped_at:
        stt_ms = round((turn.transcription_at - turn.user_stopped_at) * 1000, 2)

    llm_ttft_ms = 0.0
    if turn.llm_first_token_at and turn.transcription_at:
        llm_ttft_ms = round((turn.llm_first_token_at - turn.transcription_at) * 1000, 2)
    elif turn.llm_first_token_at and turn.user_stopped_at:
        llm_ttft_ms = round((turn.llm_first_token_at - turn.user_stopped_at) * 1000, 2)

    tts_ttfb_ms = round(max(ttfa_ms - vad_ms - stt_ms - llm_ttft_ms, 0), 2)

    logger.info(
        "[voice metric]: event=ttfa model={} stt_provider={} tts_provider={}"
        " ttfa_ms={} vad_ms={} stt_ms={} llm_ttft_ms={} tts_ttfb_ms={}",
        self._model, self._stt_provider, self._tts_provider,
        ttfa_ms, vad_ms, stt_ms, llm_ttft_ms, tts_ttfb_ms,
    )

    logger.info(
        "[voice metric]: event=tts_ttfb tts_provider={} ttfb_ms={}",
        self._tts_provider, tts_ttfb_ms,
    )
    turn.reported_ttfa = True
```

日志输出变化（新增 `vad_ms` 字段）：

```
# 修改前
[voice metric]: event=ttfa ... ttfa_ms=2339 stt_ms=0 llm_ttft_ms=1729 tts_ttfb_ms=610

# 修改后
[voice metric]: event=ttfa ... ttfa_ms=3139 vad_ms=800 stt_ms=0 llm_ttft_ms=1729 tts_ttfb_ms=610
```

关系：**`ttfa_ms = vad_ms + stt_ms + llm_ttft_ms + tts_ttfb_ms`**

**`VoiceMetricProcessor` 公开 `metric_context`**：

```python
class VoiceMetricProcessor:
    @property
    def metric_context(self) -> VoiceMetricContext:
        return self._metric_context
```

#### 3.3.2 `custom_transport.py`

**CustomInputTransport 记录音频到达时间**：

```python
class CustomInputTransport(BaseInputTransport):
    def __init__(self, params, ..., metric_context=None):
        # ... existing __init__ ...
        self._metric_context = metric_context

    async def input_audio(self, audio, audio_meta):
        if self._metric_context is not None:
            self._metric_context.last_audio_received_at = time.perf_counter()
        # ... existing logic unchanged ...
```

**CustomTransport 透传 metric_context**（只需给 input，不涉及 output）：

```python
class CustomTransport(BaseTransport):
    def __init__(self, params, ...):
        # ... existing __init__ ...
        self._metric_context = None

    @property
    def metric_context(self):
        return self._metric_context

    @metric_context.setter
    def metric_context(self, ctx):
        self._metric_context = ctx
        if self._input is not None:
            self._input._metric_context = ctx

    def input(self):
        if not self._input:
            self._input = CustomInputTransport(
                self._params,
                audio_encoding=self._audio_encoding,
                mode=self._mode,
                output_audio=self._output_audio,
                context_data=self._context_data,
                output_rtvi_message=self._output_rtvi_message,
                metric_context=self._metric_context,  # 新增
            )
        return self._input

    # output() 不需要改动
```

#### 3.3.3 `pipeline.py`

`VoiceMetricProcessor` 创建后共享 metric_context：

```python
voice_metric_processor = VoiceMetricProcessor(
    user_stop_delay_secs=0,
    stt_provider=stt_provider,
    tts_provider=tts_voice[0],
    model=agent.get_model(),
)

# 共享 metric context 给 transport，用于 TTFA 起点对齐
if isinstance(transport, CustomTransport):
    transport.metric_context = voice_metric_processor.metric_context
```

时序：`transport.input()` 在 L66 已被调用（input 已创建），`metric_context.setter` 会补设到已创建的 input 上。

#### 3.3.4 `reporter.py`

`generate_diff_section()` 表头改为"传输开销"：

```python
headers = ["并发度", "客户端 TTFA P50", "服务端 TTFA P50", "差值(传输开销)"]
```

### 3.4 对现有 Grafana 面板的影响

`event=ttfa` 的 `ttfa_ms` 语义变更：

| | 修改前 | 修改后 |
|---|---|---|
| 起点 | `user_stopped_at`（VAD 触发时） | `start_at`（= 最后音频帧到达时） |
| 终点 | `bot_started_at` | `bot_started_at`（不变） |
| 值域 | pipeline 内部处理时间 | pipeline 内部 + VAD 检测延迟 |
| 典型值 | ~2300ms | ~3100ms（增加 ~800ms VAD 延迟） |

Grafana 面板会看到 `ttfa_ms` 值上升，这是**更真实的用户体感**——用户停止说话到听到 bot 回复的延迟确实包含 VAD 等待时间。

如需保留旧语义做对比，可通过 `ttfa_ms - vad_ms` 得到（CloudWatch 支持 `stats avg(ttfa - vad)`）。

### 3.5 对 TTFA 组件占比分析的影响

压测报告第三章 "TTFA 组件占比分析" 依赖 `ttfa_breakdown` 查询：

```
filter msg like /event=ttfa/
| parse msg /stt_ms=(?<stt>[\d.]+)/
| parse msg /llm_ttft_ms=(?<llm>[\d.]+)/
| parse msg /tts_ttfb_ms=(?<tts>[\d.]+)/
| parse msg /ttfa_ms=(?<ttfa>[\d.]+)/
| stats avg(stt) as stt_avg, avg(llm) as llm_avg, avg(tts) as tts_avg, avg(ttfa) as ttfa_avg
```

修改后，`ttfa_ms` 包含了 `vad_ms`，但查询中没有提取 `vad_ms`。组件占比（`stt_avg/ttfa_avg`、`llm_avg/ttfa_avg`、`tts_avg/ttfa_avg`）之和不再接近 100%，差值即为 VAD 占比。

**需要同步更新 `collector.py` 的 `ttfa_breakdown` 查询**，新增 `vad` 字段：

```python
"ttfa_breakdown": {
    "label": "TTFA 组件占比",
    "expression": (
        "filter msg like /event=ttfa/\n"
        "| parse msg /vad_ms=(?<vad>[\\d.]+)/\n"        # 新增
        "| parse msg /stt_ms=(?<stt>[\\d.]+)/\n"
        "| parse msg /llm_ttft_ms=(?<llm>[\\d.]+)/\n"
        "| parse msg /tts_ttfb_ms=(?<tts>[\\d.]+)/\n"
        "| parse msg /ttfa_ms=(?<ttfa>[\\d.]+)/\n"
        "| stats avg(vad) as vad_avg, avg(stt) as stt_avg,"
        " avg(llm) as llm_avg, avg(tts) as tts_avg, avg(ttfa) as ttfa_avg"
    ),
},
```

`reporter.py` 的组件占比表格也需新增 VAD 列。

## 四、验证方式

1. 本地启动 `domi-voice-agent`，运行压测 baseline level
2. 检查 agent 日志 `event=ttfa` 中：
   - `ttfa_ms` 比修改前更大（包含 VAD 延迟）
   - `vad_ms` ≈ 500-1000ms（符合 VAD 配置）
   - `vad_ms + stt_ms + llm_ttft_ms + tts_ttfb_ms ≈ ttfa_ms`
3. 生成报告，第五章差值应比之前的 1781ms 显著缩小（预期剩余 output pipeline 延迟 + gRPC 传输）
4. 如残余差值仍过大（>500ms），可后续追加终点对齐（在 `CustomOutputTransport.write_audio_frame()` 打点）

## 五、兼容性

| 项目 | 影响 |
|------|------|
| Grafana `event=ttfa` 面板 | `ttfa_ms` 值上升（包含 VAD 延迟），**更准确反映用户体感**；旧值可通过 `ttfa_ms - vad_ms` 还原 |
| `event=turn_e2e` | **无影响**——`e2e_ms` 仍用 `bot_stopped_at - user_stopped_at` |
| 压测 `ttfa` CloudWatch 查询 | **无需改动**——`parse msg /ttfa_ms=.../` 自动读取新值 |
| 压测 `ttfa_breakdown` 查询 | **需更新**——新增 `vad` 字段提取 |
| 非 CustomTransport（Daily 模式） | **无影响**——`metric_context` 为 None 时 `last_audio_received_at` 不会被更新，`start_at` fallback 到 `now` |

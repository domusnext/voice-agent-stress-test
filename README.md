# Voice Agent Stress Test

语音 Agent 压力测试工具，用于测试 domi-voice-agent 在不同并发度下的性能表现。

支持两种传输模式：
- **Daily 模式**：通过 Daily.co WebRTC 平台连接（遗留方案）
- **gRPC 模式**：通过 gRPC 双向流直连（当前主力方案）

## 项目结构

```
voice-agent-stress-test/
├── src/
│   ├── run.py                # 压测主入口（编排并发、汇总结果）
│   ├── session.py            # 单会话逻辑（Transport 抽象驱动）
│   ├── transport/            # 传输层
│   │   ├── __init__.py       #   ABC + 工厂函数
│   │   ├── daily.py          #   Daily.co WebRTC 实现
│   │   └── grpc.py           #   gRPC 双向流实现
│   ├── collector.py          # CloudWatch Logs Insights 指标采集
│   ├── reporter.py           # 结构化报告生成
│   ├── gen_audio.py          # 测试音频生成（edge-tts）
│   └── proto_generated/      # protoc 生成的 gRPC 代码
├── proto/
│   ├── voice_agent_transport.proto
│   └── generate.sh           # proto 代码生成脚本
├── audio/                    # 测试音频文件
├── reports/                  # 输出报告和指标数据
├── config.yaml               # 测试配置（从 config.example.yaml 复制）
├── config.example.yaml
└── pyproject.toml
```

## 快速开始

### 1. 安装依赖

```bash
uv sync
```

### 2. 生成 gRPC 代码（仅 gRPC 模式需要）

```bash
bash proto/generate.sh
```

### 3. 生成测试音频

```bash
uv run python src/gen_audio.py
```

需要系统安装 `ffmpeg`。生成的音频文件在 `audio/` 目录下，格式为 16kHz 16-bit mono WAV。

### 4. 配置

```bash
cp config.example.yaml config.yaml
```

编辑 `config.yaml`，填入认证信息和目标服务地址。关键配置项：

```yaml
# 选择传输模式
transport: "grpc"          # "daily" | "grpc"

# Daily 模式目标服务
server:
  url: "https://dev-voice-agent.example.com"

# gRPC 模式目标服务
grpc:
  host: "localhost:8085"
  audio_encoding: "pcm"   # "pcm" | "opus"

# 认证（两种模式共用）
auth:
  token: ""                # Supabase JWT
  family_id: ""
  user_id: ""              # Daily 模式需要
```

### 5. 运行压测

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

结果保存在 `reports/stress_test_<timestamp>.json`。

## 子模块独立使用

### 生成测试音频 (`gen_audio.py`)

使用 edge-tts 将文本转为 WAV 音频文件。

```bash
uv run python src/gen_audio.py
```

修改 `src/gen_audio.py` 中的 `QUESTIONS` 列表可自定义测试语句：

```python
QUESTIONS = [
    ("question_greeting.wav", "Hello, how's the weather today?"),
    ("question_custom.wav", "帮我设置一个明天早上八点的闹钟"),
]
```

生成的文件在 `audio/` 目录，格式要求：16kHz、16-bit、单声道 WAV。

### 采集 CloudWatch 指标 (`collector.py`)

从 CloudWatch Logs Insights 采集服务端性能指标（TTFA、E2E、STT、LLM、TTS 等）。

```bash
# 指定时间窗口采集
uv run python src/collector.py --config config.yaml \
    --start '2026-03-19T08:00:00+00:00' \
    --end '2026-03-19T08:10:00+00:00' \
    --label 'baseline'

# 指定输出目录
uv run python src/collector.py --config config.yaml \
    --start '2026-03-19T08:00:00+00:00' \
    --end '2026-03-19T08:10:00+00:00' \
    --label 'high' \
    --output reports/
```

时间窗口从 `run.py` 输出的 `test_start_utc` / `test_end_utc` 获取。

需要 AWS 凭证，配置方式：
- 环境变量：`AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`
- AWS CLI profile：`config.yaml` 中 `cloudwatch.profile` 字段
- IAM Role（EC2 上运行时自动获取）

输出：`reports/metrics_<label>_<time>.json`

### 生成报告 (`reporter.py`)

合并客户端数据和 CloudWatch 指标，生成结构化 Markdown 报告。

```bash
# 基本用法
uv run python src/reporter.py \
    --data reports/stress_test_20260319_160000.json \
    --output reports/report.md

# 指定指标目录和配置（用于生成测试概览）
uv run python src/reporter.py \
    --data reports/stress_test_20260319_160000.json \
    --metrics-dir reports/ \
    --config config.yaml \
    --output reports/report.md
```

报告包含：测试概览、客户端延迟对比、服务端指标、TTFA 组件占比、瓶颈分析。

### 重新生成 gRPC 代码 (`proto/generate.sh`)

当 `voice_agent_transport.proto` 更新时重新生成：

```bash
bash proto/generate.sh
```

生成的代码在 `src/proto_generated/`，已自动处理 import 路径。

## 传输模式说明

### Daily 模式

通过 Daily.co WebRTC 平台连接。每个 session：
1. POST `/rtvi/start` → 获取 room_url + token
2. 通过 daily-python SDK 加入房间
3. 虚拟麦克风发送音频，虚拟扬声器接收回复
4. 监听 `on_participant_left` 事件判断 bot 回复结束

### gRPC 模式

通过 gRPC 双向流直连服务端。每个 session：
1. 建立 gRPC channel，通过 metadata 传递认证信息
2. 以 20ms PCM 帧发送音频
3. 接收 bot 回复音频和 RTVI 消息
4. 监听 `turn-done` server-message 判断对话结束

### 指标对齐

两种模式产出相同的 `SessionResult` 结构：

| 指标 | Daily | gRPC |
|------|-------|------|
| `connect_ms` | POST + join room + bot 加入 | channel + stream 握手 |
| `client_ttfa_ms` | 停说话 → 首帧非静音音频 | 停说话 → 首个 audio 响应 |
| `client_e2e_ms` | 停说话 → bot 离开房间 | 停说话 → turn-done |

## 并发级别

| 级别 | 并发度 | 持续时间 | 用途 |
|------|--------|----------|------|
| baseline | 3 | 120s | 性能基准线 |
| medium | 30 | 300s | 中等负载 |
| high | 50 | 300s | 高负载 |
| peak | 100 | 300s | 峰值负载 |

可在 `config.yaml` 的 `levels` 中自定义。

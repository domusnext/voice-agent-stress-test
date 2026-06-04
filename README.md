# Voice Agent Stress Test

语音 Agent 压力测试工具，用于测试 domi-voice-agent 在不同并发度下的性能表现。

支持两种传输模式：
- **Daily 模式**：通过 Daily.co WebRTC 平台连接（遗留方案）
- **gRPC 模式**：通过 gRPC 双向流直连（当前主力方案）

支持两种测试策略：
- **固定 levels 模式**：按预设的并发梯度依次压测
- **ramp 模式**：自动递增并发度，找到服务承载上限（推荐）

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
│   ├── collector.py          # CloudWatch Logs Insights / 本地日志指标采集
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

---

## 一、前期准备（首次使用必做）

### 第 1 步：安装依赖

```bash
cd voice-agent-stress-test
uv sync
```

### 第 2 步：生成 gRPC 代码（仅 gRPC 模式需要）

```bash
bash proto/generate.sh
```

生成的代码在 `src/proto_generated/`。

### 第 3 步：生成测试音频

```bash
uv run python src/gen_audio.py
```

需要系统安装 `ffmpeg`。生成的音频文件在 `audio/` 目录，格式为 16kHz 16-bit mono WAV。

### 第 4 步：创建并填写配置文件

```bash
cp config.example.yaml config.yaml
```

打开 `config.yaml`，按实际情况填写认证信息和服务地址。最关键的几项：

```yaml
transport: "grpc"           # 传输模式：grpc 或 daily

grpc:
  host: "localhost:8085"    # 本地压测填 localhost；远程压测填实际地址

auth:
  token: ""                 # Supabase JWT
  family_id: ""
```

---

## 二、ramp 模式（推荐：自动找到承载上限）

ramp 模式会自动完成两件事：

1. **建立基准带**：在并发度 1 下重复跑多轮，测量指标在无压力时的天然波动范围
2. **线性递增**：从并发度 1 开始按固定步长递增，每级结束后自动判定是否超载，超载即停

### 配置 ramp 段

在 `config.yaml` 末尾有 `ramp` 段，按需调整：

```yaml
ramp:
  enabled: false          # 也可用 --ramp 命令行参数临时开启
  start: 1
  step: 2                 # 每级递增的并发度
  max: 30                 # 并发度上限
  hold_secs: 300          # 每个递增级别维持多久
  cooldown_secs: 30       # 级别间冷却时间

  baseline_repeats: 3         # 并发度 1 重复轮数（建议 ≥3）
  baseline_hold_secs: 600     # 基准每轮时长（建议 ≥300s）

  server_metrics:
    source: "local_log"        # 本地压测填 local_log；远程压测填 cloudwatch
    local_log_path: ""         # source=local_log 时填 domi-voice-agent 的日志路径
```

---

### 场景 A：对本地服务压测（source: local_log）

本地压测不依赖 AWS，直接解析 domi-voice-agent 的本地日志文件获取服务端指标。

**第 1 步：确认本地服务已启动**

确保 domi-voice-agent 正在运行，且日志路径正确。默认日志路径：

```
/Users/lihaicheng/duomi_workspace/domi-voice-agent/logs/app.log
```

**第 2 步：配置 config.yaml**

```yaml
grpc:
  host: "localhost:8085"

ramp:
  server_metrics:
    source: "local_log"
    local_log_path: "/Users/lihaicheng/duomi_workspace/domi-voice-agent/logs/app.log"
```

**第 3 步：执行压测**

```bash
uv run python src/run.py --ramp
```

压测过程中，终端会实时打印每个并发度的判定结果，例如：

```
[ramp] conc=  3  ttfa_p99=1300ms(band_high=1500)  succ=100%  | low_fps=2% poor_conn=0 pace=1.02  → 健康
       归因: 一切健康
[ramp] conc=  5  ttfa_p99=1450ms(band_high=1500)  succ=100%  | low_fps=38% poor_conn=0 pace=1.03  → 健康
       归因: 上行受限（网络/压测机），服务端处理仍快，服务有余量
[ramp] conc=  7  ttfa_p99=1900ms(band_high=1500)  succ=99%   | low_fps=12% poor_conn=2 pace=1.03  → 预警
[ramp] conc=  9  ttfa_p99=2700ms(band_high=1500)  succ=93%   | low_fps=20% poor_conn=5 pace=1.04  → 超载

============================================================
承载能力估计：5（最后一个健康级别，指标离开基准带前的最大并发）
超载于并发度：9（停止递增，不复测）
============================================================
ramp 结果已保存: reports/ramp_result_20260603_143000.json
```

**第 4 步：查看报告（自动完成，无需额外命令）**

压测结束后，JSON 报告已自动保存到 `reports/ramp_result_<时间戳>.json`，包含：

- 每个并发度的客户端延迟（TTFA/E2E）、成功率、帧率辅助信号
- 每个并发度的服务端指标（从本地日志解析：STT/LLM/TTS 耗时、会话丢失等）
- 承载能力结论：最后一个健康的并发度、超载原因、瓶颈归因

**不需要再手动执行任何命令**，报告已完整生成。

---

### 场景 B：对远程服务压测（source: cloudwatch）

远程压测通过 CloudWatch Logs Insights 采集服务端指标。每个并发度结束后自动等待 15 秒（CloudWatch 日志摄入延迟），再采集该时间窗口的指标。

**第 1 步：配置 AWS 凭证**

选择以下任意一种方式：

```bash
# 方式一：环境变量
export AWS_ACCESS_KEY_ID=xxx
export AWS_SECRET_ACCESS_KEY=xxx

# 方式二：AWS CLI profile（在 config.yaml 的 cloudwatch.profile 字段填写）
```

**第 2 步：配置 config.yaml**

```yaml
grpc:
  host: "your-remote-host:443"
  tls: true

cloudwatch:
  log_group: "/your/log/group"
  region: "ap-northeast-1"   # 或对应的 region
  # profile: "your-aws-profile"  # 若用 profile 则取消注释

ramp:
  server_metrics:
    source: "cloudwatch"
```

**第 3 步：执行压测**

```bash
uv run python src/run.py --ramp
```

流程与本地压测完全相同，区别在于每级别结束后会多打一行：

```
  等待 CloudWatch 日志同步 (15s)...
```

然后自动采集该级别的 CloudWatch 指标，继续判定。

**第 4 步：查看报告（自动完成，无需额外命令）**

与本地压测相同，压测结束后报告自动保存到 `reports/ramp_result_<时间戳>.json`。

> **注意**：如果 CloudWatch 不可用（凭证失效、Log Group 不存在等），压测会立即中止，已完成的级别结果会先落盘保存，再打印错误信息退出。此时需要人工检查 AWS 配置后重跑。

---

## 三、固定 levels 模式

按 `config.yaml` 中预设的并发梯度依次压测，适合已经知道目标并发度、只需验证特定负载的场景。

### 配置并发梯度

```yaml
levels:
  - name: baseline
    concurrency: 3
    duration_secs: 120
  - name: medium
    concurrency: 30
    duration_secs: 300
  - name: high
    concurrency: 50
    duration_secs: 300
```

### 场景 A：对本地服务压测（无 CloudWatch）

```bash
# 执行所有级别
uv run python src/run.py

# 只跑某一个级别
uv run python src/run.py --level baseline

# 临时指定并发度和时长
uv run python src/run.py --concurrency 10 --duration 60
```

压测结束后，原始数据自动保存到 `reports/stress_test_<时间戳>.json`。

**服务端指标需要手动采集。** 压测结束后终端会打印采集命令，例如：

```
下一步：使用 collector.py 从 CloudWatch 采集服务端指标：
  python src/collector.py --config config.yaml \
    --start '2026-06-03T06:00:00+00:00' \
    --end '2026-06-03T06:02:00+00:00' \
    --label 'baseline' \
    --data reports/stress_test_20260603_140000.json
```

本地无 CloudWatch 时可跳过 collector 步骤，直接用原始 JSON 人工分析，或接入 ramp 模式的 `source: local_log` 路径（见二、场景 A）。

### 场景 B：对远程服务压测（有 CloudWatch，自动生成报告）

加上 `--report` 参数，压测结束后自动采集 CloudWatch 指标并生成 Markdown 报告：

```bash
uv run python src/run.py --report
```

流程：

1. 依次执行各级别压测
2. 全部结束后等待 15s（CloudWatch 摄入）
3. 逐级别采集 CloudWatch 指标，保存到 `reports/metrics_<级别名>_<时间>.json`
4. 自动生成 `reports/report_<时间戳>.md`，包含延迟对比、服务端指标、瓶颈分析

**整个过程无需手动操作**，一条命令搞定。

### 场景 C：对远程服务压测（不加 --report，手动分步）

适合需要在压测和报告之间插入人工检查的情况：

```bash
# 第 1 步：执行压测，保存原始数据
uv run python src/run.py

# 第 2 步：手动采集 CloudWatch 指标（复制终端打印的命令逐一执行）
uv run python src/collector.py --config config.yaml \
    --start '2026-06-03T06:00:00+00:00' \
    --end '2026-06-03T06:02:00+00:00' \
    --label 'baseline'

# 第 3 步：手动生成 Markdown 报告
uv run python src/reporter.py \
    --data reports/stress_test_20260603_140000.json \
    --metrics-dir reports/ \
    --config config.yaml \
    --output reports/report.md
```

---

## 四、操作流程速查表

| 场景 | 命令 | 报告是否自动生成 |
|------|------|----------------|
| 本地压测，自动找承载上限 | `uv run python src/run.py --ramp`（config: `source: local_log`） | **是**，JSON 自动保存 |
| 远程压测，自动找承载上限 | `uv run python src/run.py --ramp`（config: `source: cloudwatch`） | **是**，JSON 自动保存 |
| 远程压测，固定梯度，全自动 | `uv run python src/run.py --report` | **是**，JSON + Markdown 自动保存 |
| 远程压测，固定梯度，手动分步 | `uv run python src/run.py` → 手动 collector → 手动 reporter | 否，需手动执行两步 |
| 本地压测，固定梯度 | `uv run python src/run.py` | 否，只保存原始 JSON |

---

## 五、子模块独立使用

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
uv run python src/collector.py --config config.yaml \
    --start '2026-03-19T08:00:00+00:00' \
    --end '2026-03-19T08:10:00+00:00' \
    --label 'baseline'
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
uv run python src/reporter.py \
    --data reports/stress_test_20260319_160000.json \
    --metrics-dir reports/ \
    --config config.yaml \
    --output reports/report.md
```

### 重新生成 gRPC 代码 (`proto/generate.sh`)

当 `voice_agent_transport.proto` 更新时重新生成：

```bash
bash proto/generate.sh
```

---

## 六、传输模式说明

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

---

## 七、并发级别参考（固定 levels 模式）

| 级别 | 并发度 | 持续时间 | 用途 |
|------|--------|----------|------|
| baseline | 3 | 120s | 性能基准线 |
| medium | 30 | 300s | 中等负载 |
| high | 50 | 300s | 高负载 |
| peak | 100 | 300s | 峰值负载 |

可在 `config.yaml` 的 `levels` 中自定义。

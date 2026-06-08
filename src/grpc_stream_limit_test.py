#!/usr/bin/env python3
"""
gRPC 并发流上限探测脚本

目标：找出服务端能同时接受的最大 gRPC 双向流数量。

原理：
- 每条流建立后只发一帧音频触发 inited，然后持续保活（不发音频、不关闭流）
- 多条流并发保持打开状态，直到服务端开始拒绝新流
- 记录每条流的建立结果、失败时的 gRPC 错误码

用法：
    # 测试本地服务（默认探测到 100 条流）
    uv run python src/grpc_stream_limit_test.py

    # 指定目标并发数上限
    uv run python src/grpc_stream_limit_test.py --max-streams 200

    # 使用配置文件中的认证信息
    uv run python src/grpc_stream_limit_test.py --config config.yaml

    # 快速模式：不等 inited，流建立即计数（测试纯 TCP 层上限）
    uv run python src/grpc_stream_limit_test.py --no-wait-inited

    # gRPC buffer 压力测试：最小 pipeline + 持续 echo 音频写入
    # 服务端须以 ENVIRONMENT=local TEST_PIPELINE_LIMIT=1 启动
    uv run python src/grpc_stream_limit_test.py --echo-load --hold-secs 30
"""

import argparse
import asyncio
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import grpc
import grpc.aio
import yaml

# 添加 src 目录到 path
sys.path.insert(0, str(Path(__file__).parent))

from proto_generated import (
    voice_agent_transport_pb2,
    voice_agent_transport_pb2_grpc,
)
from google.protobuf.struct_pb2 import Struct


# ── 默认参数 ──────────────────────────────────────────────────────────────
DEFAULT_HOST = "localhost:8085"
DEFAULT_MAX_STREAMS = 100
DEFAULT_BATCH_SIZE = 5       # 每批同时建立的流数量
DEFAULT_CONNECT_TIMEOUT = 10.0  # 单条流建立/inited 等待超时（秒）
DEFAULT_HOLD_SECS = 30.0     # 所有流建立完成后保持打开的时长（秒）


@dataclass
class StreamResult:
    stream_id: int
    success: bool
    error_code: Optional[str] = None
    error_detail: Optional[str] = None
    connect_ms: float = 0.0      # 流建立耗时（到收到 inited）
    rejected: bool = False       # 是否被服务端明确拒绝（RESOURCE_EXHAUSTED 等）
    # echo-load 模式专用
    frames_sent: int = 0
    frames_recv: int = 0
    send_stall_ms: float = 0.0   # 最大单帧发送阻塞时长（超过 frame_ms 即为 backpressure）
    recv_lag_ms: float = 0.0     # 最大收帧滞后（从发出到收到的 RTT - 理论帧时长）


@dataclass
class TestSummary:
    host: str
    max_streams_attempted: int
    streams_succeeded: int
    streams_failed: int
    first_failure_at: Optional[int] = None
    error_breakdown: dict = field(default_factory=dict)
    connect_ms_samples: list = field(default_factory=list)
    echo_results: list = field(default_factory=list)  # echo-load 模式：成功流的 StreamResult 列表


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def make_metadata(config: Optional[dict], stream_id: int, mode: str = "standard") -> list:
    """构造 gRPC metadata，优先使用配置文件中的认证信息。"""
    if config:
        auth_cfg = config.get("auth", {})
        grpc_cfg = config.get("grpc", {})
        token = auth_cfg.get("token", "")
        # token 字段可能已包含 "Bearer " 前缀
        if token.startswith("Bearer "):
            auth_value = token
        else:
            auth_value = f"Bearer {token}"
        return [
            ("authorization", auth_value),
            ("x-device-id", grpc_cfg.get("device_id", f"limit-test-{stream_id}")),
            ("x-family-id", auth_cfg.get("family_id", "test-family")),
            ("x-user-id", auth_cfg.get("user_id", "")),
            ("x-timezone", auth_cfg.get("timezone", "Asia/Shanghai")),
            ("x-source", "limit_test"),
            ("x-conversation-id", str(uuid.uuid4())),
            ("x-audio-encoding", grpc_cfg.get("audio_encoding", "pcm")),
            ("x-mode", mode),
            ("x-follow-up-mode-on", "off"),
            ("x-voice-key", ""),
            ("x-stt-key", grpc_cfg.get("stt_key", "")),
            ("x-enable-analyze-frame-rate", "false"),
            ("x-enable-agent", grpc_cfg.get("enable_agent", "true")),
        ]
    else:
        # 无配置：不带认证（需要服务端开启 USE_MOCK_AUTH=true）
        return [
            ("authorization", "Bearer mock-token"),
            ("x-device-id", f"limit-test-{stream_id}"),
            ("x-family-id", "test-family-id"),
            ("x-user-id", "test-user-id"),
            ("x-timezone", "Asia/Shanghai"),
            ("x-source", "limit_test"),
            ("x-conversation-id", str(uuid.uuid4())),
            ("x-audio-encoding", "pcm"),
            ("x-mode", mode),
            ("x-follow-up-mode-on", "off"),
            ("x-voice-key", ""),
            ("x-stt-key", ""),
            ("x-enable-analyze-frame-rate", "false"),
            ("x-enable-agent", "true"),
        ]


async def hold_stream(
    stream_id: int,
    host: str,
    config: Optional[dict],
    connect_timeout: float,
    hold_event: asyncio.Event,
    result_queue: asyncio.Queue,
    wait_inited: bool,
    echo_load: bool = False,
):
    """
    建立一条 gRPC 双向流并保持打开，直到 hold_event 被触发。

    echo_load=False（默认）：发一帧静音触发 inited，之后静默保活。
    echo_load=True：发 x-mode=echo，inited 后按实时速率持续发静音帧，
                    记录每帧发送耗时和收到 echo 回来的帧数，用于判断
                    gRPC send buffer 是否积压。
    """
    t0 = time.perf_counter()
    result = StreamResult(stream_id=stream_id, success=False)

    channel = None
    try:
        if config and config.get("grpc", {}).get("tls", False):
            channel = grpc.aio.secure_channel(host, grpc.ssl_channel_credentials())
        else:
            channel = grpc.aio.insecure_channel(host)

        stub = voice_agent_transport_pb2_grpc.VoiceAgentTransportStub(channel)
        mode = "echo" if echo_load else "standard"
        metadata = make_metadata(config, stream_id, mode=mode)

        audio_meta = Struct()
        audio_meta.update({"sample_rate": 16000, "channels": 1})

        # 20ms @ 16kHz/16bit/mono = 640 bytes
        FRAME_BYTES = 640
        FRAME_MS = 0.020
        silent_frame = bytes(FRAME_BYTES)

        send_q: asyncio.Queue = asyncio.Queue()
        await send_q.put(silent_frame)  # 第一帧触发 pipeline 初始化

        async def request_iter():
            while True:
                item = await send_q.get()
                if item is None:
                    return
                if isinstance(item, voice_agent_transport_pb2.StreamMessage):
                    yield item
                else:
                    yield voice_agent_transport_pb2.StreamMessage(
                        type="audio",
                        raw=item,
                        data=audio_meta,
                    )

        stream = stub.Stream(request_iter(), metadata=metadata)

        inited_event = asyncio.Event()
        frames_recv = 0
        max_send_stall_ms = 0.0
        max_recv_lag_ms = 0.0
        # echo-load：记录每帧的发送时间戳，用于计算 RTT
        send_timestamps: list = []  # list of (frame_seq, sent_at)

        async def recv_loop():
            nonlocal frames_recv, max_recv_lag_ms
            try:
                async for response in stream:
                    if response.type == "rtvi_message":
                        fields = response.data.fields
                        msg_type = fields.get("type")
                        if msg_type:
                            type_str = msg_type.string_value
                            if type_str == "server-message":
                                inner = fields.get("data")
                                if inner and inner.struct_value:
                                    inner_type = inner.struct_value.fields.get("type")
                                    if inner_type and inner_type.string_value in ("inited", "bot-ready"):
                                        inited_event.set()
                            elif type_str in ("inited", "bot-ready"):
                                inited_event.set()
                    elif response.type == "audio" and echo_load:
                        frames_recv += 1
                        now = time.perf_counter()
                        # 匹配最早一个尚未 ack 的发送时间戳
                        if send_timestamps:
                            _, sent_at = send_timestamps.pop(0)
                            rtt_ms = (now - sent_at) * 1000
                            # RTT 减去一帧的理论处理时间（FRAME_MS），剩余为滞后
                            lag_ms = max(0.0, rtt_ms - FRAME_MS * 1000)
                            if lag_ms > max_recv_lag_ms:
                                max_recv_lag_ms = lag_ms
            except grpc.aio.AioRpcError:
                pass
            except asyncio.CancelledError:
                pass
            finally:
                inited_event.set()

        recv_task = asyncio.create_task(recv_loop())

        if wait_inited:
            try:
                await asyncio.wait_for(inited_event.wait(), timeout=connect_timeout)
            except asyncio.TimeoutError:
                result.error_code = "INITED_TIMEOUT"
                result.error_detail = f"等待 inited 超时 ({connect_timeout}s)"
                recv_task.cancel()
                await send_q.put(None)
                await result_queue.put(result)
                return

        result.connect_ms = (time.perf_counter() - t0) * 1000
        result.success = True
        await result_queue.put(result)

        if echo_load:
            # 按实时速率持续发静音帧，直到 hold_event
            frame_seq = 0
            while not hold_event.is_set():
                t_send = time.perf_counter()
                await send_q.put(silent_frame)
                elapsed_ms = (time.perf_counter() - t_send) * 1000
                # put() 本身应该是即时的；如果阻塞说明 send_q 积压（理论上无界）
                # 更重要的是观察整个帧周期是否超时
                if elapsed_ms > FRAME_MS * 1000 * 2:
                    if elapsed_ms > max_send_stall_ms:
                        max_send_stall_ms = elapsed_ms

                send_timestamps.append((frame_seq, t_send))
                frame_seq += 1
                result.frames_sent = frame_seq

                # 限速：每帧等待剩余的帧周期
                sleep_secs = FRAME_MS - (time.perf_counter() - t_send)
                if sleep_secs > 0:
                    await asyncio.sleep(sleep_secs)
        else:
            await hold_event.wait()

        # 记录 echo-load 统计
        result.frames_sent = getattr(result, "frames_sent", 0)
        result.frames_recv = frames_recv
        result.send_stall_ms = max_send_stall_ms
        result.recv_lag_ms = max_recv_lag_ms

        # 优雅关闭
        await send_q.put(None)
        recv_task.cancel()
        try:
            await asyncio.wait_for(recv_task, timeout=2.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass

    except grpc.aio.AioRpcError as e:
        result.error_code = str(e.code())
        result.error_detail = e.details() or ""
        result.rejected = e.code() in (
            grpc.StatusCode.RESOURCE_EXHAUSTED,
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.PERMISSION_DENIED,
        )
        await result_queue.put(result)
    except Exception as e:
        result.error_code = type(e).__name__
        result.error_detail = str(e)
        await result_queue.put(result)
    finally:
        if channel:
            try:
                await channel.close()
            except Exception:
                pass


async def run_test(
    host: str,
    max_streams: int,
    batch_size: int,
    connect_timeout: float,
    hold_secs: float,
    config: Optional[dict],
    wait_inited: bool,
    echo_load: bool = False,
) -> TestSummary:
    """
    主测试逻辑：逐批建立流，统计最终能同时保持的最大数量。

    策略：
    - 每批 batch_size 条流并发建立
    - 每批建完后立即打印当前状态
    - 直到 max_streams 全部尝试完或出现连续失败
    - 所有成功的流保持打开 hold_secs 秒后统一关闭
    """
    summary = TestSummary(
        host=host,
        max_streams_attempted=max_streams,
        streams_succeeded=0,
        streams_failed=0,
    )

    hold_event = asyncio.Event()
    result_queue: asyncio.Queue = asyncio.Queue()
    stream_tasks = []

    print(f"\n{'='*60}")
    print(f"目标: {host}")
    print(f"计划建立流数: {max_streams}  批大小: {batch_size}")
    print(f"保持时长: {hold_secs}s  等待inited: {wait_inited}  echo负载: {echo_load}")
    if echo_load:
        print(f"  [echo-load 模式] 服务端须以 ENVIRONMENT=local TEST_PIPELINE_LIMIT=1 启动")
    print(f"{'='*60}")

    consecutive_fails = 0
    FAIL_ABORT_THRESHOLD = batch_size * 2  # 连续失败超过此数则终止

    for batch_start in range(0, max_streams, batch_size):
        batch_end = min(batch_start + batch_size, max_streams)
        batch_ids = list(range(batch_start, batch_end))

        # 并发启动这一批
        for sid in batch_ids:
            task = asyncio.create_task(
                hold_stream(
                    stream_id=sid,
                    host=host,
                    config=config,
                    connect_timeout=connect_timeout,
                    hold_event=hold_event,
                    result_queue=result_queue,
                    wait_inited=wait_inited,
                    echo_load=echo_load,
                )
            )
            stream_tasks.append(task)

        # 等待这批全部回报结果
        batch_results = []
        for _ in batch_ids:
            r = await asyncio.wait_for(result_queue.get(), timeout=connect_timeout + 5)
            batch_results.append(r)

        batch_ok = sum(1 for r in batch_results if r.success)
        batch_fail = len(batch_results) - batch_ok

        summary.streams_succeeded += batch_ok
        summary.streams_failed += batch_fail

        for r in batch_results:
            if not r.success:
                if summary.first_failure_at is None:
                    summary.first_failure_at = r.stream_id
                code = r.error_code or "UNKNOWN"
                summary.error_breakdown[code] = summary.error_breakdown.get(code, 0) + 1
                consecutive_fails += 1
            else:
                consecutive_fails = 0
                summary.connect_ms_samples.append(r.connect_ms)
                if echo_load:
                    summary.echo_results.append(r)

        # 打印批次摘要
        ok_ids = [r.stream_id for r in batch_results if r.success]
        fail_info = [
            f"#{r.stream_id}:{r.error_code}"
            for r in batch_results
            if not r.success
        ]
        print(
            f"  流 {batch_start:3d}~{batch_end-1:3d} │ "
            f"成功 {batch_ok}/{len(batch_ids)} │ "
            f"累计成功 {summary.streams_succeeded:3d} │ "
            f"{'失败: ' + ', '.join(fail_info) if fail_info else '全部OK'}"
        )

        if consecutive_fails >= FAIL_ABORT_THRESHOLD:
            print(f"\n  连续失败 {consecutive_fails} 次，终止建流（服务端已达上限）")
            break

    print(f"\n{'─'*60}")
    print(f"建流完成，共成功 {summary.streams_succeeded} 条流同时保持打开")

    if summary.streams_succeeded > 0:
        if hold_secs > 0:
            print(f"保持 {hold_secs}s 后关闭所有流...")
            await asyncio.sleep(hold_secs)
        print("关闭所有流...")
        hold_event.set()

    # 等待所有 task 结束
    if stream_tasks:
        await asyncio.gather(*stream_tasks, return_exceptions=True)

    return summary


def print_summary(summary: TestSummary):
    print(f"\n{'='*60}")
    print(f"  测试结论")
    print(f"{'='*60}")
    print(f"  目标服务       : {summary.host}")
    print(f"  尝试建立总流数  : {summary.max_streams_attempted}")
    print(f"  成功建立流数    : {summary.streams_succeeded}")
    print(f"  失败流数        : {summary.streams_failed}")

    if summary.first_failure_at is not None:
        print(f"  首次失败于流 #  : {summary.first_failure_at}")

    if summary.error_breakdown:
        print(f"  失败原因分布:")
        for code, count in sorted(summary.error_breakdown.items(), key=lambda x: -x[1]):
            print(f"    {code:40s} × {count}")

    if summary.connect_ms_samples:
        samples = sorted(summary.connect_ms_samples)
        n = len(samples)
        p50 = samples[int(n * 0.50)]
        p90 = samples[min(int(n * 0.90), n - 1)]
        print(f"  连接耗时 (inited延迟):")
        print(f"    P50 = {p50:.0f}ms   P90 = {p90:.0f}ms   样本数 = {n}")

    print(f"\n  结论:")
    if summary.streams_failed == 0:
        print(f"  ✓ 全部 {summary.streams_succeeded} 条流建立成功，服务端未出现拒绝。")
        print(f"    → 实际上限 > {summary.streams_succeeded}，建议用 --max-streams 调大再测。")
    elif summary.first_failure_at is not None:
        print(f"  ! 流 #{summary.first_failure_at} 开始出现失败")
        print(f"    → 入口上限约在 {summary.first_failure_at} 附近")
        rejected = summary.error_breakdown.get("StatusCode.RESOURCE_EXHAUSTED", 0)
        if rejected > 0:
            print(f"    → RESOURCE_EXHAUSTED 错误 {rejected} 次，确认是服务端主动拒绝（非网络问题）")
    print(f"{'='*60}")


def print_echo_stats(results_by_stream: list):
    """打印 echo-load 模式下的发送/接收统计，判断哪一侧积压。"""
    sent_total = sum(r.frames_sent for r in results_by_stream)
    recv_total = sum(r.frames_recv for r in results_by_stream)
    drop_ratio = (sent_total - recv_total) / sent_total if sent_total > 0 else 0

    stall_samples = [r.send_stall_ms for r in results_by_stream if r.send_stall_ms > 0]
    lag_samples = [r.recv_lag_ms for r in results_by_stream if r.recv_lag_ms > 0]

    print(f"\n{'='*60}")
    print(f"  echo-load 统计（gRPC buffer 压力分析）")
    print(f"{'='*60}")
    print(f"  总发送帧数          : {sent_total}")
    print(f"  总接收 echo 帧数    : {recv_total}")
    print(f"  丢帧率              : {drop_ratio:.1%}")
    print()
    if stall_samples:
        stall_samples.sort()
        n = len(stall_samples)
        print(f"  发送侧 backpressure（send_stall）:")
        print(f"    出现次数={n}  P50={stall_samples[int(n*0.5)]:.1f}ms  max={stall_samples[-1]:.1f}ms")
        print(f"    ★ 若 max >> 40ms，说明客户端 → 服务端写入被 gRPC send buffer 阻塞")
    else:
        print(f"  发送侧 backpressure：未检测到（发送队列无明显阻塞）")
    print()
    if lag_samples:
        lag_samples.sort()
        n = len(lag_samples)
        print(f"  接收侧滞后（recv_lag）:")
        print(f"    出现次数={n}  P50={lag_samples[int(n*0.5)]:.1f}ms  max={lag_samples[-1]:.1f}ms")
        print(f"    ★ 若 max >> 100ms 且丢帧率 > 0，说明服务端 → 客户端 send buffer 积压")
    else:
        print(f"  接收侧滞后：未检测到（echo 回来的帧无明显延迟）")
    print()
    if drop_ratio > 0.05:
        print(f"  ⚠ 丢帧率 {drop_ratio:.1%} > 5%，强烈怀疑服务端 send buffer 满后丢弃了帧")
    elif drop_ratio == 0 and not stall_samples and not lag_samples:
        print(f"  ✓ 发送和接收均无积压，gRPC buffer 不是瓶颈")
    print(f"{'='*60}")


async def main_async(args):
    config = None
    if args.config:
        config = load_config(args.config)

    host = args.host
    if not host and config:
        host = config.get("grpc", {}).get("host", DEFAULT_HOST)
    if not host:
        host = DEFAULT_HOST

    summary = await run_test(
        host=host,
        max_streams=args.max_streams,
        batch_size=args.batch_size,
        connect_timeout=args.connect_timeout,
        hold_secs=args.hold_secs,
        config=config,
        wait_inited=not args.no_wait_inited,
        echo_load=args.echo_load,
    )
    print_summary(summary)

    if args.echo_load and summary.echo_results:
        print_echo_stats(summary.echo_results)


def main():
    parser = argparse.ArgumentParser(
        description="gRPC 并发流上限探测工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--host", default="", help=f"gRPC 服务地址 (默认: {DEFAULT_HOST})")
    parser.add_argument("--config", default="config.yaml", help="配置文件路径（用于读取认证信息）")
    parser.add_argument("--max-streams", type=int, default=DEFAULT_MAX_STREAMS,
                        help=f"最大尝试建立的流数量 (默认: {DEFAULT_MAX_STREAMS})")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                        help=f"每批并发建立的流数量 (默认: {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--connect-timeout", type=float, default=DEFAULT_CONNECT_TIMEOUT,
                        help=f"单流连接/inited 超时秒数 (默认: {DEFAULT_CONNECT_TIMEOUT})")
    parser.add_argument("--hold-secs", type=float, default=DEFAULT_HOLD_SECS,
                        help=f"所有流建完后保持打开的秒数 (默认: {DEFAULT_HOLD_SECS})")
    parser.add_argument("--no-wait-inited", action="store_true",
                        help="不等待 inited 事件，流建立即计数（测试纯 TCP/HTTP2 握手层上限）")
    parser.add_argument("--echo-load", action="store_true",
                        help="gRPC buffer 压力测试：inited 后持续发音频帧并接收 echo，"
                             "检测 send buffer 是否积压。服务端须以 "
                             "ENVIRONMENT=local TEST_PIPELINE_LIMIT=1 启动")
    args = parser.parse_args()

    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()

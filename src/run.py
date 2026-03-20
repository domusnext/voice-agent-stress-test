#!/usr/bin/env python3
"""run.py — 压测主入口（持续并发模型：Semaphore 令牌池）"""

import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict
from datetime import datetime, timezone

import yaml

from session import StressTestSession, SessionResult


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def run_single_session(kwargs) -> dict:
    """在子进程中执行单个会话（进程池的 worker 函数）"""
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    session = StressTestSession(**kwargs)
    result = session.run()
    return asdict(result)


class AtomicCounter:
    """线程安全的自增计数器，用于动态分配 session ID"""

    def __init__(self, start=0):
        self._value = start
        self._lock = threading.Lock()

    def next(self):
        with self._lock:
            val = self._value
            self._value += 1
            return val


def run_one_level(
    level_name: str,
    concurrency: int,
    duration_secs: float,
    config: dict,
    ramp_up_secs: float,
) -> dict:
    """执行一个并发级别的测试（持续并发模型）"""
    print(f"\n{'='*60}")
    print(f"开始测试: {level_name} (并发={concurrency}, 持续={duration_secs}s)")
    print(f"{'='*60}")

    test_cfg = config["test"]
    auth_cfg = config["auth"]
    transport_type = config.get("transport", "daily")

    counter = AtomicCounter()
    sem = threading.Semaphore(concurrency)
    results = []
    results_lock = threading.Lock()

    def make_session_kwargs():
        sid = counter.next()
        base = {
            "session_id": sid,
            "transport_type": transport_type,
            "audio_file": test_cfg["audio_file"],
            "max_wait": test_cfg["max_wait_response"],
            "sample_rate": test_cfg.get("sample_rate", 16000),
        }

        if transport_type == "daily":
            base["transport_kwargs"] = {
                "server_url": config["server"]["url"],
                "authorization": auth_cfg["token"],
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
                "user_id": auth_cfg.get("user_id", ""),
                "timezone": auth_cfg.get("timezone", "Asia/Shanghai"),
            }

        return base

    def on_session_done(future):
        """session 完成回调：收集结果 + 释放令牌"""
        try:
            result = future.result(timeout=300)
            with results_lock:
                results.append(result)
            sid = result["session_id"]
            ok = result["success"]
            status = "OK" if ok else f"FAIL: {result['error'][:50]}"
            print(f"  Session {sid:3d}: {status}")
        except Exception as e:
            print(f"  Session ???: 进程异常 — {e}")
        finally:
            sem.release()

    # 记录测试时间窗口（UTC，用于后续 CloudWatch 查询）
    test_start_utc = datetime.now(timezone.utc).isoformat()
    deadline = time.monotonic() + duration_secs

    # max_workers 限制：避免进程过多导致系统不稳定
    max_workers = min(concurrency, os.cpu_count() * 4 or 32)

    ramp_delay = ramp_up_secs / max(concurrency, 1)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        submitted = 0
        while time.monotonic() < deadline:
            sem.acquire()  # 阻塞，直到有空位

            # 再次检查是否超时（acquire 可能阻塞了一段时间）
            if time.monotonic() >= deadline:
                sem.release()
                break

            kwargs = make_session_kwargs()
            future = executor.submit(run_single_session, kwargs)
            future.add_done_callback(on_session_done)
            submitted += 1

            # Ramp-up：仅对初始 N 个 session 生效
            if submitted <= concurrency and submitted < concurrency:
                time.sleep(ramp_delay)

        # Drain：等待所有 in-flight session 完成
        # 获取全部 N 个 semaphore 令牌 = 所有 session 都已完成
        print(f"\n  持续时间到期，等待 in-flight session 完成...")
        for _ in range(concurrency):
            sem.acquire()

    test_end_utc = datetime.now(timezone.utc).isoformat()

    return {
        "level_name": level_name,
        "concurrency": concurrency,
        "duration_secs": duration_secs,
        "total_sessions": len(results),
        "test_start_utc": test_start_utc,
        "test_end_utc": test_end_utc,
        "results": results,
    }


def print_level_summary(level_data: dict):
    """打印单个级别的摘要统计"""
    results = level_data["results"]
    name = level_data["level_name"]
    concurrency = level_data["concurrency"]
    duration_secs = level_data.get("duration_secs", 0)
    total_sessions = level_data.get("total_sessions", len(results))

    success_results = [r for r in results if r["success"]]

    if not success_results:
        print(f"\n[{name}] 无成功的 session 数据")
        return

    ttfa_values = [r["client_ttfa_ms"] for r in success_results if r["client_ttfa_ms"] > 0]
    e2e_values = [r["client_e2e_ms"] for r in success_results if r["client_e2e_ms"] > 0]

    success_count = len(success_results)
    throughput = total_sessions / duration_secs if duration_secs > 0 else 0

    def percentile(data, p):
        if not data:
            return 0
        data = sorted(data)
        k = (len(data) - 1) * p / 100
        f = int(k)
        c = min(f + 1, len(data) - 1)
        return data[f] + (data[c] - data[f]) * (k - f)

    print(f"\n┌─────────────────────────────────────────────────────┐")
    print(f"│ {name:^51s} │")
    print(f"├──────────────────────┬──────────────────────────────┤")
    print(f"│ 并发度               │ {concurrency:>28d} │")
    print(f"│ 持续时间             │ {duration_secs:>25.0f} s │")
    print(f"│ 总会话数             │ {total_sessions:>28d} │")
    print(f"│ 成功会话             │ {f'{success_count}/{total_sessions}':>28s} │")
    print(f"│ 吞吐量               │ {throughput:>23.2f} sess/s │")
    print(f"├──────────────────────┼──────────────────────────────┤")
    if ttfa_values:
        print(f"│ Client TTFA P50      │ {percentile(ttfa_values, 50):>24.0f} ms │")
        print(f"│ Client TTFA P90      │ {percentile(ttfa_values, 90):>24.0f} ms │")
        print(f"│ Client TTFA P99      │ {percentile(ttfa_values, 99):>24.0f} ms │")
    if e2e_values:
        print(f"│ Client E2E P50       │ {percentile(e2e_values, 50):>24.0f} ms │")
        print(f"│ Client E2E P90       │ {percentile(e2e_values, 90):>24.0f} ms │")
        print(f"│ Client E2E P99       │ {percentile(e2e_values, 99):>24.0f} ms │")
    print(f"└──────────────────────┴──────────────────────────────┘")
    print(f"  时间窗口: {level_data['test_start_utc']} → {level_data['test_end_utc']}")


def main():
    parser = argparse.ArgumentParser(description="语音 Agent 压力测试")
    parser.add_argument(
        "--config", default="config.yaml", help="配置文件路径"
    )
    parser.add_argument(
        "--level",
        type=str,
        default=None,
        help="只运行指定级别 (baseline/medium/high/peak)，不指定则顺序执行全部",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=None,
        help="直接指定并发度（覆盖配置文件中的 levels）",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=None,
        help="测试持续时间（秒），配合 --concurrency 使用",
    )
    parser.add_argument(
        "--output",
        default="reports",
        help="报告输出目录",
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="压测完成后自动采集 CloudWatch 指标并生成报告",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    os.makedirs(args.output, exist_ok=True)

    # 确定要执行的级别
    if args.concurrency:
        duration = args.duration or 120
        levels = [{"name": f"custom-{args.concurrency}", "concurrency": args.concurrency, "duration_secs": duration}]
    elif args.level:
        levels = [l for l in config["levels"] if l["name"] == args.level]
        if not levels:
            print(f"未找到级别: {args.level}")
            sys.exit(1)
    else:
        levels = config["levels"]

    all_level_data = []
    ramp_up = config["test"].get("ramp_up_secs", 10.0)
    cooldown = config["test"].get("cooldown_between_levels", 60)

    for level_idx, level in enumerate(levels):
        level_data = run_one_level(
            level_name=level["name"],
            concurrency=level["concurrency"],
            duration_secs=level["duration_secs"],
            config=config,
            ramp_up_secs=ramp_up,
        )
        all_level_data.append(level_data)
        print_level_summary(level_data)

        # 级别间冷却
        if level_idx < len(levels) - 1:
            print(f"\n冷却中... ({cooldown}s)")
            time.sleep(cooldown)

    # 保存原始数据
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(args.output, f"stress_test_{timestamp}.json")
    with open(report_path, "w") as f:
        json.dump(all_level_data, f, indent=2, ensure_ascii=False)
    print(f"\n原始数据已保存: {report_path}")

    # 打印总览
    print(f"\n{'='*60}")
    print(f"全部测试完成 — 总览")
    print(f"{'='*60}")
    for ld in all_level_data:
        print_level_summary(ld)

    # 自动采集指标 + 生成报告
    if args.report:
        print("\n等待 CloudWatch 日志同步 (15s)...")
        time.sleep(15)

        from collector import create_logs_client, collect_metrics, parse_iso_to_epoch
        from reporter import generate_report

        logs_client = create_logs_client(config)
        log_group = config["cloudwatch"]["log_group"]

        print("\n开始采集 CloudWatch 指标...")
        for ld in all_level_data:
            label = ld["level_name"]
            start_epoch = parse_iso_to_epoch(ld["test_start_utc"])
            end_epoch = parse_iso_to_epoch(ld["test_end_utc"])

            print(f"\n采集: {label}")
            metrics = collect_metrics(logs_client, log_group, start_epoch, end_epoch)

            metrics_path = os.path.join(
                args.output,
                f"metrics_{label}_{datetime.now().strftime('%H%M%S')}.json",
            )
            with open(metrics_path, "w") as f:
                json.dump(
                    {
                        "label": label,
                        "start": ld["test_start_utc"],
                        "end": ld["test_end_utc"],
                        "metrics": metrics,
                    },
                    f,
                    indent=2,
                    ensure_ascii=False,
                )
            print(f"  指标已保存: {metrics_path}")

        # 生成报告
        report_md = generate_report(report_path, args.output, config)
        report_file = os.path.join(args.output, f"report_{timestamp}.md")
        with open(report_file, "w") as f:
            f.write(report_md)
        print(f"\n报告已生成: {report_file}")
    else:
        # 提示手动采集
        print(f"\n下一步：使用 collector.py 从 CloudWatch 采集服务端指标：")
        for ld in all_level_data:
            print(
                f"  python src/collector.py --config {args.config}"
                f" --start '{ld['test_start_utc']}'"
                f" --end '{ld['test_end_utc']}'"
                f" --label '{ld['level_name']}'"
                f" --data {report_path}"
            )


if __name__ == "__main__":
    main()

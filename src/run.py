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
    ramp_mode: bool = False,
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
                "voice_key": grpc_cfg.get("voice_key", ""),
                "stt_key": grpc_cfg.get("stt_key", ""),
                "enable_analyze_frame_rate": "true" if ramp_mode else "false",
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


# ──────────── ramp 模式：辅助函数 ────────────

def _percentile(data: list, p: int) -> float:
    if not data:
        return 0.0
    s = sorted(data)
    k = (len(s) - 1) * p / 100
    f = int(k)
    c = min(f + 1, len(s) - 1)
    return round(s[f] + (s[c] - s[f]) * (k - f), 2)


def aggregate_level(level_data: dict) -> dict:
    """把一个级别的所有会话结果聚合为 §4.2 客户端侧画像。"""
    results = level_data["results"]
    succ = [r for r in results if r["success"]]
    ttfa = [r["client_ttfa_ms"] for r in succ if r.get("client_ttfa_ms", 0) > 0]
    e2e  = [r["client_e2e_ms"]  for r in succ if r.get("client_e2e_ms", 0)  > 0]
    fw   = sum(r.get("frame_windows", 0)     for r in succ)
    lfw  = sum(r.get("low_fps_windows", 0)   for r in succ)
    pcw  = sum(r.get("poor_conn_windows", 0) for r in succ)
    all_fps = [fps for r in succ for fps in r.get("fps_samples", [])]
    pace = [r.get("send_pace_ratio", 1.0) for r in succ]
    total = level_data["total_sessions"]
    return {
        "concurrency": level_data["concurrency"],
        "client_ttfa_p50": _percentile(ttfa, 50),
        "client_ttfa_p99": _percentile(ttfa, 99),
        "client_e2e_p99":  _percentile(e2e, 99),
        "success_rate": (len(succ) / total) if total else 0.0,
        "frame_windows": fw,
        "low_fps_windows": lfw,
        "poor_conn_windows": pcw,
        "low_fps_ratio":   (lfw / fw) if fw else 0.0,
        "poor_conn_ratio": (pcw / fw) if fw else 0.0,
        "fps_p50": _percentile(all_fps, 50),
        "fps_p10": _percentile(all_fps, 10),
        "send_pace_ratio_avg": round(sum(pace) / len(pace), 3) if pace else 1.0,
    }


def build_baseline_band(samples: dict, cfg: dict) -> dict:
    """§4.0：由 concurrency=1 多轮样本构建各指标基准带。"""
    band = {}
    for metric, xs in samples.items():
        if not xs:
            continue
        xs_sorted = sorted(xs)
        center = _percentile(xs_sorted, 50)
        mad_vals = sorted(abs(x - center) for x in xs)
        mad = _percentile(mad_vals, 50)
        p90 = _percentile(xs_sorted, 90)
        band_high = max(
            center * (1 + cfg["band_rel_margin"]),
            p90,
            center + cfg["band_mad_k"] * mad,
        )
        band[metric] = {
            "center": round(center, 2),
            "p90": round(p90, 2),
            "mad": round(mad, 2),
            "band_high": round(band_high, 2),
        }
    return band


def _server_val(sm: dict, key: str, subkey: str, fallback: float) -> float:
    """从 server_metrics dict 安全取数值，失败时用 fallback。"""
    try:
        return float(sm[key]["data"][subkey])
    except (KeyError, TypeError, ValueError):
        return fallback


def _session_lost_ratio(sm: dict) -> float:
    try:
        d = sm["sessions"]["data"]
        started = int(d.get("started", 0))
        stopped = int(d.get("stopped", 0))
        if started == 0:
            return 0.0
        return max(0, started - stopped) / started
    except (KeyError, TypeError):
        return 0.0


def _error_counts(sm: dict) -> tuple:
    stt_err = tts_err = 0
    try:
        rows = sm["errors"]["data"]
        if isinstance(rows, list):
            for row in rows:
                if "stt_error" in row.get("event_type", ""):
                    stt_err += int(row.get("error_count", 0))
                elif "tts_error" in row.get("event_type", ""):
                    tts_err += int(row.get("error_count", 0))
    except (KeyError, TypeError):
        pass
    return stt_err, tts_err


def _any_component_over_band(sm: dict, band: dict, cfg: dict) -> bool:
    ratio = cfg["degrade_band_ratio"]
    for metric_key, band_key in [("stt", "stt_p99"), ("llm_ttft", "llm_ttft_p99"), ("tts_ttfb", "tts_ttfb_p99")]:
        val = _server_val(sm, metric_key, "p99", 0.0)
        if val > 0 and band_key in band:
            if val > band[band_key]["band_high"] * ratio:
                return True
    return False


def _attribute(status: str, fr_degraded: bool, sm: dict) -> str:
    """§4.4 帧率归因矩阵。"""
    exp_ok = status == "健康"
    if exp_ok and not fr_degraded:
        return "一切健康"
    if exp_ok and fr_degraded:
        return "上行受限（网络/压测机），服务端处理仍快，服务有余量"
    if not exp_ok and not fr_degraded:
        return "服务端处理瓶颈：摄入正常但响应变慢，查 ttfa_breakdown 定位 STT/LLM/TTS"
    return "交叉印证服务端过载：上行摄入与处理同时被拖慢"


def judge_level(agg: dict, sm: dict, band: dict, cfg: dict) -> dict:
    """§4.3/§4.4/§4.5：主判据判定 + 帧率归因。"""
    # 可信度护栏优先
    if agg["send_pace_ratio_avg"] > cfg["send_pace_ratio_unreliable"]:
        return {
            "status": "不可信",
            "status_reason": f"send_pace_ratio={agg['send_pace_ratio_avg']:.2f} 超阈，疑似压测机/网络瓶颈，非服务端承载上限",
            "attribution": "数据不可信",
            "frame_rate_degraded": False,
        }

    # 主判据取值：服务端优先，否则用客户端
    ttfa_p99 = _server_val(sm, "ttfa", "p99", agg["client_ttfa_p99"])
    e2e_p99  = _server_val(sm, "e2e",  "p99", agg["client_e2e_p99"])
    succ = agg["success_rate"]
    lost = _session_lost_ratio(sm)
    stt_err, tts_err = _error_counts(sm)

    bh_ttfa = band.get("ttfa_p99", {}).get("band_high", float("inf"))
    bh_e2e  = band.get("e2e_p99",  {}).get("band_high", float("inf"))

    status = "健康"
    reasons = []

    # 超载判断
    if ttfa_p99 > bh_ttfa * cfg["fail_band_ratio"]:
        status = "超载"
        reasons.append(f"TTFA越带{ttfa_p99/bh_ttfa:.2f}x(>{cfg['fail_band_ratio']}x)")
    if e2e_p99 > bh_e2e * cfg["fail_band_ratio"]:
        status = "超载"
        reasons.append(f"E2E越带{e2e_p99/bh_e2e:.2f}x")
    if succ < cfg["session_success_fail"]:
        status = "超载"
        reasons.append(f"成功率{succ:.0%}<{cfg['session_success_fail']:.0%}")
    if lost >= cfg["session_lost_ratio_fail"]:
        status = "超载"
        reasons.append(f"会话丢失{lost:.0%}>={cfg['session_lost_ratio_fail']:.0%}")

    # 预警判断（未超载时）
    if status == "健康":
        if ttfa_p99 > bh_ttfa * cfg["degrade_band_ratio"]:
            status = "预警"
            reasons.append(f"TTFA越基准带{ttfa_p99/bh_ttfa:.2f}x")
        if _any_component_over_band(sm, band, cfg):
            status = "预警"
            reasons.append("组件越带")
        if succ < cfg["session_success_degrade"]:
            status = "预警"
            reasons.append(f"成功率{succ:.0%}<{cfg['session_success_degrade']:.0%}")
        if stt_err + tts_err >= cfg["error_count_fail"]:
            status = "预警"
            reasons.append(f"错误数{stt_err+tts_err}")

    # 帧率归因
    fr_degraded = (
        agg["low_fps_ratio"] >= cfg["low_fps_ratio_attn"]
        or agg["poor_conn_ratio"] >= cfg["poor_conn_ratio_attn"]
    )
    attribution = _attribute(status, fr_degraded, sm)

    return {
        "status": status,
        "status_reason": "；".join(reasons) if reasons else "主判据在基准带内",
        "attribution": attribution,
        "frame_rate_degraded": fr_degraded,
        "ttfa_p99_used": ttfa_p99,
        "ttfa_band_high": bh_ttfa,
    }


def _collect_server_metrics(config: dict, level_data: dict, src: str) -> dict:
    """按 source 采集服务端指标，CW 不可用则落盘退出。"""
    start = level_data["test_start_utc"]
    end   = level_data["test_end_utc"]

    if src == "local_log":
        from collector import LocalLogCollector
        log_path = config["ramp"]["server_metrics"]["local_log_path"]
        return LocalLogCollector(log_path).collect(start, end)

    # cloudwatch
    try:
        from collector import create_logs_client, collect_metrics, parse_iso_to_epoch
        logs_client = create_logs_client(config)
        log_group = config["cloudwatch"]["log_group"]
        print("  等待 CloudWatch 日志同步 (15s)...")
        time.sleep(15)
        metrics = collect_metrics(
            logs_client, log_group,
            parse_iso_to_epoch(start), parse_iso_to_epoch(end),
        )
        if not metrics:
            raise RuntimeError("collect_metrics 返回空结果")
        return metrics
    except Exception as e:
        print(f"\n[ramp] CloudWatch 不可用：{e}")
        print("[ramp] 已完成级别将落盘，停止压测，请人工处理。")
        return None  # 调用方检查 None 后退出


def _emit_capacity(all_levels: list, band: dict, output_dir: str):
    """输出承载能力结论（§5.1 控制台 + §5.2 JSON）。"""
    ramp_levels = [ld for ld in all_levels if ld.get("level_name", "").startswith("ramp-")]
    healthy = [ld for ld in ramp_levels if ld.get("status") == "健康"]
    failed  = [ld for ld in ramp_levels if ld.get("status") in ("超载", "不可信")]

    last_healthy_conc = healthy[-1]["concurrency"] if healthy else None
    first_fail_conc   = failed[0]["concurrency"]   if failed  else None

    print("\n" + "=" * 60)
    if last_healthy_conc:
        print(f"承载能力估计：{last_healthy_conc}（最后一个健康级别，指标离开基准带前的最大并发）")
    else:
        print("承载能力估计：未达到健康状态（基准级别即出现问题）")
    if first_fail_conc:
        print(f"超载于并发度：{first_fail_conc}（停止递增，不复测）")
    if failed:
        print(f"判定依据：{failed[0].get('status_reason', '')}")
    print("=" * 60)

    # 组装 §5.2 JSON
    capacity_json = {
        "mode": "ramp",
        "baseline": {"band": band},
        "levels": [
            {
                "concurrency": ld["concurrency"],
                "experience": {
                    "client_ttfa_p99": ld.get("aggregate", {}).get("client_ttfa_p99"),
                    "client_e2e_p99":  ld.get("aggregate", {}).get("client_e2e_p99"),
                    "success_rate":    ld.get("aggregate", {}).get("success_rate"),
                    "ttfa_band_high":  ld.get("ttfa_band_high"),
                    "ttfa_in_band":    (ld.get("status") == "健康"),
                },
                "frame_rate": {
                    "frame_windows":    ld.get("aggregate", {}).get("frame_windows"),
                    "low_fps_windows":  ld.get("aggregate", {}).get("low_fps_windows"),
                    "low_fps_ratio":    ld.get("aggregate", {}).get("low_fps_ratio"),
                    "poor_conn_count":  ld.get("aggregate", {}).get("poor_conn_windows"),
                    "fps_p50":          ld.get("aggregate", {}).get("fps_p50"),
                    "fps_p10":          ld.get("aggregate", {}).get("fps_p10"),
                },
                "reliability": {
                    "send_pace_ratio_avg": ld.get("aggregate", {}).get("send_pace_ratio_avg"),
                    "trusted": ld.get("status") != "不可信",
                },
                "status":        ld.get("status"),
                "status_reason": ld.get("status_reason"),
                "attribution":   ld.get("attribution"),
                "test_start_utc": ld.get("test_start_utc"),
                "test_end_utc":   ld.get("test_end_utc"),
            }
            for ld in ramp_levels
        ],
        "capacity": {
            "last_healthy_concurrency": last_healthy_conc,
            "first_fail_concurrency":   first_fail_conc,
            "stop_reason": failed[0].get("status_reason") if failed else "达到 max 并发上限",
            "retested": False,
        },
    }

    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(output_dir, f"ramp_result_{ts}.json")
    with open(path, "w") as f:
        json.dump(capacity_json, f, indent=2, ensure_ascii=False)
    print(f"ramp 结果已保存: {path}")


def run_ramp(config: dict, output_dir: str):
    """§3.1 两阶段编排：建基准 → 线性递增。"""
    rc = config["ramp"]
    jc = rc["judge"]
    src = rc["server_metrics"]["source"]
    ramp_up = config["test"].get("ramp_up_secs", 10.0)
    cooldown = rc.get("cooldown_secs", 30)

    all_levels = []

    # ── 阶段一：建立基准带 ──
    print("\n" + "=" * 60)
    print(f"[ramp] 阶段一：建立基准带（concurrency=1，重复 {rc['baseline_repeats']} 轮×{rc['baseline_hold_secs']}s）")
    base_samples = {"ttfa_p99": [], "e2e_p99": []}

    for i in range(rc["baseline_repeats"]):
        ld = run_one_level(
            f"baseline-{i}",
            rc["start"],
            rc["baseline_hold_secs"],
            config,
            ramp_up,
            ramp_mode=True,
        )
        all_levels.append(ld)
        for r in ld["results"]:
            if r["success"]:
                if r.get("client_ttfa_ms", 0) > 0:
                    base_samples["ttfa_p99"].append(r["client_ttfa_ms"])
                if r.get("client_e2e_ms", 0) > 0:
                    base_samples["e2e_p99"].append(r["client_e2e_ms"])
        print_level_summary(ld)
        if i < rc["baseline_repeats"] - 1:
            print(f"  基准轮间冷却 ({cooldown}s)...")
            time.sleep(cooldown)

    band = build_baseline_band(base_samples, jc)
    print(f"\n[ramp] 基准带建立完成:")
    for metric, b in band.items():
        print(f"  {metric}: center={b['center']}ms  p90={b['p90']}ms  +3MAD  → band_high={b['band_high']}ms")

    # ── 阶段二：线性递增 ──
    print(f"\n[ramp] 阶段二：线性递增（step={rc['step']}，max={rc['max']}）")
    streak = 0
    conc = rc["start"] + rc["step"]

    while conc <= rc["max"]:
        ld = run_one_level(
            f"ramp-{conc}",
            conc,
            rc["hold_secs"],
            config,
            ramp_up,
            ramp_mode=True,
        )
        agg = aggregate_level(ld)
        sm = _collect_server_metrics(config, ld, src)

        if sm is None:
            # CloudWatch 不可用，落盘退出
            all_levels.append(ld)
            _emit_capacity(all_levels, band, output_dir)
            sys.exit(1)

        verdict = judge_level(agg, sm, band, jc)
        ld.update({
            "aggregate": agg,
            "server_metrics": sm,
            "status": verdict["status"],
            "status_reason": verdict["status_reason"],
            "attribution": verdict["attribution"],
            "frame_rate_degraded": verdict["frame_rate_degraded"],
            "ttfa_band_high": verdict["ttfa_band_high"],
        })
        all_levels.append(ld)

        # 控制台一行摘要
        fr_note = f"low_fps={agg['low_fps_ratio']:.0%} poor_conn={agg['poor_conn_windows']} pace={agg['send_pace_ratio_avg']:.2f}"
        print(
            f"[ramp] conc={conc:3d}  ttfa_p99={agg['client_ttfa_p99']:.0f}ms"
            f"(band_high={verdict['ttfa_band_high']:.0f})  succ={agg['success_rate']:.0%}"
            f"  | {fr_note}  → {verdict['status']}"
        )
        if verdict["attribution"]:
            print(f"       归因: {verdict['attribution']}")

        if verdict["status"] == "超载":
            break
        if verdict["status"] == "不可信":
            print("[ramp] 数据不可信，停止递增，请降低单机并发或检查网络。")
            break
        if verdict["status"] == "预警":
            streak += 1
            if streak >= jc["degrade_streak_fail"]:
                ld["status"] = "超载"
                ld["status_reason"] += f"（连续{streak}次预警达上限）"
                print(f"[ramp] 连续预警 {streak} 次，视为超载，熔断。")
                break
        else:
            streak = 0

        conc += rc["step"]
        if conc <= rc["max"]:
            print(f"  冷却 ({cooldown}s)...")
            time.sleep(cooldown)

    _emit_capacity(all_levels, band, output_dir)
    return all_levels


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
    parser.add_argument(
        "--ramp",
        action="store_true",
        help="走自动递增并发度模式（ramp）",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    os.makedirs(args.output, exist_ok=True)

    # ramp 模式优先
    if args.ramp or config.get("ramp", {}).get("enabled"):
        run_ramp(config, args.output)
        return

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

#!/usr/bin/env python3
"""reporter.py — 合并客户端数据 + CloudWatch 指标，生成结构化压测报告"""

import argparse
import glob
import json
import os
from datetime import datetime


# ──── 数据加载 ────


def load_json(path):
    with open(path) as f:
        return json.load(f)


def percentile(data, p):
    if not data:
        return 0
    data = sorted(data)
    k = (len(data) - 1) * p / 100
    f_idx = int(k)
    c_idx = min(f_idx + 1, len(data) - 1)
    return data[f_idx] + (data[c_idx] - data[f_idx]) * (k - f_idx)


# ──── 客户端指标提取 ────


def extract_client_stats(level_data: dict) -> dict:
    """从 run.py 的原始数据中提取客户端侧统计"""
    results = level_data["results"]
    ttfa_vals = []
    e2e_vals = []
    success_sessions = 0

    for r in results:
        if r["success"]:
            success_sessions += 1
            if r["client_ttfa_ms"] > 0:
                ttfa_vals.append(r["client_ttfa_ms"])
            if r["client_e2e_ms"] > 0:
                e2e_vals.append(r["client_e2e_ms"])

    total_sessions = level_data.get("total_sessions", len(results))
    duration_secs = level_data.get("duration_secs", 0)
    throughput = total_sessions / duration_secs if duration_secs > 0 else 0

    return {
        "concurrency": level_data["concurrency"],
        "level_name": level_data["level_name"],
        "duration_secs": duration_secs,
        "total_sessions": total_sessions,
        "session_success": f"{success_sessions}/{total_sessions}",
        "throughput": f"{throughput:.2f}",
        "client_ttfa_p50": round(percentile(ttfa_vals, 50)),
        "client_ttfa_p90": round(percentile(ttfa_vals, 90)),
        "client_ttfa_p99": round(percentile(ttfa_vals, 99)),
        "client_e2e_p50": round(percentile(e2e_vals, 50)),
        "client_e2e_p90": round(percentile(e2e_vals, 90)),
        "client_e2e_p99": round(percentile(e2e_vals, 99)),
    }


def format_comparison_table(all_stats: list) -> str:
    """生成 Markdown 对比表格"""
    headers = [
        "并发度",
        "持续时间",
        "总会话数",
        "会话成功",
        "吞吐量",
        "TTFA P50",
        "TTFA P90",
        "TTFA P99",
        "E2E P50",
        "E2E P90",
        "E2E P99",
    ]
    rows = []
    for s in all_stats:
        rows.append(
            [
                str(s["concurrency"]),
                f"{s['duration_secs']}s",
                str(s["total_sessions"]),
                s["session_success"],
                f"{s['throughput']} sess/s",
                f"{s['client_ttfa_p50']}ms",
                f"{s['client_ttfa_p90']}ms",
                f"{s['client_ttfa_p99']}ms",
                f"{s['client_e2e_p50']}ms",
                f"{s['client_e2e_p90']}ms",
                f"{s['client_e2e_p99']}ms",
            ]
        )

    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


# ──── 服务端指标整理 ────


def _get_metric_data(metrics: dict, key: str) -> dict:
    """安全获取某个指标的 data 字典，不存在时返回空字典"""
    entry = metrics.get(key, {})
    if "error" in entry:
        return {}
    data = entry.get("data", {})
    if isinstance(data, list):
        return data[0] if data else {}
    return data


def _fmt(value, suffix="ms") -> str:
    """格式化数值，保留整数"""
    if value is None or value == "":
        return "—"
    try:
        v = float(value)
        return f"{int(round(v))}{suffix}"
    except (ValueError, TypeError):
        return str(value)


def extract_server_latency_table(all_metrics: list[dict]) -> str:
    """生成服务端延迟指标对比表（TTFA / E2E 的 P50/P90/P99）"""
    headers = ["指标", "并发度", "P50", "P90", "P99", "Avg", "样本数"]
    rows = []

    for m in all_metrics:
        label = m["label"]
        metrics = m["metrics"]
        for key, name in [("ttfa", "TTFA"), ("e2e", "E2E")]:
            data = _get_metric_data(metrics, key)
            if data:
                rows.append(
                    [
                        name,
                        label,
                        _fmt(data.get("p50")),
                        _fmt(data.get("p90")),
                        _fmt(data.get("p99")),
                        _fmt(data.get("avg")),
                        str(int(data.get("cnt", 0))),
                    ]
                )

    if not rows:
        return "*无延迟指标数据*"

    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def extract_component_table(all_metrics: list[dict]) -> str:
    """生成组件耗时对比表（STT / LLM TTFT / TTS TTFB）"""
    headers = ["组件", "并发度", "P50", "P90", "P99", "Avg"]
    rows = []

    components = [
        ("stt", "STT"),
        ("llm_ttft", "LLM TTFT"),
        ("tts_ttfb", "TTS TTFB"),
    ]

    for m in all_metrics:
        label = m["label"]
        metrics = m["metrics"]
        for key, name in components:
            data = _get_metric_data(metrics, key)
            if data:
                rows.append(
                    [
                        name,
                        label,
                        _fmt(data.get("p50")),
                        _fmt(data.get("p90")),
                        _fmt(data.get("p99")),
                        _fmt(data.get("avg")),
                    ]
                )

    if not rows:
        return "*无组件耗时数据*"

    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def _ascii_bar(pct: float, width: int = 20) -> str:
    """生成 ASCII 条形图"""
    filled = int(round(pct / 100 * width))
    filled = max(0, min(filled, width))
    return "█" * filled + "░" * (width - filled)


def extract_ttfa_breakdown(all_metrics: list[dict]) -> str:
    """生成 TTFA 占比分析（含 ASCII 条形图）"""
    headers = ["并发度", "STT 占比", "LLM 占比", "TTS 占比", "其他"]
    rows = []

    for m in all_metrics:
        label = m["label"]
        data = _get_metric_data(m["metrics"], "ttfa_breakdown")
        if not data:
            continue

        stt_avg = float(data.get("stt_avg", 0))
        llm_avg = float(data.get("llm_avg", 0))
        tts_avg = float(data.get("tts_avg", 0))
        ttfa_avg = float(data.get("ttfa_avg", 0))

        if ttfa_avg <= 0:
            continue

        stt_pct = stt_avg / ttfa_avg * 100
        llm_pct = llm_avg / ttfa_avg * 100
        tts_pct = tts_avg / ttfa_avg * 100
        other_pct = max(0, 100 - stt_pct - llm_pct - tts_pct)

        rows.append(
            [
                label,
                f"{stt_pct:.0f}% {_ascii_bar(stt_pct)}",
                f"{llm_pct:.0f}% {_ascii_bar(llm_pct)}",
                f"{tts_pct:.0f}% {_ascii_bar(tts_pct)}",
                f"{other_pct:.0f}%",
            ]
        )

    if not rows:
        return "*无 TTFA 组件占比数据*"

    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def extract_error_table(all_metrics: list[dict]) -> str:
    """生成错误统计表"""
    headers = ["并发度", "STT 错误", "TTS 错误", "LLM 成功率", "打断率"]
    rows = []

    for m in all_metrics:
        label = m["label"]
        metrics = m["metrics"]

        # 错误统计
        stt_errors = 0
        tts_errors = 0
        errors_data = metrics.get("errors", {})
        if "error" not in errors_data:
            err_data = errors_data.get("data", [])
            if isinstance(err_data, list):
                for row in err_data:
                    etype = row.get("event_type", "")
                    count = int(row.get("error_count", 0))
                    if "stt" in etype:
                        stt_errors += count
                    elif "tts" in etype:
                        tts_errors += count
            elif isinstance(err_data, dict):
                # 单行结果
                etype = err_data.get("event_type", "")
                count = int(err_data.get("error_count", 0))
                if "stt" in etype:
                    stt_errors = count
                elif "tts" in etype:
                    tts_errors = count

        # LLM 成功率
        llm_data = _get_metric_data(metrics, "llm_call")
        llm_rate = llm_data.get("success_rate", "—")
        if llm_rate != "—":
            llm_rate = f"{float(llm_rate):.1f}%"

        # 打断率
        intr_data = _get_metric_data(metrics, "interrupts")
        intr_rate = intr_data.get("interrupt_rate", "—")
        if intr_rate != "—":
            intr_rate = f"{float(intr_rate):.1f}%"

        rows.append(
            [
                label,
                str(stt_errors),
                str(tts_errors),
                str(llm_rate),
                str(intr_rate),
            ]
        )

    if not rows:
        return "*无错误统计数据*"

    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def extract_session_table(all_metrics: list[dict]) -> str:
    """生成会话统计表"""
    headers = ["并发度", "服务端 Started", "服务端 Stopped", "丢失会话"]
    rows = []

    for m in all_metrics:
        label = m["label"]
        data = _get_metric_data(m["metrics"], "sessions")
        if not data:
            continue

        started = int(data.get("started", 0))
        stopped = int(data.get("stopped", 0))
        lost = started - stopped

        rows.append(
            [
                label,
                str(started),
                str(stopped),
                str(lost),
            ]
        )

    if not rows:
        return "*无会话统计数据*"

    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


# ──── 瓶颈分析 ────


def analyze_bottleneck(
    baseline: dict, level: dict, level_name: str
) -> dict:
    """
    对比当前级别与 baseline，自动判定瓶颈组件。

    返回：
    {
        "component": "LLM" | "STT" | "TTS" | "全链路" | "—",
        "observation": "描述",
        "suggestion": "建议",
    }
    """
    observations = []
    bottlenecks = []

    # 获取 TTFA P99
    bl_ttfa = _get_metric_data(baseline, "ttfa")
    lv_ttfa = _get_metric_data(level, "ttfa")

    bl_p99 = float(bl_ttfa.get("p99", 0)) if bl_ttfa else 0
    lv_p99 = float(lv_ttfa.get("p99", 0)) if lv_ttfa else 0

    # 规则 1：TTFA P99 劣化判定
    if bl_p99 > 0 and lv_p99 > bl_p99 * 1.5:
        degradation = (lv_p99 - bl_p99) / bl_p99 * 100
        observations.append(f"TTFA P99 上升 {degradation:.0f}%")

    # 规则 2：瓶颈组件定位（TTFA 组件占比变化）
    bl_bd = _get_metric_data(baseline, "ttfa_breakdown")
    lv_bd = _get_metric_data(level, "ttfa_breakdown")

    if bl_bd and lv_bd:
        bl_ttfa_avg = float(bl_bd.get("ttfa_avg", 1))
        lv_ttfa_avg = float(lv_bd.get("ttfa_avg", 1))

        if bl_ttfa_avg > 0 and lv_ttfa_avg > 0:
            bl_llm_pct = float(bl_bd.get("llm_avg", 0)) / bl_ttfa_avg * 100
            lv_llm_pct = float(lv_bd.get("llm_avg", 0)) / lv_ttfa_avg * 100
            bl_stt_pct = float(bl_bd.get("stt_avg", 0)) / bl_ttfa_avg * 100
            lv_stt_pct = float(lv_bd.get("stt_avg", 0)) / lv_ttfa_avg * 100
            bl_tts_pct = float(bl_bd.get("tts_avg", 0)) / bl_ttfa_avg * 100
            lv_tts_pct = float(lv_bd.get("tts_avg", 0)) / lv_ttfa_avg * 100

            if lv_llm_pct - bl_llm_pct > 10:
                bottlenecks.append("LLM")
                observations.append(
                    f"LLM 占比从 {bl_llm_pct:.0f}%→{lv_llm_pct:.0f}%"
                )
            if lv_stt_pct - bl_stt_pct > 10:
                bottlenecks.append("STT")
                observations.append(
                    f"STT 占比从 {bl_stt_pct:.0f}%→{lv_stt_pct:.0f}%"
                )
            if lv_tts_pct - bl_tts_pct > 10:
                bottlenecks.append("TTS")
                observations.append(
                    f"TTS 占比从 {bl_tts_pct:.0f}%→{lv_tts_pct:.0f}%"
                )

    # 规则 3：错误率判定
    errors_entry = level.get("errors", {})
    if "error" not in errors_entry:
        err_data = errors_entry.get("data", [])
        if isinstance(err_data, list):
            for row in err_data:
                etype = row.get("event_type", "")
                count = int(row.get("error_count", 0))
                if count > 0:
                    observations.append(f"{etype} 错误 {count} 次")

    llm_data = _get_metric_data(level, "llm_call")
    llm_rate = float(llm_data.get("success_rate", 100)) if llm_data else 100
    if llm_rate < 99:
        observations.append(f"LLM 成功率 {llm_rate:.1f}%")
        if "LLM" not in bottlenecks:
            bottlenecks.append("LLM")

    # 规则 4：会话丢失判定
    sess_data = _get_metric_data(level, "sessions")
    if sess_data:
        started = int(sess_data.get("started", 0))
        stopped = int(sess_data.get("stopped", 0))
        if started > 0 and (started - stopped) > started * 0.05:
            observations.append(f"会话丢失 {started - stopped}/{started}")

    # 规则 5：建议生成
    suggestions = {
        "LLM": "检查 LLM API (OpenRouter) 的 RPM/TPM 限制，考虑升级 plan 或增加 API key 轮转",
        "STT": "检查 Deepgram 并发 WebSocket 连接数限制",
        "TTS": "检查 ElevenLabs/Cartesia 并发限制，考虑升级 API 计划",
    }

    if len(bottlenecks) >= 3:
        component = "全链路"
        suggestion = "系统整体过载，建议多实例部署 + 外部服务扩容"
    elif bottlenecks:
        component = " + ".join(bottlenecks)
        suggestion = "；".join(suggestions.get(b, "") for b in bottlenecks if b in suggestions)
    else:
        component = "—"
        suggestion = "—" if not observations else "持续监控"

    observation_text = "；".join(observations) if observations else "各指标稳定"

    return {
        "component": component,
        "observation": observation_text,
        "suggestion": suggestion if suggestion else "—",
    }


def generate_bottleneck_section(all_metrics: list[dict]) -> str:
    """生成第四章 瓶颈分析"""
    if not all_metrics:
        return "*无服务端指标，无法进行瓶颈分析*"

    headers = ["并发度", "瓶颈组件", "表现", "建议"]
    rows = []

    # 以第一个 level 作为 baseline
    baseline_metrics = all_metrics[0]["metrics"]

    for i, m in enumerate(all_metrics):
        label = m["label"]
        if i == 0:
            rows.append([f"{label} (baseline)", "—", "作为基准线", "—"])
        else:
            result = analyze_bottleneck(baseline_metrics, m["metrics"], label)
            rows.append(
                [label, result["component"], result["observation"], result["suggestion"]]
            )

    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


# ──── 差异分析 ────


def generate_diff_section(client_stats: list, all_metrics: list) -> str:
    """生成第五章 客户端 vs 服务端差异"""
    headers = ["并发度", "客户端 TTFA P50", "服务端 TTFA P50", "差值(网络开销)"]
    rows = []

    # 构建 metrics label -> data 的映射
    metrics_by_label = {}
    for m in all_metrics:
        metrics_by_label[m["label"]] = m["metrics"]

    for cs in client_stats:
        label = cs["level_name"]
        client_p50 = cs["client_ttfa_p50"]

        server_p50 = None
        # 尝试匹配同名 metrics
        if label in metrics_by_label:
            ttfa_data = _get_metric_data(metrics_by_label[label], "ttfa")
            if ttfa_data:
                server_p50 = float(ttfa_data.get("p50", 0))

        if server_p50 is not None and server_p50 > 0:
            diff = client_p50 - int(round(server_p50))
            rows.append(
                [
                    label,
                    f"{client_p50}ms",
                    f"{int(round(server_p50))}ms",
                    f"+{diff}ms" if diff >= 0 else f"{diff}ms",
                ]
            )

    if not rows:
        return "*无法对比：客户端与服务端指标的 level 名称不匹配，或服务端缺少 TTFA 数据*"

    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


# ──── 主报告生成 ────


def generate_report(
    stress_data_path: str,
    metrics_dir: str = None,
    config: dict = None,
) -> str:
    """生成完整的对比报告（Markdown 格式）"""
    raw_data = load_json(stress_data_path)

    report = []
    report.append("# 语音 Agent 压力测试报告")
    report.append(f"\n生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # 一、测试概览
    report.append("## 一、测试概览\n")
    if config:
        overview_rows = []
        server_url = config.get("server", {}).get("url", "—")
        overview_rows.append(("目标服务", server_url))

        level_names = []
        for lv in config.get("levels", []):
            name = lv["name"]
            duration = lv.get("duration_secs", 0)
            level_names.append(f"{name}({duration}s)")
        overview_rows.append(("测试级别", ", ".join(level_names) if level_names else "—"))

        test_cfg = config.get("test", {})
        overview_rows.append(("音频文件", test_cfg.get("audio_file", "—")))

        # 总持续时间
        if raw_data:
            first_start = raw_data[0].get("test_start_utc", "")
            last_end = raw_data[-1].get("test_end_utc", "")
            if first_start and last_end:
                try:
                    from datetime import datetime as dt

                    t0 = dt.fromisoformat(first_start)
                    t1 = dt.fromisoformat(last_end)
                    dur = t1 - t0
                    total_secs = int(dur.total_seconds())
                    mins, secs = divmod(total_secs, 60)
                    overview_rows.append(("总持续时间", f"{mins} min {secs} sec"))
                except Exception:
                    pass

        report.append("| 项目 | 值 |")
        report.append("| --- | --- |")
        for k, v in overview_rows:
            report.append(f"| {k} | {v} |")
    else:
        report.append("*未提供配置信息*")

    # 二、客户端侧延迟对比
    report.append("\n## 二、客户端侧延迟对比\n")
    all_stats = [extract_client_stats(ld) for ld in raw_data]
    report.append(format_comparison_table(all_stats))

    # 加载服务端指标
    all_metrics = []
    if metrics_dir and os.path.isdir(metrics_dir):
        metric_files = sorted(glob.glob(os.path.join(metrics_dir, "metrics_*.json")))
        for mf in metric_files:
            all_metrics.append(load_json(mf))

    if all_metrics:
        # 三、服务端指标（CloudWatch）
        report.append("\n## 三、服务端指标（CloudWatch）\n")

        report.append("### 3.1 延迟指标对比\n")
        report.append(extract_server_latency_table(all_metrics))

        report.append("\n### 3.2 组件耗时对比\n")
        report.append(extract_component_table(all_metrics))

        report.append("\n### 3.3 TTFA 组件占比分析\n")
        report.append(extract_ttfa_breakdown(all_metrics))

        report.append("\n### 3.4 错误统计\n")
        report.append(extract_error_table(all_metrics))

        report.append("\n### 3.5 会话统计\n")
        report.append(extract_session_table(all_metrics))

        # 四、瓶颈分析
        report.append("\n## 四、瓶颈分析\n")
        report.append(generate_bottleneck_section(all_metrics))

        # 五、客户端 vs 服务端差异
        report.append("\n## 五、客户端 vs 服务端指标差异\n")
        report.append(generate_diff_section(all_stats, all_metrics))
    else:
        report.append("\n## 三、瓶颈分析\n")
        report.append("*无服务端指标数据，请先运行 collector.py 采集指标*\n")

    return "\n".join(report)


def main():
    parser = argparse.ArgumentParser(description="生成压测对比报告")
    parser.add_argument("--data", required=True, help="run.py 生成的 JSON 数据文件")
    parser.add_argument("--metrics-dir", default="reports", help="指标 JSON 目录")
    parser.add_argument("--output", default=None, help="输出文件路径")
    parser.add_argument(
        "--config", default=None, help="配置文件路径（用于生成测试概览）"
    )
    args = parser.parse_args()

    config = None
    if args.config:
        import yaml

        with open(args.config) as f:
            config = yaml.safe_load(f)

    report_md = generate_report(args.data, args.metrics_dir, config)

    if args.output:
        with open(args.output, "w") as f:
            f.write(report_md)
        print(f"报告已保存: {args.output}")
    else:
        print(report_md)


if __name__ == "__main__":
    main()

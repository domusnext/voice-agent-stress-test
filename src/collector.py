#!/usr/bin/env python3
"""collector.py — 从 CloudWatch Logs Insights 采集服务端指标"""

import argparse
import json
import os
import time
from datetime import datetime

import boto3
import yaml


# ──────────── CloudWatch Logs Insights 查询 ────────────


# 复用日志指标方案中 dashboard 的 CloudWatch Logs Insights 查询
METRIC_QUERIES = {
    "ttfa": {
        "label": "TTFA (首音延迟)",
        "expression": (
            "filter msg like /event=ttfa/\n"
            "| parse msg /ttfa_ms=(?<ttfa>[\\d.]+)/\n"
            "| filter ttfa > 0\n"
            "| stats pct(ttfa, 50) as p50, pct(ttfa, 90) as p90,"
            " pct(ttfa, 99) as p99, avg(ttfa) as avg, count(*) as cnt"
        ),
    },
    "e2e": {
        "label": "E2E (端到端)",
        "expression": (
            "filter msg like /event=turn_e2e/\n"
            "| parse msg /e2e_ms=(?<e2e>[\\d.]+)/\n"
            "| stats pct(e2e, 50) as p50, pct(e2e, 90) as p90,"
            " pct(e2e, 99) as p99, avg(e2e) as avg, count(*) as cnt"
        ),
    },
    "stt": {
        "label": "STT 耗时",
        "expression": (
            "filter msg like /event=stt stt_provider/\n"
            "| parse msg /duration_ms=(?<dur>[\\d.]+)/\n"
            "| stats pct(dur, 50) as p50, pct(dur, 90) as p90,"
            " pct(dur, 99) as p99, avg(dur) as avg"
        ),
    },
    "llm_ttft": {
        "label": "LLM TTFT",
        "expression": (
            "filter msg like /event=llm_ttft/\n"
            "| parse msg /ttfb_ms=(?<ttft>[\\d.]+)/\n"
            "| filter ttft > 0\n"
            "| stats pct(ttft, 50) as p50, pct(ttft, 90) as p90,"
            " pct(ttft, 99) as p99, avg(ttft) as avg"
        ),
    },
    "llm_call": {
        "label": "LLM 调用耗时",
        "expression": (
            "filter msg like /event=llm_call/\n"
            "| parse msg /duration_ms=(?<dur>[\\d.]+)/\n"
            "| parse msg /success=(?<s>\\d)/\n"
            "| stats pct(dur, 50) as p50, pct(dur, 90) as p90,"
            " pct(dur, 99) as p99, avg(dur) as avg,"
            " sum(s) / count(*) * 100 as success_rate"
        ),
    },
    "tts_ttfb": {
        "label": "TTS TTFB",
        "expression": (
            "filter msg like /event=tts_ttfb/\n"
            "| parse msg /ttfb_ms=(?<ttfb>[\\d.]+)/\n"
            "| stats pct(ttfb, 50) as p50, pct(ttfb, 90) as p90,"
            " pct(ttfb, 99) as p99, avg(ttfb) as avg"
        ),
    },
    "tts": {
        "label": "TTS 合成耗时",
        "expression": (
            "filter msg like /event=tts tts_provider/\n"
            "| parse msg /duration_ms=(?<dur>[\\d.]+)/\n"
            "| stats pct(dur, 50) as p50, pct(dur, 90) as p90,"
            " pct(dur, 99) as p99, avg(dur) as avg"
        ),
    },
    "ttfa_breakdown": {
        "label": "TTFA 组件占比",
        "expression": (
            "filter msg like /event=ttfa/\n"
            "| parse msg /stt_ms=(?<stt>[\\d.]+)/\n"
            "| parse msg /llm_ttft_ms=(?<llm>[\\d.]+)/\n"
            "| parse msg /tts_ttfb_ms=(?<tts>[\\d.]+)/\n"
            "| parse msg /ttfa_ms=(?<ttfa>[\\d.]+)/\n"
            "| stats avg(stt) as stt_avg, avg(llm) as llm_avg,"
            " avg(tts) as tts_avg, avg(ttfa) as ttfa_avg"
        ),
    },
    "errors": {
        "label": "STT/TTS 错误",
        "expression": (
            "filter msg like /event=stt_error/ or msg like /event=tts_error/\n"
            "| parse msg /event=(?<event_type>\\S+)/\n"
            "| stats count(*) as error_count by event_type"
        ),
    },
    "sessions": {
        "label": "会话统计",
        "expression": (
            "filter msg like /event=bot_session/\n"
            "| parse msg /action=(?<action>\\w+)/\n"
            "| stats sum(strcontains(action, 'start')) as started,"
            " sum(strcontains(action, 'stop')) as stopped"
        ),
    },
    "interrupts": {
        "label": "打断率",
        "expression": (
            "filter msg like /event=turn_e2e/\n"
            "| parse msg /interrupted=(?<intr>\\d)/\n"
            "| stats sum(intr) / count(*) * 100 as interrupt_rate,"
            " count(*) as total_turns"
        ),
    },
}


def create_logs_client(config: dict):
    """根据配置创建 CloudWatch Logs 客户端"""
    cw_cfg = config["cloudwatch"]
    session_kwargs = {}
    if cw_cfg.get("profile"):
        session_kwargs["profile_name"] = cw_cfg["profile"]
    session = boto3.Session(**session_kwargs)
    return session.client("logs", region_name=cw_cfg["region"])


def parse_cloudwatch_result(raw: dict) -> list[dict]:
    """
    解析 CloudWatch get_query_results 返回值。

    返回 list[dict]：
    - stats 查询（无 group by）：返回单元素列表 [{"p50": 1150.0, "p90": 1320.0, ...}]
    - group by 查询：返回多行 [{"event_type": "stt_error", "error_count": 3}, ...]
    """
    rows = []
    for result_row in raw.get("results", []):
        row = {}
        for field_pair in result_row:
            name = field_pair["field"]
            value = field_pair["value"]
            # 尝试转为数值
            try:
                value = float(value)
                if value == int(value):
                    value = int(value)
            except (ValueError, TypeError):
                pass
            row[name] = value
        rows.append(row)
    return rows


def collect_metrics(logs_client, log_group: str, start: int, end: int) -> dict:
    """并发启动所有查询，然后批量轮询结果"""

    # Phase 1：并发启动全部查询
    pending = {}
    for key, qdef in METRIC_QUERIES.items():
        try:
            resp = logs_client.start_query(
                logGroupName=log_group,
                startTime=start,
                endTime=end,
                queryString=qdef["expression"],
            )
            pending[key] = resp["queryId"]
        except Exception as e:
            print(f"  ! {qdef['label']}: 启动查询失败 — {e}")

    # Phase 2：轮询直到全部完成（最多 60 秒）
    collected = {}
    deadline = time.time() + 60

    while pending and time.time() < deadline:
        for key in list(pending.keys()):
            try:
                result = logs_client.get_query_results(queryId=pending[key])
                status = result["status"]
                if status == "Complete":
                    parsed = parse_cloudwatch_result(result)
                    collected[key] = {
                        "label": METRIC_QUERIES[key]["label"],
                        "data": parsed[0] if len(parsed) == 1 else parsed,
                    }
                    del pending[key]
                    print(f"  ✓ {METRIC_QUERIES[key]['label']}: {collected[key]['data']}")
                elif status in ("Failed", "Cancelled", "Timeout"):
                    collected[key] = {
                        "label": METRIC_QUERIES[key]["label"],
                        "error": f"查询状态: {status}",
                    }
                    del pending[key]
                    print(f"  ✗ {METRIC_QUERIES[key]['label']}: {status}")
            except Exception as e:
                collected[key] = {
                    "label": METRIC_QUERIES[key]["label"],
                    "error": str(e),
                }
                del pending[key]
                print(f"  ✗ {METRIC_QUERIES[key]['label']}: {e}")

        if pending:
            time.sleep(1)

    # 超时未完成的查询
    for key in pending:
        collected[key] = {
            "label": METRIC_QUERIES[key]["label"],
            "error": "查询超时 (60s)",
        }
        print(f"  ✗ {METRIC_QUERIES[key]['label']}: 查询超时")

    return collected


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def parse_iso_to_epoch(iso_str: str) -> int:
    """将 ISO 8601 时间字符串转为 Unix 秒时间戳"""
    dt = datetime.fromisoformat(iso_str)
    return int(dt.timestamp())


def main():
    parser = argparse.ArgumentParser(description="从 CloudWatch 采集压测指标")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--start", required=True, help="测试开始时间 (ISO 8601)")
    parser.add_argument("--end", required=True, help="测试结束时间 (ISO 8601)")
    parser.add_argument("--label", default="test", help="测试标签")
    parser.add_argument("--output", default="reports")
    parser.add_argument("--data", default=None, help="压测客户端数据文件路径 (stress_test_*.json)，用于生成报告命令提示")
    args = parser.parse_args()

    config = load_config(args.config)
    os.makedirs(args.output, exist_ok=True)

    print(f"采集指标: {args.label}")
    print(f"  时间窗口: {args.start} → {args.end}")
    print()

    logs_client = create_logs_client(config)
    start_epoch = parse_iso_to_epoch(args.start)
    end_epoch = parse_iso_to_epoch(args.end)

    metrics = collect_metrics(logs_client, config["cloudwatch"]["log_group"], start_epoch, end_epoch)

    # 保存
    idx = datetime.now().strftime('%H%M%S')
    output_path = os.path.join(
        args.output, f"metrics_{args.label}_{idx}.json"
    )
    with open(output_path, "w") as f:
        json.dump(
            {
                "label": args.label,
                "start": args.start,
                "end": args.end,
                "metrics": metrics,
            },
            f,
            indent=2,
            ensure_ascii=False,
        )
    print(f"\n指标已保存: {output_path}")
    data_arg = args.data if args.data else "<stress_test_*.json>"
    print(f"\n生成报告命令:")
    print(f"  uv run python src/reporter.py --data {data_arg} --metrics-dir {args.output} --config {args.config} --output {args.output}/report_{args.label}_{idx}.md")


if __name__ == "__main__":
    main()

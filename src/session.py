#!/usr/bin/env python3
"""session.py — 单个压测会话（通过 Transport 抽象支持 Daily / gRPC）"""

import time
import wave
from dataclasses import dataclass

from transport import create_transport


@dataclass
class SessionResult:
    session_id: int
    transport_type: str = ""
    connect_ms: float = 0.0
    client_ttfa_ms: float = 0.0
    client_e2e_ms: float = 0.0
    total_duration_ms: float = 0.0
    success: bool = True
    error: str = ""


def load_wav_pcm(filepath: str, target_sr: int = 16000) -> bytes:
    """加载 WAV 文件，返回 16-bit PCM bytes（单声道）"""
    with wave.open(filepath, "rb") as wf:
        assert wf.getsampwidth() == 2, "仅支持 16-bit WAV"
        assert wf.getnchannels() == 1, "仅支持单声道 WAV"
        sr = wf.getframerate()
        frames = wf.readframes(wf.getnframes())

        if sr != target_sr:
            import array

            src = array.array("h", frames)
            ratio = target_sr / sr
            new_len = int(len(src) * ratio)
            dst = array.array("h", [0] * new_len)
            for i in range(new_len):
                src_idx = i / ratio
                idx = int(src_idx)
                frac = src_idx - idx
                if idx + 1 < len(src):
                    dst[i] = int(src[idx] * (1 - frac) + src[idx + 1] * frac)
                else:
                    dst[i] = src[min(idx, len(src) - 1)]
            return dst.tobytes()

        return frames


class StressTestSession:
    """
    运行一个完整的压测会话（单轮模型），通过 Transport 抽象驱动。
    """

    def __init__(
        self,
        session_id: int,
        transport_type: str,
        transport_kwargs: dict,
        audio_file: str,
        max_wait: float,
        sample_rate: int = 16000,
    ):
        self.session_id = session_id
        self.transport_type = transport_type
        self.transport_kwargs = transport_kwargs
        self.audio_file = audio_file
        self.max_wait = max_wait
        self.sample_rate = sample_rate

    def run(self) -> SessionResult:
        """执行会话，返回结果（在子进程中调用）"""
        session_start = time.perf_counter()
        result = SessionResult(
            session_id=self.session_id,
            transport_type=self.transport_type,
        )

        transport = create_transport(
            self.transport_type,
            session_id=self.session_id,
            **self.transport_kwargs,
        )

        try:
            audio_pcm = load_wav_pcm(self.audio_file, self.sample_rate)

            transport.connect()
            result.connect_ms = transport.result.connect_ms

            transport.send_audio(audio_pcm, self.sample_rate)

            tr = transport.wait_for_completion(self.max_wait)
            result.client_ttfa_ms = tr.client_ttfa_ms
            result.client_e2e_ms = tr.client_e2e_ms
            result.success = tr.success
            result.error = tr.error

        except Exception as e:
            result.success = False
            result.error = str(e)

        finally:
            transport.close()
            result.total_duration_ms = (time.perf_counter() - session_start) * 1000

        return result

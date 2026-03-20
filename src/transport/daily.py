"""Daily.co WebRTC 传输实现。"""

import struct
import threading
import time
from typing import Optional

import daily

from transport import BaseTransport, TransportResult


class DailyEventHandler(daily.EventHandler):
    """Daily CallClient 事件回调"""

    def __init__(self):
        super().__init__()
        self.bot_joined = threading.Event()
        self.bot_left = threading.Event()
        self.joined = threading.Event()
        self.error: Optional[str] = None
        self.bot_left_at: Optional[float] = None

    def on_joined(self, data, error):
        if error:
            self.error = str(error)
        self.joined.set()

    def on_participant_joined(self, participant):
        if not participant.get("local", False):
            self.bot_joined.set()

    def on_participant_left(self, participant, reason=None):
        if not participant.get("local", False):
            self.bot_left_at = time.perf_counter()
            self.bot_left.set()

    def on_error(self, message):
        self.error = message


def _is_non_silent(pcm_bytes: bytes, threshold: int = 200) -> bool:
    """检测 PCM 数据是否包含非静音内容"""
    if len(pcm_bytes) < 2:
        return False
    samples = struct.unpack(f"<{len(pcm_bytes) // 2}h", pcm_bytes)
    rms = (sum(s * s for s in samples) / len(samples)) ** 0.5
    return rms > threshold


class DailyTransport(BaseTransport):
    """
    Daily.co WebRTC 传输。

    1. POST /rtvi/start → room_url + token
    2. 创建虚拟麦克风/扬声器
    3. join room → 等 bot 加入
    4. 播放音频 → 监听首帧 → 等 bot 离开
    """

    def __init__(
        self,
        session_id: int,
        server_url: str,
        auth_token: str,
        family_id: str,
        user_id: str,
        timezone: str,
        sample_rate: int = 16000,
    ):
        self.session_id = session_id
        self.server_url = server_url
        self.auth_token = auth_token
        self.family_id = family_id
        self.user_id = user_id
        self.timezone = timezone
        self.sample_rate = sample_rate

        self.result = TransportResult()
        self._client = None
        self._handler = None
        self._mic_device = None
        self._spk_device = None
        self._stop_monitor = None
        self._stop_speaking_at: Optional[float] = None
        self._first_audio_holder: dict = {}

    def connect(self) -> None:
        import httpx

        daily.Daily.init()

        t0 = time.perf_counter()
        headers = {
            "Authorization": f"Bearer {self.auth_token}",
            "X-Family-ID": self.family_id,
            "X-User-ID": self.user_id,
            "X-Timezone": self.timezone,
            "Content-Type": "application/json",
        }
        with httpx.Client(timeout=15.0) as http_client:
            resp = http_client.post(
                f"{self.server_url}/rtvi/start",
                headers=headers,
                json={"source": "stress_test"},
            )
            resp.raise_for_status()
            data = resp.json()["data"]
            room_url, token = data["room_url"], data["token"]

        # PLACEHOLDER_DAILY_REST

        ts = int(time.time())
        mic_name = f"stress-mic-{self.session_id}-{ts}"
        spk_name = f"stress-spk-{self.session_id}-{ts}"

        self._mic_device = daily.Daily.create_microphone_device(
            mic_name,
            sample_rate=self.sample_rate,
            channels=1,
        )
        self._spk_device = daily.Daily.create_speaker_device(
            spk_name,
            sample_rate=self.sample_rate,
            channels=1,
        )
        daily.Daily.select_speaker_device(spk_name)

        self._handler = DailyEventHandler()
        self._client = daily.CallClient(event_handler=self._handler)
        self._client.update_inputs(
            {
                "microphone": {
                    "isEnabled": True,
                    "settings": {"deviceId": mic_name},
                },
            }
        )
        self._client.join(room_url, meeting_token=token)

        if not self._handler.joined.wait(timeout=10):
            raise TimeoutError("加入房间超时")
        if self._handler.error:
            raise RuntimeError(f"加入房间失败: {self._handler.error}")
        if not self._handler.bot_joined.wait(timeout=15):
            raise TimeoutError("等待 bot 加入超时")

        self.result.connect_ms = (time.perf_counter() - t0) * 1000

    def send_audio(self, audio_pcm: bytes, sample_rate: int) -> None:
        self._stop_monitor = threading.Event()
        monitor = threading.Thread(
            target=self._monitor_for_first_audio,
            args=(self._spk_device, self._stop_monitor, self._first_audio_holder),
            daemon=True,
        )
        monitor.start()

        chunk_bytes = sample_rate * 2  # 1 秒 16-bit mono
        for offset in range(0, len(audio_pcm), chunk_bytes):
            chunk = audio_pcm[offset : offset + chunk_bytes]
            self._mic_device.write_frames(chunk)
            time.sleep(1.0)

        self._stop_speaking_at = time.perf_counter()

    def wait_for_completion(self, timeout: float) -> TransportResult:
        handler = self._handler

        if not handler.bot_left.wait(timeout=timeout):
            self.result.success = False
            self.result.error = "等待 bot 离开超时"
        else:
            self.result.success = True

        if self._stop_monitor:
            self._stop_monitor.set()

        first_bot_audio_at = self._first_audio_holder.get("first_bot_audio_at")
        if first_bot_audio_at is not None and self._stop_speaking_at is not None:
            self.result.client_ttfa_ms = round(
                (first_bot_audio_at - self._stop_speaking_at) * 1000, 2
            )
        if handler.bot_left_at is not None and self._stop_speaking_at is not None:
            self.result.client_e2e_ms = round(
                (handler.bot_left_at - self._stop_speaking_at) * 1000, 2
            )
        if first_bot_audio_at is None and self.result.success:
            self.result.success = False
            self.result.error = "未收到 bot 音频回复"

        return self.result

    def close(self) -> None:
        if self._client:
            try:
                self._client.leave()
            except Exception:
                pass

    @staticmethod
    def _monitor_for_first_audio(spk, stop_event, result_holder):
        """检测首个非静音音频帧，用于计算 TTFA"""
        CHUNK_FRAMES = 1600  # 100ms at 16kHz
        while not stop_event.is_set():
            try:
                raw = spk.read_frames(CHUNK_FRAMES)
            except Exception:
                break
            if raw and _is_non_silent(raw):
                result_holder["first_bot_audio_at"] = time.perf_counter()
                return
            time.sleep(0.1)

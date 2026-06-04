"""gRPC 双向流传输实现。"""

import asyncio
import logging
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Optional

import grpc
import grpc.aio

from transport import BaseTransport, TransportResult

logger = logging.getLogger(__name__)

# 标记会话结束的 RTVI 事件类型集合
_COMPLETION_TYPES = {"turn-done", "conversation-end", "will-disconnect"}

# 音频参数常量
_BYTES_PER_SAMPLE = 2  # 16-bit mono


@dataclass
class TtsSegment:
    """跟踪单个 TTS 段的播放确认状态。"""

    tts_id: str
    start_offset_sec: float  # 收到 tts-start 时的 received_audio_duration
    end_offset_sec: Optional[float] = None  # 收到 tts-end 时的 received_audio_duration
    started_at_client: bool = False
    stopped_at_client: bool = False


def _extract_rtvi_type(data_struct) -> Optional[str]:
    """从 StreamMessage.data 中提取 RTVI 消息类型。

    支持两种格式：
    1. server-message 包装:
       {"type": "server-message", "data": {"type": "turn-done"}} → "turn-done"
    2. 直接 RTVI 事件:
       {"type": "bot-tts-stopped", "label": "rtvi-ai"} → "bot-tts-stopped"
    """
    fields = data_struct.fields
    msg_type = fields.get("type")
    if not msg_type:
        return None

    type_str = msg_type.string_value

    if type_str == "server-message":
        inner_data = fields.get("data")
        if inner_data and inner_data.struct_value:
            inner_type = inner_data.struct_value.fields.get("type")
            if inner_type:
                return inner_type.string_value
        return None

    return type_str


def _extract_tts_id_from_response(response) -> Optional[str]:
    """从 server-message 的内层 data 中提取 tts_id。

    消息结构: {"type": "server-message", "data": {"type": "tts-start", "tts_id": "xxx"}}
    """
    inner_data = response.data.fields.get("data")
    if inner_data and inner_data.struct_value:
        tts_id_field = inner_data.struct_value.fields.get("tts_id")
        if tts_id_field:
            return tts_id_field.string_value
    return None


def _extract_frame_rate_payload(response) -> Optional[dict]:
    """从 input-frame-rate 消息提取 payload。

    结构: {"type":"server-message","data":{"type":"input-frame-rate","payload":{...}}}
    """
    inner = response.data.fields.get("data")
    if not (inner and inner.struct_value):
        return None
    payload = inner.struct_value.fields.get("payload")
    if not (payload and payload.struct_value):
        return None
    f = payload.struct_value.fields
    return {
        "frame_count": f["frame_count"].number_value if "frame_count" in f else 0,
        "is_poor_connection": f["is_poor_connection"].bool_value if "is_poor_connection" in f else False,
    }


def _struct_to_dict(struct) -> dict:
    """将 protobuf Struct 简单转为 dict 用于日志输出。"""
    from google.protobuf.json_format import MessageToDict

    try:
        return MessageToDict(struct, preserving_proto_field_name=True)
    except Exception:
        return {"_raw_fields": list(struct.fields.keys())}


class GrpcTransport(BaseTransport):
    """
    gRPC 双向流传输。

    流程：
    1. 建立 gRPC 双向流连接
    2. 通过流发送音频帧（20ms PCM）
    3. 从流接收 bot 回复：
       - type="audio" → 累计接收音频时长 + 检测 TTFA
       - type="rtvi_message" → 解析生命周期事件
       - tts-start → 记录段起始偏移
       - tts-end → 记录段结束偏移
    4. 模拟播放进度（20ms 步进）：
       - 播放到达 start_offset → 发送 bot_started_speaking
       - 播放到达 end_offset → 发送 bot_stopped_speaking
    5. 服务端收到确认后触发 turn-done/conversation-end → 标记 E2E 终点
    """

    def __init__(
        self,
        session_id: int,
        grpc_host: str,
        auth_token: str,
        device_id: str,
        family_id: str,
        sample_rate: int = 16000,
        audio_encoding: str = "pcm",
        tls: bool = False,
        user_id: str = "",
        timezone: str = "Asia/Shanghai",
        source: str = "web",
        voice_key: str = "",
        follow_up_mode_on: str = "off",
        enable_analyze_frame_rate: str = "false",
    ):
        self.session_id = session_id
        self._host = grpc_host
        self._auth_token = auth_token
        self._device_id = device_id
        self._family_id = family_id
        self._sample_rate = sample_rate
        self._audio_encoding = audio_encoding
        self._tls = tls
        self._user_id = user_id
        self._timezone = timezone
        self._source = source
        self._voice_key = voice_key
        self._follow_up_mode_on = follow_up_mode_on
        self._enable_analyze_frame_rate = enable_analyze_frame_rate

        self.result = TransportResult()

        # 帧率累加器（会话级，wait_for_completion 时回填到 result）
        self._frame_windows: int = 0
        self._low_fps_windows: int = 0
        self._poor_conn_windows: int = 0
        self._fps_samples: list = []
        self._poor_fps_threshold: int = 40  # 与服务端 _poor_connection_threshold 一致

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._send_queue: Optional[asyncio.Queue] = None
        self._connected = threading.Event()
        self._send_done = threading.Event()
        self._conversation_end = threading.Event()
        self._stop_speaking_at: Optional[float] = None
        self._first_audio_at: Optional[float] = None
        self._conversation_end_at: Optional[float] = None
        self._error: Optional[str] = None

        # TTS 播放模拟状态
        self._tts_segments: list[TtsSegment] = []
        self._received_audio_duration_sec: float = 0.0
        self._playback_done = threading.Event()
        self._playback_task: Optional[asyncio.Task] = None

    def connect(self) -> None:
        t0 = time.perf_counter()

        self._loop = asyncio.new_event_loop()
        self._send_queue = asyncio.Queue()
        self._thread = threading.Thread(
            target=self._run_event_loop,
            daemon=True,
        )
        self._thread.start()

        if not self._connected.wait(timeout=15):
            raise TimeoutError("gRPC 连接建立超时")
        if self._error:
            raise RuntimeError(f"gRPC 连接失败: {self._error}")

        self.result.connect_ms = (time.perf_counter() - t0) * 1000

    def send_audio(self, audio_pcm: bytes, sample_rate: int) -> None:
        frame_duration_ms = 20
        samples_per_frame = sample_rate * frame_duration_ms // 1000
        bytes_per_frame = samples_per_frame * 2  # 16-bit mono
        sleep_secs = frame_duration_ms / 1000

        n_frames = 0
        t_send_start = time.perf_counter()
        for offset in range(0, len(audio_pcm), bytes_per_frame):
            frame = audio_pcm[offset : offset + bytes_per_frame]
            asyncio.run_coroutine_threadsafe(
                self._send_queue.put(frame),
                self._loop,
            ).result(timeout=5)
            time.sleep(sleep_secs)
            n_frames += 1

        elapsed = time.perf_counter() - t_send_start
        ideal = n_frames * (frame_duration_ms / 1000)
        self.result.send_pace_ratio = round(elapsed / ideal, 3) if ideal > 0 else 1.0

        self._stop_speaking_at = time.perf_counter()
        self._send_done.set()

    def wait_for_completion(self, timeout: float) -> TransportResult:
        self._conversation_end.wait(timeout=timeout)

        if self._error:
            self.result.success = False
            self.result.error = self._error
        elif not self._conversation_end.is_set():
            self.result.success = False
            self.result.error = "等待完成信号超时（turn-done/conversation-end/will-disconnect）"
        else:
            self.result.success = True

        if self._first_audio_at and self._stop_speaking_at:
            self.result.client_ttfa_ms = round(
                (self._first_audio_at - self._stop_speaking_at) * 1000, 2
            )
        if self._conversation_end_at and self._stop_speaking_at:
            self.result.client_e2e_ms = round(
                (self._conversation_end_at - self._stop_speaking_at) * 1000, 2
            )
        if self._first_audio_at is None and self.result.success:
            self.result.success = False
            self.result.error = "未收到 bot 音频回复"

        # 回填帧率辅助信号（send_pace_ratio 已在 send_audio 写入）
        self.result.frame_windows = self._frame_windows
        self.result.low_fps_windows = self._low_fps_windows
        self.result.poor_conn_windows = self._poor_conn_windows
        self.result.fps_samples = self._fps_samples

        return self.result

    def close(self) -> None:
        self._playback_done.set()
        self._conversation_end.set()
        if self._loop and self._loop.is_running():
            # 向 send_queue 发送 None 让 request_iter 正常退出
            try:
                asyncio.run_coroutine_threadsafe(
                    self._send_queue.put(None),
                    self._loop,
                ).result(timeout=2)
            except Exception:
                pass
        # 等待 event loop 自然结束
        if self._thread:
            self._thread.join(timeout=5)
        # 如果仍未退出，强制停止
        if self._thread and self._thread.is_alive():
            if self._loop and self._loop.is_running():
                self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=3)

    # ──── 内部异步逻辑 ────

    def _run_event_loop(self):
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._async_session())
        except RuntimeError:
            pass  # Event loop stopped externally during cleanup

    async def _async_session(self):
        from proto_generated import (
            voice_agent_transport_pb2,
            voice_agent_transport_pb2_grpc,
        )
        from google.protobuf.struct_pb2 import Struct

        sid = self.session_id
        if self._tls:
            channel = grpc.aio.secure_channel(self._host, grpc.ssl_channel_credentials())
        else:
            channel = grpc.aio.insecure_channel(self._host)

        try:
            metadata = [
                ("authorization", f"Bearer {self._auth_token}"),
                ("x-device-id", self._device_id),
                ("x-family-id", self._family_id),
                ("x-user-id", self._user_id),
                ("x-timezone", self._timezone),
                ("x-source", self._source),
                ("x-conversation-id", str(uuid.uuid4())),
                ("x-audio-encoding", self._audio_encoding),
                ("x-mode", "standard"),
                ("x-follow-up-mode-on", self._follow_up_mode_on),
                ("x-voice-key", self._voice_key),
                ("x-enable-analyze-frame-rate", self._enable_analyze_frame_rate),
            ]

            stub = voice_agent_transport_pb2_grpc.VoiceAgentTransportStub(channel)

            audio_meta = Struct()
            audio_meta.update(
                {
                    "sample_rate": self._sample_rate,
                    "channels": 1,
                }
            )

            async def request_iter():
                """双向流的请求迭代器。

                支持发送两种消息：
                - bytes: 包装为 audio StreamMessage
                - StreamMessage: 直接发送（如 bot-stopped-speaking）
                - None: 结束迭代器，关闭客户端发送流
                """
                # 不在此处 set _connected，等待服务端 bot-ready/inited 事件
                logger.info("[Session %d] request_iter started", sid)
                while True:
                    item = await self._send_queue.get()
                    if item is None:
                        logger.info("[Session %d] request_iter 退出", sid)
                        return
                    if isinstance(
                        item, voice_agent_transport_pb2.StreamMessage
                    ):
                        logger.info(
                            "[Session %d] 发送 rtvi_message: %s",
                            sid,
                            item.type,
                        )
                        yield item
                    else:
                        yield voice_agent_transport_pb2.StreamMessage(
                            type="audio",
                            raw=item,
                            data=audio_meta,
                        )

            def _create_bot_started_speaking_msg(tts_id):
                """构造 bot-started-speaking 确认消息。

                服务端 CustomOutputTransport 需要先收到此消息
                将 _is_bot_speaking 设为 True，后续 bot-stopped-speaking 才能通过守卫。
                """
                data = Struct()
                data.update(
                    {
                        "type": "client-message",
                        "data": {
                            "t": "bot_started_speaking",
                            "d": {"tts_id": tts_id},
                        },
                        "id": str(uuid.uuid4()),
                        "label": "rtvi-ai",
                    }
                )
                return voice_agent_transport_pb2.StreamMessage(
                    type="rtvi_message",
                    data=data,
                )

            def _create_bot_stopped_speaking_msg(tts_id=None):
                """构造 bot-stopped-speaking 确认消息。

                服务端开启了 use_client_bot_stopped_speaking_event，
                需要客户端在音频播放完成后回传此消息才能触发 turn-done。
                tts_id 必须与 bot-started-speaking 中的一致，
                服务端据此从 _step_ids 中移除并递减 _step_text_num。
                """
                data = Struct()
                data.update(
                    {
                        "type": "client-message",
                        "data": {
                            "t": "bot_stopped_speaking",
                            "d": {"tts_id": tts_id} if tts_id else {},
                        },
                        "id": str(uuid.uuid4()),
                        "label": "rtvi-ai",
                    }
                )
                return voice_agent_transport_pb2.StreamMessage(
                    type="rtvi_message",
                    data=data,
                )

            logger.info("[Session %d] 建立 gRPC 双向流...", sid)
            stream = stub.Stream(request_iter(), metadata=metadata)

            # 启动播放模拟 task
            self._playback_task = asyncio.ensure_future(
                self._simulate_playback(sid, _create_bot_started_speaking_msg, _create_bot_stopped_speaking_msg)
            )

            msg_count = 0
            audio_count = 0
            async for response in stream:
                now = time.perf_counter()
                msg_count += 1

                if response.type == "audio" and len(response.raw) > 0:
                    audio_count += 1
                    # 累计接收到的音频时长
                    self._received_audio_duration_sec += (
                        len(response.raw) / (self._sample_rate * _BYTES_PER_SAMPLE)
                    )
                    if self._first_audio_at is None:
                        self._first_audio_at = now
                        logger.info(
                            "[Session %d] 收到首帧音频 (第 %d 条消息)",
                            sid,
                            msg_count,
                        )

                elif response.type == "rtvi_message":
                    event_type = _extract_rtvi_type(response.data)
                    logger.info(
                        "[Session %d] rtvi: %s (第 %d 条消息)",
                        sid,
                        event_type,
                        msg_count,
                    )

                    # input-frame-rate → 累计帧率统计（辅助归因信号）
                    if event_type == "input-frame-rate":
                        fr = _extract_frame_rate_payload(response)
                        if fr is not None:
                            fps = fr["frame_count"]  # 窗口≈1s，帧数≈fps
                            self._frame_windows += 1
                            self._fps_samples.append(fps)
                            if fps < self._poor_fps_threshold:
                                self._low_fps_windows += 1
                            if fr["is_poor_connection"]:
                                self._poor_conn_windows += 1
                        continue  # 帧率消息不参与 tts/完成信号判断

                    # bot-ready / inited → 服务端 pipeline 就绪，允许发送音频
                    if event_type in ("bot-ready", "inited"):
                        self._connected.set()
                        logger.info(
                            "[Session %d] 服务端就绪 (%s)，开始发送音频",
                            sid,
                            event_type,
                        )

                    # tts-start → 记录段起始偏移
                    if event_type == "tts-start":
                        tts_id = _extract_tts_id_from_response(response)
                        if tts_id:
                            self._tts_segments.append(
                                TtsSegment(
                                    tts_id=tts_id,
                                    start_offset_sec=self._received_audio_duration_sec,
                                )
                            )
                            logger.info(
                                "[Session %d] tts-start (tts_id=%s, offset=%.3fs)",
                                sid,
                                tts_id,
                                self._received_audio_duration_sec,
                            )

                    # tts-end → 记录段结束偏移
                    elif event_type == "tts-end":
                        tts_id = _extract_tts_id_from_response(response)
                        if tts_id:
                            for seg in reversed(self._tts_segments):
                                if seg.tts_id == tts_id and seg.end_offset_sec is None:
                                    seg.end_offset_sec = self._received_audio_duration_sec
                                    logger.info(
                                        "[Session %d] tts-end (tts_id=%s, offset=%.3fs)",
                                        sid,
                                        tts_id,
                                        self._received_audio_duration_sec,
                                    )
                                    break

                    # 完成信号 → 记录时间 + 关闭流
                    if event_type in _COMPLETION_TYPES:
                        self._conversation_end_at = now
                        self._conversation_end.set()
                        self._playback_done.set()
                        logger.info(
                            "[Session %d] 检测到完成信号: %s", sid, event_type
                        )
                        # 通知 request_iter 退出，优雅关闭客户端发送流
                        await self._send_queue.put(None)

                else:
                    logger.debug(
                        "[Session %d] 收到消息: type=%s",
                        sid,
                        response.type,
                    )

            # 停止播放模拟
            self._playback_done.set()
            if self._playback_task and not self._playback_task.done():
                self._playback_task.cancel()
                try:
                    await self._playback_task
                except asyncio.CancelledError:
                    pass

            logger.info(
                "[Session %d] gRPC 流结束, 共收到 %d 条消息 (音频帧 %d)",
                sid,
                msg_count,
                audio_count,
            )

            # 流结束但没收到完成信号（异常兜底）
            if not self._conversation_end.is_set():
                logger.warning(
                    "[Session %d] 流已关闭但未收到完成信号，使用流结束时间作为兜底",
                    sid,
                )
                self._conversation_end_at = time.perf_counter()
                self._conversation_end.set()

        except grpc.aio.AioRpcError as e:
            self._error = f"gRPC 错误: {e.code()} - {e.details()}"
            logger.error("[Session %d] %s", sid, self._error)
            self._playback_done.set()
            self._conversation_end.set()
        except Exception as e:
            self._error = str(e)
            logger.error(
                "[Session %d] 异常: %s", sid, self._error, exc_info=True
            )
            self._playback_done.set()
            self._conversation_end.set()
        finally:
            await channel.close()

    async def _simulate_playback(self, sid, _create_bot_started_speaking_msg, _create_bot_stopped_speaking_msg):
        """模拟音频播放进度，基于播放偏移发送 bot_started/stopped_speaking 确认。

        按实时速率（20ms 步进）推进 played_duration，当播放进度到达
        某个 TTS 段的 start/end offset 时发送对应确认消息。
        """
        played_duration_sec = 0.0
        step_sec = 0.02  # 20ms 步进，与音频帧时长一致

        while not self._playback_done.is_set():
            await asyncio.sleep(step_sec)
            played_duration_sec += step_sec

            for seg in self._tts_segments:
                if seg.stopped_at_client:
                    continue

                # 播放进度到达段起始 → 发送 bot_started_speaking
                if not seg.started_at_client and played_duration_sec >= seg.start_offset_sec:
                    seg.started_at_client = True
                    await self._send_queue.put(
                        _create_bot_started_speaking_msg(seg.tts_id)
                    )
                    logger.info(
                        "[Session %d] 模拟播放: bot_started_speaking (tts_id=%s, played=%.3fs)",
                        sid,
                        seg.tts_id,
                        played_duration_sec,
                    )

                # 播放进度到达段结束 → 发送 bot_stopped_speaking
                if (
                    seg.end_offset_sec is not None
                    and not seg.stopped_at_client
                    and played_duration_sec >= seg.end_offset_sec
                ):
                    if not seg.started_at_client:
                        seg.started_at_client = True
                        await self._send_queue.put(
                            _create_bot_started_speaking_msg(seg.tts_id)
                        )
                    seg.stopped_at_client = True
                    await self._send_queue.put(
                        _create_bot_stopped_speaking_msg(seg.tts_id)
                    )
                    logger.info(
                        "[Session %d] 模拟播放: bot_stopped_speaking (tts_id=%s, played=%.3fs)",
                        sid,
                        seg.tts_id,
                        played_duration_sec,
                    )

            # 清理已完成的段
            self._tts_segments = [s for s in self._tts_segments if not s.stopped_at_client]

"""Transport 抽象层：统一 Daily 和 gRPC 两种传输模式的接口。"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class TransportResult:
    """Transport 层执行结果，用于向 Session 传递延迟指标。"""

    connect_ms: float = 0.0
    client_ttfa_ms: float = 0.0
    client_e2e_ms: float = 0.0
    success: bool = True
    error: str = ""


class BaseTransport(ABC):
    """
    压测传输层的统一抽象。

    生命周期：
        connect() → send_audio() → wait_for_completion() → close()
    """

    result: TransportResult

    @abstractmethod
    def connect(self) -> None:
        """建立连接（Daily: 创建房间+加入；gRPC: 建立双向流）。

        应在内部记录耗时到 self.result.connect_ms。
        连接失败应抛出异常。
        """

    @abstractmethod
    def send_audio(self, audio_pcm: bytes, sample_rate: int) -> None:
        """发送音频数据。

        应在内部按实时速率分片发送，并在发送完毕后记录 stop_speaking 时间戳。
        """

    @abstractmethod
    def wait_for_completion(self, timeout: float) -> TransportResult:
        """等待 bot 回复结束，返回结果。

        Daily: 等待 on_participant_left 事件
        gRPC: 等待 turn-done 消息
        """

    @abstractmethod
    def close(self) -> None:
        """释放资源（Daily: leave room；gRPC: 关闭 channel）。"""


def create_transport(transport_type: str, **kwargs) -> BaseTransport:
    """工厂函数：根据 transport 类型创建实例。"""
    if transport_type == "daily":
        from transport.daily import DailyTransport

        return DailyTransport(**kwargs)
    elif transport_type == "grpc":
        from transport.grpc import GrpcTransport

        return GrpcTransport(**kwargs)
    else:
        raise ValueError(f"不支持的传输类型: {transport_type}")

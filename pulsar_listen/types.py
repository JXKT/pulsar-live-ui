from typing import TypedDict, NotRequired, Any
from dataclasses import dataclass
from datetime import datetime

# Python 3.12+ type alias syntax
type TopicName = str
type MessageID = str

class PulsarConfig(TypedDict):
    """Configuration for connecting to the Pulsar Cluster."""
    broker_service_url: str  # e.g., 'pulsar://localhost:6650'
    admin_service_url: str   # e.g., 'http://localhost:8080'
    token: NotRequired[str]  # Optional auth token

@dataclass(kw_only=True, frozen=True, slots=True)
class StreamMessage:
    """Immutable representation of a Pulsar message for the UI."""
    message_id: MessageID
    publish_time: datetime
    content: str | bytes
    properties: dict[str, str]
    key: str | None = None

    @property
    def decode_content(self) -> str:
        """Helper to try to decode bytes to string safely."""
        if isinstance(self.content, str):
            return self.content
        try:
            return self.content.decode("utf-8")
        except UnicodeDecodeError:
            return f"<Binary Data: {len(self.content)} bytes>"
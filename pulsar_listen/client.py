import pulsar
import httpx
import logging
from typing import Generator, ContextManager
from contextlib import contextmanager
from datetime import datetime
from .types import PulsarConfig, StreamMessage, TopicName

logger = logging.getLogger(__name__)


class PulsarManager:
    def __init__(self, config: PulsarConfig) -> None:
        self._config = config
        self._admin_client = httpx.Client(
            base_url=config["admin_service_url"],
            headers=self._get_auth_headers(),
            timeout=5.0
        )

    def _get_auth_headers(self) -> dict[str, str]:
        if token := self._config.get("token"):
            return {"Authorization": f"Bearer {token}"}
        return {}

    def get_tenants(self) -> list[str]:
        """Fetch tenants using the Admin API."""
        try:
            response = self._admin_client.get("/admin/v2/tenants")
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch tenants: {e}")
            return []

    def get_namespaces(self, tenant: str) -> list[str]:
        """Fetch namespaces for a specific tenant."""
        try:
            response = self._admin_client.get(f"/admin/v2/namespaces/{tenant}")
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch namespaces: {e}")
            return []

    def get_topics(self, namespace: str) -> list[TopicName]:
        """Fetch topics for a specific namespace."""
        try:
            # "persistent" is the domain. You might want "non-persistent" too.
            # The API returns full topic names like 'persistent://tenant/ns/topic'
            response = self._admin_client.get(f"/admin/v2/namespaces/{namespace}/topics")
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch topics: {e}")
            return []

    @contextmanager
    def create_listener(self, topic: TopicName, limit: int = 100) -> Generator[
        Generator[StreamMessage, None, None], None, None]:
        """
        Creates a Reader to peek at messages.

        Using a context manager ensures the connection closes cleanly
        even if the UI refreshes or errors out.
        """
        client = pulsar.Client(
            self._config["broker_service_url"],
            authentication=pulsar.AuthenticationToken(self._config["token"]) if self._config.get("token") else None
        )

        try:
            # We use a Reader because it doesn't modify the cursor (unlike a Consumer).
            # We start at the latest message minus 'limit' if possible,
            # but Pulsar Reader usually starts at specific ID.
            # For simplicity in this 'Live' UI, we will start at the Latest
            # and wait for new messages, or read from Earliest if you want history.

            reader = client.create_reader(
                topic=topic,
                start_message_id=pulsar.MessageId.earliest,  # Or latest
                reader_listener=None,
            )

            def message_generator() -> Generator[StreamMessage, None, None]:
                count = 0
                while count < limit:
                    # Non-blocking check for UI responsiveness could be done here
                    # with a smaller timeout
                    try:
                        msg = reader.read_next(timeout_millis=2000)

                        yield StreamMessage(
                            message_id=str(msg.message_id()),
                            publish_time=datetime.fromtimestamp(msg.publish_timestamp() / 1000),
                            content=msg.data(),
                            properties=msg.properties(),
                            key=msg.partition_key()
                        )
                        count += 1
                    except Exception:
                        # Timeout or end of stream
                        break

            yield message_generator()

        finally:
            client.close()

    def close(self):
        self._admin_client.close()

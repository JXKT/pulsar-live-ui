import pulsar
import httpx
from loguru import logger  # <--- Modern replacement for 'logging'
from typing import Generator
from contextlib import contextmanager
from datetime import datetime

from .types import PulsarConfig, StreamMessage, TopicName

class PulsarManager:
    def __init__(self, config: PulsarConfig) -> None:
        self._config = config
        self._admin_client = httpx.Client(
            base_url=config["admin_service_url"],
            headers=self._get_auth_headers(),
            timeout=5.0
        )
        logger.debug(f"Initialized PulsarManager with Admin URL: {config['admin_service_url']}")

    def _get_auth_headers(self) -> dict[str, str]:
        if token := self._config.get("token"):
            return {"Authorization": f"Bearer {token}"}
        return {}

    def get_tenants(self) -> list[str]:
        """Fetch tenants using the Admin API."""
        try:
            response = self._admin_client.get("/admin/v2/tenants")
            response.raise_for_status()
            tenants = response.json()
            logger.debug(f"Fetched {len(tenants)} tenants")
            return tenants
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch tenants: {e}")
            return []

    def get_namespaces(self, tenant: str) -> list[str]:
        """Fetch namespaces for a specific tenant."""
        try:
            response = self._admin_client.get(f"/admin/v2/namespaces/{tenant}")
            response.raise_for_status()
            namespaces = response.json()
            logger.debug(f"Fetched {len(namespaces)} namespaces for tenant '{tenant}'")
            return namespaces
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch namespaces for {tenant}: {e}")
            return []

    def get_topics(self, namespace: str) -> list[TopicName]:
        """Fetch topics for a specific namespace."""
        try:
            # The API returns full topic names like 'persistent://tenant/ns/topic'
            response = self._admin_client.get(f"/admin/v2/namespaces/{namespace}/topics")
            response.raise_for_status()
            topics = response.json()
            logger.debug(f"Fetched {len(topics)} topics in '{namespace}'")
            return topics
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch topics for {namespace}: {e}")
            return []

    @contextmanager
    def create_listener(self, topic: TopicName, limit: int = 100) -> Generator[
        Generator[StreamMessage, None, None], None, None]:
        """
        Creates a Reader to peek at messages.
        Using a context manager ensures the connection closes cleanly.
        """
        broker_url = self._config["broker_service_url"]
        logger.info(f"Connecting to Broker at {broker_url} for topic: {topic}")

        try:
            client = pulsar.Client(
                broker_url,
                authentication=pulsar.AuthenticationToken(self._config["token"]) if self._config.get("token") else None
            )
        except Exception as e:
            logger.critical(f"Failed to create Pulsar Client: {e}")
            raise e

        try:
            # We use a Reader because it doesn't modify the cursor (unlike a Consumer).
            reader = client.create_reader(
                topic=topic,
                start_message_id=pulsar.MessageId.earliest,  # Or latest
                reader_listener=None,
            )
            logger.debug("Reader created successfully")

            def message_generator() -> Generator[StreamMessage, None, None]:
                count = 0
                while count < limit:
                    try:
                        # timeout_millis=2000 ensures we don't hang forever if topic is empty
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
                        # This usually happens on timeout (end of current stream)
                        # We break silently to finish the generator
                        break

                logger.success(f"Finished reading {count} messages from {topic}")

            yield message_generator()

        except Exception as e:
            logger.error(f"Error during stream reading: {e}")
            raise e
        finally:
            logger.debug("Closing Pulsar client connection...")
            client.close()

    def close(self):
        self._admin_client.close()
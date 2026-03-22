from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Mapping

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

try:
    from src.config import Settings, get_logger, get_settings
except ModuleNotFoundError:
    from config import Settings, get_logger, get_settings


LOGGER = get_logger(__name__)


def _serialize_message(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )


@dataclass(slots=True)
class KafkaEventProducer:
    producer: KafkaProducer
    delivery_timeout_seconds: int

    def send(
        self, topic: str, payload: Mapping[str, Any], key: str | None = None
    ) -> None:
        try:
            future = self.producer.send(
                topic=topic,
                key=key.encode("utf-8") if key else None,
                value=dict(payload),
            )
            metadata = future.get(timeout=self.delivery_timeout_seconds)
            LOGGER.debug(
                "Published Kafka message topic=%s partition=%s offset=%s",
                metadata.topic,
                metadata.partition,
                metadata.offset,
            )
        except KafkaError:
            LOGGER.exception("Failed to publish message to Kafka topic=%s", topic)
            raise

    def flush(self) -> None:
        self.producer.flush()

    def close(self) -> None:
        self.producer.flush()
        self.producer.close()


def create_kafka_producer(settings: Settings | None = None) -> KafkaEventProducer:
    resolved_settings = settings or get_settings()
    last_error: Exception | None = None

    for attempt in range(1, resolved_settings.kafka_producer_startup_max_attempts + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=resolved_settings.kafka_bootstrap_servers,
                client_id=resolved_settings.kafka_client_id,
                acks=resolved_settings.kafka_producer_acks,
                retries=resolved_settings.kafka_producer_retries,
                linger_ms=resolved_settings.kafka_producer_linger_ms,
                batch_size=resolved_settings.kafka_producer_batch_size,
                max_block_ms=resolved_settings.kafka_producer_max_block_ms,
                request_timeout_ms=resolved_settings.kafka_producer_request_timeout_ms,
                max_in_flight_requests_per_connection=resolved_settings.kafka_producer_max_in_flight_requests_per_connection,
                compression_type=resolved_settings.kafka_producer_compression_type,
                value_serializer=_serialize_message,
            )

            LOGGER.info(
                "Initialized Kafka producer for broker=%s client_id=%s",
                resolved_settings.kafka_broker,
                resolved_settings.kafka_client_id,
            )
            return KafkaEventProducer(
                producer=producer,
                delivery_timeout_seconds=resolved_settings.kafka_producer_delivery_timeout_seconds,
            )
        except (KafkaError, NoBrokersAvailable) as exc:
            last_error = exc
            LOGGER.exception(
                "Kafka producer initialization failed on attempt %s/%s",
                attempt,
                resolved_settings.kafka_producer_startup_max_attempts,
            )

            if attempt == resolved_settings.kafka_producer_startup_max_attempts:
                break

            time.sleep(resolved_settings.kafka_producer_startup_backoff_seconds)

    raise RuntimeError("Unable to initialize Kafka producer.") from last_error

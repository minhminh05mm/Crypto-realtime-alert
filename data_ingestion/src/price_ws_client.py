from __future__ import annotations

import json
import signal
from decimal import Decimal, InvalidOperation
from threading import Event
from typing import Any

import websocket

try:
    from src.config import Settings, get_logger, get_settings
    from src.kafka_utils import KafkaEventProducer, create_kafka_producer
except ModuleNotFoundError:
    from config import Settings, get_logger, get_settings
    from kafka_utils import KafkaEventProducer, create_kafka_producer


LOGGER = get_logger(__name__)


class BinancePriceWsClient:
    def __init__(
        self,
        settings: Settings | None = None,
        producer: KafkaEventProducer | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.producer = producer or create_kafka_producer(self.settings)
        self.stop_event = Event()
        self.ws_app: websocket.WebSocketApp | None = None
        self.published_events = 0
        self.tracked_symbols = self.settings.tracked_symbols

    def run(self) -> None:
        while not self.stop_event.is_set():
            LOGGER.info(
                "Connecting to Binance WebSocket url=%s", self.settings.binance_ws_url
            )
            self.ws_app = websocket.WebSocketApp(
                self.settings.binance_ws_url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )

            try:
                self.ws_app.run_forever(
                    ping_interval=self.settings.binance_ws_ping_interval_seconds,
                    ping_timeout=self.settings.binance_ws_ping_timeout_seconds,
                )
            except Exception:
                LOGGER.exception(
                    "Unexpected failure while running Binance WebSocket loop."
                )

            if self.stop_event.is_set():
                break

            LOGGER.warning(
                "Binance WebSocket disconnected. Reconnecting in %s seconds.",
                self.settings.binance_ws_reconnect_delay_seconds,
            )
            self.stop_event.wait(self.settings.binance_ws_reconnect_delay_seconds)

        self.shutdown()

    def stop(self) -> None:
        self.stop_event.set()
        if self.ws_app is not None:
            self.ws_app.close()

    def shutdown(self) -> None:
        try:
            self.producer.flush()
        finally:
            self.producer.close()
            LOGGER.info("Binance price ingestion stopped gracefully.")

    def _on_open(self, _: websocket.WebSocketApp) -> None:
        LOGGER.info("Connected to Binance WebSocket successfully.")

    def _on_message(self, _: websocket.WebSocketApp, raw_message: str) -> None:
        try:
            payload = json.loads(raw_message)
        except json.JSONDecodeError:
            LOGGER.exception("Failed to decode Binance WebSocket message.")
            return

        events = self._extract_events(payload)
        published_now = 0

        for event in events:
            if not isinstance(event, dict):
                LOGGER.warning("Skipping unexpected Binance payload: %s", event)
                continue

            normalized_event = self._normalize_event(event)
            if normalized_event is None:
                continue
            if (
                self.tracked_symbols
                and normalized_event["symbol"] not in self.tracked_symbols
            ):
                continue

            self.producer.send(
                topic=self.settings.kafka_topic_raw_prices,
                key=normalized_event["symbol"],
                payload=normalized_event,
            )
            published_now += 1
            self.published_events += 1

        if (
            published_now
            and self.published_events % self.settings.price_log_interval == 0
        ):
            LOGGER.info("Published %s price events to Kafka.", self.published_events)

    def _on_error(self, _: websocket.WebSocketApp, error: Any) -> None:
        if self.stop_event.is_set():
            return

        LOGGER.error("Binance WebSocket error: %s", error)

    def _on_close(
        self,
        _: websocket.WebSocketApp,
        close_status_code: int | None,
        close_message: str | None,
    ) -> None:
        if self.stop_event.is_set():
            LOGGER.info("Binance WebSocket closed during shutdown.")
            return

        LOGGER.warning(
            "Binance WebSocket closed status_code=%s message=%s",
            close_status_code,
            close_message,
        )

    @staticmethod
    def _extract_events(payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]

        if isinstance(payload, dict):
            nested_payload = payload.get("data")
            if isinstance(nested_payload, list):
                return [item for item in nested_payload if isinstance(item, dict)]
            if isinstance(nested_payload, dict):
                return [nested_payload]
            return [payload]

        return []

    @staticmethod
    def _normalize_event(event: dict[str, Any]) -> dict[str, Any] | None:
        try:
            symbol = str(event["s"]).strip().upper()
            price = float(Decimal(str(event["c"])))
            timestamp = int(event["E"])
        except (KeyError, TypeError, ValueError, InvalidOperation):
            LOGGER.exception("Skipping malformed Binance event: %s", event)
            return None

        if not symbol:
            LOGGER.warning("Skipping Binance event with empty symbol.")
            return None

        return {
            "symbol": symbol,
            "price": price,
            "timestamp": timestamp,
        }


def _register_signal_handlers(client: BinancePriceWsClient) -> None:
    def _handle_signal(signum: int, _: Any) -> None:
        LOGGER.info("Received signal=%s. Stopping Binance WebSocket client.", signum)
        client.stop()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)


def main() -> None:
    client = BinancePriceWsClient()
    _register_signal_handlers(client)
    client.run()


if __name__ == "__main__":
    main()

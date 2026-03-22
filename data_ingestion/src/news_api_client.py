from __future__ import annotations

import signal
from datetime import UTC, datetime
from threading import Event
from typing import Any

import requests

try:
    from src.config import Settings, get_logger, get_settings
    from src.kafka_utils import KafkaEventProducer, create_kafka_producer
except ModuleNotFoundError:
    from config import Settings, get_logger, get_settings
    from kafka_utils import KafkaEventProducer, create_kafka_producer


LOGGER = get_logger(__name__)


class CryptoPanicNewsClient:
    def __init__(
        self,
        settings: Settings | None = None,
        producer: KafkaEventProducer | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.producer = producer or create_kafka_producer(self.settings)
        self.session = session or requests.Session()
        self.stop_event = Event()
        self.seen_news_ids: set[str] = set()

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                fetched_count, published_count = self._poll_once()
                LOGGER.info(
                    "CryptoPanic poll completed fetched=%s published=%s",
                    fetched_count,
                    published_count,
                )
            except requests.RequestException:
                LOGGER.exception("Network error while polling CryptoPanic API.")
            except Exception:
                LOGGER.exception("Unexpected error while polling CryptoPanic API.")

            self.stop_event.wait(self.settings.cryptopanic_poll_interval_seconds)

        self.shutdown()

    def stop(self) -> None:
        self.stop_event.set()

    def shutdown(self) -> None:
        try:
            self.producer.flush()
        finally:
            self.producer.close()
            self.session.close()
            LOGGER.info("CryptoPanic news ingestion stopped gracefully.")

    def _poll_once(self) -> tuple[int, int]:
        response = self.session.get(
            self.settings.cryptopanic_base_url,
            params={
                "auth_token": self.settings.cryptopanic_api_key,
                "kind": self.settings.cryptopanic_filter_kind,
            },
            timeout=self.settings.http_timeout_seconds,
        )
        response.raise_for_status()

        payload = response.json()
        raw_results = payload.get("results", [])
        if not isinstance(raw_results, list):
            raise ValueError("CryptoPanic API returned an unexpected payload structure.")

        fetched_count = len(raw_results)
        published_count = 0

        for article in reversed(raw_results[: self.settings.cryptopanic_page_size]):
            if not isinstance(article, dict):
                LOGGER.warning("Skipping unexpected CryptoPanic item: %s", article)
                continue

            normalized_article = self._normalize_article(article)
            if normalized_article is None:
                continue

            article_id = self._extract_article_id(article, normalized_article)
            if article_id in self.seen_news_ids:
                continue

            self.producer.send(
                topic=self.settings.kafka_topic_raw_news,
                payload=normalized_article,
                key=article_id,
            )
            self.seen_news_ids.add(article_id)
            published_count += 1

        return fetched_count, published_count

    @staticmethod
    def _extract_article_id(
        article: dict[str, Any], normalized_article: dict[str, str]
    ) -> str:
        identifier = article.get("id")
        if identifier is not None:
            return str(identifier)

        return (
            f"{normalized_article['title']}|{normalized_article['published_at']}"
        )

    @staticmethod
    def _normalize_article(article: dict[str, Any]) -> dict[str, str] | None:
        try:
            title = str(article["title"]).strip()
            published_at = CryptoPanicNewsClient._normalize_timestamp(
                str(article["published_at"]).strip()
            )
        except (KeyError, TypeError, ValueError):
            LOGGER.exception("Skipping malformed CryptoPanic article: %s", article)
            return None

        if not title:
            LOGGER.warning("Skipping CryptoPanic article with empty title.")
            return None

        return {
            "title": title,
            "published_at": published_at,
        }

    @staticmethod
    def _normalize_timestamp(value: str) -> str:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
        return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def _register_signal_handlers(client: CryptoPanicNewsClient) -> None:
    def _handle_signal(signum: int, _: Any) -> None:
        LOGGER.info("Received signal=%s. Stopping CryptoPanic client.", signum)
        client.stop()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)


def main() -> None:
    client = CryptoPanicNewsClient()
    _register_signal_handlers(client)
    client.run()


if __name__ == "__main__":
    main()

from __future__ import annotations

import logging
import time
from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


ROOT_DIR = Path(__file__).resolve().parents[2]
ENV_FILE = ROOT_DIR / ".env"
_LOGGING_CONFIGURED = False


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_env: str = Field(validation_alias="APP_ENV")
    tz: str = Field(validation_alias="TZ")
    log_level: str = Field(validation_alias="LOG_LEVEL")

    kafka_broker: str = Field(validation_alias="KAFKA_BROKER")
    kafka_topic_raw_prices: str = Field(validation_alias="KAFKA_TOPIC_RAW_PRICES")
    kafka_topic_raw_news: str = Field(validation_alias="KAFKA_TOPIC_RAW_NEWS")
    kafka_client_id: str = Field(validation_alias="KAFKA_CLIENT_ID")
    kafka_producer_acks: str = Field(validation_alias="KAFKA_PRODUCER_ACKS")
    kafka_producer_retries: int = Field(
        ge=0, validation_alias="KAFKA_PRODUCER_RETRIES"
    )
    kafka_producer_linger_ms: int = Field(
        ge=0, validation_alias="KAFKA_PRODUCER_LINGER_MS"
    )
    kafka_producer_batch_size: int = Field(
        gt=0, validation_alias="KAFKA_PRODUCER_BATCH_SIZE"
    )
    kafka_producer_max_block_ms: int = Field(
        gt=0, validation_alias="KAFKA_PRODUCER_MAX_BLOCK_MS"
    )
    kafka_producer_request_timeout_ms: int = Field(
        gt=0, validation_alias="KAFKA_PRODUCER_REQUEST_TIMEOUT_MS"
    )
    kafka_producer_delivery_timeout_seconds: int = Field(
        gt=0, validation_alias="KAFKA_PRODUCER_DELIVERY_TIMEOUT_SECONDS"
    )
    kafka_producer_startup_max_attempts: int = Field(
        gt=0, validation_alias="KAFKA_PRODUCER_STARTUP_MAX_ATTEMPTS"
    )
    kafka_producer_startup_backoff_seconds: int = Field(
        ge=0, validation_alias="KAFKA_PRODUCER_STARTUP_BACKOFF_SECONDS"
    )
    kafka_producer_compression_type: str = Field(
        validation_alias="KAFKA_PRODUCER_COMPRESSION_TYPE"
    )
    kafka_producer_max_in_flight_requests_per_connection: int = Field(
        gt=0,
        validation_alias="KAFKA_PRODUCER_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION",
    )

    binance_ws_url: str = Field(validation_alias="BINANCE_WS_URL")
    binance_ws_reconnect_delay_seconds: int = Field(
        ge=0, validation_alias="BINANCE_WS_RECONNECT_DELAY_SECONDS"
    )
    binance_ws_ping_interval_seconds: int = Field(
        gt=0, validation_alias="BINANCE_WS_PING_INTERVAL_SECONDS"
    )
    binance_ws_ping_timeout_seconds: int = Field(
        gt=0, validation_alias="BINANCE_WS_PING_TIMEOUT_SECONDS"
    )
    price_log_interval: int = Field(gt=0, validation_alias="PRICE_LOG_INTERVAL")
    binance_tracked_symbols: str = Field(validation_alias="BINANCE_TRACKED_SYMBOLS")

    cryptopanic_api_key: str = Field(validation_alias="CRYPTOPANIC_API_KEY")
    cryptopanic_base_url: str = Field(validation_alias="CRYPTOPANIC_BASE_URL")
    cryptopanic_filter_kind: str = Field(validation_alias="CRYPTOPANIC_FILTER_KIND")
    cryptopanic_poll_interval_seconds: int = Field(
        gt=0, validation_alias="CRYPTOPANIC_POLL_INTERVAL_SECONDS"
    )
    cryptopanic_page_size: int = Field(
        gt=0, validation_alias="CRYPTOPANIC_PAGE_SIZE"
    )
    cryptopanic_backoff_base_seconds: int = Field(
        gt=0, validation_alias="CRYPTOPANIC_BACKOFF_BASE_SECONDS"
    )
    cryptopanic_backoff_max_seconds: int = Field(
        gt=0, validation_alias="CRYPTOPANIC_BACKOFF_MAX_SECONDS"
    )
    cryptopanic_rss_fallback_enabled: bool = Field(
        validation_alias="CRYPTOPANIC_RSS_FALLBACK_ENABLED"
    )
    cryptopanic_rss_fallback_urls: str = Field(
        validation_alias="CRYPTOPANIC_RSS_FALLBACK_URLS"
    )
    cryptopanic_rss_fallback_page_size: int = Field(
        gt=0, validation_alias="CRYPTOPANIC_RSS_FALLBACK_PAGE_SIZE"
    )
    http_timeout_seconds: int = Field(gt=0, validation_alias="HTTP_TIMEOUT_SECONDS")

    @field_validator(
        "app_env",
        "tz",
        "kafka_broker",
        "kafka_topic_raw_prices",
        "kafka_topic_raw_news",
        "kafka_client_id",
        "binance_ws_url",
        "binance_tracked_symbols",
        "cryptopanic_api_key",
        "cryptopanic_base_url",
        "cryptopanic_filter_kind",
        "cryptopanic_rss_fallback_urls",
        mode="before",
    )
    @classmethod
    def strip_text_fields(cls, value: str) -> str:
        if not isinstance(value, str):
            raise TypeError("Expected a string environment variable.")

        normalized = value.strip()
        if not normalized:
            raise ValueError("Environment variable must not be empty.")

        return normalized

    @field_validator("log_level", mode="before")
    @classmethod
    def validate_log_level(cls, value: str) -> str:
        if not isinstance(value, str):
            raise TypeError("LOG_LEVEL must be a string.")

        normalized = value.strip().upper()
        if normalized not in logging.getLevelNamesMapping():
            raise ValueError(f"Unsupported LOG_LEVEL: {value}")

        return normalized

    @property
    def tracked_symbols(self) -> set[str]:
        return {
            symbol.strip().upper()
            for symbol in self.binance_tracked_symbols.split(",")
            if symbol.strip()
        }

    @property
    def cryptopanic_rss_urls(self) -> list[str]:
        return [
            url.strip()
            for url in self.cryptopanic_rss_fallback_urls.split(",")
            if url.strip()
        ]

    @property
    def kafka_bootstrap_servers(self) -> list[str]:
        return [self.kafka_broker]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def configure_logging() -> None:
    global _LOGGING_CONFIGURED

    if _LOGGING_CONFIGURED:
        return

    settings = get_settings()
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    for logger_name, level_name in {
        "kafka": "WARNING",
        "websocket": "WARNING",
        "urllib3": "WARNING",
    }.items():
        logging.getLogger(logger_name).setLevel(getattr(logging, level_name))
    _LOGGING_CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    configure_logging()
    return logging.getLogger(name)

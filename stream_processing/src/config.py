from __future__ import annotations

import json
import logging
import time
from functools import lru_cache
from pathlib import Path

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

    redis_host: str = Field(validation_alias="REDIS_HOST")
    redis_port: int = Field(gt=0, validation_alias="REDIS_PORT")
    redis_db: int = Field(ge=0, validation_alias="REDIS_DB")
    redis_password: str = Field(default="", validation_alias="REDIS_PASSWORD")
    redis_alert_key_prefix: str = Field(validation_alias="REDIS_ALERT_KEY_PREFIX")
    redis_alert_ttl_seconds: int = Field(gt=0, validation_alias="REDIS_ALERT_TTL_SECONDS")

    clickhouse_host: str = Field(validation_alias="CLICKHOUSE_HOST")
    clickhouse_native_port: int = Field(gt=0, validation_alias="CLICKHOUSE_NATIVE_PORT")
    clickhouse_db: str = Field(validation_alias="CLICKHOUSE_DB")
    clickhouse_user: str = Field(validation_alias="CLICKHOUSE_USER")
    clickhouse_password: str = Field(validation_alias="CLICKHOUSE_PASSWORD")
    clickhouse_alerts_table: str = Field(validation_alias="CLICKHOUSE_ALERTS_TABLE")

    spark_app_name: str = Field(validation_alias="SPARK_APP_NAME")
    spark_master: str = Field(validation_alias="SPARK_MASTER")
    spark_local_ip: str = Field(validation_alias="SPARK_LOCAL_IP")
    spark_local_hostname: str = Field(validation_alias="SPARK_LOCAL_HOSTNAME")
    spark_driver_host: str = Field(validation_alias="SPARK_DRIVER_HOST")
    spark_driver_bind_address: str = Field(
        validation_alias="SPARK_DRIVER_BIND_ADDRESS"
    )
    spark_sql_shuffle_partitions: int = Field(
        gt=0, validation_alias="SPARK_SQL_SHUFFLE_PARTITIONS"
    )
    spark_kafka_starting_offsets: str = Field(
        validation_alias="SPARK_KAFKA_STARTING_OFFSETS"
    )
    spark_trigger_interval_seconds: int = Field(
        gt=0, validation_alias="SPARK_TRIGGER_INTERVAL_SECONDS"
    )
    spark_output_mode: str = Field(validation_alias="SPARK_OUTPUT_MODE")
    spark_window_duration: str = Field(validation_alias="SPARK_WINDOW_DURATION")
    spark_watermark_delay: str = Field(validation_alias="SPARK_WATERMARK_DELAY")
    spark_checkpoint_dir: str = Field(validation_alias="SPARK_CHECKPOINT_DIR")
    spark_ivy_cache_dir: str = Field(validation_alias="SPARK_IVY_CACHE_DIR")
    spark_jars_packages: str = Field(validation_alias="SPARK_JARS_PACKAGES")

    finbert_model_name: str = Field(validation_alias="FINBERT_MODEL_NAME")
    finbert_max_length: int = Field(gt=0, validation_alias="FINBERT_MAX_LENGTH")

    alert_price_change_threshold_pct: float = Field(
        gt=0, validation_alias="ALERT_PRICE_CHANGE_THRESHOLD_PCT"
    )
    alert_status_warning_high_volatility: str = Field(
        validation_alias="ALERT_STATUS_WARNING_HIGH_VOLATILITY"
    )
    alert_status_normal: str = Field(validation_alias="ALERT_STATUS_NORMAL")

    news_symbol_keywords_json: str = Field(validation_alias="NEWS_SYMBOL_KEYWORDS_JSON")

    @field_validator(
        "app_env",
        "tz",
        "kafka_broker",
        "kafka_topic_raw_prices",
        "kafka_topic_raw_news",
        "redis_host",
        "redis_alert_key_prefix",
        "clickhouse_host",
        "clickhouse_db",
        "clickhouse_user",
        "clickhouse_password",
        "clickhouse_alerts_table",
        "spark_app_name",
        "spark_master",
        "spark_local_ip",
        "spark_local_hostname",
        "spark_driver_host",
        "spark_driver_bind_address",
        "spark_kafka_starting_offsets",
        "spark_window_duration",
        "spark_watermark_delay",
        "spark_output_mode",
        "spark_checkpoint_dir",
        "spark_ivy_cache_dir",
        "spark_jars_packages",
        "finbert_model_name",
        "alert_status_warning_high_volatility",
        "alert_status_normal",
        "news_symbol_keywords_json",
        mode="before",
    )
    @classmethod
    def strip_required_strings(cls, value: str) -> str:
        if not isinstance(value, str):
            raise TypeError("Expected a string environment variable.")

        normalized = value.strip()
        if not normalized:
            raise ValueError("Environment variable must not be empty.")

        return normalized

    @field_validator("redis_password", mode="before")
    @classmethod
    def normalize_optional_string(cls, value: str | None) -> str:
        if value is None:
            return ""

        if not isinstance(value, str):
            raise TypeError("Expected a string environment variable.")

        return value.strip()

    @field_validator("log_level", mode="before")
    @classmethod
    def validate_log_level(cls, value: str) -> str:
        if not isinstance(value, str):
            raise TypeError("LOG_LEVEL must be a string.")

        normalized = value.strip().upper()
        if normalized not in logging.getLevelNamesMapping():
            raise ValueError(f"Unsupported LOG_LEVEL: {value}")

        return normalized

    @field_validator("spark_kafka_starting_offsets", mode="before")
    @classmethod
    def validate_kafka_offsets(cls, value: str) -> str:
        if not isinstance(value, str):
            raise TypeError("SPARK_KAFKA_STARTING_OFFSETS must be a string.")

        normalized = value.strip().lower()
        if normalized not in {"latest", "earliest"}:
            raise ValueError(
                "SPARK_KAFKA_STARTING_OFFSETS must be either 'latest' or 'earliest'."
            )

        return normalized

    @field_validator("spark_output_mode", mode="before")
    @classmethod
    def validate_output_mode(cls, value: str) -> str:
        if not isinstance(value, str):
            raise TypeError("SPARK_OUTPUT_MODE must be a string.")

        normalized = value.strip().lower()
        if normalized not in {"append", "update"}:
            raise ValueError("SPARK_OUTPUT_MODE must be either 'append' or 'update'.")

        return normalized

    @property
    def news_symbol_keywords(self) -> dict[str, list[str]]:
        payload = json.loads(self.news_symbol_keywords_json)
        if not isinstance(payload, dict):
            raise ValueError("NEWS_SYMBOL_KEYWORDS_JSON must be a JSON object.")

        normalized: dict[str, list[str]] = {}
        for symbol, keywords in payload.items():
            if not isinstance(symbol, str) or not symbol.strip():
                raise ValueError("NEWS symbol mapping contains an invalid symbol key.")

            if not isinstance(keywords, list) or not keywords:
                raise ValueError(
                    f"NEWS symbol mapping for {symbol} must be a non-empty list."
                )

            normalized[symbol.strip().upper()] = [
                keyword.strip().upper()
                for keyword in keywords
                if isinstance(keyword, str) and keyword.strip()
            ]

        return normalized


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
        "httpx": "WARNING",
        "huggingface_hub": "WARNING",
        "transformers": "WARNING",
        "kafka": "WARNING",
        "py4j": "WARNING",
        "urllib3": "WARNING",
    }.items():
        logging.getLogger(logger_name).setLevel(getattr(logging, level_name))
    _LOGGING_CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    configure_logging()
    return logging.getLogger(name)

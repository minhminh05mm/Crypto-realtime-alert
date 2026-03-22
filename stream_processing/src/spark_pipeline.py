from __future__ import annotations

import json
import os
import re
from pathlib import Path
import sys
from typing import Any, Iterable

from clickhouse_driver import Client as ClickHouseClient
import redis
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from pyspark.storagelevel import StorageLevel

try:
    from src.config import Settings, get_logger, get_settings
    from src.nlp_analyzer import FinBertSentimentAnalyzer
except ModuleNotFoundError:
    from config import Settings, get_logger, get_settings
    from nlp_analyzer import FinBertSentimentAnalyzer


LOGGER = get_logger(__name__)

PRICE_SCHEMA = T.StructType(
    [
        T.StructField("symbol", T.StringType(), nullable=False),
        T.StructField("price", T.DoubleType(), nullable=False),
        T.StructField("timestamp", T.LongType(), nullable=False),
    ]
)

NEWS_SCHEMA = T.StructType(
    [
        T.StructField("title", T.StringType(), nullable=False),
        T.StructField("published_at", T.StringType(), nullable=False),
    ]
)

SENTIMENT_SCHEMA = T.StructType(
    [
        T.StructField("label", T.StringType(), nullable=False),
        T.StructField("sentiment_score", T.DoubleType(), nullable=False),
    ]
)

CLICKHOUSE_COLUMNS = [
    "timestamp",
    "symbol",
    "current_price",
    "first_price",
    "min_price",
    "max_price",
    "price_change_pct",
    "price_tick_count",
    "news_count",
    "sentiment_label",
    "sentiment_score",
    "alert_status",
    "headline",
    "window_start",
    "window_end",
    "processed_at",
]


def build_spark_session(settings: Settings) -> SparkSession:
    os.environ["SPARK_LOCAL_IP"] = settings.spark_local_ip
    os.environ["SPARK_LOCAL_HOSTNAME"] = settings.spark_local_hostname
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    Path(settings.spark_ivy_cache_dir).mkdir(parents=True, exist_ok=True)
    _checkpoint_location(settings).mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder.appName(settings.spark_app_name)
        .master(settings.spark_master)
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", settings.spark_driver_host)
        .config("spark.driver.bindAddress", settings.spark_driver_bind_address)
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.jars.ivy", settings.spark_ivy_cache_dir)
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.session.timeZone", settings.tz)
        .config("spark.sql.shuffle.partitions", settings.spark_sql_shuffle_partitions)
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
        .config("spark.jars.packages", settings.spark_jars_packages)
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    _configure_spark_loggers(spark)
    return spark


def _checkpoint_location(settings: Settings) -> Path:
    return Path(settings.spark_checkpoint_dir) / settings.spark_output_mode


def _configure_spark_loggers(spark: SparkSession) -> None:
    try:
        logger_names = [
            "org.apache.kafka.clients.admin.AdminClientConfig",
            "org.apache.spark.sql.kafka010.KafkaDataConsumer",
            "org.apache.spark.sql.kafka010.consumer.KafkaDataConsumer",
            "org.apache.hadoop.util.NativeCodeLoader",
            "org.apache.spark.sql.execution.streaming.ResolveWriteToStream",
            "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
        ]

        try:
            configurator = spark._jvm.org.apache.logging.log4j.core.config.Configurator
            log4j2_level = spark._jvm.org.apache.logging.log4j.Level.ERROR
            for logger_name in logger_names:
                configurator.setLevel(logger_name, log4j2_level)
        except Exception:
            pass

        try:
            log4j_manager = spark._jvm.org.apache.log4j.LogManager
            log4j_level = spark._jvm.org.apache.log4j.Level.ERROR
            for logger_name in logger_names:
                log4j_manager.getLogger(logger_name).setLevel(log4j_level)
        except Exception:
            pass
    except Exception:
        LOGGER.debug("Unable to customize JVM logger levels for Spark warning cleanup.")


def _build_sentiment_udf(settings: Settings):
    @F.udf(returnType=SENTIMENT_SCHEMA)
    def sentiment_udf(title: str | None) -> dict[str, Any]:
        analyzer = FinBertSentimentAnalyzer.get_instance(
            model_name=settings.finbert_model_name,
            max_length=settings.finbert_max_length,
        )
        prediction = analyzer.predict(title)
        return {
            "label": prediction.label,
            "sentiment_score": prediction.sentiment_score,
        }

    return sentiment_udf


def _build_symbol_extraction_udf(symbol_mapping: dict[str, list[str]]):
    compiled_mapping = {
        symbol: [
            re.compile(rf"(?<![A-Z0-9]){re.escape(keyword)}(?![A-Z0-9])")
            for keyword in keywords
        ]
        for symbol, keywords in symbol_mapping.items()
    }

    @F.udf(returnType=T.StringType())
    def extract_symbol_udf(title: str | None) -> str | None:
        if not title or not title.strip():
            return None

        normalized_title = title.upper()
        for symbol, patterns in compiled_mapping.items():
            if any(pattern.search(normalized_title) for pattern in patterns):
                return symbol

        return None

    return extract_symbol_udf


def _read_kafka_topic(
    spark: SparkSession, settings: Settings, topic_name: str
) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_broker)
        .option("subscribe", topic_name)
        .option("startingOffsets", settings.spark_kafka_starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )


def _build_price_stream(spark: SparkSession, settings: Settings) -> DataFrame:
    raw_prices = _read_kafka_topic(spark, settings, settings.kafka_topic_raw_prices)

    parsed_prices = (
        raw_prices.selectExpr("CAST(value AS STRING) AS message")
        .select(F.from_json(F.col("message"), PRICE_SCHEMA).alias("payload"))
        .select("payload.*")
        .withColumn("symbol", F.upper(F.trim(F.col("symbol"))))
        .withColumn(
            "event_time",
            F.to_timestamp(
                F.from_unixtime((F.col("timestamp") / F.lit(1000)).cast("double"))
            ),
        )
        .where(
            F.col("symbol").isNotNull()
            & F.col("price").isNotNull()
            & F.col("timestamp").isNotNull()
            & F.col("event_time").isNotNull()
        )
    )

    price_windows = (
        parsed_prices.withWatermark("event_time", settings.spark_watermark_delay)
        .groupBy(
            F.window(F.col("event_time"), settings.spark_window_duration),
            F.col("symbol"),
        )
        .agg(
            F.min(F.struct(F.col("event_time"), F.col("timestamp"), F.col("price"))).alias(
                "first_observation"
            ),
            F.max(F.struct(F.col("event_time"), F.col("timestamp"), F.col("price"))).alias(
                "last_observation"
            ),
            F.min(F.col("price")).alias("min_price"),
            F.max(F.col("price")).alias("max_price"),
            F.count(F.lit(1)).alias("price_tick_count"),
        )
        .select(
            F.col("window"),
            F.col("symbol"),
            F.col("first_observation.price").alias("first_price"),
            F.col("last_observation.price").alias("current_price"),
            F.col("last_observation.timestamp").alias("timestamp_ms"),
            F.col("min_price"),
            F.col("max_price"),
            F.col("price_tick_count"),
        )
        .withColumn(
            "price_change_pct",
            F.when(
                F.col("first_price") > F.lit(0.0),
                F.abs(
                    (F.col("current_price") - F.col("first_price"))
                    / F.col("first_price")
                )
                * F.lit(100.0),
            ).otherwise(F.lit(0.0)),
        )
    )

    return price_windows


def _build_news_stream(spark: SparkSession, settings: Settings) -> DataFrame:
    raw_news = _read_kafka_topic(spark, settings, settings.kafka_topic_raw_news)
    sentiment_udf = _build_sentiment_udf(settings)
    extract_symbol_udf = _build_symbol_extraction_udf(settings.news_symbol_keywords)

    parsed_news = (
        raw_news.selectExpr("CAST(value AS STRING) AS message")
        .select(F.from_json(F.col("message"), NEWS_SCHEMA).alias("payload"))
        .select("payload.*")
        .withColumn("published_at_ts", F.to_timestamp(F.col("published_at")))
        .withColumn("symbol", extract_symbol_udf(F.col("title")))
        .withColumn("sentiment", sentiment_udf(F.col("title")))
        .select(
            F.col("symbol"),
            F.col("title"),
            F.col("published_at"),
            F.col("published_at_ts"),
            F.col("sentiment.label").alias("sentiment_label"),
            F.col("sentiment.sentiment_score").alias("sentiment_score"),
        )
        .where(
            F.col("symbol").isNotNull()
            & F.col("title").isNotNull()
            & F.col("published_at_ts").isNotNull()
        )
    )

    dominant_news = F.max(
        F.struct(
            F.abs(F.col("sentiment_score")).alias("sentiment_strength"),
            F.col("published_at_ts").alias("published_at_ts"),
            F.col("sentiment_label").alias("sentiment_label"),
            F.col("sentiment_score").alias("sentiment_score"),
            F.col("title").alias("headline"),
        )
    )

    news_windows = (
        parsed_news.withWatermark("published_at_ts", settings.spark_watermark_delay)
        .groupBy(
            F.window(F.col("published_at_ts"), settings.spark_window_duration),
            F.col("symbol"),
        )
        .agg(
            F.count(F.lit(1)).alias("news_count"),
            dominant_news.alias("dominant_news"),
        )
        .select(
            F.col("window"),
            F.col("symbol"),
            F.col("news_count"),
            F.col("dominant_news.headline").alias("headline"),
            F.col("dominant_news.sentiment_label").alias("sentiment_label"),
            F.col("dominant_news.sentiment_score").alias("sentiment_score"),
        )
    )

    return news_windows


def _build_alert_stream(
    price_windows: DataFrame, news_windows: DataFrame, settings: Settings
) -> DataFrame:
    joined = (
        price_windows.alias("prices")
        .join(
            news_windows.alias("news"),
            on=(
                (F.col("prices.symbol") == F.col("news.symbol"))
                & (F.col("prices.window.start") == F.col("news.window.start"))
                & (F.col("prices.window.end") == F.col("news.window.end"))
            ),
            how="inner",
        )
        .select(
            F.to_timestamp(
                F.from_unixtime((F.col("prices.timestamp_ms") / F.lit(1000)).cast("double"))
            ).alias("timestamp"),
            F.col("prices.symbol").alias("symbol"),
            F.col("prices.current_price").alias("current_price"),
            F.col("prices.first_price").alias("first_price"),
            F.col("prices.min_price").alias("min_price"),
            F.col("prices.max_price").alias("max_price"),
            F.col("prices.price_change_pct").alias("price_change_pct"),
            F.col("prices.price_tick_count").alias("price_tick_count"),
            F.col("news.news_count").alias("news_count"),
            F.col("news.sentiment_label").alias("sentiment_label"),
            F.col("news.sentiment_score").alias("sentiment_score"),
            F.col("news.headline").alias("headline"),
            F.col("prices.window.start").alias("window_start"),
            F.col("prices.window.end").alias("window_end"),
        )
    )

    return (
        joined.withColumn(
            "alert_status",
            F.when(
                (F.col("price_change_pct") >= F.lit(settings.alert_price_change_threshold_pct))
                & F.col("sentiment_label").isin("Positive", "Negative"),
                F.lit(settings.alert_status_warning_high_volatility),
            ).otherwise(F.lit(settings.alert_status_normal)),
        )
        .withColumn("processed_at", F.current_timestamp())
    )


def _write_to_redis(batch_df: DataFrame, settings: Settings) -> None:
    connection_kwargs = {
        "host": settings.redis_host,
        "port": settings.redis_port,
        "db": settings.redis_db,
        "decode_responses": True,
    }
    if settings.redis_password:
        connection_kwargs["password"] = settings.redis_password

    key_prefix = settings.redis_alert_key_prefix
    ttl_seconds = settings.redis_alert_ttl_seconds

    def _write_partition(rows: Iterable[Any]) -> None:
        client = redis.Redis(**connection_kwargs)
        pipeline = client.pipeline(transaction=False)
        counter = 0

        try:
            for row in rows:
                payload = {
                    "symbol": row["symbol"],
                    "current_price": row["current_price"],
                    "price_change_pct": row["price_change_pct"],
                    "sentiment_label": row["sentiment_label"],
                    "sentiment_score": row["sentiment_score"],
                    "news_count": row["news_count"],
                    "headline": row["headline"],
                    "alert_status": row["alert_status"],
                    "timestamp": row["timestamp"].isoformat()
                    if row["timestamp"] is not None
                    else None,
                    "window_start": row["window_start"].isoformat()
                    if row["window_start"] is not None
                    else None,
                    "window_end": row["window_end"].isoformat()
                    if row["window_end"] is not None
                    else None,
                    "processed_at": row["processed_at"].isoformat()
                    if row["processed_at"] is not None
                    else None,
                }
                key = f"{key_prefix}:{row['symbol']}"
                pipeline.set(name=key, value=json.dumps(payload, default=str))
                pipeline.expire(name=key, time=ttl_seconds)
                counter += 1

            if counter > 0:
                pipeline.execute()
        finally:
            pipeline.reset()
            client.close()

    batch_df.foreachPartition(_write_partition)


def _write_to_clickhouse(batch_df: DataFrame, settings: Settings) -> None:
    insert_statement = f"""
        INSERT INTO {settings.clickhouse_db}.{settings.clickhouse_alerts_table}
        ({", ".join(CLICKHOUSE_COLUMNS)})
        VALUES
    """

    def _write_partition(rows: Iterable[Any]) -> None:
        client = ClickHouseClient(
            host=settings.clickhouse_host,
            port=settings.clickhouse_native_port,
            database=settings.clickhouse_db,
            user=settings.clickhouse_user,
            password=settings.clickhouse_password,
        )
        records: list[tuple[Any, ...]] = []

        try:
            for row in rows:
                records.append(
                    tuple(row[column] for column in CLICKHOUSE_COLUMNS)
                )

            if records:
                client.execute(insert_statement, records)
        finally:
            client.disconnect()

    batch_df.foreachPartition(_write_partition)


def _process_batch(batch_df: DataFrame, batch_id: int, settings: Settings) -> None:
    if batch_df.rdd.isEmpty():
        LOGGER.info("Skipping empty micro-batch batch_id=%s", batch_id)
        return

    persisted_df = batch_df.persist(StorageLevel.MEMORY_AND_DISK)

    try:
        total_rows = persisted_df.count()
        warning_rows = persisted_df.filter(
            F.col("alert_status") == settings.alert_status_warning_high_volatility
        ).count()
        LOGGER.info(
            "Processing alert micro-batch batch_id=%s rows=%s warning_rows=%s",
            batch_id,
            total_rows,
            warning_rows,
        )

        _write_to_redis(persisted_df, settings)
        _write_to_clickhouse(persisted_df, settings)
    finally:
        persisted_df.unpersist()


def build_pipeline(spark: SparkSession, settings: Settings) -> DataFrame:
    price_windows = _build_price_stream(spark, settings)
    news_windows = _build_news_stream(spark, settings)
    return _build_alert_stream(price_windows, news_windows, settings)


def run() -> None:
    settings = get_settings()
    spark = build_spark_session(settings)
    alerts_stream = build_pipeline(spark, settings)

    query = (
        alerts_stream.writeStream.outputMode(settings.spark_output_mode)
        .trigger(processingTime=f"{settings.spark_trigger_interval_seconds} seconds")
        .option("checkpointLocation", str(_checkpoint_location(settings)))
        .foreachBatch(lambda batch_df, batch_id: _process_batch(batch_df, batch_id, settings))
        .queryName("crypto_realtime_alert_pipeline")
        .start()
    )

    LOGGER.info("Spark streaming query started successfully.")
    query.awaitTermination()


if __name__ == "__main__":
    run()

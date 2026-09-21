from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_events: str = "events"
    kafka_topic_dlq: str = "events.dlq"
    kafka_consumer_group: str = "event-processors"

    redis_url: str = "redis://localhost:6379/0"

    postgres_dsn: str = "postgresql://events:events@localhost:5432/events"

    max_retries: int = 5
    retry_backoff_base_seconds: float = 0.5
    idempotency_ttl_seconds: int = 86400

    # Injects synthetic transient failures into processing so the retry/DLQ
    # path is exercised without needing to break real infrastructure.
    simulate_failure_rate: float = 0.0

    aws_region: str = "us-east-1"
    s3_archive_bucket: str | None = None
    enable_cloudwatch_metrics: bool = False


settings = Settings()

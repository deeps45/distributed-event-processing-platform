"""Optional AWS integration points.

Both are no-ops unless explicitly configured, so the platform runs fully
offline by default (see .env.example). They're written against the real
boto3 client contracts so flipping them on against a real AWS account is a
config change, not a code change.
"""

import json
import logging
from datetime import datetime

import boto3

from src.config import settings

logger = logging.getLogger(__name__)

_s3_client = None
_cloudwatch_client = None


def _s3():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3", region_name=settings.aws_region)
    return _s3_client


def _cloudwatch():
    global _cloudwatch_client
    if _cloudwatch_client is None:
        _cloudwatch_client = boto3.client("cloudwatch", region_name=settings.aws_region)
    return _cloudwatch_client


def archive_event_to_s3(event_id: str, event_type: str, payload: dict, produced_at: datetime) -> None:
    if not settings.s3_archive_bucket:
        return
    key = f"events/{event_type}/{produced_at:%Y/%m/%d}/{event_id}.json"
    try:
        _s3().put_object(
            Bucket=settings.s3_archive_bucket,
            Key=key,
            Body=json.dumps(payload).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception:
        logger.exception("failed to archive event to S3", extra={"event_id": event_id})


def emit_cloudwatch_metric(metric_name: str, value: float, unit: str = "Count") -> None:
    if not settings.enable_cloudwatch_metrics:
        return
    try:
        _cloudwatch().put_metric_data(
            Namespace="DistributedEventProcessingPlatform",
            MetricData=[{"MetricName": metric_name, "Value": value, "Unit": unit}],
        )
    except Exception:
        logger.exception("failed to emit CloudWatch metric", extra={"metric_name": metric_name})

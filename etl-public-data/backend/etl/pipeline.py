import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Any

from config import settings
from db.database import SessionLocal
from db.models import EtlRunLog
from etl.extractors.air_quality import AirQualityExtractor
from etl.extractors.weather import WeatherExtractor
from etl.extractors.subway import SubwayExtractor
from etl.transformers.common import MissingValueInterpolator
from etl.transformers.region_mapper import RegionMapper
from etl.transformers.schema_normalizer import (
    AirQualityNormalizer, WeatherNormalizer, SubwayNormalizer,
)
from etl.loaders.db_loader import upsert_records
from etl.context import run_id_var

logger = logging.getLogger(__name__)


PIPELINE_CONFIG = {
    "air_quality": {
        "extractor_cls": AirQualityExtractor,
        "api_key_attr": "air_quality_api_key",
        "normalizer": AirQualityNormalizer(),
        "interpolator": MissingValueInterpolator(
            numeric_fields=["pm10", "pm25", "o3", "no2", "co", "so2"]
        ),
    },
    "weather": {
        "extractor_cls": WeatherExtractor,
        "api_key_attr": "weather_api_key",
        "normalizer": WeatherNormalizer(),
        "interpolator": MissingValueInterpolator(
            numeric_fields=["temperature", "humidity", "wind_speed", "precipitation"]
        ),
    },
    "subway": {
        "extractor_cls": SubwayExtractor,
        "api_key_attr": "subway_api_key",
        "normalizer": SubwayNormalizer(),
        "interpolator": MissingValueInterpolator(
            numeric_fields=["boarding_count", "alighting_count"]
        ),
    },
}


def run_pipeline(sources: list[str] | None = None) -> dict[str, Any]:
    if sources is None:
        sources = list(PIPELINE_CONFIG.keys())

    region_mapper = RegionMapper()
    results = {}

    for source in sources:
        config = PIPELINE_CONFIG.get(source)
        if not config:
            logger.warning(f"Unknown source: {source}")
            continue

        run_id = uuid.uuid4().hex[:8]
        run_id_var.set(run_id)
        log = _create_run_log(source)
        t_total = time.perf_counter()
        try:
            # Extract
            api_key = getattr(settings, config["api_key_attr"], "")
            extractor = config["extractor_cls"](api_key=api_key)

            t0 = time.perf_counter()
            if settings.use_mock_data or not api_key:
                raw_data = extractor.mock_extract()
                logger.info(f"[{source}] Using mock data",
                            extra={"rows": len(raw_data), "duration_ms": _ms(t0), "mock": True})
            else:
                raw_data = extractor.extract()
                logger.info(f"[{source}] Extract complete",
                            extra={"rows": len(raw_data), "duration_ms": _ms(t0)})

            extractor.close()

            # Transform
            t0 = time.perf_counter()
            mapped = region_mapper.transform(raw_data)
            normalized = config["normalizer"].transform(mapped)
            interpolated = config["interpolator"].transform(normalized)
            logger.info(f"[{source}] Transform complete",
                        extra={"rows": len(interpolated), "duration_ms": _ms(t0)})

            # Load
            t0 = time.perf_counter()
            loaded_count = upsert_records(source, interpolated)
            logger.info(f"[{source}] Load complete",
                        extra={"rows": loaded_count, "duration_ms": _ms(t0)})

            _update_run_log(log.id, "success", len(raw_data), loaded_count)
            logger.info(f"[{source}] Pipeline complete",
                        extra={"extracted": len(raw_data), "loaded": loaded_count, "duration_ms": _ms(t_total)})
            results[source] = {
                "status": "success",
                "extracted": len(raw_data),
                "loaded": loaded_count,
            }
        except Exception as e:
            logger.error(
                f"[{source}] Pipeline failed",
                extra={"error_type": type(e).__name__, "error_msg": str(e), "duration_ms": _ms(t_total)},
                exc_info=True,
            )
            _update_run_log(log.id, "failed", error_message=str(e))
            results[source] = {"status": "failed", "error": str(e)}

    return results


def _ms(t_start: float) -> int:
    return int((time.perf_counter() - t_start) * 1000)


def _create_run_log(source: str) -> EtlRunLog:
    db = SessionLocal()
    try:
        log = EtlRunLog(source=source, started_at=datetime.utcnow() + timedelta(hours=9), status="running")
        db.add(log)
        db.commit()
        db.refresh(log)
        return log
    finally:
        db.close()


def _update_run_log(
    log_id: int,
    status: str,
    records_extracted: int = 0,
    records_loaded: int = 0,
    error_message: str | None = None,
):
    db = SessionLocal()
    try:
        log = db.query(EtlRunLog).get(log_id)
        if log:
            log.status = status
            log.finished_at = datetime.utcnow() + timedelta(hours=9)
            log.records_extracted = records_extracted
            log.records_loaded = records_loaded
            log.error_message = error_message
            db.commit()
    finally:
        db.close()

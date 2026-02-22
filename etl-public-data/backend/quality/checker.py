import logging
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from db.database import SessionLocal
from db.models import AirQuality, Weather, Subway, QualityReport

logger = logging.getLogger(__name__)

RANGE_CHECKS = {
    "air_quality": {
        "pm10": (0, 500),
        "pm25": (0, 300),
        "o3": (0, 0.6),
        "no2": (0, 0.2),
        "co": (0, 50),
        "so2": (0, 0.15),
    },
    "weather": {
        "temperature": (-50, 50),
        "humidity": (0, 100),
        "wind_speed": (0, 100),
        "precipitation": (0, 500),
    },
    "subway": {
        "boarding_count": (0, 1_000_000),
        "alighting_count": (0, 1_000_000),
    },
}

MODEL_MAP = {
    "air_quality": AirQuality,
    "weather": Weather,
    "subway": Subway,
}

NULLABLE_FIELDS = {
    "air_quality": ["pm10", "pm25", "o3", "no2", "co", "so2"],
    "weather": ["temperature", "humidity", "wind_speed", "precipitation"],
    "subway": ["boarding_count", "alighting_count"],
}


def check_quality(source: str, target_date: datetime | None = None) -> dict[str, Any]:
    if target_date is None:
        target_date = (datetime.utcnow() + timedelta(hours=9)).replace(hour=0, minute=0, second=0, microsecond=0)

    next_day = target_date + timedelta(days=1)
    model = MODEL_MAP.get(source)
    if not model:
        return {"error": f"Unknown source: {source}"}

    db: Session = SessionLocal()
    try:
        base_query = db.query(model).filter(model.collected_at >= target_date, model.collected_at < next_day)
        total = base_query.count()

        if total == 0:
            return {
                "source": source,
                "date": target_date.isoformat(),
                "total_records": 0,
                "null_count": 0,
                "duplicate_count": 0,
                "outlier_count": 0,
                "null_rate": 0.0,
                "overall_score": 100.0,
                "field_details": {},
            }

        # Null check
        null_count = 0
        field_details = {}
        for field in NULLABLE_FIELDS.get(source, []):
            col = getattr(model, field)
            nulls = base_query.filter(col.is_(None)).count()
            null_count += nulls
            field_details[field] = {"null_count": nulls, "null_rate": round(nulls / total * 100, 2)}

        # Range outlier check
        outlier_count = 0
        for field, (low, high) in RANGE_CHECKS.get(source, {}).items():
            col = getattr(model, field)
            outliers = base_query.filter((col < low) | (col > high)).count()
            outlier_count += outliers
            if field in field_details:
                field_details[field]["outlier_count"] = outliers
            else:
                field_details[field] = {"outlier_count": outliers}

        # Duplicate check
        dup_query = _get_duplicate_count(db, source, target_date, next_day)
        duplicate_count = dup_query

        total_fields = total * len(NULLABLE_FIELDS.get(source, []))
        null_rate = round(null_count / max(total_fields, 1) * 100, 2)
        issue_count = null_count + outlier_count + duplicate_count
        max_issues = total_fields + total
        overall_score = round(max(0, (1 - issue_count / max(max_issues, 1)) * 100), 2)

        result = {
            "source": source,
            "date": target_date.isoformat(),
            "total_records": total,
            "null_count": null_count,
            "duplicate_count": duplicate_count,
            "outlier_count": outlier_count,
            "null_rate": null_rate,
            "overall_score": overall_score,
            "field_details": field_details,
        }

        # Save report to DB
        report = QualityReport(
            report_date=target_date,
            source=source,
            total_records=total,
            null_count=null_count,
            duplicate_count=duplicate_count,
            outlier_count=outlier_count,
            null_rate=null_rate,
            overall_score=overall_score,
            details=str(field_details),
        )
        db.add(report)
        db.commit()

        return result
    finally:
        db.close()


def _get_duplicate_count(db: Session, source: str, start: datetime, end: datetime) -> int:
    if source == "air_quality":
        result = db.execute(text(
            "SELECT COUNT(*) - COUNT(DISTINCT (station_name, measured_at)) "
            "FROM air_quality WHERE collected_at >= :s AND collected_at < :e"
        ), {"s": start, "e": end})
    elif source == "weather":
        result = db.execute(text(
            "SELECT COUNT(*) - COUNT(DISTINCT (region, forecast_date)) "
            "FROM weather WHERE collected_at >= :s AND collected_at < :e"
        ), {"s": start, "e": end})
    elif source == "subway":
        result = db.execute(text(
            "SELECT COUNT(*) - COUNT(DISTINCT (station_name, line, use_date)) "
            "FROM subway WHERE collected_at >= :s AND collected_at < :e"
        ), {"s": start, "e": end})
    else:
        return 0
    return result.scalar() or 0


def run_all_quality_checks(target_date: datetime | None = None) -> list[dict[str, Any]]:
    results = []
    for source in MODEL_MAP:
        result = check_quality(source, target_date)
        results.append(result)
    return results

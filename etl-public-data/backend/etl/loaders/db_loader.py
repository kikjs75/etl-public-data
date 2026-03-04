import logging
from datetime import datetime, timedelta
from typing import Any, Type

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from db.database import SessionLocal
from db.models import AirQuality, Weather, Subway

logger = logging.getLogger(__name__)

MODEL_MAP = {
    "air_quality": AirQuality,
    "weather": Weather,
    "subway": Subway,
}

CONFLICT_KEYS = {
    "air_quality": ["station_name", "measured_at"],
    "weather": ["region", "forecast_date"],
    "subway": ["station_name", "line", "use_date"],
}


def upsert_records(source: str, records: list[dict[str, Any]]) -> int:
    model = MODEL_MAP.get(source)
    if not model:
        raise ValueError(f"Unknown source: {source}")

    conflict_cols = CONFLICT_KEYS[source]
    loaded = 0

    db: Session = SessionLocal()
    try:
        for record in records:
            record["collected_at"] = datetime.utcnow() + timedelta(hours=9)
            stmt = insert(model).values(**record)
            update_cols = {k: v for k, v in record.items() if k not in conflict_cols and k != "id"}
            stmt = stmt.on_conflict_do_update(
                constraint=_get_constraint_name(source),
                set_=update_cols,
            )
            db.execute(stmt)
            loaded += 1

        db.commit()
        logger.info(f"[{source}] Upserted {loaded} records")
    except Exception as e:
        db.rollback()
        logger.error(
            f"[{source}] Load failed "
            f"error_type={type(e).__name__} "
            f"error_msg={str(e)!r}",
            exc_info=True,
        )
        raise
    finally:
        db.close()

    return loaded


def _get_constraint_name(source: str) -> str:
    mapping = {
        "air_quality": "uq_air_station_time",
        "weather": "uq_weather_region_date",
        "subway": "uq_subway_station_line_date",
    }
    return mapping[source]

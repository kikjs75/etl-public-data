import logging
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, Query, BackgroundTasks
from sqlalchemy import func, desc
from sqlalchemy.orm import Session

from db.database import get_db
from db.models import AirQuality, Weather, Subway, EtlRunLog, QualityReport
from etl.pipeline import run_pipeline
from quality.report_generator import generate_report
from catalog.lineage import get_catalog, get_lineage

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api")

SOURCE_MODELS = {
    "air_quality": AirQuality,
    "weather": Weather,
    "subway": Subway,
}


@router.get("/dashboard")
def get_dashboard(db: Session = Depends(get_db)):
    total_air = db.query(AirQuality).count()
    total_weather = db.query(Weather).count()
    total_subway = db.query(Subway).count()

    recent_runs = (
        db.query(EtlRunLog)
        .order_by(desc(EtlRunLog.started_at))
        .limit(20)
        .all()
    )

    # Daily collection counts for the last 7 days
    daily_counts = []
    for i in range(7):
        day = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=i)
        next_day = day + timedelta(days=1)
        counts = {}
        for source, model in SOURCE_MODELS.items():
            counts[source] = db.query(model).filter(
                model.collected_at >= day, model.collected_at < next_day
            ).count()
        daily_counts.append({
            "date": day.strftime("%Y-%m-%d"),
            **counts,
        })

    return {
        "total_air_quality": total_air,
        "total_weather": total_weather,
        "total_subway": total_subway,
        "recent_runs": [
            {
                "id": r.id,
                "source": r.source,
                "started_at": r.started_at.isoformat() if r.started_at else None,
                "finished_at": r.finished_at.isoformat() if r.finished_at else None,
                "status": r.status,
                "records_extracted": r.records_extracted,
                "records_loaded": r.records_loaded,
                "error_message": r.error_message,
            }
            for r in recent_runs
        ],
        "daily_counts": list(reversed(daily_counts)),
    }


@router.get("/quality/reports")
def get_quality_reports(
    limit: int = Query(default=30, le=100),
    db: Session = Depends(get_db),
):
    reports = (
        db.query(QualityReport)
        .order_by(desc(QualityReport.report_date))
        .limit(limit)
        .all()
    )
    return [
        {
            "id": r.id,
            "report_date": r.report_date.strftime("%Y-%m-%d") if r.report_date else None,
            "source": r.source,
            "total_records": r.total_records,
            "null_count": r.null_count,
            "duplicate_count": r.duplicate_count,
            "outlier_count": r.outlier_count,
            "null_rate": r.null_rate,
            "overall_score": r.overall_score,
        }
        for r in reports
    ]


@router.get("/quality/reports/{date}")
def get_quality_report_detail(date: str):
    try:
        target_date = datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        return {"error": "Invalid date format. Use YYYY-MM-DD"}
    result = generate_report(target_date)
    return result


@router.get("/catalog")
def get_catalog_data():
    return {
        "catalog": get_catalog(),
        "lineage": get_lineage(),
    }


@router.get("/data/{source}")
def get_data(
    source: str,
    limit: int = Query(default=100, le=1000),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
):
    model = SOURCE_MODELS.get(source)
    if not model:
        return {"error": f"Unknown source: {source}. Available: {list(SOURCE_MODELS.keys())}"}

    total = db.query(model).count()
    records = db.query(model).order_by(desc(model.id)).offset(offset).limit(limit).all()

    return {
        "source": source,
        "total": total,
        "records": [_model_to_dict(r) for r in records],
    }


@router.post("/etl/run")
def trigger_etl(background_tasks: BackgroundTasks, sources: list[str] | None = None):
    if sources:
        invalid = [s for s in sources if s not in SOURCE_MODELS]
        if invalid:
            return {"error": f"Unknown sources: {invalid}"}

    background_tasks.add_task(run_pipeline, sources)
    return {"message": "ETL pipeline triggered", "sources": sources or list(SOURCE_MODELS.keys())}


def _model_to_dict(obj) -> dict:
    d = {}
    for col in obj.__table__.columns:
        val = getattr(obj, col.name)
        if isinstance(val, datetime):
            val = val.isoformat()
        d[col.name] = val
    return d

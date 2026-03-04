import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta

from pythonjsonlogger import jsonlogger
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler

from config import settings
from db.migrations import run_migrations
from etl.pipeline import run_pipeline
from etl.context import run_id_var
from quality.report_generator import generate_report
from api.routes import router

KST = timezone(timedelta(hours=9))

_STDLIB_ATTRS = frozenset({
    "args", "asctime", "created", "exc_info", "exc_text", "filename",
    "funcName", "levelname", "levelno", "lineno", "message", "module",
    "msecs", "msg", "name", "pathname", "process", "processName",
    "relativeCreated", "stack_info", "thread", "threadName", "taskName",
    "run_id",
})


class RunIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = run_id_var.get()
        return True


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record: dict, record: logging.LogRecord, message_dict: dict) -> None:
        super().add_fields(log_record, record, message_dict)
        log_record["timestamp"] = datetime.now(KST).isoformat(timespec="milliseconds")
        log_record["level"] = record.levelname
        log_record["logger"] = record.name
        log_record["run_id"] = getattr(record, "run_id", "-")
        log_record["service"] = "etl-backend"
        for key, value in record.__dict__.items():
            if key not in _STDLIB_ATTRS and key not in log_record:
                log_record[key] = value
        if record.exc_info:
            log_record["traceback"] = self.formatException(record.exc_info)
            record.exc_info = None
            record.exc_text = None


_handler = logging.StreamHandler()
_handler.setFormatter(CustomJsonFormatter())
_handler.addFilter(RunIdFilter())
logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(_handler)
logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler()


def scheduled_etl():
    logger.info("Scheduled ETL pipeline starting")
    try:
        results = run_pipeline()
        logger.info(f"Scheduled ETL completed: {results}")
    except Exception as e:
        logger.error(f"Scheduled ETL failed: {e}")


def scheduled_quality_report():
    logger.info("Scheduled quality report starting")
    try:
        report = generate_report()
        logger.info("Scheduled quality report generated")
    except Exception as e:
        logger.error(f"Scheduled quality report failed: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Running database migrations...")
    run_migrations()

    scheduler.add_job(
        scheduled_etl,
        "cron",
        hour=settings.etl_cron_hour,
        minute=settings.etl_cron_minute,
        id="etl_pipeline",
    )
    scheduler.add_job(
        scheduled_quality_report,
        "cron",
        hour=settings.quality_report_hour,
        minute=settings.quality_report_minute,
        id="quality_report",
    )
    scheduler.start()
    logger.info("Scheduler started")

    yield

    # Shutdown
    scheduler.shutdown()
    logger.info("Scheduler stopped")


app = FastAPI(
    title="공공데이터 ETL 파이프라인",
    description="미세먼지/날씨/지하철 데이터 수집 ETL + 데이터 품질 리포트",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)


@app.get("/")
def root():
    return {"message": "공공데이터 ETL API", "docs": "/docs"}

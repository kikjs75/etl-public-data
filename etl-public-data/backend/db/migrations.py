import logging
from datetime import datetime

from sqlalchemy import text, inspect

from db.database import engine, Base
from db.models import SchemaVersion

logger = logging.getLogger(__name__)

MIGRATIONS = [
    {
        "version": 1,
        "description": "Initial schema",
        "sql": [],  # Tables created via Base.metadata.create_all
    },
    {
        "version": 2,
        "description": "Add data_source column to quality_report",
        "sql": [
            "ALTER TABLE quality_report ADD COLUMN IF NOT EXISTS data_source VARCHAR(100)",
        ],
    },
]


def get_current_version() -> int:
    try:
        with engine.connect() as conn:
            inspector = inspect(engine)
            if "schema_version" not in inspector.get_table_names():
                return 0
            result = conn.execute(
                text("SELECT COALESCE(MAX(version), 0) FROM schema_version")
            )
            return result.scalar() or 0
    except Exception:
        return 0


def run_migrations():
    Base.metadata.create_all(bind=engine)
    current = get_current_version()
    logger.info(f"Current schema version: {current}")

    for migration in MIGRATIONS:
        ver = migration["version"]
        if ver <= current:
            continue
        logger.info(f"Applying migration v{ver}: {migration['description']}")
        with engine.begin() as conn:
            for sql in migration["sql"]:
                conn.execute(text(sql))
            conn.execute(
                text(
                    "INSERT INTO schema_version (version, description, applied_at) "
                    "VALUES (:v, :d, :t)"
                ),
                {"v": ver, "d": migration["description"], "t": datetime.utcnow()},
            )
        logger.info(f"Migration v{ver} applied successfully")

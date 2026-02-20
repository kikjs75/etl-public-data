from datetime import datetime

from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Text, Boolean, UniqueConstraint,
)
from db.database import Base


class AirQuality(Base):
    __tablename__ = "air_quality"

    id = Column(Integer, primary_key=True, autoincrement=True)
    station_name = Column(String(100), nullable=False)
    region = Column(String(100))
    measured_at = Column(DateTime, nullable=False)
    pm10 = Column(Float)
    pm25 = Column(Float)
    o3 = Column(Float)
    no2 = Column(Float)
    co = Column(Float)
    so2 = Column(Float)
    grade = Column(String(20))
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("station_name", "measured_at", name="uq_air_station_time"),
    )


class Weather(Base):
    __tablename__ = "weather"

    id = Column(Integer, primary_key=True, autoincrement=True)
    region = Column(String(100), nullable=False)
    forecast_date = Column(DateTime, nullable=False)
    temperature = Column(Float)
    humidity = Column(Float)
    wind_speed = Column(Float)
    precipitation = Column(Float)
    sky_condition = Column(String(50))
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("region", "forecast_date", name="uq_weather_region_date"),
    )


class Subway(Base):
    __tablename__ = "subway"

    id = Column(Integer, primary_key=True, autoincrement=True)
    station_name = Column(String(100), nullable=False)
    line = Column(String(50), nullable=False)
    use_date = Column(DateTime, nullable=False)
    boarding_count = Column(Integer)
    alighting_count = Column(Integer)
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("station_name", "line", "use_date", name="uq_subway_station_line_date"),
    )


class EtlRunLog(Base):
    __tablename__ = "etl_run_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String(50), nullable=False)
    started_at = Column(DateTime, nullable=False)
    finished_at = Column(DateTime)
    status = Column(String(20), nullable=False, default="running")
    records_extracted = Column(Integer, default=0)
    records_loaded = Column(Integer, default=0)
    error_message = Column(Text)


class QualityReport(Base):
    __tablename__ = "quality_report"

    id = Column(Integer, primary_key=True, autoincrement=True)
    report_date = Column(DateTime, nullable=False)
    source = Column(String(50), nullable=False)
    total_records = Column(Integer, default=0)
    null_count = Column(Integer, default=0)
    duplicate_count = Column(Integer, default=0)
    outlier_count = Column(Integer, default=0)
    null_rate = Column(Float, default=0.0)
    overall_score = Column(Float, default=0.0)
    details = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)


class SchemaVersion(Base):
    __tablename__ = "schema_version"

    id = Column(Integer, primary_key=True, autoincrement=True)
    version = Column(Integer, nullable=False, unique=True)
    description = Column(String(255))
    applied_at = Column(DateTime, default=datetime.utcnow)

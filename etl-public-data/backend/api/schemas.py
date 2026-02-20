from datetime import datetime
from pydantic import BaseModel


class EtlRunResponse(BaseModel):
    source: str
    status: str
    extracted: int = 0
    loaded: int = 0
    error: str | None = None


class PipelineRunResponse(BaseModel):
    results: dict[str, EtlRunResponse]


class DashboardSummary(BaseModel):
    total_air_quality: int
    total_weather: int
    total_subway: int
    recent_runs: list[dict]
    daily_counts: list[dict]


class QualityReportSummary(BaseModel):
    id: int
    report_date: str
    source: str
    total_records: int
    null_count: int
    duplicate_count: int
    outlier_count: int
    null_rate: float
    overall_score: float


class QualityReportDetail(BaseModel):
    html: str
    markdown: str
    results: list[dict]


class CatalogResponse(BaseModel):
    catalog: dict
    lineage: dict


class DataResponse(BaseModel):
    source: str
    total: int
    records: list[dict]

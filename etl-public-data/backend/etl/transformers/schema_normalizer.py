from datetime import datetime, timedelta
from typing import Any

from etl.base import BaseTransformer


class AirQualityNormalizer(BaseTransformer):
    """Normalizes air quality API response to internal schema."""

    def transform(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized = []
        for r in records:
            try:
                measured_at = datetime.strptime(r.get("dataTime", ""), "%Y-%m-%d %H:%M")
            except (ValueError, TypeError):
                measured_at = datetime.utcnow() + timedelta(hours=9)

            grade_map = {"1": "좋음", "2": "보통", "3": "나쁨", "4": "매우나쁨"}
            normalized.append({
                "station_name": r.get("stationName", ""),
                "region": r.get("region", r.get("sidoName", "")),
                "measured_at": measured_at,
                "pm10": _safe_float(r.get("pm10Value")),
                "pm25": _safe_float(r.get("pm25Value")),
                "o3": _safe_float(r.get("o3Value")),
                "no2": _safe_float(r.get("no2Value")),
                "co": _safe_float(r.get("coValue")),
                "so2": _safe_float(r.get("so2Value")),
                "grade": grade_map.get(str(r.get("pm10Grade", "")), ""),
            })
        return normalized


class WeatherNormalizer(BaseTransformer):
    """Normalizes weather API response to internal schema."""

    def transform(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized = []
        sky_map = {"1": "맑음", "3": "구름많음", "4": "흐림"}

        for r in records:
            try:
                forecast_date = datetime.strptime(
                    f"{r.get('fcstDate', '')} {r.get('fcstTime', '0000')}", "%Y%m%d %H%M"
                )
            except (ValueError, TypeError):
                forecast_date = datetime.utcnow() + timedelta(hours=9)

            pcp = r.get("PCP", "강수없음")
            precipitation = 0.0
            if pcp and pcp != "강수없음":
                try:
                    precipitation = float(pcp.replace("mm", "").strip())
                except (ValueError, TypeError):
                    precipitation = 0.0

            normalized.append({
                "region": r.get("region", ""),
                "forecast_date": forecast_date,
                "temperature": _safe_float(r.get("TMP")),
                "humidity": _safe_float(r.get("REH")),
                "wind_speed": _safe_float(r.get("WSD")),
                "precipitation": precipitation,
                "sky_condition": sky_map.get(str(r.get("SKY", "")), ""),
            })
        return normalized


class SubwayNormalizer(BaseTransformer):
    """Normalizes subway API response to internal schema."""

    def transform(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized = []
        for r in records:
            try:
                use_date = datetime.strptime(str(r.get("USE_YMD", r.get("USE_DT", ""))), "%Y%m%d")
            except (ValueError, TypeError):
                use_date = datetime.utcnow() + timedelta(hours=9)

            normalized.append({
                "station_name": r.get("SBWY_STNS_NM", r.get("SUB_STA_NM", "")),
                "line": r.get("SBWY_ROUT_LN_NM", r.get("LINE_NUM", "")),
                "use_date": use_date,
                "boarding_count": _safe_int(r.get("GTON_TNOPE", r.get("RIDE_PASGR_NUM"))),
                "alighting_count": _safe_int(r.get("GTOFF_TNOPE", r.get("ALIGHT_PASGR_NUM"))),
            })
        return normalized


def _safe_float(val) -> float | None:
    if val is None or val == "" or val == "-":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> int | None:
    if val is None or val == "" or val == "-":
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None

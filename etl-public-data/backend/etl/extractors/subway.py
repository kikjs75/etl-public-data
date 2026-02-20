import random
from datetime import datetime, timedelta
from typing import Any

from etl.base import BaseExtractor


class SubwayExtractor(BaseExtractor):
    BASE_URL = "http://openapi.seoul.go.kr:8088"

    @property
    def source_name(self) -> str:
        return "subway"

    def extract(self) -> list[dict[str, Any]]:
        yesterday = (datetime.utcnow() + timedelta(hours=9) - timedelta(days=1)).strftime("%Y%m%d")
        url = f"{self.BASE_URL}/{self.api_key}/json/CardSubwayStatsNew/1/1000/{yesterday}"
        data = self.fetch(url)
        rows = data.get("CardSubwayStatsNew", {}).get("row", [])
        return rows if isinstance(rows, list) else []

    def mock_extract(self) -> list[dict[str, Any]]:
        stations = [
            ("서울역", "1호선"), ("시청", "1호선"), ("종각", "1호선"),
            ("강남", "2호선"), ("역삼", "2호선"), ("삼성", "2호선"),
            ("교대", "3호선"), ("고속터미널", "3호선"),
            ("명동", "4호선"), ("동대문역사문화공원", "4호선"),
            ("여의도", "5호선"), ("공덕", "5호선"),
        ]
        now = datetime.utcnow()
        records = []
        for day_offset in range(7):
            use_date = now - timedelta(days=day_offset + 1)
            for station, line in stations:
                records.append({
                    "USE_DT": use_date.strftime("%Y%m%d"),
                    "SUB_STA_NM": station,
                    "LINE_NUM": line,
                    "RIDE_PASGR_NUM": random.randint(5000, 200000),
                    "ALIGHT_PASGR_NUM": random.randint(5000, 200000),
                })
        return records

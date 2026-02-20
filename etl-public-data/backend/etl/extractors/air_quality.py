import random
from datetime import datetime, timedelta
from typing import Any

from etl.base import BaseExtractor


class AirQualityExtractor(BaseExtractor):
    BASE_URL = "http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty"

    @property
    def source_name(self) -> str:
        return "air_quality"

    def extract(self) -> list[dict[str, Any]]:
        params = {
            "serviceKey": self.api_key,
            "returnType": "json",
            "numOfRows": "100",
            "pageNo": "1",
            "sidoName": "서울",
            "ver": "1.0",
        }
        data = self.fetch(self.BASE_URL, params)
        items = data.get("response", {}).get("body", {}).get("items", [])
        return items if isinstance(items, list) else []

    def mock_extract(self) -> list[dict[str, Any]]:
        stations = ["종로구", "중구", "용산구", "성동구", "광진구", "동대문구", "중랑구", "성북구", "강북구", "도봉구"]
        now = datetime.utcnow()
        records = []
        for station in stations:
            for h in range(24):
                measured = now.replace(hour=h, minute=0, second=0, microsecond=0) - timedelta(days=1)
                records.append({
                    "stationName": station,
                    "sidoName": "서울",
                    "dataTime": measured.strftime("%Y-%m-%d %H:%M"),
                    "pm10Value": str(random.randint(10, 150)),
                    "pm25Value": str(random.randint(5, 80)),
                    "o3Value": str(round(random.uniform(0.01, 0.15), 3)),
                    "no2Value": str(round(random.uniform(0.01, 0.08), 3)),
                    "coValue": str(round(random.uniform(0.2, 1.5), 1)),
                    "so2Value": str(round(random.uniform(0.001, 0.02), 3)),
                    "pm10Grade": str(random.choice([1, 2, 3, 4])),
                })
        return records

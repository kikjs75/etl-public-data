import random
from datetime import datetime, timedelta
from typing import Any

from etl.base import BaseExtractor


class WeatherExtractor(BaseExtractor):
    BASE_URL = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst"

    @property
    def source_name(self) -> str:
        return "weather"

    def extract(self) -> list[dict[str, Any]]:
        now = datetime.utcnow() + timedelta(hours=9)  # KST
        base_date = now.strftime("%Y%m%d")
        params = {
            "serviceKey": self.api_key,
            "pageNo": "1",
            "numOfRows": "1000",
            "dataType": "JSON",
            "base_date": base_date,
            "base_time": "0500",
            "nx": "60",
            "ny": "127",
        }
        data = self.fetch(self.BASE_URL, params)
        items = data.get("response", {}).get("body", {}).get("items", {}).get("item", [])
        return items if isinstance(items, list) else []

    def mock_extract(self) -> list[dict[str, Any]]:
        regions = [
            {"nx": "60", "ny": "127", "name": "서울"},
            {"nx": "97", "ny": "76", "name": "부산"},
            {"nx": "89", "ny": "90", "name": "대구"},
            {"nx": "58", "ny": "74", "name": "광주"},
            {"nx": "68", "ny": "100", "name": "대전"},
        ]
        now = datetime.utcnow()
        records = []
        for region in regions:
            for h in range(0, 24, 3):
                forecast = now.replace(hour=h, minute=0, second=0, microsecond=0)
                records.append({
                    "region_name": region["name"],
                    "nx": region["nx"],
                    "ny": region["ny"],
                    "fcstDate": forecast.strftime("%Y%m%d"),
                    "fcstTime": f"{h:02d}00",
                    "TMP": str(random.randint(-5, 35)),
                    "REH": str(random.randint(20, 95)),
                    "WSD": str(round(random.uniform(0.5, 15.0), 1)),
                    "PCP": random.choice(["강수없음", "1.0mm", "5.0mm", "10.0mm"]),
                    "SKY": str(random.choice([1, 3, 4])),
                })
        return records

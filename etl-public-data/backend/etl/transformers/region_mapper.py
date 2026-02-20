from typing import Any

from etl.base import BaseTransformer

REGION_MAP = {
    "서울": "서울특별시",
    "부산": "부산광역시",
    "대구": "대구광역시",
    "인천": "인천광역시",
    "광주": "광주광역시",
    "대전": "대전광역시",
    "울산": "울산광역시",
    "세종": "세종특별자치시",
    "경기": "경기도",
    "강원": "강원특별자치도",
    "충북": "충청북도",
    "충남": "충청남도",
    "전북": "전북특별자치도",
    "전남": "전라남도",
    "경북": "경상북도",
    "경남": "경상남도",
    "제주": "제주특별자치도",
}

GRID_TO_REGION = {
    ("60", "127"): "서울",
    ("97", "76"): "부산",
    ("89", "90"): "대구",
    ("58", "74"): "광주",
    ("68", "100"): "대전",
}


class RegionMapper(BaseTransformer):
    """Maps short region names or grid coordinates to full region names."""

    def transform(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        for record in records:
            if "sidoName" in record:
                short = record["sidoName"]
                record["region"] = REGION_MAP.get(short, short)
            elif "nx" in record and "ny" in record:
                key = (str(record["nx"]), str(record["ny"]))
                short = GRID_TO_REGION.get(key, "")
                record["region"] = REGION_MAP.get(short, short)
                if "region_name" in record:
                    record["region"] = REGION_MAP.get(record["region_name"], record["region_name"])
        return records

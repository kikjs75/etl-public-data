"""Data catalog and lineage documentation."""

CATALOG = {
    "air_quality": {
        "name": "미세먼지 데이터",
        "source": "data.go.kr - 한국환경공단 대기오염정보",
        "api_url": "http://apis.data.go.kr/B552584/ArpltnInforInqireSvc",
        "update_frequency": "매시간",
        "description": "전국 대기오염 측정소별 실시간 미세먼지(PM10, PM2.5) 및 대기오염 물질 측정 데이터",
        "fields": {
            "station_name": {"type": "string", "description": "측정소명"},
            "region": {"type": "string", "description": "시/도 (행정구역)"},
            "measured_at": {"type": "datetime", "description": "측정일시"},
            "pm10": {"type": "float", "unit": "㎍/㎥", "description": "미세먼지(PM10) 농도"},
            "pm25": {"type": "float", "unit": "㎍/㎥", "description": "초미세먼지(PM2.5) 농도"},
            "o3": {"type": "float", "unit": "ppm", "description": "오존 농도"},
            "no2": {"type": "float", "unit": "ppm", "description": "이산화질소 농도"},
            "co": {"type": "float", "unit": "ppm", "description": "일산화탄소 농도"},
            "so2": {"type": "float", "unit": "ppm", "description": "아황산가스 농도"},
            "grade": {"type": "string", "description": "통합대기환경등급 (좋음/보통/나쁨/매우나쁨)"},
        },
    },
    "weather": {
        "name": "날씨 데이터",
        "source": "data.go.kr - 기상청 단기예보",
        "api_url": "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0",
        "update_frequency": "3시간",
        "description": "전국 주요 지역 단기예보 데이터 (기온, 습도, 풍속, 강수량, 하늘상태)",
        "fields": {
            "region": {"type": "string", "description": "예보 지역"},
            "forecast_date": {"type": "datetime", "description": "예보 일시"},
            "temperature": {"type": "float", "unit": "℃", "description": "기온"},
            "humidity": {"type": "float", "unit": "%", "description": "습도"},
            "wind_speed": {"type": "float", "unit": "m/s", "description": "풍속"},
            "precipitation": {"type": "float", "unit": "mm", "description": "강수량"},
            "sky_condition": {"type": "string", "description": "하늘상태 (맑음/구름많음/흐림)"},
        },
    },
    "subway": {
        "name": "지하철 이용 데이터",
        "source": "서울 열린데이터광장 - 교통카드 지하철 이용현황",
        "api_url": "http://openapi.seoul.go.kr:8088/{key}/json/CardSubwayStatsNew",
        "update_frequency": "일 1회 (전일 데이터)",
        "description": "서울시 지하철 역별/호선별 일별 승하차 인원 데이터",
        "fields": {
            "station_name": {"type": "string", "description": "지하철역명"},
            "line": {"type": "string", "description": "호선"},
            "use_date": {"type": "datetime", "description": "이용 날짜"},
            "boarding_count": {"type": "integer", "description": "승차 인원"},
            "alighting_count": {"type": "integer", "description": "하차 인원"},
        },
    },
}

LINEAGE = {
    "air_quality": {
        "stages": [
            {"name": "Extract", "description": "data.go.kr API에서 측정소별 실시간 데이터 수집", "output": "raw JSON"},
            {"name": "RegionMap", "description": "시도명을 행정구역 전체 이름으로 매핑", "output": "mapped records"},
            {"name": "Normalize", "description": "API 필드명을 내부 스키마로 변환, 타입 캐스팅", "output": "normalized records"},
            {"name": "Interpolate", "description": "결측치 선형 보간 및 최근값 대체", "output": "clean records"},
            {"name": "Load", "description": "PostgreSQL air_quality 테이블에 UPSERT", "output": "DB rows"},
        ],
    },
    "weather": {
        "stages": [
            {"name": "Extract", "description": "기상청 단기예보 API에서 격자별 예보 데이터 수집", "output": "raw JSON"},
            {"name": "RegionMap", "description": "격자 좌표(nx,ny)를 지역명으로 매핑", "output": "mapped records"},
            {"name": "Normalize", "description": "카테고리별 값을 단일 레코드로 병합, 단위 변환", "output": "normalized records"},
            {"name": "Interpolate", "description": "결측치 보간", "output": "clean records"},
            {"name": "Load", "description": "PostgreSQL weather 테이블에 UPSERT", "output": "DB rows"},
        ],
    },
    "subway": {
        "stages": [
            {"name": "Extract", "description": "서울 열린데이터 API에서 일별 승하차 데이터 수집", "output": "raw JSON"},
            {"name": "Normalize", "description": "API 필드명을 내부 스키마로 변환", "output": "normalized records"},
            {"name": "Interpolate", "description": "결측치 보간", "output": "clean records"},
            {"name": "Load", "description": "PostgreSQL subway 테이블에 UPSERT", "output": "DB rows"},
        ],
    },
}


def get_catalog() -> dict:
    return CATALOG


def get_lineage() -> dict:
    return LINEAGE


def get_catalog_for_source(source: str) -> dict | None:
    return CATALOG.get(source)


def get_lineage_for_source(source: str) -> dict | None:
    return LINEAGE.get(source)

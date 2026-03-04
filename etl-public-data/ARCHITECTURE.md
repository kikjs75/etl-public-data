# 공공데이터 ETL 파이프라인 — 소스코드 분석 문서

## 목차
1. [프로젝트 개요](#1-프로젝트-개요)
2. [전체 구조](#2-전체-구조)
3. [데이터 흐름 (ETL)](#3-데이터-흐름-etl)
4. [파일별 상세 설명](#4-파일별-상세-설명)
5. [데이터베이스 스키마](#5-데이터베이스-스키마)
6. [REST API 엔드포인트](#6-rest-api-엔드포인트)
7. [스케줄 자동화](#7-스케줄-자동화)
8. [로깅 구조](#8-로깅-구조)
9. [설계 패턴](#10-설계-패턴)
10. [새 데이터 소스 추가 방법](#11-새-데이터-소스-추가-방법)

---

## 1. 프로젝트 개요

공공 API(미세먼지/날씨/지하철)에서 데이터를 수집하여 PostgreSQL에 저장하고, 데이터 품질을 자동으로 검사하며 리포트를 생성하는 ETL 파이프라인.

| 구성 요소 | 기술 |
|----------|------|
| 백엔드 서버 | Python 3.11 + FastAPI |
| DB | PostgreSQL 15 + SQLAlchemy ORM |
| 스케줄링 | APScheduler (cron 기반) |
| HTTP 클라이언트 | httpx (재시도 지원) |
| 구조화 로깅 | python-json-logger (JSON 포맷, ELK 연동 대비) |
| 리포트 템플릿 | Jinja2 (HTML/Markdown) |
| 인프라 | Docker Compose |

---

## 2. 전체 구조

```
backend/
├── main.py                          # FastAPI 진입점 + 스케줄러
├── config.py                        # 환경변수 설정 (.env 로드)
├── db/
│   ├── database.py                  # DB 연결 풀 + 세션 관리
│   ├── models.py                    # ORM 테이블 정의 (6개)
│   └── migrations.py                # 수동 마이그레이션 관리
├── etl/
│   ├── context.py                   # run_id ContextVar (스레드별 실행 추적 ID)
│   ├── base.py                      # 추상 클래스 (BaseExtractor, BaseTransformer)
│   ├── pipeline.py                  # ETL 오케스트레이션 (run_id 생성, duration_ms 측정)
│   ├── extractors/
│   │   ├── air_quality.py           # 미세먼지 API 추출
│   │   ├── weather.py               # 날씨 API 추출
│   │   └── subway.py                # 지하철 API 추출
│   ├── transformers/
│   │   ├── schema_normalizer.py     # 필드명 변환 + 타입 변환
│   │   ├── common.py                # 결측치 보간
│   │   └── region_mapper.py         # 지역명 정규화
│   └── loaders/
│       └── db_loader.py             # PostgreSQL UPSERT
├── api/
│   └── routes.py                    # REST API 엔드포인트 (전체)
├── quality/
│   ├── checker.py                   # Null/이상치/중복 검사
│   ├── report_generator.py          # HTML/Markdown 리포트 생성
│   └── templates/
│       └── report.html              # Jinja2 HTML 템플릿
├── catalog/
│   └── lineage.py                   # 데이터 카탈로그 + 리니지 정의
└── tests/
    └── test_run_id_logging.py       # 로깅 동작 검증 테스트 (run_id + duration_ms + JSON 포맷, 16개)
```

### 의존성 관계도

```
main.py
  ├── config.py
  ├── etl/context.py                  ← RunIdFilter가 참조 (run_id 주입)
  ├── db/migrations.py
  │   └── db/database.py ← db/models.py
  ├── etl/pipeline.py
  │   ├── etl/context.py              ← run_id 생성 및 set
  │   ├── etl/base.py
  │   ├── etl/extractors/*.py
  │   ├── etl/transformers/*.py
  │   └── etl/loaders/db_loader.py ← db/models.py
  ├── quality/report_generator.py
  │   └── quality/checker.py ← db/models.py
  ├── api/routes.py
  │   ├── etl/pipeline.py
  │   ├── quality/report_generator.py
  │   ├── catalog/lineage.py
  │   └── db/models.py
  └── catalog/lineage.py
```

---

## 3. 데이터 흐름 (ETL)

```
[트리거]
  APScheduler (매 5분) 또는 POST /api/etl/run
          │
          ▼
  run_pipeline()  ← etl/pipeline.py
          │
          ├─ run_id = uuid.uuid4().hex[:8]  ← 소스별 고유 추적 ID 생성
          ├─ run_id_var.set(run_id)          ← ContextVar에 저장 (스레드 격리)
          ├─ EtlRunLog 생성 (status="running")
          │
          ├─────────────────────────────────────────────────────┐
          │          EXTRACT 단계                               │
          │  t0 = perf_counter()                                │
          │  use_mock_data=true → mock_extract()                │
          │  use_mock_data=false → extract()                    │
          │    └─ fetch(): HTTP GET + 재시도, duration_ms 기록  │
          │  log: Extract complete rows=N duration_ms=N         │
          └─────────────────────────────────────────────────────┘
          │
          ▼
  ┌─── TRANSFORM 단계 (순서 중요) ───────────────────────────┐
  │  t0 = perf_counter()                                     │
  │  1. RegionMapper                                         │
  │     sidoName/nx,ny → region (정규화)                     │
  │     "서울" → "서울특별시"                               │
  │                                                          │
  │  2. SchemaNormalizer                                     │
  │     API 필드명 → 내부 필드명                            │
  │     문자열 → float/int/datetime 타입변환                │
  │     등급 코드 → 한글 (1→"좋음" 등)                     │
  │                                                          │
  │  3. MissingValueInterpolator                             │
  │     None/""/"-" → 이전값 또는 0.0                      │
  │  log: Transform complete rows=N duration_ms=N            │
  └──────────────────────────────────────────────────────────┘
          │
          ▼
  ┌─── LOAD 단계 ─────────────────────────────────────────────┐
  │  t0 = perf_counter()                                      │
  │  collected_at = 현재 KST 시간 추가                        │
  │  INSERT ... ON CONFLICT DO UPDATE (UPSERT)                │
  │  중복키: (station_name, measured_at) 등                   │
  │  log: Load complete rows=N duration_ms=N                  │
  └───────────────────────────────────────────────────────────┘
          │
          ▼
  EtlRunLog 업데이트
  (status="success"/"failed", 레코드 수 기록)
  log: Pipeline complete extracted=N loaded=N duration_ms=N
```

### 소스별 수집 현황

| 소스 | API | 수집 단위 | 데이터 지연 |
|------|-----|----------|------------|
| 미세먼지 | data.go.kr (한국환경공단) | 서울 40개 측정소 실시간 | 없음 |
| 날씨 | data.go.kr (기상청 단기예보) | 서울 기준 약 907건/회 | 없음 |
| 지하철 | data.seoul.go.kr | 60일 전 하루치 전체역 617건 | 약 2개월 |

---

## 4. 파일별 상세 설명

---

### main.py — FastAPI 진입점

**역할:** 앱 시작/종료 처리, APScheduler 등록, CORS 설정, 로깅 초기화

```python
# 로깅 초기화 (앱 최상단)
class RunIdFilter(logging.Filter):
    def filter(self, record):
        record.run_id = run_id_var.get()  # ContextVar에서 run_id 읽어 레코드에 주입
        return True

class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        log_record["timestamp"] = datetime.now(KST).isoformat(timespec="milliseconds")
        log_record["level"]   = record.levelname
        log_record["logger"]  = record.name
        log_record["run_id"]  = getattr(record, "run_id", "-")
        log_record["service"] = "etl-backend"
        if record.exc_info:
            log_record["traceback"] = self.formatException(record.exc_info)
            record.exc_info = None   # JSON에 traceback 필드로 직렬화 후 중복 방지

_handler = logging.StreamHandler()
_handler.setFormatter(CustomJsonFormatter())
_handler.addFilter(RunIdFilter())  # 루트 핸들러에 등록 → 모든 자식 로거에 자동 전파
logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(_handler)

# lifespan: 앱 시작 시 실행
1. run_migrations()      # 스키마 자동 생성/업그레이드
2. scheduler.add_job()   # ETL + 품질리포트 cron 등록
3. scheduler.start()     # 스케줄러 시작

# 앱 종료 시
scheduler.shutdown()
```

> **필터를 logger가 아닌 handler에 등록하는 이유:** 자식 로거(`etl.pipeline` 등)의 레코드가
> 루트로 전파될 때 루트 logger의 filter는 실행되지 않고 루트 handler의 filter만 실행된다.

**등록된 cron 작업:**

| 함수 | 실행 시간 | 설정 환경변수 |
|------|----------|-------------|
| `scheduled_etl()` | 매 N분 (현재 5분) | `ETL_CRON_HOUR`, `ETL_CRON_MINUTE` |
| `scheduled_quality_report()` | 매일 새벽 1시 | `QUALITY_REPORT_HOUR`, `QUALITY_REPORT_MINUTE` |

---

### config.py — 환경변수 설정

**역할:** `.env` 파일을 읽어 Pydantic Settings로 관리

```python
settings.postgres_host        # DB 호스트
settings.air_quality_api_key  # 미세먼지 API 키
settings.use_mock_data        # Mock 데이터 사용 여부 (True/False)
settings.database_url         # "postgresql://user:pw@host:port/db" (자동 생성)
```

---

### db/models.py — ORM 모델 정의

**역할:** PostgreSQL 테이블을 Python 클래스로 정의. 6개 테이블.

#### 데이터 저장 테이블 (3개)

| 테이블 | 유니크 제약 | 주요 필드 |
|--------|-----------|----------|
| `air_quality` | (station_name, measured_at) | pm10, pm25, o3, no2, co, so2, grade |
| `weather` | (region, forecast_date) | temperature, humidity, wind_speed, precipitation, sky_condition |
| `subway` | (station_name, line, use_date) | boarding_count, alighting_count |

모든 데이터 테이블에 `collected_at` (수집 시각, KST) 컬럼 있음.

#### 메타데이터 테이블 (3개)

| 테이블 | 용도 |
|--------|------|
| `etl_run_log` | ETL 실행 이력 (시작/종료/성공/실패) |
| `quality_report` | 데이터 품질 검사 결과 저장 |
| `schema_version` | 마이그레이션 버전 추적 |

> **_kst_now()**: 모든 `created_at`, `collected_at` 기본값으로 사용. `datetime.utcnow() + timedelta(hours=9)` 반환.

---

### db/database.py — DB 연결 관리

**역할:** SQLAlchemy 엔진, 세션, FastAPI 의존성 주입 제공

```python
engine = create_engine(settings.database_url, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()   # 모든 모델이 상속하는 기본 클래스

def get_db():           # FastAPI Depends()로 주입하는 DB 세션 생성기
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
```

---

### db/migrations.py — 마이그레이션 관리

**역할:** Alembic 없이 수동으로 스키마 버전 관리. 서버 시작 시 자동 실행.

```python
# MIGRATIONS 리스트에 버전 추가하는 방식
MIGRATIONS = [
    {"version": 1, "description": "Initial schema", "sql": []},
    {"version": 2, "description": "Add column...", "sql": ["ALTER TABLE ..."]},
]

# 동작 원리
1. Base.metadata.create_all()로 ORM 모델 기반 테이블 자동 생성
2. schema_version 테이블에서 현재 버전 조회
3. 현재 버전보다 높은 MIGRATIONS의 SQL을 순서대로 실행
4. schema_version 업데이트
```

---

### etl/context.py — run_id 컨텍스트 변수

**역할:** 스레드별 독립적인 run_id를 관리하는 ContextVar 정의

```python
from contextvars import ContextVar
run_id_var: ContextVar[str] = ContextVar("run_id", default="-")
```

- APScheduler 백그라운드 스레드는 각자 독립된 컨텍스트를 가지므로, 소스별 run_id가 섞이지 않음
- 파이프라인 외부(서버 시작, 스케줄러 등록 등)에서는 기본값 `"-"` 사용

---

### etl/base.py — 추상 기본 클래스

**역할:** 모든 Extractor/Transformer가 상속해야 하는 추상 클래스 정의

#### BaseExtractor
```python
# 반드시 구현해야 하는 메서드
@property
def source_name(self) -> str: ...      # "air_quality", "weather", "subway"
def extract(self) -> list[dict]: ...    # 실제 API 호출
def mock_extract(self) -> list[dict]:  # 테스트용 Mock 데이터

# 이미 구현된 메서드 (상속받아 사용)
def fetch(url, params):
    # max_retries(기본 3)회 재시도
    # 각 시도마다 t0 = perf_counter() 로 duration_ms 측정
    # 성공: log INFO  "[source] HTTP GET success attempt=N duration_ms=N"
    # 중간 실패: log WARNING error_type=X error_msg='...' retry_exhausted=false
    # 최종 실패: log ERROR   error_type=X error_msg='...' retry_exhausted=true (exc_info=True)
    # 실패 시 2^n초 대기 (exponential backoff)
    # 최종 실패 후 RuntimeError raise
    # 성공 시 응답 후 rate_limit_delay(기본 1초) 대기
```

#### BaseTransformer
```python
def transform(self, records: list[dict]) -> list[dict]: ...  # 반드시 구현
```

---

### etl/pipeline.py — ETL 오케스트레이션

**역할:** 소스별 Extract → Transform → Load 파이프라인 실행 및 로그 기록

```python
# PIPELINE_CONFIG: 소스별 처리 구성
PIPELINE_CONFIG = {
    "air_quality": {
        "extractor_cls": AirQualityExtractor,
        "api_key_attr": "air_quality_api_key",   # settings에서 읽을 속성명
        "normalizer": AirQualityNormalizer(),
        "interpolator": MissingValueInterpolator(
            numeric_fields=["pm10", "pm25", "o3", "no2", "co", "so2"]
        ),
    },
    # weather, subway도 동일 구조
}

# run_pipeline() 실행 순서 (소스별 반복)
1. run_id = uuid.uuid4().hex[:8]    # 소스별 고유 추적 ID 생성
   run_id_var.set(run_id)           # ContextVar에 저장
2. _create_run_log(source)          # EtlRunLog 생성 (started_at=KST)
   t_total = perf_counter()         # 전체 소요시간 측정 시작
3. API 키 가져오기 (settings에서)
4. t0 = perf_counter()
   use_mock_data=true → mock_extract(), false → extract()
   log: "[source] Extract complete rows=N duration_ms=N"
5. t0 = perf_counter()
   region_mapper.transform(raw_data)
   normalizer.transform(mapped)
   interpolator.transform(normalized)
   log: "[source] Transform complete rows=N duration_ms=N"
6. t0 = perf_counter()
   upsert_records(source, interpolated)
   log: "[source] Load complete rows=N duration_ms=N"
7. _update_run_log(log_id, "success", extracted, loaded)
   log: "[source] Pipeline complete extracted=N loaded=N duration_ms=N"
   └─ 실패 시: _update_run_log(log_id, "failed", error_message=...)
              log ERROR: "[source] Pipeline failed
                          error_type=X error_msg='...' duration_ms=N"  exc_info=True

# _ms(t_start) 헬퍼
int((time.perf_counter() - t_start) * 1000)  # float초 → int밀리초
```

---

### etl/extractors/ — API 데이터 추출

#### air_quality.py
```python
# API: 한국환경공단 대기오염정보
# URL: http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty
# 파라미터: sidoName="서울", numOfRows=100
# 응답 경로: response.body.items[]

# 실제 API 주요 필드
{
    "stationName": "종로구",
    "sidoName": "서울",
    "dataTime": "2026-02-22 10:00",
    "pm10Value": "45",
    "pm25Value": "25",
    "o3Value": "0.045",
    "pm10Grade": "2"    # 1=좋음, 2=보통, 3=나쁨, 4=매우나쁨
}
```

#### weather.py
```python
# API: 기상청 단기예보 조회서비스
# URL: http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst
# 파라미터: base_date=오늘, base_time=0500, nx=60, ny=127 (서울 격자)
# 응답 경로: response.body.items.item[]

# 실제 API 주요 필드 (각 예보 항목이 별도 레코드)
{
    "fcstDate": "20260222",
    "fcstTime": "1000",
    "category": "TMP",    # 기온
    "fcstValue": "15"
}
# 주의: 하나의 예보 시점에 category별로 레코드가 분리됨
```

#### subway.py
```python
# API: 서울 열린데이터 지하철 호선별 역별 승하차
# URL: http://openapi.seoul.go.kr:8088/{API키}/json/CardSubwayStatsNew/1/1000/{날짜}
# 날짜: 현재 KST - 60일 (데이터 제공 지연 약 2개월)

# 실제 API 주요 필드
{
    "USE_YMD": "20251224",
    "SBWY_STNS_NM": "서울역",
    "SBWY_ROUT_LN_NM": "1호선",
    "GTON_TNOPE": "78058",     # 승차 총 이용객수
    "GTOFF_TNOPE": "72251"     # 하차 총 이용객수
}
```

---

### etl/transformers/schema_normalizer.py — 스키마 정규화

**역할:** 각 API의 비표준 필드명을 내부 스키마로 통일, 타입 변환

#### AirQualityNormalizer 매핑
```
stationName        → station_name
sidoName/region    → region
dataTime           → measured_at (datetime 파싱)
pm10Value          → pm10 (float)
pm10Grade (1~4)    → grade ("좋음"/"보통"/"나쁨"/"매우나쁨")
```

#### WeatherNormalizer 매핑
```
region             → region
fcstDate+fcstTime  → forecast_date (datetime)
TMP                → temperature (float)
REH                → humidity (float)
WSD                → wind_speed (float)
PCP                → precipitation (float, "강수없음"→0.0, "5.0mm"→5.0)
SKY (1/3/4)        → sky_condition ("맑음"/"구름많음"/"흐림")
```

#### SubwayNormalizer 매핑
```
SBWY_STNS_NM (또는 SUB_STA_NM)       → station_name
SBWY_ROUT_LN_NM (또는 LINE_NUM)       → line
USE_YMD (또는 USE_DT, YYYYMMDD형식)  → use_date (datetime)
GTON_TNOPE (또는 RIDE_PASGR_NUM)      → boarding_count (int)
GTOFF_TNOPE (또는 ALIGHT_PASGR_NUM)  → alighting_count (int)
```

> `_safe_float()`, `_safe_int()`: None / "" / "-" 값을 None으로 안전하게 변환

---

### etl/transformers/common.py — 결측치 처리

**역할:** 숫자 필드의 결측치를 이전값으로 보간 (Last-value carry-forward)

```python
# MissingValueInterpolator 동작 예시

입력: [{"pm10": 50.0}, {"pm10": None}, {"pm10": None}, {"pm10": 40.0}]
출력: [{"pm10": 50.0}, {"pm10": 50.0}, {"pm10": 50.0}, {"pm10": 40.0}]

입력: [{"pm10": None}, {"pm10": None}, {"pm10": 30.0}]
출력: [{"pm10": 0.0},  {"pm10": 0.0},  {"pm10": 30.0}]
# 이전값 없으면 default(0.0) 사용
```

---

### etl/transformers/region_mapper.py — 지역명 정규화

**역할:** API별로 다른 지역명 표기를 행정구역 전체 이름으로 통일

```python
REGION_MAP = {
    "서울": "서울특별시",
    "부산": "부산광역시",
    "대구": "대구광역시",
    ...
}

GRID_TO_REGION = {
    ("60", "127"): "서울",   # 날씨 API 격자 좌표 → 지역명
    ("97", "76"): "부산",
    ...
}

# 처리 우선순위:
# 1. region_name 있으면 → REGION_MAP 정규화
# 2. nx, ny 있으면 → GRID_TO_REGION으로 변환 후 REGION_MAP 정규화
# 3. sidoName 있으면 → REGION_MAP 정규화
```

---

### etl/loaders/db_loader.py — DB 저장

**역할:** 변환된 데이터를 PostgreSQL에 UPSERT (중복 시 업데이트, 없으면 삽입)

```python
# 소스별 중복키 (이 키가 같으면 UPDATE, 다르면 INSERT)
CONFLICT_KEYS = {
    "air_quality": ["station_name", "measured_at"],
    "weather":     ["region", "forecast_date"],
    "subway":      ["station_name", "line", "use_date"],
}

# 실행되는 SQL (PostgreSQL 방언)
INSERT INTO air_quality (station_name, measured_at, pm10, ...)
VALUES (...)
ON CONFLICT ON CONSTRAINT uq_air_station_time
DO UPDATE SET pm10=excluded.pm10, ...

# upsert_records() 흐름
1. 각 레코드에 collected_at = KST 현재시각 추가
2. INSERT ON CONFLICT DO UPDATE 실행
3. 성공 시 commit, 실패 시 rollback + 에러 재발생
4. 로드된 레코드 수 반환
```

---

### api/routes.py — REST API 엔드포인트

**역할:** 모든 REST API를 단일 파일에서 관리. 프리픽스: `/api`

#### 전체 엔드포인트 목록

| 메서드 | 경로 | 설명 |
|--------|------|------|
| GET | `/api/dashboard` | 대시보드: 통계 + 최근 ETL 로그 + 7일 수집 현황 |
| GET | `/api/data/{source}` | 데이터 조회 (페이징, limit/offset) |
| GET | `/api/quality/reports` | 품질 리포트 목록 (최근 30개) |
| GET | `/api/quality/reports/{date}` | 특정 날짜 품질 리포트 상세 |
| GET | `/api/catalog` | 데이터 카탈로그 + 리니지 |
| POST | `/api/etl/run` | ETL 수동 실행 (백그라운드) |

#### GET /api/dashboard 응답 구조
```json
{
  "total_air_quality": 1200,
  "total_weather": 500,
  "total_subway": 617,
  "recent_runs": [
    {
      "source": "air_quality",
      "started_at": "2026-02-22T17:00:00",
      "status": "success",
      "records_extracted": 40,
      "records_loaded": 40
    }
  ],
  "daily_counts": [
    {"date": "2026-02-22", "air_quality": 80, "weather": 75, "subway": 617}
  ]
}
```

#### GET /api/data/{source}?limit=100&offset=0
```
source: "air_quality", "weather", "subway" 중 하나
limit: 최대 1000 (기본 100)
offset: 0 이상 (기본 0)

응답: {"source": "...", "total": 1200, "records": [...]}
정렬: id 역순 (최신 먼저)
```

#### POST /api/etl/run
```json
// 요청 (body 없이 호출 가능)
// 특정 소스만: ?sources=air_quality&sources=weather

// 응답 (즉시 반환, 실제 실행은 백그라운드)
{"message": "ETL pipeline triggered", "sources": ["air_quality", "weather", "subway"]}
```

---

### quality/checker.py — 데이터 품질 검사

**역할:** 특정 날짜 수집 데이터의 Null/이상치/중복을 검사하고 점수 계산

```python
# 품질 검사 대상 필드
NULLABLE_FIELDS = {
    "air_quality": ["pm10", "pm25", "o3", "no2", "co", "so2"],
    "weather":     ["temperature", "humidity", "wind_speed", "precipitation"],
    "subway":      ["boarding_count", "alighting_count"],
}

# 이상치 허용 범위 (이 범위를 벗어나면 이상치)
RANGE_CHECKS = {
    "air_quality": {"pm10": (0, 500), "pm25": (0, 300), "o3": (0, 0.6), ...},
    "weather":     {"temperature": (-50, 50), "humidity": (0, 100), ...},
    "subway":      {"boarding_count": (0, 1_000_000), ...},
}
```

#### check_quality() 로직
```
입력: source="air_quality", target_date=2026-02-22

1. 해당 날짜(collected_at 기준)의 레코드 조회
2. total == 0 → {"overall_score": 100.0, ...} 반환

3. Null 검사:
   - 각 nullable_field마다 IS NULL 레코드 수 집계

4. 이상치 검사:
   - 각 필드마다 (값 < min OR 값 > max) 레코드 수 집계

5. 중복 검사 (SQL):
   SELECT COUNT(*) - COUNT(DISTINCT (station_name, measured_at))

6. 종합점수 계산:
   issue_count = null_count + outlier_count + duplicate_count
   max_issues  = (total × nullable_fields수) + total
   overall_score = (1 - issue_count / max_issues) × 100

7. QualityReport DB 저장 후 결과 반환
```

---

### quality/report_generator.py — 리포트 생성

**역할:** 품질 검사 결과를 HTML과 Markdown 리포트로 변환

```python
# generate_report() 흐름
1. run_all_quality_checks(target_date)  # 3개 소스 검사
2. _render_html()    # templates/report.html (Jinja2)
3. _render_markdown() # 직접 문자열 생성

# 반환
{
    "html":     "<html>...</html>",
    "markdown": "# 데이터 품질 리포트...",
    "results":  [ {...air_quality...}, {...weather...}, {...subway...} ]
}
```

---

### catalog/lineage.py — 데이터 카탈로그 & 리니지

**역할:** 데이터 소스 메타데이터와 처리 흐름을 정적 dict로 정의. API로 제공.

```python
# CATALOG: 데이터 소스 메타데이터
CATALOG = {
    "air_quality": {
        "name": "미세먼지 데이터",
        "source": "data.go.kr - 한국환경공단",
        "api_url": "http://apis.data.go.kr/...",
        "update_frequency": "매시간",
        "fields": {
            "pm10": {"type": "float", "unit": "㎍/㎥", "description": "미세먼지"},
            ...
        }
    }
}

# LINEAGE: 데이터 처리 단계별 변환 흐름
LINEAGE = {
    "air_quality": {
        "stages": [
            {"name": "Extract",     "output": "raw JSON"},
            {"name": "RegionMap",   "output": "mapped records"},
            {"name": "Normalize",   "output": "normalized records"},
            {"name": "Interpolate", "output": "clean records"},
            {"name": "Load",        "output": "DB rows"},
        ]
    }
}
```

---

## 5. 데이터베이스 스키마

```sql
-- 미세먼지 (매 수집마다 약 40건)
CREATE TABLE air_quality (
    id           SERIAL PRIMARY KEY,
    station_name VARCHAR(100) NOT NULL,
    region       VARCHAR(100),
    measured_at  TIMESTAMP NOT NULL,
    pm10         FLOAT,
    pm25         FLOAT,
    o3           FLOAT,
    no2          FLOAT,
    co           FLOAT,
    so2          FLOAT,
    grade        VARCHAR(20),
    collected_at TIMESTAMP,     -- KST 수집 시각
    UNIQUE (station_name, measured_at)
);

-- 날씨 (매 수집마다 약 907건)
CREATE TABLE weather (
    id             SERIAL PRIMARY KEY,
    region         VARCHAR(100) NOT NULL,
    forecast_date  TIMESTAMP NOT NULL,
    temperature    FLOAT,
    humidity       FLOAT,
    wind_speed     FLOAT,
    precipitation  FLOAT,
    sky_condition  VARCHAR(50),
    collected_at   TIMESTAMP,
    UNIQUE (region, forecast_date)
);

-- 지하철 (60일 전 하루치, 약 617건)
CREATE TABLE subway (
    id              SERIAL PRIMARY KEY,
    station_name    VARCHAR(100) NOT NULL,
    line            VARCHAR(50) NOT NULL,
    use_date        TIMESTAMP NOT NULL,
    boarding_count  INTEGER,
    alighting_count INTEGER,
    collected_at    TIMESTAMP,
    UNIQUE (station_name, line, use_date)
);

-- ETL 실행 로그
CREATE TABLE etl_run_log (
    id                SERIAL PRIMARY KEY,
    source            VARCHAR(50) NOT NULL,
    started_at        TIMESTAMP NOT NULL,
    finished_at       TIMESTAMP,
    status            VARCHAR(20) NOT NULL DEFAULT 'running',
    records_extracted INTEGER DEFAULT 0,
    records_loaded    INTEGER DEFAULT 0,
    error_message     TEXT
);

-- 데이터 품질 리포트
CREATE TABLE quality_report (
    id               SERIAL PRIMARY KEY,
    report_date      TIMESTAMP NOT NULL,
    source           VARCHAR(50) NOT NULL,
    total_records    INTEGER DEFAULT 0,
    null_count       INTEGER DEFAULT 0,
    duplicate_count  INTEGER DEFAULT 0,
    outlier_count    INTEGER DEFAULT 0,
    null_rate        FLOAT DEFAULT 0.0,
    overall_score    FLOAT DEFAULT 0.0,
    details          TEXT,           -- 필드별 상세 (JSON 문자열)
    created_at       TIMESTAMP
);

-- 마이그레이션 버전 추적
CREATE TABLE schema_version (
    id          SERIAL PRIMARY KEY,
    version     INTEGER NOT NULL UNIQUE,
    description VARCHAR(255),
    applied_at  TIMESTAMP
);
```

---

## 6. REST API 엔드포인트

| 메서드 | 경로 | 주요 응답 필드 |
|--------|------|--------------|
| GET | `/api/dashboard` | total_*, recent_runs[], daily_counts[] |
| GET | `/api/data/air_quality` | total, records[] |
| GET | `/api/data/weather` | total, records[] |
| GET | `/api/data/subway` | total, records[] |
| GET | `/api/quality/reports` | id, report_date, source, overall_score |
| GET | `/api/quality/reports/2026-02-22` | html, markdown, results[] |
| GET | `/api/catalog` | catalog{}, lineage{} |
| POST | `/api/etl/run` | message, sources[] |

**Swagger UI:** http://localhost:8000/docs

---

## 7. 스케줄 자동화

| 환경변수 | 기본값 | 설명 |
|---------|--------|------|
| `ETL_CRON_HOUR` | `*` | ETL 실행 시간 (cron 형식, `*`=매시간) |
| `ETL_CRON_MINUTE` | `*/5` | ETL 실행 분 (현재 5분마다) |
| `QUALITY_REPORT_HOUR` | `1` | 품질 리포트 시간 (새벽 1시) |
| `QUALITY_REPORT_MINUTE` | `0` | 품질 리포트 분 (정각) |

cron 형식 예시:
- `*/5` = 5분마다 (0, 5, 10, 15...)
- `0` = 정각에만
- `*` = 매분 / 매시간

---

## 8. 로깅 구조

### 로그 포맷 (JSON)

모든 로그는 `python-json-logger`의 `CustomJsonFormatter`를 통해 JSON으로 출력됩니다. 로거 호출 코드는 변경 없이 포맷터 교체만으로 JSON 전환이 이루어집니다.

#### JSON 필드 구조

| 필드 | 출처 | 설명 |
|------|------|------|
| `timestamp` | `datetime.now(KST).isoformat()` | KST 기준 ISO 8601 (밀리초까지) |
| `level` | `record.levelname` | INFO / WARNING / ERROR |
| `logger` | `record.name` | 로거 계층 (예: `etl.pipeline`) |
| `run_id` | `ContextVar` | ETL 소스 실행 단위 추적 ID (8자리 hex), 외부는 `"-"` |
| `service` | 고정값 | `"etl-backend"` |
| `message` | 로그 메시지 | 기존 f-string 메시지 그대로 |
| `traceback` | `formatException()` | ERROR 로그에만 추가, 원본 exc_info 제거로 중복 방지 |

#### 예시 — 정상 흐름:
```json
{"message": "[air_quality] Extract complete rows=40 duration_ms=312", "timestamp": "2026-03-04T10:30:00.123+09:00", "level": "INFO", "logger": "etl.pipeline", "run_id": "a1b2c3d4", "service": "etl-backend"}
{"message": "[air_quality] HTTP GET success attempt=1 duration_ms=287", "timestamp": "2026-03-04T10:30:00.435+09:00", "level": "INFO", "logger": "etl.base", "run_id": "a1b2c3d4", "service": "etl-backend"}
{"message": "[air_quality] Transform complete rows=40 duration_ms=5", "timestamp": "2026-03-04T10:30:00.440+09:00", "level": "INFO", "logger": "etl.pipeline", "run_id": "a1b2c3d4", "service": "etl-backend"}
{"message": "[air_quality] Load complete rows=40 duration_ms=88", "timestamp": "2026-03-04T10:30:00.528+09:00", "level": "INFO", "logger": "etl.loaders.db_loader", "run_id": "a1b2c3d4", "service": "etl-backend"}
{"message": "[air_quality] Pipeline complete extracted=40 loaded=40 duration_ms=406", "timestamp": "2026-03-04T10:30:00.529+09:00", "level": "INFO", "logger": "etl.pipeline", "run_id": "a1b2c3d4", "service": "etl-backend"}
```

#### 예시 — 재시도 후 최종 실패:
```json
{"message": "[air_quality] Fetch attempt 1/3 failed error_type=TimeoutError error_msg='read timeout' duration_ms=5000 retry_exhausted=false", "timestamp": "2026-03-04T10:30:00.100+09:00", "level": "WARNING", "logger": "etl.base", "run_id": "a1b2c3d4", "service": "etl-backend"}
{"message": "[air_quality] Fetch attempt 2/3 failed error_type=TimeoutError error_msg='read timeout' duration_ms=5000 retry_exhausted=false", "timestamp": "2026-03-04T10:30:02.200+09:00", "level": "WARNING", "logger": "etl.base", "run_id": "a1b2c3d4", "service": "etl-backend"}
{"message": "[air_quality] Fetch retry exhausted error_type=TimeoutError ... retry_exhausted=true duration_ms=5000", "timestamp": "2026-03-04T10:30:06.400+09:00", "level": "ERROR", "logger": "etl.base", "run_id": "a1b2c3d4", "service": "etl-backend", "traceback": "Traceback (most recent call last): ..."}
{"message": "[air_quality] Pipeline failed error_type=RuntimeError error_msg='...' duration_ms=16301", "timestamp": "2026-03-04T10:30:06.401+09:00", "level": "ERROR", "logger": "etl.pipeline", "run_id": "a1b2c3d4", "service": "etl-backend", "traceback": "Traceback (most recent call last): ..."}
```

> **ELK 연동 시 장점:** JSON 한 줄 = 로그 이벤트 1개. Filebeat가 파싱 없이 Elasticsearch로 전송 가능. `run_id`, `level`, `logger`, `service` 필드로 Kibana 필터/대시보드 즉시 구성 가능.

### run_id 전파 흐름

```
pipeline.py: run_id_var.set("a1b2c3d4")
     │
     ├─ etl.pipeline          logger → run_id=a1b2c3d4  ✓
     ├─ etl.base              logger → run_id=a1b2c3d4  ✓  (자동, 코드 수정 없음)
     └─ etl.loaders.db_loader logger → run_id=a1b2c3d4  ✓  (자동, 코드 수정 없음)
```

- `RunIdFilter`가 루트 **핸들러**에 등록되어, 모든 자식 로거가 루트로 전파할 때 자동 주입
- 파이프라인 외부(서버 시작 등)는 ContextVar 기본값 `"-"` 사용

### duration_ms 측정 포인트

| 위치 | 측정 대상 | 로그 레벨 |
|------|----------|----------|
| `etl/pipeline.py` Extract | API 호출 전체 | INFO |
| `etl/pipeline.py` Transform | 3단계 변환 합산 | INFO |
| `etl/pipeline.py` Load | DB upsert 전체 | INFO |
| `etl/pipeline.py` 전체 | 소스 1개 파이프라인 합산 | INFO |
| `etl/pipeline.py` 실패 | 실패 시점까지 합산 | ERROR |
| `etl/base.py` fetch | HTTP 요청 1회 (재시도별) | INFO / WARNING / ERROR |

### 에러 로그 표준화 필드

모든 ERROR / WARNING 에러 로그는 아래 필드를 포함합니다.

| 필드 | 예시 | 설명 |
|------|------|------|
| `error_type` | `error_type=TimeoutError` | `type(e).__name__` — 예외 클래스명 |
| `error_msg` | `error_msg='read timeout'` | `str(e)!r` — 따옴표로 감싸 공백/특수문자 보호 |
| `exc_info` | _(스택 트레이스 자동 첨부)_ | `exc_info=True` — ERROR 로그에만 첨부 |

#### 재시도 추가 필드 (`etl/base.py` fetch)

| 필드 | 값 | 조건 |
|------|----|------|
| `retry_exhausted` | `false` | 중간 재시도 (WARNING) |
| `retry_exhausted` | `true` | 최종 실패 (ERROR) |
| `attempt` | `3` | 실패한 시도 번호 |
| `max_retries` | `3` | 최대 재시도 횟수 |

#### 재시도 로그 레벨 정책

```
attempt 1/3 실패 → WARNING  retry_exhausted=false  exc_info 없음
attempt 2/3 실패 → WARNING  retry_exhausted=false  exc_info 없음
attempt 3/3 실패 → ERROR    retry_exhausted=true   exc_info=True (스택 첨부)
```

중간 재시도에 exc_info를 붙이지 않는 이유: 예상된 일시적 실패이므로 스택 트레이스가 불필요하고 로그 볼륨을 줄이기 위함.

---

## 10. 설계 패턴

### Template Method Pattern
`BaseExtractor`와 `BaseTransformer`가 골격을 정의하고, 각 데이터 소스 클래스가 세부 구현.

```python
# 골격 (base.py)
class BaseExtractor:
    def fetch(url):        # 이미 구현 (재시도 포함)
    def extract(): ...     # 서브클래스 구현 필수
    def mock_extract(): .. # 서브클래스 구현 필수

# 구현 (air_quality.py)
class AirQualityExtractor(BaseExtractor):
    def extract(self):        # API 실제 호출
    def mock_extract(self):   # Mock 데이터 반환
```

### Upsert Pattern (멱등성)
같은 데이터를 여러 번 수집해도 중복 없이 최신값으로 업데이트됨.

```sql
INSERT INTO air_quality (station_name, measured_at, pm10, ...)
VALUES ('종로구', '2026-02-22 10:00', 45.0, ...)
ON CONFLICT (station_name, measured_at)
DO UPDATE SET pm10 = 45.0, ...
```

### Background Task (FastAPI)
ETL을 HTTP 요청에서 분리하여 즉시 응답 후 백그라운드 실행.

```python
@router.post("/etl/run")
def trigger_etl(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_pipeline)
    return {"message": "ETL pipeline triggered"}  # 즉시 응답
    # run_pipeline()은 별도 스레드에서 실행됨
```

---

## 11. 새 데이터 소스 추가 방법

총 7단계 작업 필요:

```
1. etl/extractors/new_source.py      # Extractor 작성
2. etl/transformers/schema_normalizer.py  # Normalizer 추가
3. etl/pipeline.py (PIPELINE_CONFIG) # 파이프라인 구성 등록
4. db/models.py                      # ORM 모델 추가
5. etl/loaders/db_loader.py          # MODEL_MAP, CONFLICT_KEYS 등록
6. quality/checker.py                # RANGE_CHECKS, NULLABLE_FIELDS 추가
7. catalog/lineage.py                # CATALOG, LINEAGE 추가
```

#### 예시: 버스 데이터 추가

```python
# 1. etl/extractors/bus.py
class BusExtractor(BaseExtractor):
    source_name = "bus"
    def extract(self): ...
    def mock_extract(self): ...

# 2. schema_normalizer.py에 BusNormalizer 추가
class BusNormalizer(BaseTransformer):
    def transform(self, records): ...

# 3. pipeline.py PIPELINE_CONFIG에 추가
"bus": {
    "extractor_cls": BusExtractor,
    "api_key_attr": "bus_api_key",
    "normalizer": BusNormalizer(),
    "interpolator": MissingValueInterpolator(numeric_fields=["passenger_count"]),
}

# 4. db/models.py에 Bus 모델 추가
class Bus(Base):
    __tablename__ = "bus"
    id = Column(Integer, primary_key=True)
    ...
    __table_args__ = (UniqueConstraint("route_id", "stop_id", "use_date"),)

# 5. db_loader.py 업데이트
MODEL_MAP["bus"] = Bus
CONFLICT_KEYS["bus"] = ["route_id", "stop_id", "use_date"]

# 6. checker.py 업데이트
RANGE_CHECKS["bus"] = {"passenger_count": (0, 500)}
NULLABLE_FIELDS["bus"] = ["passenger_count"]

# 7. catalog/lineage.py 업데이트
CATALOG["bus"] = { ... }
LINEAGE["bus"] = { ... }
```

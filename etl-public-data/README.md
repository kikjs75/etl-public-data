# 공공데이터 수집 ETL + 데이터 정합성 리포트

미세먼지, 날씨, 지하철 공공데이터 API를 수집하여 PostgreSQL에 저장하고, 데이터 품질 리포트를 자동 생성하는 ETL 파이프라인입니다. React 대시보드로 수집 현황과 품질 리포트를 시각화합니다.

## 기술스택

| 영역 | 기술 |
|------|------|
| Backend | Python 3.11, FastAPI, SQLAlchemy, APScheduler |
| DB | PostgreSQL 15 |
| Frontend | React 18, Vite, Recharts, React Router |
| Infra | Docker Compose (v1.25.0 이상, 파일 포맷 v3.7) |

## 빠른 시작

### 1. 환경 변수 설정

```bash
cp .env.example .env
```

기본값으로 `USE_MOCK_DATA=true`가 설정되어 있어 API 키 없이도 동작합니다. 실제 API를 사용하려면 `.env`에 키를 입력하고 `USE_MOCK_DATA=false`로 변경하세요.

### 2. Docker Compose 실행

```bash
docker-compose up --build
```

> **참고**: devcontainer 환경에서는 `sudo docker-compose up --build`로 실행하세요.
> 코드 변경 후 반영하려면 `--build` 옵션을 붙여 다시 실행해야 합니다 (bind mount 미사용).

3개 서비스가 기동됩니다:

| 서비스 | URL | 설명 |
|--------|-----|------|
| Frontend | http://localhost:3000 | React 대시보드 |
| Backend | http://localhost:8000 | FastAPI (Swagger: `/docs`) |
| PostgreSQL | localhost:5432 | DB |

### 3. ETL 수동 실행

```bash
# 전체 파이프라인
curl -X POST http://localhost:8000/api/etl/run

# 특정 소스만
curl -X POST http://localhost:8000/api/etl/run \
  -H "Content-Type: application/json" \
  -d '{"sources": ["air_quality"]}'
```

또는 대시보드에서 **ETL 수동 실행** 버튼을 클릭합니다.

## 프로젝트 구조

```
etl-public-data/
├── docker-compose.yml
├── .env.example
├── backend/
│   ├── main.py                      # FastAPI + APScheduler 엔트리포인트
│   ├── config.py                    # 환경변수 관리
│   ├── db/
│   │   ├── database.py              # SQLAlchemy 엔진/세션
│   │   ├── models.py                # ORM 모델
│   │   └── migrations.py            # 스키마 버전 관리
│   ├── etl/
│   │   ├── base.py                  # BaseExtractor / BaseTransformer
│   │   ├── extractors/              # 미세먼지, 날씨, 지하철 추출기
│   │   ├── transformers/            # 결측치 보간, 지역 매핑, 스키마 정규화
│   │   ├── loaders/db_loader.py     # DB upsert
│   │   └── pipeline.py              # ETL 오케스트레이션
│   ├── quality/
│   │   ├── checker.py               # 품질 검사 (null률, 이상치, 중복)
│   │   ├── report_generator.py      # HTML/Markdown 리포트 생성
│   │   └── templates/report.html    # Jinja2 템플릿
│   ├── catalog/lineage.py           # 데이터 카탈로그 / 리니지
│   └── api/
│       ├── routes.py                # REST API 엔드포인트
│       └── schemas.py               # Pydantic 스키마
└── frontend/
    └── src/
        ├── App.tsx
        ├── pages/                   # Dashboard, QualityReport, Catalog
        └── components/              # Charts, DataTable
```

## API 엔드포인트

| Method | Path | 설명 |
|--------|------|------|
| GET | `/api/dashboard` | 수집 현황 요약 (총 레코드, 일별 건수, 최근 실행 로그) |
| GET | `/api/quality/reports` | 품질 리포트 목록 |
| GET | `/api/quality/reports/{date}` | 일자별 상세 리포트 (HTML + Markdown) |
| GET | `/api/catalog` | 데이터 카탈로그 + 리니지 |
| GET | `/api/data/{source}` | 수집 데이터 조회 (`air_quality`, `weather`, `subway`) |
| POST | `/api/etl/run` | ETL 파이프라인 수동 실행 |

## 데이터 소스

| 데이터셋 | 출처 | 갱신 주기 |
|----------|------|----------|
| 미세먼지 | [data.go.kr - 대기오염정보](http://apis.data.go.kr/B552584/ArpltnInforInqireSvc) | 매시간 |
| 날씨 | [data.go.kr - 기상청 단기예보](http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0) | 3시간 |
| 지하철 | [서울 열린데이터 - 교통카드 이용현황](http://openapi.seoul.go.kr:8088) | 일 1회 |

## 스케줄

| 작업 | 기본 주기 | 환경변수 |
|------|----------|---------|
| ETL 파이프라인 | 매시 정각 | `ETL_CRON_HOUR`, `ETL_CRON_MINUTE` |
| 품질 리포트 생성 | 매일 01:00 | `QUALITY_REPORT_HOUR`, `QUALITY_REPORT_MINUTE` |

## 품질 검사 항목

- **Null률**: 필드별 결측치 비율
- **범위 이상치**: 물리적으로 불가능한 값 (예: PM10 > 500, 습도 > 100%)
- **중복 레코드**: 동일 키 조합의 중복 여부
- **종합 점수**: 위 항목을 종합한 0~100 품질 점수

## 스키마 진화

`db/migrations.py`에서 버전 기반 마이그레이션을 관리합니다. 새 마이그레이션 추가:

```python
# db/migrations.py의 MIGRATIONS 리스트에 추가
{
    "version": 3,
    "description": "Add new_column to air_quality",
    "sql": [
        "ALTER TABLE air_quality ADD COLUMN IF NOT EXISTS new_column VARCHAR(100)",
    ],
}
```

서버 시작 시 자동으로 미적용 마이그레이션이 실행됩니다.

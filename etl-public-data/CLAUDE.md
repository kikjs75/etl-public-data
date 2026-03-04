# CLAUDE.md

## 프로젝트 개요
공공데이터 API(미세먼지/날씨/지하철)를 수집하여 PostgreSQL에 저장하고, 데이터 품질 리포트를 자동 생성하는 ETL 파이프라인. React 대시보드로 시각화.

## 기술스택
- Backend: Python 3.11 + FastAPI + SQLAlchemy + APScheduler
- DB: PostgreSQL 15
- Frontend: React 18 + Vite + Recharts + React Router
- Infra: Docker Compose

## 빌드/실행 명령어
```bash
# 전체 실행
docker-compose up --build

# 백엔드만 로컬 실행 (DB 필요)
cd backend && pip install -r requirements.txt && uvicorn main:app --reload

# 프론트엔드만 로컬 실행
cd frontend && npm install && npm run dev

# Python 문법 검사
cd backend && python -m py_compile <파일명>
```

## 디렉토리 구조 핵심
- `backend/main.py` — FastAPI 엔트리포인트 + APScheduler
- `backend/db/models.py` — 모든 ORM 모델 정의
- `backend/db/migrations.py` — MIGRATIONS 리스트에 버전 추가하여 스키마 진화
- `backend/etl/pipeline.py` — ETL 오케스트레이션 (extract → transform → load)
- `backend/etl/base.py` — BaseExtractor / BaseTransformer 추상 클래스
- `backend/api/routes.py` — 모든 REST API 엔드포인트
- `backend/catalog/lineage.py` — 데이터 카탈로그/리니지 (정적 dict)
- `frontend/src/pages/` — Dashboard, QualityReport, Catalog 페이지

## 코딩 컨벤션
- Python: 타입 힌트 사용 (`list[str]`, `dict[str, Any]`, `str | None`)
- 새 Extractor 추가 시 `BaseExtractor`를 상속하고 `extract()`, `mock_extract()`, `source_name` 구현
- 새 Transformer 추가 시 `BaseTransformer`를 상속하고 `transform()` 구현
- DB 모델 추가 시 `db/models.py`에 정의, `UniqueConstraint` 설정, `etl/loaders/db_loader.py`의 `MODEL_MAP`과 `CONFLICT_KEYS`에 등록
- 프론트엔드: 함수형 컴포넌트 + hooks, inline style 사용 (CSS 파일 없음)
- 커밋 메시지: 한국어, 명령형 ("추가", "수정", "삭제")

## API 경로 패턴
모든 엔드포인트는 `/api` 프리픽스. 라우터는 `backend/api/routes.py`에 단일 파일로 관리.

## 환경변수
`.env` 파일 사용. `USE_MOCK_DATA=true`이면 API 키 없이 mock 데이터로 동작.

## 마이그레이션
Alembic 미사용. `backend/db/migrations.py`의 `MIGRATIONS` 리스트에 `{"version": N, "description": "...", "sql": [...]}` 형태로 추가. 서버 시작 시 자동 실행.

## 주의사항
- `quality/checker.py`의 `RANGE_CHECKS`에 필드별 유효 범위 정의됨 — 새 필드 추가 시 여기도 갱신
- `catalog/lineage.py`의 `CATALOG`, `LINEAGE` dict — 새 데이터소스 추가 시 여기도 갱신
- `etl/pipeline.py`의 `PIPELINE_CONFIG` — 새 소스 추가 시 여기에 등록

## ELK 스택 진행 현황

단계별로 구성 중. 각 단계 완료 후 다음 단계로 진행.

| 단계 | 구성 | Logstash output | 상태 |
|------|------|----------------|------|
| Stage 1 | Filebeat + Logstash | stdout (rubydebug) | ✅ 완료 |
| Stage 2 | + Elasticsearch | elasticsearch | 🔲 미완료 |
| Stage 3 | + Kibana | elasticsearch | 🔲 미완료 |

### 관련 파일
- `elk/filebeat/filebeat.yml` — container input, Logstash 출력
- `elk/logstash/pipeline/logstash.conf` — beats input, JSON 파싱, stdout 출력

### Stage 1 확인된 사항
- macOS Docker Desktop 환경에서 `add_docker_metadata`의 `container.name` 필드 enrichment 미동작
- Filebeat의 `drop_event` 대신 Logstash 필터에서 처리 권장
- ETL 실행 시 `run_id`, `rows`, `duration_ms` 등 필드가 Logstash stdout에서 확인됨

### Stage 2 다음 작업
- `elk/logstash/pipeline/logstash.conf`의 output을 elasticsearch로 변경
- `docker-compose.yml`에 elasticsearch 서비스 추가
- 검증: `curl localhost:9200/_cat/indices`로 인덱스 확인

"""
run_id 로깅 동작 검증 테스트

검증 항목:
  1. 파이프라인 외부 로그는 run_id="-" (기본값)
  2. 파이프라인 실행 중 모든 레이어(pipeline, base, db_loader)가 동일 run_id 공유
  3. 소스별로 run_id가 독립적으로 생성됨
  4. run_id는 8자리 hex 문자열

실행 방법:
  cd backend
  python -m tests.test_run_id_logging
"""

import logging
import sys
import uuid

sys.path.insert(0, ".")  # backend/ 기준 실행

from etl.context import run_id_var


# ── 로깅 설정 (main.py와 동일한 구성) ──────────────────────────────────────


class RunIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = run_id_var.get()
        return True


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s run_id=%(run_id)s: %(message)s",
    stream=sys.stdout,
)
for _handler in logging.getLogger().handlers:
    _handler.addFilter(RunIdFilter())


# ── 각 레이어를 대표하는 로거 ─────────────────────────────────────────────

log_main     = logging.getLogger("main")
log_pipeline = logging.getLogger("etl.pipeline")
log_base     = logging.getLogger("etl.base")
log_loader   = logging.getLogger("etl.loaders.db_loader")
log_quality  = logging.getLogger("quality.checker")


# ── 테스트 헬퍼 ───────────────────────────────────────────────────────────

captured: list[logging.LogRecord] = []


class CaptureHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        captured.append(record)


capture_handler = CaptureHandler()
capture_handler.addFilter(RunIdFilter())
logging.getLogger().addHandler(capture_handler)


def _records_for(logger_name: str) -> list[logging.LogRecord]:
    return [r for r in captured if r.name == logger_name]


def _assert(condition: bool, msg: str) -> None:
    status = "PASS" if condition else "FAIL"
    print(f"  [{status}] {msg}")
    if not condition:
        raise AssertionError(msg)


# ── 테스트 케이스 ─────────────────────────────────────────────────────────


def test_default_run_id() -> None:
    """파이프라인 외부 로그는 run_id='-' 기본값을 가져야 한다."""
    print("\n[TEST 1] 파이프라인 외부 로그 — run_id 기본값")
    captured.clear()

    log_main.info("서버 시작")
    log_main.info("Running database migrations...")

    for rec in _records_for("main"):
        _assert(rec.run_id == "-", f"run_id='{rec.run_id}' 이어야 '-'")


def test_run_id_shared_across_layers() -> None:
    """Extract → Transform → Load 단계가 동일 run_id를 공유해야 한다."""
    print("\n[TEST 2] 레이어 간 run_id 공유 (air_quality)")
    captured.clear()

    run_id = uuid.uuid4().hex[:8]
    run_id_var.set(run_id)

    log_pipeline.info("[air_quality] Extracted 40 records")
    log_base.warning("[air_quality] Attempt 1 failed: timeout")
    log_pipeline.info("[air_quality] Transform complete")
    log_loader.info("[air_quality] Upserted 40 records")

    for rec in captured:
        _assert(rec.run_id == run_id,
                f"{rec.name}: run_id='{rec.run_id}' should be '{run_id}'")


def test_run_id_isolated_per_source() -> None:
    """소스별로 run_id가 독립적으로 생성되어야 한다."""
    print("\n[TEST 3] 소스별 run_id 독립성")
    captured.clear()

    run_id_aq = uuid.uuid4().hex[:8]
    run_id_var.set(run_id_aq)
    log_pipeline.info("[air_quality] Extracted 40 records")
    log_loader.info("[air_quality] Upserted 40 records")

    run_id_wt = uuid.uuid4().hex[:8]
    run_id_var.set(run_id_wt)
    log_pipeline.info("[weather] Extracted 907 records")
    log_loader.info("[weather] Upserted 907 records")

    _assert(run_id_aq != run_id_wt, "소스마다 run_id가 달라야 함")

    aq_records = [r for r in captured if "air_quality" in r.getMessage()]
    wt_records = [r for r in captured if "weather" in r.getMessage()]

    for rec in aq_records:
        _assert(rec.run_id == run_id_aq,
                f"air_quality 로그 run_id='{rec.run_id}' should be '{run_id_aq}'")
    for rec in wt_records:
        _assert(rec.run_id == run_id_wt,
                f"weather 로그 run_id='{rec.run_id}' should be '{run_id_wt}'")


def test_run_id_format() -> None:
    """run_id는 8자리 hex 문자열이어야 한다."""
    print("\n[TEST 4] run_id 포맷 (8자리 hex)")
    captured.clear()

    for _ in range(5):
        run_id = uuid.uuid4().hex[:8]
        run_id_var.set(run_id)
        log_pipeline.info("dummy")

        last = captured[-1]
        _assert(len(last.run_id) == 8, f"길이 {len(last.run_id)} (expected 8)")
        _assert(all(c in "0123456789abcdef" for c in last.run_id),
                f"hex 문자만 허용: '{last.run_id}'")


def test_error_log_preserves_run_id() -> None:
    """에러 로그도 동일 run_id를 유지해야 한다."""
    print("\n[TEST 5] 에러 로그 run_id 유지")
    captured.clear()

    run_id = uuid.uuid4().hex[:8]
    run_id_var.set(run_id)

    log_pipeline.info("[subway] Extracted 617 records")
    log_pipeline.error("[subway] Pipeline failed: connection error")

    for rec in captured:
        _assert(rec.run_id == run_id,
                f"{rec.levelname} 로그 run_id='{rec.run_id}' should be '{run_id}'")


# ── 실행 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        test_default_run_id,
        test_run_id_shared_across_layers,
        test_run_id_isolated_per_source,
        test_run_id_format,
        test_error_log_preserves_run_id,
    ]

    print("=" * 60)
    print("run_id 로깅 검증 테스트")
    print("=" * 60)

    failed = 0
    for test in tests:
        try:
            test()
        except AssertionError as e:
            print(f"  → 실패: {e}")
            failed += 1

    print("\n" + "=" * 60)
    total = len(tests)
    passed = total - failed
    print(f"결과: {passed}/{total} 통과" + ("" if failed == 0 else f" ({failed}개 실패)"))
    print("=" * 60)
    sys.exit(failed)

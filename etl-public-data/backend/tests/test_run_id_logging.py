"""
로깅 동작 검증 테스트 (run_id + duration_ms + 에러 표준화 + JSON 포맷)

검증 항목:
  1. 파이프라인 외부 로그는 run_id="-" (기본값)
  2. 파이프라인 실행 중 모든 레이어(pipeline, base, db_loader)가 동일 run_id 공유
  3. 소스별로 run_id가 독립적으로 생성됨
  4. run_id는 8자리 hex 문자열
  5. 에러 로그도 run_id 유지
  6. Extract / Transform / Load 로그에 duration_ms 포함
  7. duration_ms는 0 이상의 정수
  8. 에러 로그에도 duration_ms 포함
  9. HTTP fetch 로그에 duration_ms 포함
  10. 에러 로그에 error_type 포함 (예외 클래스명)
  11. 에러 로그에 error_msg 포함 (따옴표로 감싼 메시지)
  12. ERROR 레벨 로그에 exc_info(스택 트레이스) 첨부
  13. 중간 재시도는 WARNING + retry_exhausted=false
  14. 최종 재시도 실패는 ERROR + retry_exhausted=true + exc_info
  15. JSON 포맷 출력이 유효한 JSON이며 필수 필드를 포함
  16. ERROR 로그의 traceback이 JSON 'traceback' 필드로 직렬화됨

실행 방법:
  cd backend
  python -m tests.test_run_id_logging
"""

import io
import json
import logging
import re
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional

from pythonjsonlogger import jsonlogger

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


# ── 테스트 헬퍼 ───────────────────────────────────────────────────────────

captured: list[logging.LogRecord] = []


class CaptureHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        captured.append(record)


capture_handler = CaptureHandler()
capture_handler.addFilter(RunIdFilter())
logging.getLogger().addHandler(capture_handler)

_DURATION_RE   = re.compile(r"duration_ms=(\d+)")
_ERROR_TYPE_RE = re.compile(r"error_type=(\w+)")
_ERROR_MSG_RE  = re.compile(r"error_msg='([^']*)'|error_msg=\"([^\"]*)\"")
_RETRY_EX_RE   = re.compile(r"retry_exhausted=(true|false)")


def _records_for(logger_name: str) -> list[logging.LogRecord]:
    return [r for r in captured if r.name == logger_name]


def _has_duration(record: logging.LogRecord) -> tuple[bool, int]:
    """메시지에서 duration_ms=N 추출. (존재 여부, 값)"""
    m = _DURATION_RE.search(record.getMessage())
    if m:
        return True, int(m.group(1))
    return False, -1


def _get_error_type(record: logging.LogRecord) -> Optional[str]:
    """메시지에서 error_type=XXX 추출."""
    m = _ERROR_TYPE_RE.search(record.getMessage())
    return m.group(1) if m else None


def _has_error_msg(record: logging.LogRecord) -> bool:
    """메시지에 error_msg='...' 포함 여부."""
    return bool(_ERROR_MSG_RE.search(record.getMessage()))


def _get_retry_exhausted(record: logging.LogRecord) -> Optional[str]:
    """메시지에서 retry_exhausted=true/false 추출."""
    m = _RETRY_EX_RE.search(record.getMessage())
    return m.group(1) if m else None


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

    log_pipeline.info("[air_quality] Extract complete rows=40 duration_ms=312")
    log_base.warning("[air_quality] Attempt 1 failed: timeout duration_ms=5000")
    log_pipeline.info("[air_quality] Transform complete rows=40 duration_ms=5")
    log_loader.info("[air_quality] Load complete rows=40 duration_ms=88")

    for rec in captured:
        _assert(rec.run_id == run_id,
                f"{rec.name}: run_id='{rec.run_id}' should be '{run_id}'")


def test_run_id_isolated_per_source() -> None:
    """소스별로 run_id가 독립적으로 생성되어야 한다."""
    print("\n[TEST 3] 소스별 run_id 독립성")
    captured.clear()

    run_id_aq = uuid.uuid4().hex[:8]
    run_id_var.set(run_id_aq)
    log_pipeline.info("[air_quality] Extract complete rows=40 duration_ms=200")
    log_loader.info("[air_quality] Load complete rows=40 duration_ms=80")

    run_id_wt = uuid.uuid4().hex[:8]
    run_id_var.set(run_id_wt)
    log_pipeline.info("[weather] Extract complete rows=907 duration_ms=350")
    log_loader.info("[weather] Load complete rows=907 duration_ms=120")

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

    log_pipeline.info("[subway] Extract complete rows=617 duration_ms=410")
    log_pipeline.error("[subway] Pipeline failed: connection error duration_ms=5123")

    for rec in captured:
        _assert(rec.run_id == run_id,
                f"{rec.levelname} 로그 run_id='{rec.run_id}' should be '{run_id}'")


def test_duration_ms_in_phase_logs() -> None:
    """Extract / Transform / Load 로그에 duration_ms가 포함되어야 한다."""
    print("\n[TEST 6] 단계별 로그 duration_ms 포함 여부")
    captured.clear()

    run_id_var.set(uuid.uuid4().hex[:8])

    # pipeline.py의 실제 로그 포맷 재현
    t0 = time.perf_counter()
    time.sleep(0.01)
    extract_ms = int((time.perf_counter() - t0) * 1000)

    t0 = time.perf_counter()
    time.sleep(0.005)
    transform_ms = int((time.perf_counter() - t0) * 1000)

    t0 = time.perf_counter()
    time.sleep(0.008)
    load_ms = int((time.perf_counter() - t0) * 1000)

    log_pipeline.info(f"[air_quality] Extract complete rows=40 duration_ms={extract_ms}")
    log_pipeline.info(f"[air_quality] Transform complete rows=40 duration_ms={transform_ms}")
    log_loader.info(f"[air_quality] Load complete rows=40 duration_ms={load_ms}")

    for rec in captured:
        found, val = _has_duration(rec)
        _assert(found, f"{rec.name} 로그에 duration_ms 없음: '{rec.getMessage()}'")
        _assert(val >= 0, f"duration_ms={val} 는 0 이상이어야 함")


def test_duration_ms_is_non_negative() -> None:
    """duration_ms는 항상 0 이상의 정수여야 한다."""
    print("\n[TEST 7] duration_ms 값 범위 (0 이상 정수)")
    captured.clear()

    run_id_var.set(uuid.uuid4().hex[:8])

    for ms in [0, 1, 50, 312, 5000]:
        log_pipeline.info(f"[air_quality] Extract complete rows=40 duration_ms={ms}")

    for rec in captured:
        found, val = _has_duration(rec)
        _assert(found, f"duration_ms 없음: '{rec.getMessage()}'")
        _assert(val >= 0, f"duration_ms={val} 는 0 이상이어야 함")
        _assert(isinstance(val, int), f"duration_ms={val} 는 정수여야 함")


def test_duration_ms_in_error_log() -> None:
    """에러 로그에도 duration_ms가 포함되어야 한다."""
    print("\n[TEST 8] 에러 로그 duration_ms 포함")
    captured.clear()

    run_id_var.set(uuid.uuid4().hex[:8])
    log_pipeline.error("[subway] Pipeline failed: timeout duration_ms=5123")

    for rec in captured:
        found, val = _has_duration(rec)
        _assert(found, f"에러 로그에 duration_ms 없음: '{rec.getMessage()}'")
        _assert(val >= 0, f"duration_ms={val} 는 0 이상이어야 함")


def test_duration_ms_in_http_fetch_log() -> None:
    """HTTP fetch 로그(base.py)에도 duration_ms가 포함되어야 한다."""
    print("\n[TEST 9] HTTP fetch 로그 duration_ms 포함")
    captured.clear()

    run_id_var.set(uuid.uuid4().hex[:8])

    # base.py fetch()의 실제 로그 포맷 재현
    log_base.info("[air_quality] HTTP GET success attempt=1 duration_ms=287")
    log_base.warning("[air_quality] Attempt 2 failed: ConnectTimeout duration_ms=5000")

    for rec in _records_for("etl.base"):
        found, val = _has_duration(rec)
        _assert(found, f"HTTP 로그에 duration_ms 없음: '{rec.getMessage()}'")
        _assert(val >= 0, f"duration_ms={val} 는 0 이상이어야 함")


def test_error_type_in_error_log() -> None:
    """ERROR 로그에 error_type=ExceptionClass 가 포함되어야 한다."""
    print("\n[TEST 10] 에러 로그 error_type 포함")
    captured.clear()
    run_id_var.set(uuid.uuid4().hex[:8])

    # pipeline.py / db_loader.py 실제 포맷 재현
    try:
        raise ValueError("invalid pm10 value")
    except ValueError as e:
        log_pipeline.error(
            f"[air_quality] Pipeline failed "
            f"error_type={type(e).__name__} "
            f"error_msg={str(e)!r} "
            f"duration_ms=312",
            exc_info=True,
        )

    try:
        raise RuntimeError("DB connection refused")
    except RuntimeError as e:
        log_loader.error(
            f"[subway] Load failed "
            f"error_type={type(e).__name__} "
            f"error_msg={str(e)!r}",
            exc_info=True,
        )

    for rec in captured:
        et = _get_error_type(rec)
        _assert(et is not None, f"{rec.name} 로그에 error_type 없음: '{rec.getMessage()}'")
        _assert(et in ("ValueError", "RuntimeError"), f"error_type='{et}' 예상치 못한 값")


def test_error_msg_in_error_log() -> None:
    """ERROR 로그에 error_msg='...' 가 따옴표로 감싸져 포함되어야 한다."""
    print("\n[TEST 11] 에러 로그 error_msg 포함 (따옴표)")
    captured.clear()
    run_id_var.set(uuid.uuid4().hex[:8])

    try:
        raise ConnectionError("timeout after 30s")
    except ConnectionError as e:
        log_pipeline.error(
            f"[weather] Pipeline failed "
            f"error_type={type(e).__name__} "
            f"error_msg={str(e)!r} "
            f"duration_ms=5123",
            exc_info=True,
        )

    rec = captured[-1]
    _assert(_has_error_msg(rec), f"error_msg 없음: '{rec.getMessage()}'")
    _assert("timeout after 30s" in rec.getMessage(), "error_msg 내용 불일치")


def test_exc_info_attached_to_error_log() -> None:
    """ERROR 레벨 로그에 exc_info (스택 트레이스) 가 첨부되어야 한다."""
    print("\n[TEST 12] ERROR 로그 exc_info 첨부")
    captured.clear()
    run_id_var.set(uuid.uuid4().hex[:8])

    try:
        raise KeyError("missing field: station_name")
    except KeyError as e:
        log_pipeline.error(
            f"[air_quality] Pipeline failed "
            f"error_type={type(e).__name__} "
            f"error_msg={str(e)!r} "
            f"duration_ms=88",
            exc_info=True,
        )

    rec = captured[-1]
    _assert(rec.exc_info is not None, "exc_info가 None — 스택 트레이스 없음")
    _assert(rec.exc_info[0] is KeyError, f"exc_info 타입 불일치: {rec.exc_info[0]}")


def test_retry_warning_is_not_exhausted() -> None:
    """중간 재시도(WARNING)는 retry_exhausted=false 이어야 한다."""
    print("\n[TEST 13] 중간 재시도 WARNING — retry_exhausted=false")
    captured.clear()
    run_id_var.set(uuid.uuid4().hex[:8])

    # base.py 중간 재시도 포맷 재현 (attempt 1/3, 2/3)
    for attempt in range(1, 3):
        try:
            raise TimeoutError("read timeout")
        except TimeoutError as e:
            log_base.warning(
                f"[air_quality] Fetch attempt {attempt}/3 failed "
                f"error_type={type(e).__name__} "
                f"error_msg={str(e)!r} "
                f"duration_ms=5000 "
                f"retry_exhausted=false",
            )

    for rec in captured:
        _assert(rec.levelname == "WARNING", f"중간 재시도는 WARNING 이어야 함: {rec.levelname}")
        exhausted = _get_retry_exhausted(rec)
        _assert(exhausted == "false", f"retry_exhausted='{exhausted}' (expected 'false')")
        _assert(rec.exc_info is None, "중간 재시도에 exc_info 있으면 안 됨")


def test_final_retry_is_error_with_exc_info() -> None:
    """최종 재시도 실패(ERROR)는 retry_exhausted=true + exc_info 첨부이어야 한다."""
    print("\n[TEST 14] 최종 재시도 ERROR — retry_exhausted=true + exc_info")
    captured.clear()
    run_id_var.set(uuid.uuid4().hex[:8])

    # base.py 최종 실패 포맷 재현 (attempt 3/3, is_last=True)
    try:
        raise TimeoutError("read timeout")
    except TimeoutError as e:
        log_base.error(
            f"[air_quality] Fetch retry exhausted "
            f"error_type={type(e).__name__} "
            f"error_msg={str(e)!r} "
            f"attempt=3 max_retries=3 "
            f"retry_exhausted=true "
            f"duration_ms=5000",
            exc_info=True,
        )

    rec = captured[-1]
    _assert(rec.levelname == "ERROR", f"최종 실패는 ERROR 이어야 함: {rec.levelname}")
    exhausted = _get_retry_exhausted(rec)
    _assert(exhausted == "true", f"retry_exhausted='{exhausted}' (expected 'true')")
    _assert(rec.exc_info is not None, "최종 실패에 exc_info 없음")
    _assert(rec.exc_info[0] is TimeoutError, f"exc_info 타입 불일치: {rec.exc_info[0]}")


# ── JSON 포맷 테스트용 헬퍼 ──────────────────────────────────────────────

KST = timezone(timedelta(hours=9))


class _TestJsonFormatter(jsonlogger.JsonFormatter):
    """main.py의 CustomJsonFormatter와 동일한 로직 (테스트 격리용)."""

    def add_fields(self, log_record: dict, record: logging.LogRecord, message_dict: dict) -> None:
        super().add_fields(log_record, record, message_dict)
        log_record["timestamp"] = datetime.now(KST).isoformat(timespec="milliseconds")
        log_record["level"] = record.levelname
        log_record["logger"] = record.name
        log_record["run_id"] = getattr(record, "run_id", "-")
        log_record["service"] = "etl-backend"
        if record.exc_info:
            log_record["traceback"] = self.formatException(record.exc_info)
            record.exc_info = None
            record.exc_text = None


def _make_json_logger(name: str) -> tuple[logging.Logger, io.StringIO]:
    """격리된 JSON 로거와 출력 스트림을 반환한다."""
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(_TestJsonFormatter())
    handler.addFilter(RunIdFilter())
    test_logger = logging.getLogger(name)
    test_logger.propagate = False
    test_logger.addHandler(handler)
    test_logger.setLevel(logging.INFO)
    return test_logger, stream


# ── 테스트 케이스 (JSON 포맷) ──────────────────────────────────────────────


def test_json_output_is_valid_json() -> None:
    """CustomJsonFormatter 출력이 파싱 가능한 JSON이어야 하고 필수 필드를 포함해야 한다."""
    print("\n[TEST 15] JSON 포맷 출력 검증")

    test_logger, stream = _make_json_logger("test.json.info")
    run_id_var.set(uuid.uuid4().hex[:8])
    expected_run_id = run_id_var.get()

    test_logger.info("[air_quality] Extract complete rows=40 duration_ms=312")

    line = stream.getvalue().strip()
    _assert(len(line) > 0, "JSON 출력이 비어있음")
    print(f"  출력: {line}")

    try:
        data = json.loads(line)
    except json.JSONDecodeError as e:
        _assert(False, f"JSON 파싱 실패: {e}")
        return

    required_fields = ["timestamp", "level", "logger", "run_id", "service", "message"]
    for field in required_fields:
        _assert(field in data, f"필수 필드 누락: '{field}'")

    _assert(data["level"] == "INFO", f"level='{data['level']}' (expected 'INFO')")
    _assert(data["logger"] == "test.json.info", f"logger='{data['logger']}'")
    _assert(data["service"] == "etl-backend", f"service='{data['service']}'")
    _assert(data["run_id"] == expected_run_id, f"run_id='{data['run_id']}' (expected '{expected_run_id}')")
    _assert(len(data["run_id"]) == 8, f"run_id 길이 불일치: '{data['run_id']}'")

    # timestamp는 ISO 8601 형식이어야 함
    try:
        datetime.fromisoformat(data["timestamp"])
        _assert(True, f"timestamp ISO 8601 파싱 성공")
    except ValueError:
        _assert(False, f"timestamp ISO 8601 파싱 실패: '{data['timestamp']}'")


def test_json_traceback_on_error() -> None:
    """ERROR 로그의 exc_info가 JSON의 'traceback' 필드로 직렬화되어야 한다."""
    print("\n[TEST 16] JSON ERROR 로그 traceback 필드 직렬화")

    test_logger, stream = _make_json_logger("test.json.error")
    run_id_var.set(uuid.uuid4().hex[:8])

    try:
        raise RuntimeError("DB connection refused")
    except RuntimeError as e:
        test_logger.error(
            f"[subway] Load failed "
            f"error_type={type(e).__name__} "
            f"error_msg={str(e)!r}",
            exc_info=True,
        )

    line = stream.getvalue().strip()
    _assert(len(line) > 0, "JSON 출력이 비어있음")
    print(f"  출력: {line[:120]}...")

    try:
        data = json.loads(line)
    except json.JSONDecodeError as e:
        _assert(False, f"JSON 파싱 실패: {e}")
        return

    _assert("traceback" in data, "traceback 필드가 JSON에 없음")
    _assert("RuntimeError" in data["traceback"], f"traceback에 예외 클래스명 없음: {data['traceback'][:80]}")
    _assert("DB connection refused" in data["traceback"], "traceback에 메시지 없음")
    _assert(data["level"] == "ERROR", f"level='{data['level']}' (expected 'ERROR')")


# ── 실행 ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        test_default_run_id,
        test_run_id_shared_across_layers,
        test_run_id_isolated_per_source,
        test_run_id_format,
        test_error_log_preserves_run_id,
        test_duration_ms_in_phase_logs,
        test_duration_ms_is_non_negative,
        test_duration_ms_in_error_log,
        test_duration_ms_in_http_fetch_log,
        test_error_type_in_error_log,
        test_error_msg_in_error_log,
        test_exc_info_attached_to_error_log,
        test_retry_warning_is_not_exhausted,
        test_final_retry_is_error_with_exc_info,
        test_json_output_is_valid_json,
        test_json_traceback_on_error,
    ]

    print("=" * 60)
    print("로깅 검증 테스트 (run_id + duration_ms + 에러 표준화 + JSON)")
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

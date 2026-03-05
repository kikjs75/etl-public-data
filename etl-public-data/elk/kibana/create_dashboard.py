#!/usr/bin/env python3
"""
Kibana ETL 파이프라인 대시보드 생성 스크립트
- Index Pattern, Visualization 4개, Dashboard 1개를 Saved Objects API로 임포트
"""
import json
import urllib.request
import urllib.error
import sys
import os

KIBANA_URL = os.getenv("KIBANA_URL", "http://host.docker.internal:5601")
INDEX_PATTERN_ID = "etl-logs-pattern"


def make_objects():
    ref = [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index",
            "type": "index-pattern", "id": INDEX_PATTERN_ID}]

    search_all = json.dumps({
        "index": INDEX_PATTERN_ID,
        "query": {"query": "", "language": "kuery"},
        "filter": []
    })
    search_etl_loggers = json.dumps({
        "index": INDEX_PATTERN_ID,
        "query": {"query": "logger: etl.*", "language": "kuery"},
        "filter": []
    })
    search_pipeline = json.dumps({
        "index": INDEX_PATTERN_ID,
        "query": {"query": 'logger: "etl.pipeline"', "language": "kuery"},
        "filter": []
    })
    search_duration = json.dumps({
        "index": INDEX_PATTERN_ID,
        "query": {"query": 'duration_ms: * and logger: "etl.pipeline"', "language": "kuery"},
        "filter": []
    })

    objects = []

    # ── 1. Index Pattern ─────────────────────────────────────────────
    objects.append({
        "type": "index-pattern",
        "id": INDEX_PATTERN_ID,
        "attributes": {
            "title": "etl-logs-*",
            "timeFieldName": "@timestamp",
            "name": "ETL Logs"
        },
        "references": [],
        "coreMigrationVersion": "8.12.2"
    })

    # ── 2. Metric: ETL Pipeline 총 실행 횟수 ─────────────────────────
    objects.append({
        "type": "visualization",
        "id": "etl-total-runs",
        "attributes": {
            "title": "ETL Pipeline 총 실행 횟수",
            "visState": json.dumps({
                "title": "ETL Pipeline 총 실행 횟수",
                "type": "metric",
                "params": {
                    "addTooltip": True,
                    "addLegend": False,
                    "type": "metric",
                    "metric": {
                        "percentageMode": False,
                        "useRanges": False,
                        "colorSchema": "Green to Red",
                        "metricColorMode": "None",
                        "colorsRange": [{"from": 0, "to": 10000}],
                        "labels": {"show": True},
                        "invertColors": False,
                        "style": {
                            "bgFill": "#000", "bgColor": False,
                            "labelColor": False, "subText": "", "fontSize": 60
                        }
                    }
                },
                "aggs": [
                    {"id": "1", "enabled": True, "type": "count",
                     "schema": "metric", "params": {}}
                ]
            }),
            "uiStateJSON": "{}",
            "description": "etl.pipeline logger의 총 이벤트 수",
            "kibanaSavedObjectMeta": {"searchSourceJSON": search_pipeline}
        },
        "references": ref,
        "coreMigrationVersion": "8.12.2"
    })

    # ── 3. Pie: 로그 레벨 분포 (INFO / WARNING / ERROR) ──────────────
    objects.append({
        "type": "visualization",
        "id": "etl-level-pie",
        "attributes": {
            "title": "ETL 로그 레벨 분포",
            "visState": json.dumps({
                "title": "ETL 로그 레벨 분포",
                "type": "pie",
                "params": {
                    "type": "pie",
                    "addTooltip": True,
                    "addLegend": True,
                    "legendPosition": "right",
                    "isDonut": True,
                    "labels": {"show": False, "values": True,
                               "last_level": True, "truncate": 100}
                },
                "aggs": [
                    {"id": "1", "enabled": True, "type": "count",
                     "schema": "metric", "params": {}},
                    {"id": "2", "enabled": True, "type": "terms",
                     "schema": "segment", "params": {
                         "field": "level.keyword",
                         "size": 5, "order": "desc", "orderBy": "1",
                         "otherBucket": False, "missingBucket": False
                     }}
                ]
            }),
            "uiStateJSON": "{}",
            "description": "INFO / WARNING / ERROR 비율",
            "kibanaSavedObjectMeta": {"searchSourceJSON": search_pipeline}
        },
        "references": ref,
        "coreMigrationVersion": "8.12.2"
    })

    # ── 4. Line: duration_ms 추이 ─────────────────────────────────────
    objects.append({
        "type": "visualization",
        "id": "etl-duration-line",
        "attributes": {
            "title": "ETL duration_ms 추이",
            "visState": json.dumps({
                "title": "ETL duration_ms 추이",
                "type": "line",
                "params": {
                    "type": "line",
                    "grid": {"categoryLines": False},
                    "categoryAxes": [{
                        "id": "CategoryAxis-1", "type": "category",
                        "position": "bottom", "show": True, "style": {},
                        "scale": {"type": "linear"},
                        "labels": {"show": True, "filter": True, "truncate": 100},
                        "title": {}
                    }],
                    "valueAxes": [{
                        "id": "ValueAxis-1", "name": "LeftAxis-1",
                        "type": "value", "position": "left", "show": True,
                        "style": {}, "scale": {"type": "linear", "mode": "normal"},
                        "labels": {"show": True, "rotate": 0,
                                   "filter": False, "truncate": 100},
                        "title": {"text": "duration_ms (avg)"}
                    }],
                    "seriesParams": [{
                        "show": True, "type": "line", "mode": "normal",
                        "data": {"label": "평균 duration_ms", "id": "1"},
                        "valueAxis": "ValueAxis-1",
                        "drawLinesBetweenPoints": True,
                        "lineWidth": 2, "showCircles": True
                    }],
                    "addTooltip": True, "addLegend": True,
                    "legendPosition": "right", "times": [],
                    "addTimeMarker": False,
                    "truncateLegend": True, "maxLegendLines": 1
                },
                "aggs": [
                    {"id": "1", "enabled": True, "type": "avg",
                     "schema": "metric", "params": {"field": "duration_ms"}},
                    {"id": "2", "enabled": True, "type": "date_histogram",
                     "schema": "segment", "params": {
                         "field": "@timestamp",
                         "useNormalizedEsInterval": True,
                         "scaleMetricValues": False,
                         "interval": "auto", "drop_partials": False,
                         "min_doc_count": 1, "extended_bounds": {}
                     }}
                ]
            }),
            "uiStateJSON": "{}",
            "description": "Pipeline 단계 평균 소요시간 추이",
            "kibanaSavedObjectMeta": {"searchSourceJSON": search_duration}
        },
        "references": ref,
        "coreMigrationVersion": "8.12.2"
    })

    # ── 5. Bar: 단계별(logger) 로그 수 시계열 ────────────────────────
    objects.append({
        "type": "visualization",
        "id": "etl-logger-bar",
        "attributes": {
            "title": "ETL 단계별 로그 수",
            "visState": json.dumps({
                "title": "ETL 단계별 로그 수",
                "type": "histogram",
                "params": {
                    "type": "histogram",
                    "grid": {"categoryLines": False},
                    "categoryAxes": [{
                        "id": "CategoryAxis-1", "type": "category",
                        "position": "bottom", "show": True, "style": {},
                        "scale": {"type": "linear"},
                        "labels": {"show": True, "filter": True, "truncate": 100},
                        "title": {}
                    }],
                    "valueAxes": [{
                        "id": "ValueAxis-1", "name": "LeftAxis-1",
                        "type": "value", "position": "left", "show": True,
                        "style": {}, "scale": {"type": "linear", "mode": "normal"},
                        "labels": {"show": True, "rotate": 0,
                                   "filter": False, "truncate": 100},
                        "title": {"text": "로그 수"}
                    }],
                    "seriesParams": [{
                        "show": True, "type": "histogram", "mode": "stacked",
                        "data": {"label": "로그 수", "id": "1"},
                        "valueAxis": "ValueAxis-1",
                        "drawLinesBetweenPoints": True,
                        "lineWidth": 2, "showCircles": True
                    }],
                    "addTooltip": True, "addLegend": True,
                    "legendPosition": "right", "times": [],
                    "addTimeMarker": False,
                    "truncateLegend": True, "maxLegendLines": 1
                },
                "aggs": [
                    {"id": "1", "enabled": True, "type": "count",
                     "schema": "metric", "params": {}},
                    {"id": "2", "enabled": True, "type": "terms",
                     "schema": "group", "params": {
                         "field": "logger.keyword", "size": 10,
                         "order": "desc", "orderBy": "1",
                         "otherBucket": False, "missingBucket": False
                     }},
                    {"id": "3", "enabled": True, "type": "date_histogram",
                     "schema": "segment", "params": {
                         "field": "@timestamp",
                         "useNormalizedEsInterval": True,
                         "scaleMetricValues": False,
                         "interval": "auto", "drop_partials": False,
                         "min_doc_count": 1, "extended_bounds": {}
                     }}
                ]
            }),
            "uiStateJSON": "{}",
            "description": "logger별(etl.pipeline / etl.base / etl.loaders.db_loader) 로그 수",
            "kibanaSavedObjectMeta": {"searchSourceJSON": search_etl_loggers}
        },
        "references": ref,
        "coreMigrationVersion": "8.12.2"
    })

    # ── 6. Dashboard ──────────────────────────────────────────────────
    panels = [
        {"version": "8.12.2", "type": "visualization",
         "gridData": {"x": 0,  "y": 0, "w": 12, "h": 8,  "i": "1"},
         "panelIndex": "1", "embeddableConfig": {}, "panelRefName": "panel_1"},
        {"version": "8.12.2", "type": "visualization",
         "gridData": {"x": 12, "y": 0, "w": 12, "h": 8,  "i": "2"},
         "panelIndex": "2", "embeddableConfig": {}, "panelRefName": "panel_2"},
        {"version": "8.12.2", "type": "visualization",
         "gridData": {"x": 24, "y": 0, "w": 24, "h": 8,  "i": "3"},
         "panelIndex": "3", "embeddableConfig": {}, "panelRefName": "panel_3"},
        {"version": "8.12.2", "type": "visualization",
         "gridData": {"x": 0,  "y": 8, "w": 48, "h": 15, "i": "4"},
         "panelIndex": "4", "embeddableConfig": {}, "panelRefName": "panel_4"},
    ]
    objects.append({
        "type": "dashboard",
        "id": "etl-pipeline-dashboard",
        "attributes": {
            "title": "ETL 파이프라인 대시보드",
            "description": "ETL 실행 현황 및 성능 모니터링",
            "panelsJSON": json.dumps(panels),
            "optionsJSON": json.dumps({
                "useMargins": True, "syncColors": False, "hidePanelTitles": False
            }),
            "version": 1,
            "timeRestore": False,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {"query": "", "language": "kuery"}, "filter": []
                })
            }
        },
        "references": [
            {"name": "panel_1", "type": "visualization", "id": "etl-total-runs"},
            {"name": "panel_2", "type": "visualization", "id": "etl-level-pie"},
            {"name": "panel_3", "type": "visualization", "id": "etl-duration-line"},
            {"name": "panel_4", "type": "visualization", "id": "etl-logger-bar"},
        ],
        "coreMigrationVersion": "8.12.2"
    })

    return "\n".join(json.dumps(obj, ensure_ascii=False) for obj in objects)


def import_ndjson(ndjson_content):
    boundary = "KibanaImportBoundary"
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="dashboard.ndjson"\r\n'
        f"Content-Type: application/ndjson\r\n\r\n"
        + ndjson_content
        + f"\r\n--{boundary}--\r\n"
    ).encode("utf-8")

    req = urllib.request.Request(
        f"{KIBANA_URL}/api/saved_objects/_import?overwrite=true",
        data=body,
        headers={
            "kbn-xsrf": "true",
            "Content-Type": f"multipart/form-data; boundary={boundary}"
        }
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


if __name__ == "__main__":
    print(f"Kibana: {KIBANA_URL}")

    ndjson = make_objects()

    os.makedirs("elk/kibana", exist_ok=True)
    with open("elk/kibana/dashboard.ndjson", "w", encoding="utf-8") as f:
        f.write(ndjson)
    print("elk/kibana/dashboard.ndjson 저장 완료")

    print("Kibana에 임포트 중...")
    try:
        result = import_ndjson(ndjson)
    except urllib.error.HTTPError as e:
        print(f"HTTP {e.code}: {e.read().decode()}")
        sys.exit(1)

    if result.get("success"):
        count = result.get("successCount", 0)
        print(f"임포트 성공: {count}개 오브젝트")
        print(f"\n대시보드: {KIBANA_URL}/app/dashboards")
    else:
        errors = result.get("errors", [])
        for err in errors:
            print(f"  오류: {err}")
        sys.exit(1)

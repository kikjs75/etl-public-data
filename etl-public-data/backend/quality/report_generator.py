import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

from quality.checker import run_all_quality_checks

logger = logging.getLogger(__name__)

TEMPLATE_DIR = Path(__file__).parent / "templates"


def generate_report(target_date: datetime | None = None) -> dict[str, str]:
    if target_date is None:
        target_date = (datetime.utcnow() + timedelta(hours=9)).replace(hour=0, minute=0, second=0, microsecond=0)

    results = run_all_quality_checks(target_date)

    html = _render_html(target_date, results)
    markdown = _render_markdown(target_date, results)

    return {"html": html, "markdown": markdown, "results": results}


def _render_html(target_date: datetime, results: list[dict[str, Any]]) -> str:
    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)))
    template = env.get_template("report.html")
    return template.render(
        report_date=target_date.strftime("%Y-%m-%d"),
        generated_at=(datetime.utcnow() + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M:%S KST"),
        results=results,
    )


def _render_markdown(target_date: datetime, results: list[dict[str, Any]]) -> str:
    lines = [
        f"# 데이터 품질 리포트 - {target_date.strftime('%Y-%m-%d')}",
        f"생성 시각: {(datetime.utcnow() + timedelta(hours=9)).strftime('%Y-%m-%d %H:%M:%S KST')}",
        "",
        "## 요약",
        "",
        "| 데이터셋 | 총 레코드 | Null 수 | 중복 수 | 이상치 수 | Null률(%) | 종합점수 |",
        "|---------|----------|---------|---------|----------|---------|---------|",
    ]
    for r in results:
        lines.append(
            f"| {r['source']} | {r['total_records']} | {r['null_count']} | "
            f"{r['duplicate_count']} | {r['outlier_count']} | {r['null_rate']} | {r['overall_score']} |"
        )

    lines.append("")
    lines.append("## 필드별 상세")
    for r in results:
        lines.append(f"\n### {r['source']}")
        details = r.get("field_details", {})
        if details:
            lines.append("| 필드 | Null 수 | Null률(%) | 이상치 수 |")
            lines.append("|------|---------|---------|----------|")
            for field, info in details.items():
                lines.append(
                    f"| {field} | {info.get('null_count', '-')} | "
                    f"{info.get('null_rate', '-')} | {info.get('outlier_count', '-')} |"
                )
        else:
            lines.append("데이터 없음")

    return "\n".join(lines)

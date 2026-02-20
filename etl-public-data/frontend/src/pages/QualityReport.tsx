import { useEffect, useState } from "react";
import { QualityScoreChart, NullRateChart } from "../components/Charts";
import DataTable from "../components/DataTable";

const cardStyle: React.CSSProperties = {
  background: "white",
  borderRadius: "8px",
  padding: "1.5rem",
  boxShadow: "0 2px 8px rgba(0,0,0,0.08)",
  marginBottom: "2rem",
};

export default function QualityReport() {
  const [reports, setReports] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/quality/reports")
      .then((r) => r.json())
      .then(setReports)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <p>로딩 중...</p>;

  return (
    <div>
      <h1>데이터 품질 리포트</h1>

      <div style={cardStyle}>
        <h2>종합 품질 점수 추이</h2>
        <QualityScoreChart data={reports} />
      </div>

      <div style={cardStyle}>
        <h2>Null률 추이</h2>
        <NullRateChart data={reports} />
      </div>

      <div style={cardStyle}>
        <h2>리포트 목록</h2>
        <DataTable
          columns={[
            { key: "report_date", label: "날짜" },
            { key: "source", label: "데이터셋" },
            { key: "total_records", label: "총 레코드" },
            { key: "null_count", label: "Null 수" },
            { key: "duplicate_count", label: "중복 수" },
            { key: "outlier_count", label: "이상치 수" },
            {
              key: "null_rate",
              label: "Null률(%)",
              render: (v: number) => v?.toFixed(2),
            },
            {
              key: "overall_score",
              label: "종합점수",
              render: (v: number) => (
                <span
                  style={{
                    color: v >= 90 ? "#27ae60" : v >= 70 ? "#f39c12" : "#e74c3c",
                    fontWeight: "bold",
                  }}
                >
                  {v?.toFixed(1)}
                </span>
              ),
            },
          ]}
          data={reports}
        />
      </div>
    </div>
  );
}

import { useEffect, useState } from "react";
import { DailyCollectionChart } from "../components/Charts";
import DataTable from "../components/DataTable";

interface DashboardData {
  total_air_quality: number;
  total_weather: number;
  total_subway: number;
  recent_runs: any[];
  daily_counts: any[];
}

const cardStyle: React.CSSProperties = {
  background: "white",
  borderRadius: "8px",
  padding: "1.5rem",
  boxShadow: "0 2px 8px rgba(0,0,0,0.08)",
};

const statCardStyle: React.CSSProperties = {
  ...cardStyle,
  textAlign: "center",
};

export default function Dashboard() {
  const [data, setData] = useState<DashboardData | null>(null);
  const [loading, setLoading] = useState(true);
  const [triggering, setTriggering] = useState(false);

  const fetchData = () => {
    setLoading(true);
    fetch("/api/dashboard")
      .then((r) => r.json())
      .then(setData)
      .catch(console.error)
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    fetchData();
  }, []);

  const triggerEtl = () => {
    setTriggering(true);
    fetch("/api/etl/run", { method: "POST" })
      .then((r) => r.json())
      .then(() => {
        setTimeout(fetchData, 3000);
      })
      .catch(console.error)
      .finally(() => setTriggering(false));
  };

  if (loading) return <p>로딩 중...</p>;
  if (!data) return <p>데이터를 불러올 수 없습니다.</p>;

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <h1>대시보드</h1>
        <button
          onClick={triggerEtl}
          disabled={triggering}
          style={{
            padding: "0.6rem 1.2rem",
            background: triggering ? "#999" : "#16213e",
            color: "white",
            border: "none",
            borderRadius: "4px",
            cursor: triggering ? "not-allowed" : "pointer",
          }}
        >
          {triggering ? "실행 중..." : "ETL 수동 실행"}
        </button>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: "1rem", marginBottom: "2rem" }}>
        <div style={statCardStyle}>
          <div style={{ fontSize: "2rem", fontWeight: "bold", color: "#8884d8" }}>
            {data.total_air_quality.toLocaleString()}
          </div>
          <div style={{ color: "#888" }}>미세먼지 레코드</div>
        </div>
        <div style={statCardStyle}>
          <div style={{ fontSize: "2rem", fontWeight: "bold", color: "#82ca9d" }}>
            {data.total_weather.toLocaleString()}
          </div>
          <div style={{ color: "#888" }}>날씨 레코드</div>
        </div>
        <div style={statCardStyle}>
          <div style={{ fontSize: "2rem", fontWeight: "bold", color: "#ffc658" }}>
            {data.total_subway.toLocaleString()}
          </div>
          <div style={{ color: "#888" }}>지하철 레코드</div>
        </div>
      </div>

      <div style={{ ...cardStyle, marginBottom: "2rem" }}>
        <h2>일별 수집 현황</h2>
        <DailyCollectionChart data={data.daily_counts} />
      </div>

      <div style={cardStyle}>
        <h2>최근 ETL 실행 로그</h2>
        <DataTable
          columns={[
            { key: "source", label: "데이터셋" },
            { key: "status", label: "상태", render: (v: string) => (
              <span style={{ color: v === "success" ? "#27ae60" : v === "failed" ? "#e74c3c" : "#f39c12" }}>
                {v}
              </span>
            )},
            { key: "records_extracted", label: "추출" },
            { key: "records_loaded", label: "적재" },
            { key: "started_at", label: "시작 시각", render: (v: string) => v ? new Date(v).toLocaleString("ko-KR") : "-" },
            { key: "error_message", label: "오류", render: (v: string) => v ? v.substring(0, 50) : "-" },
          ]}
          data={data.recent_runs}
        />
      </div>
    </div>
  );
}

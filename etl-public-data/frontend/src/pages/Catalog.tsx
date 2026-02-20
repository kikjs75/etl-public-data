import { useEffect, useState } from "react";

const cardStyle: React.CSSProperties = {
  background: "white",
  borderRadius: "8px",
  padding: "1.5rem",
  boxShadow: "0 2px 8px rgba(0,0,0,0.08)",
  marginBottom: "2rem",
};

const stageBoxStyle: React.CSSProperties = {
  display: "inline-flex",
  alignItems: "center",
  padding: "0.5rem 1rem",
  background: "#e8f4fd",
  borderRadius: "4px",
  border: "1px solid #b8daff",
  fontSize: "0.85rem",
};

const arrowStyle: React.CSSProperties = {
  margin: "0 0.5rem",
  color: "#666",
  fontSize: "1.2rem",
};

interface CatalogData {
  catalog: Record<string, any>;
  lineage: Record<string, any>;
}

export default function Catalog() {
  const [data, setData] = useState<CatalogData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/catalog")
      .then((r) => r.json())
      .then(setData)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <p>로딩 중...</p>;
  if (!data) return <p>데이터를 불러올 수 없습니다.</p>;

  return (
    <div>
      <h1>데이터 카탈로그</h1>

      {Object.entries(data.catalog).map(([key, info]) => (
        <div key={key} style={cardStyle}>
          <h2>{info.name}</h2>
          <p style={{ color: "#666" }}>{info.description}</p>

          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1rem", margin: "1rem 0" }}>
            <div>
              <strong>출처:</strong> {info.source}
            </div>
            <div>
              <strong>갱신 주기:</strong> {info.update_frequency}
            </div>
          </div>

          <h3>필드 목록</h3>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.9rem" }}>
            <thead>
              <tr>
                <th style={{ background: "#f0f0f0", padding: "0.5rem", textAlign: "left" }}>필드명</th>
                <th style={{ background: "#f0f0f0", padding: "0.5rem", textAlign: "left" }}>타입</th>
                <th style={{ background: "#f0f0f0", padding: "0.5rem", textAlign: "left" }}>단위</th>
                <th style={{ background: "#f0f0f0", padding: "0.5rem", textAlign: "left" }}>설명</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(info.fields).map(([fname, finfo]: [string, any]) => (
                <tr key={fname}>
                  <td style={{ padding: "0.5rem", borderBottom: "1px solid #eee", fontFamily: "monospace" }}>
                    {fname}
                  </td>
                  <td style={{ padding: "0.5rem", borderBottom: "1px solid #eee" }}>{finfo.type}</td>
                  <td style={{ padding: "0.5rem", borderBottom: "1px solid #eee" }}>{finfo.unit || "-"}</td>
                  <td style={{ padding: "0.5rem", borderBottom: "1px solid #eee" }}>{finfo.description}</td>
                </tr>
              ))}
            </tbody>
          </table>

          {data.lineage[key] && (
            <>
              <h3 style={{ marginTop: "1.5rem" }}>데이터 리니지</h3>
              <div style={{ display: "flex", alignItems: "center", flexWrap: "wrap", gap: "0.25rem" }}>
                {data.lineage[key].stages.map((stage: any, i: number) => (
                  <span key={i} style={{ display: "inline-flex", alignItems: "center" }}>
                    <span style={stageBoxStyle}>
                      <strong>{stage.name}</strong>
                      <span style={{ marginLeft: "0.5rem", color: "#555" }}>{stage.description}</span>
                    </span>
                    {i < data.lineage[key].stages.length - 1 && (
                      <span style={arrowStyle}>→</span>
                    )}
                  </span>
                ))}
              </div>
            </>
          )}
        </div>
      ))}
    </div>
  );
}

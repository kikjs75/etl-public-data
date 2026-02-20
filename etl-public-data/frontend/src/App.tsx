import { BrowserRouter, Routes, Route, NavLink } from "react-router-dom";
import Dashboard from "./pages/Dashboard";
import QualityReport from "./pages/QualityReport";
import Catalog from "./pages/Catalog";

const navStyle: React.CSSProperties = {
  display: "flex",
  gap: "1rem",
  padding: "1rem 2rem",
  background: "#16213e",
  color: "white",
};

const linkStyle: React.CSSProperties = {
  color: "#ccc",
  textDecoration: "none",
  padding: "0.5rem 1rem",
  borderRadius: "4px",
};

const activeLinkStyle: React.CSSProperties = {
  ...linkStyle,
  color: "white",
  background: "#0f3460",
};

export default function App() {
  return (
    <BrowserRouter>
      <nav style={navStyle}>
        <strong style={{ marginRight: "1rem", fontSize: "1.1rem" }}>
          공공데이터 ETL
        </strong>
        <NavLink
          to="/"
          style={({ isActive }) => (isActive ? activeLinkStyle : linkStyle)}
        >
          대시보드
        </NavLink>
        <NavLink
          to="/quality"
          style={({ isActive }) => (isActive ? activeLinkStyle : linkStyle)}
        >
          품질 리포트
        </NavLink>
        <NavLink
          to="/catalog"
          style={({ isActive }) => (isActive ? activeLinkStyle : linkStyle)}
        >
          데이터 카탈로그
        </NavLink>
      </nav>

      <div style={{ padding: "2rem", maxWidth: "1200px", margin: "0 auto" }}>
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/quality" element={<QualityReport />} />
          <Route path="/catalog" element={<Catalog />} />
        </Routes>
      </div>
    </BrowserRouter>
  );
}

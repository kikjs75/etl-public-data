import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  LineChart,
  Line,
} from "recharts";

interface DailyCount {
  date: string;
  air_quality: number;
  weather: number;
  subway: number;
}

interface QualityScore {
  report_date: string;
  source: string;
  overall_score: number;
  null_rate: number;
}

export function DailyCollectionChart({ data }: { data: DailyCount[] }) {
  return (
    <ResponsiveContainer width="100%" height={350}>
      <BarChart data={data}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="date" />
        <YAxis />
        <Tooltip />
        <Legend />
        <Bar dataKey="air_quality" name="미세먼지" fill="#8884d8" />
        <Bar dataKey="weather" name="날씨" fill="#82ca9d" />
        <Bar dataKey="subway" name="지하철" fill="#ffc658" />
      </BarChart>
    </ResponsiveContainer>
  );
}

export function QualityScoreChart({ data }: { data: QualityScore[] }) {
  const grouped: Record<string, { date: string; [key: string]: number | string }> = {};
  for (const item of data) {
    if (!grouped[item.report_date]) {
      grouped[item.report_date] = { date: item.report_date };
    }
    grouped[item.report_date][item.source] = item.overall_score;
  }
  const chartData = Object.values(grouped).sort((a, b) =>
    (a.date as string).localeCompare(b.date as string)
  );

  return (
    <ResponsiveContainer width="100%" height={350}>
      <LineChart data={chartData}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="date" />
        <YAxis domain={[0, 100]} />
        <Tooltip />
        <Legend />
        <Line type="monotone" dataKey="air_quality" name="미세먼지" stroke="#8884d8" />
        <Line type="monotone" dataKey="weather" name="날씨" stroke="#82ca9d" />
        <Line type="monotone" dataKey="subway" name="지하철" stroke="#ffc658" />
      </LineChart>
    </ResponsiveContainer>
  );
}

export function NullRateChart({ data }: { data: QualityScore[] }) {
  const grouped: Record<string, { date: string; [key: string]: number | string }> = {};
  for (const item of data) {
    if (!grouped[item.report_date]) {
      grouped[item.report_date] = { date: item.report_date };
    }
    grouped[item.report_date][item.source] = item.null_rate;
  }
  const chartData = Object.values(grouped).sort((a, b) =>
    (a.date as string).localeCompare(b.date as string)
  );

  return (
    <ResponsiveContainer width="100%" height={350}>
      <LineChart data={chartData}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="date" />
        <YAxis domain={[0, "auto"]} />
        <Tooltip />
        <Legend />
        <Line type="monotone" dataKey="air_quality" name="미세먼지" stroke="#e74c3c" />
        <Line type="monotone" dataKey="weather" name="날씨" stroke="#f39c12" />
        <Line type="monotone" dataKey="subway" name="지하철" stroke="#3498db" />
      </LineChart>
    </ResponsiveContainer>
  );
}

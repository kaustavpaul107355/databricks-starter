import { useEffect, useState } from "react";
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from "recharts";

const COLORS = ["#3B82F6", "#10B981", "#F59E0B", "#EF4444", "#8B5CF6"];
const chartTickStyle = { fill: "#6b7280", fontSize: 11, fontFamily: "Inter, system-ui, sans-serif" };
const legendStyle = { fontSize: "12px", fontFamily: "Inter, system-ui, sans-serif" };
const tooltipStyle = {
  backgroundColor: "white",
  border: "1px solid #e5e7eb",
  borderRadius: "8px",
  fontSize: "12px",
  fontFamily: "Inter, system-ui, sans-serif",
};

const formatStoreLabel = (value: string) => {
  const cleaned = value.replace(/^Discount Tire\s*/i, "").trim();
  if (cleaned.length <= 16) {
    return cleaned;
  }
  return `${cleaned.slice(0, 14)}…`;
};

interface ChartCardProps {
  title: string;
  children: React.ReactNode;
}

function ChartCard({ title, children }: ChartCardProps) {
  return (
    <div className="glass-panel rounded-xl p-6">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-semibold text-gray-900">{title}</h3>
      </div>
      {children}
    </div>
  );
}

export function ChartSection() {
  const [revenueData, setRevenueData] = useState<Array<{ month: string; revenue: number }>>([]);
  const [tireModelsData, setTireModelsData] = useState<Array<{ model: string; units: number }>>([]);
  const [inventoryData, setInventoryData] = useState<
    Array<{ store: string; healthy: number; low: number; critical: number }>
  >([]);
  const [satisfactionData, setSatisfactionData] = useState<Array<{ region: string; score: number }>>([]);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    let mounted = true;
    setIsLoading(true);
    fetch("/api/dashboard/charts")
      .then((response) => response.json())
      .then((payload) => {
        if (!mounted) {
          return;
        }
        const revenueTrend = (payload.revenueTrend || []).map((row: any) => ({
          month: row.month ? new Date(row.month).toLocaleString("en-US", { month: "short" }) : "—",
          revenue: Number(row.revenue || 0),
        }));
        const topTires = (payload.topTires || []).map((row: any) => ({
          model: row.model || "Unknown",
          units: Number(row.units || 0),
        }));
        const inventoryHealth = (payload.inventoryHealth || []).map((row: any) => ({
          store: row.store || "Unknown",
          healthy: Number(row.healthy || 0),
          low: Number(row.low || 0),
          critical: Number(row.critical || 0),
        }));
        const satisfaction = (payload.satisfactionByRegion || []).map((row: any) => ({
          region: row.region || "Unknown",
          score: Number(row.score || 0),
        }));
        setRevenueData(revenueTrend);
        setTireModelsData(topTires);
        setInventoryData(inventoryHealth);
        setSatisfactionData(satisfaction);
        setLastUpdated(new Date());
        setIsLoading(false);
      })
      .catch(() => {
        if (mounted) {
          setIsLoading(false);
        }
      });
    return () => {
      mounted = false;
    };
  }, []);

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-base font-semibold text-gray-900">Dynamic Visual Insights</h2>
        <div className="text-xs text-gray-500">
          {isLoading ? "Loading live charts..." : lastUpdated ? `Last updated ${lastUpdated.toLocaleTimeString()}` : ""}
        </div>
      </div>
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Revenue Trend */}
        <ChartCard title="📈 Revenue Trend (Monthly)">
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={revenueData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis dataKey="month" tick={chartTickStyle} tickMargin={6} />
              <YAxis tick={chartTickStyle} tickMargin={6} />
              <Tooltip contentStyle={tooltipStyle} />
              <Line
                type="monotone"
                dataKey="revenue"
                stroke="#3B82F6"
                strokeWidth={3}
                dot={{ fill: "#3B82F6", r: 5 }}
                activeDot={{ r: 7 }}
              />
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* Top-Selling Tire Models */}
        <ChartCard title="🛞 Top-Selling Tire Models">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={tireModelsData} layout="vertical">
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis type="number" tick={chartTickStyle} />
              <YAxis
                type="category"
                dataKey="model"
                tick={chartTickStyle}
                width={120}
              />
              <Tooltip contentStyle={tooltipStyle} />
              <Bar dataKey="units" fill="#10B981" radius={[0, 8, 8, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* Inventory Health by Store */}
        <ChartCard title="📦 Inventory Health by Store">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={inventoryData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis
                dataKey="store"
                tick={chartTickStyle}
                tickMargin={6}
                interval={0}
                angle={-35}
                textAnchor="end"
                height={80}
                tickFormatter={formatStoreLabel}
              />
              <YAxis tick={chartTickStyle} tickMargin={6} />
              <Tooltip contentStyle={tooltipStyle} />
              <Legend wrapperStyle={legendStyle} />
              <Bar dataKey="healthy" stackId="a" fill="#10B981" radius={[0, 0, 0, 0]} />
              <Bar dataKey="low" stackId="a" fill="#F59E0B" radius={[0, 0, 0, 0]} />
              <Bar dataKey="critical" stackId="a" fill="#EF4444" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* Customer Satisfaction by Region */}
        <ChartCard title="⭐ Customer Satisfaction by Region">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={satisfactionData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis dataKey="region" tick={chartTickStyle} />
              <YAxis domain={[0, 5]} tick={chartTickStyle} />
              <Tooltip contentStyle={tooltipStyle} />
              <Bar dataKey="score" fill="#F59E0B" radius={[8, 8, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>
    </div>
  );
}

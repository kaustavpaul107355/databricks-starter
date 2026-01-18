import { useEffect, useState } from "react";
import { LineChart, Line, BarChart, Bar, AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from "recharts";
import { DollarSign, TrendingUp, Calendar, MapPin } from "lucide-react";

const chartTickStyle = { fill: "#6b7280", fontSize: 11, fontFamily: "Inter, system-ui, sans-serif" };
const legendStyle = { fontSize: "12px", fontFamily: "Inter, system-ui, sans-serif" };
const tooltipStyle = {
  backgroundColor: "white",
  border: "1px solid #e5e7eb",
  borderRadius: "8px",
  fontSize: "12px",
  fontFamily: "Inter, system-ui, sans-serif",
};

interface StatCardProps {
  title: string;
  value: string;
  change: string;
  icon: React.ReactNode;
  positive?: boolean;
}

function StatCard({ title, value, change, icon, positive = true }: StatCardProps) {
  return (
    <div className="glass-panel rounded-xl p-6">
      <div className="flex items-start justify-between mb-3">
        <div className="p-2 accent-pill rounded-lg">
          {icon}
        </div>
        <span className={`text-sm font-semibold ${positive ? 'text-green-600' : 'text-red-600'}`}>
          {change}
        </span>
      </div>
      <p className="text-sm text-gray-600 mb-1">{title}</p>
      <p className="text-3xl font-semibold text-gray-900">{value}</p>
    </div>
  );
}

export function RevenueAnalytics() {
  const [monthlyRevenueData, setMonthlyRevenueData] = useState<
    Array<{ month: string; revenue: number; target: number; lastYear: number }>
  >([]);
  const [regionRevenueData, setRegionRevenueData] = useState<
    Array<{ region: string; Q1?: number; Q2?: number; Q3?: number; Q4?: number }>
  >([]);
  const [categoryBreakdownData, setCategoryBreakdownData] = useState<
    Array<{ category: string; amount: number; percentage: number }>
  >([]);
  const [stats, setStats] = useState<{
    currentMonthRevenue: number | null;
    ytdRevenue: number | null;
    quarterlyGrowth: number | null;
    topRegion: string | null;
    currentMonthLabel: string | null;
  }>({
    currentMonthRevenue: null,
    ytdRevenue: null,
    quarterlyGrowth: null,
    topRegion: null,
    currentMonthLabel: null,
  });
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    let mounted = true;
    setIsLoading(true);
    fetch("/api/dashboard/revenue")
      .then((response) => response.json())
      .then((payload) => {
        if (!mounted) {
          return;
        }
        const monthly = (payload.monthly || []).map((row: any) => {
          const date = row.month ? new Date(row.month) : null;
          const label = date
            ? `${date.toLocaleString("en-US", { month: "short" })} '${String(date.getFullYear()).slice(-2)}`
            : "—";
          return {
            month: label,
            revenue: Number(row.revenue || 0),
            target: Number(row.target || 0),
            lastYear: Number(row.last_year || 0),
          };
        });
        const regionMap: Record<string, any> = {};
        (payload.regional || []).forEach((row: any) => {
          const region = row.region || "Unknown";
          if (!regionMap[region]) {
            regionMap[region] = { region };
          }
          const quarter = row.quarter ? `Q${row.quarter}` : "Q1";
          regionMap[region][quarter] = Number(row.revenue || 0);
        });
        const regional = Object.values(regionMap);
        const categoryRows = (payload.category || []).map((row: any) => ({
          category: row.category || "Other",
          amount: Number(row.amount || 0),
        }));
        const total = categoryRows.reduce((sum: number, row: any) => sum + row.amount, 0) || 1;
        const category = categoryRows.map((row: any) => ({
          ...row,
          percentage: Number(((row.amount / total) * 100).toFixed(1)),
          isZero: row.amount === 0,
        }));

        setMonthlyRevenueData(monthly);
        setRegionRevenueData(regional as any);
        setCategoryBreakdownData(category);
        setStats({
          currentMonthRevenue: payload.stats?.currentMonthRevenue ?? null,
          ytdRevenue: payload.stats?.ytdRevenue ?? null,
          quarterlyGrowth: payload.stats?.quarterlyGrowth ?? null,
          topRegion: payload.stats?.topRegion ?? null,
          currentMonthLabel: payload.currentMonthLabel ?? null,
        });
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

  const formatCurrency = (value: number | null) =>
    value === null ? "—" : `$${value.toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
  const formatPercent = (value: number | null) =>
    value === null ? "—" : `${(value * 100).toFixed(1)}%`;

  return (
    <div className="space-y-8">
      <div>
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-2xl font-semibold text-gray-900">Revenue Performance</h2>
          <div className="text-xs text-gray-500">
            {isLoading
              ? "Loading live revenue..."
              : lastUpdated
                ? `Last updated ${lastUpdated.toLocaleTimeString()}`
                : ""}
          </div>
        </div>
        {stats.currentMonthLabel && (
          <p className="text-xs text-gray-500 mb-4">Current month based on data: {stats.currentMonthLabel}</p>
        )}
        
        {/* Revenue Stats Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
          <StatCard
            title="Current Month Revenue"
            value={formatCurrency(stats.currentMonthRevenue)}
            change={stats.quarterlyGrowth ? `+${(stats.quarterlyGrowth * 100).toFixed(1)}%` : "—"}
            icon={<DollarSign className="w-5 h-5 text-blue-600" />}
          />
          <StatCard
            title="Year-to-Date"
            value={formatCurrency(stats.ytdRevenue)}
            change={stats.quarterlyGrowth ? `+${(stats.quarterlyGrowth * 100).toFixed(1)}%` : "—"}
            icon={<TrendingUp className="w-5 h-5 text-green-600" />}
          />
          <StatCard
            title="Quarterly Growth"
            value={formatPercent(stats.quarterlyGrowth)}
            change={stats.quarterlyGrowth ? `+${(stats.quarterlyGrowth * 100).toFixed(1)}%` : "—"}
            icon={<Calendar className="w-5 h-5 text-purple-600" />}
          />
          <StatCard
            title="Top Region"
            value={stats.topRegion || "—"}
            change={stats.quarterlyGrowth ? `+${(stats.quarterlyGrowth * 100).toFixed(1)}%` : "—"}
            icon={<MapPin className="w-5 h-5 text-orange-600" />}
          />
        </div>

        {/* Revenue Trend Chart */}
        <div className="glass-panel rounded-xl p-6 mb-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Monthly Revenue Trend vs Target</h3>
          <ResponsiveContainer width="100%" height={350}>
            <AreaChart data={monthlyRevenueData}>
              <defs>
                <linearGradient id="colorRevenue" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#3B82F6" stopOpacity={0.3}/>
                  <stop offset="95%" stopColor="#3B82F6" stopOpacity={0}/>
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis dataKey="month" tick={chartTickStyle} />
              <YAxis tick={chartTickStyle} />
              <Tooltip contentStyle={tooltipStyle} />
              <Legend wrapperStyle={legendStyle} />
              <Area
                type="monotone"
                dataKey="revenue"
                stroke="#3B82F6"
                strokeWidth={3}
                fillOpacity={1}
                fill="url(#colorRevenue)"
                name="Actual Revenue ($)"
              />
              <Line
                type="monotone"
                dataKey="target"
                stroke="#10B981"
                strokeWidth={2}
                strokeDasharray="5 5"
                dot={false}
                name="Target ($)"
              />
              <Line
                type="monotone"
                dataKey="lastYear"
                stroke="#9CA3AF"
                strokeWidth={2}
                dot={false}
                name="Last Year ($)"
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>

        {/* Regional Performance */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="glass-panel rounded-xl p-6">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Revenue by Region (Quarterly)</h3>
            <ResponsiveContainer width="100%" height={320}>
              <BarChart data={regionRevenueData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                <XAxis dataKey="region" tick={chartTickStyle} />
                <YAxis tick={chartTickStyle} />
                <Tooltip contentStyle={tooltipStyle} />
                <Legend wrapperStyle={legendStyle} />
                <Bar dataKey="Q4" fill="#3B82F6" radius={[4, 4, 0, 0]} />
                <Bar dataKey="Q3" fill="#8B5CF6" radius={[4, 4, 0, 0]} />
                <Bar dataKey="Q2" fill="#10B981" radius={[4, 4, 0, 0]} />
                <Bar dataKey="Q1" fill="#F59E0B" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>

          <div className="glass-panel rounded-xl p-6">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Revenue by Category</h3>
            <div className="space-y-4">
              {categoryBreakdownData.map((item: any, index) => (
                <div key={index}>
                  <div className="flex justify-between text-sm mb-2">
                    <span className="text-gray-700 font-medium">{item.category}</span>
                    <span className="text-gray-900 font-semibold">
                      {formatCurrency(item.amount)} ({item.percentage}%)
                    </span>
                  </div>
                  <div className="w-full bg-gray-100 rounded-full h-2.5">
                    <div
                      className="bg-blue-600 h-2.5 rounded-full transition-all"
                      style={{ width: `${item.percentage}%` }}
                    ></div>
                  </div>
                  {item.isZero && (
                    <p className="text-xs text-gray-500 mt-2">No recorded revenue for this category.</p>
                  )}
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

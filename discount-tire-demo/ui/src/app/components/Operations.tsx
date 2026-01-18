import { useEffect, useState } from "react";
import { BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Cell } from "recharts";
import { Package, AlertTriangle, TrendingUp, Store } from "lucide-react";

const COLORS = ["#EF4444", "#F59E0B", "#10B981"];
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

interface MetricCardProps {
  title: string;
  value: string;
  subtitle: string;
  icon: React.ReactNode;
  alert?: boolean;
}

function MetricCard({ title, value, subtitle, icon, alert }: MetricCardProps) {
  return (
    <div className={`glass-panel rounded-xl p-6 ${alert ? 'border-orange-300 bg-orange-50/40' : ''}`}>
      <div className="flex items-start justify-between mb-3">
        <div className={`p-2 rounded-lg ${alert ? 'bg-orange-100' : 'bg-white/70'}`}>
          {icon}
        </div>
      </div>
      <p className="text-sm text-gray-600 mb-1">{title}</p>
      <p className="text-3xl font-semibold text-gray-900 mb-1">{value}</p>
      <p className="text-xs text-gray-500">{subtitle}</p>
    </div>
  );
}

export function Operations() {
  const [inventoryByStoreData, setInventoryByStoreData] = useState<
    Array<{ store: string; available: number; reserved: number; lowStock: number }>
  >([]);
  const [stockTurnoverData, setStockTurnoverData] = useState<Array<{ month: string; turnover: number }>>([]);
  const [criticalInventoryItems, setCriticalInventoryItems] = useState<
    Array<{ item: string; currentStock: number; reorderPoint: number; status: string }>
  >([]);
  const [storePerformanceData, setStorePerformanceData] = useState<
    Array<{ store: string; efficiency: number; satisfaction: number; throughput: number }>
  >([]);
  const [metrics, setMetrics] = useState<{
    total_units?: string;
    critical_items?: string;
    active_stores?: string;
  }>({});
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    let mounted = true;
    setIsLoading(true);
    fetch("/api/dashboard/operations")
      .then((response) => response.json())
      .then((payload) => {
        if (!mounted) {
          return;
        }
        const inventory = (payload.inventoryByStore || []).map((row: any) => ({
          store: row.store || "Unknown",
          available: Number(row.available || 0),
          reserved: Number(row.reserved || 0),
          lowStock: Number(row.low_stock || 0),
        }));
        const turnover = (payload.stockTurnover || []).map((row: any) => ({
          month: row.month ? new Date(row.month).toLocaleString("en-US", { month: "short" }) : "—",
          turnover: Number(row.turnover || 0),
        }));
        const critical = (payload.criticalItems || []).map((row: any) => ({
          item: row.item || "Unknown",
          currentStock: Number(row.current_stock || 0),
          reorderPoint: Number(row.reorder_point || 0),
          status: row.status || "Low",
        }));
        const performance = (payload.storePerformance || []).map((row: any) => ({
          store: row.store || "Unknown",
          efficiency: Number(row.efficiency || 0),
          satisfaction: Number(row.satisfaction || 0),
          throughput: Number(row.throughput || 0),
        }));
        setInventoryByStoreData(inventory);
        setStockTurnoverData(turnover);
        setCriticalInventoryItems(critical);
        setStorePerformanceData(performance);
        setMetrics(payload.metrics || {});
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

  const formatNumber = (value?: string) =>
    value ? Number(value).toLocaleString(undefined, { maximumFractionDigits: 0 }) : "—";

  return (
    <div className="space-y-8">
      <div>
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-2xl font-semibold text-gray-900">Operations Dashboard</h2>
          <div className="text-xs text-gray-500">
            {isLoading ? "Loading live operations..." : lastUpdated ? `Last updated ${lastUpdated.toLocaleTimeString()}` : ""}
          </div>
        </div>
        
        {/* Operations Metrics */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
          <MetricCard
            title="Total Inventory Units"
            value={formatNumber(metrics.total_units)}
            subtitle="Across all locations"
            icon={<Package className="w-5 h-5 text-blue-600" />}
          />
          <MetricCard
            title="Critical Stock Items"
            value={formatNumber(metrics.critical_items)}
            subtitle="Requires immediate attention"
            icon={<AlertTriangle className="w-5 h-5 text-orange-600" />}
            alert={true}
          />
          <MetricCard
            title="Avg. Stock Turnover"
            value={stockTurnoverData.length ? `${stockTurnoverData[stockTurnoverData.length - 1].turnover.toFixed(1)}x` : "—"}
            subtitle="Monthly rate (↑ 12%)"
            icon={<TrendingUp className="w-5 h-5 text-green-600" />}
          />
          <MetricCard
            title="Active Stores"
            value={formatNumber(metrics.active_stores)}
            subtitle="All regions operational"
            icon={<Store className="w-5 h-5 text-purple-600" />}
          />
        </div>

        {/* Inventory by Store */}
        <div className="glass-panel rounded-xl p-6 mb-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Inventory Status by Store</h3>
          <ResponsiveContainer width="100%" height={350}>
            <BarChart data={inventoryByStoreData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis
                dataKey="store"
                tick={chartTickStyle}
                interval={0}
                angle={-35}
                textAnchor="end"
                height={80}
                tickFormatter={formatStoreLabel}
              />
              <YAxis tick={chartTickStyle} />
              <Tooltip contentStyle={tooltipStyle} />
              <Legend wrapperStyle={legendStyle} />
              <Bar dataKey="available" stackId="a" fill="#10B981" name="Available" radius={[0, 0, 0, 0]} />
              <Bar dataKey="reserved" stackId="a" fill="#3B82F6" name="Reserved" radius={[0, 0, 0, 0]} />
              <Bar dataKey="lowStock" stackId="a" fill="#F59E0B" name="Low Stock" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Stock Turnover & Critical Items */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
          <div className="glass-panel rounded-xl p-6">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Stock Turnover Trend</h3>
            <ResponsiveContainer width="100%" height={280}>
              <LineChart data={stockTurnoverData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                <XAxis dataKey="month" tick={chartTickStyle} />
                <YAxis tick={chartTickStyle} />
                <Tooltip contentStyle={tooltipStyle} />
                <Line
                  type="monotone"
                  dataKey="turnover"
                  stroke="#10B981"
                  strokeWidth={3}
                  dot={{ fill: "#10B981", r: 5 }}
                  name="Turnover Rate"
                />
              </LineChart>
            </ResponsiveContainer>
          </div>

          <div className="glass-panel rounded-xl p-6">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Critical Inventory Items</h3>
            <div className="space-y-3 max-h-[280px] overflow-y-auto">
              {criticalInventoryItems.map((item, index) => (
                <div
                  key={index}
                  className={`p-3 rounded-lg border ${
                    item.status === "Critical"
                      ? "bg-red-50 border-red-200"
                      : "bg-orange-50 border-orange-200"
                  }`}
                >
                  <div className="flex justify-between items-start mb-2">
                    <span className="text-sm font-medium text-gray-900">{item.item}</span>
                    <span
                      className={`text-xs font-semibold px-2 py-1 rounded ${
                        item.status === "Critical"
                          ? "bg-red-100 text-red-700"
                          : "bg-orange-100 text-orange-700"
                      }`}
                    >
                      {item.status}
                    </span>
                  </div>
                  <div className="flex justify-between text-xs text-gray-600">
                    <span>Current: {item.currentStock}</span>
                    <span>Reorder Point: {item.reorderPoint}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Store Performance Table */}
        <div className="glass-panel rounded-xl p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Store Performance Metrics</h3>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b border-gray-200">
                  <th className="text-left py-3 px-4 text-sm font-semibold text-gray-700">Store</th>
                  <th className="text-left py-3 px-4 text-sm font-semibold text-gray-700">Operational Efficiency</th>
                  <th className="text-left py-3 px-4 text-sm font-semibold text-gray-700">Customer Satisfaction</th>
                  <th className="text-left py-3 px-4 text-sm font-semibold text-gray-700">Daily Throughput</th>
                </tr>
              </thead>
              <tbody>
                {storePerformanceData.map((store, index) => (
                  <tr key={index} className="border-b border-gray-100 hover:bg-gray-50">
                    <td className="py-3 px-4 font-medium text-gray-900">{store.store}</td>
                    <td className="py-3 px-4">
                      <div className="flex items-center gap-2">
                        <div className="flex-1 bg-gray-100 rounded-full h-2">
                          <div
                            className={`h-2 rounded-full ${
                              store.efficiency >= 90 ? 'bg-green-500' : 'bg-orange-500'
                            }`}
                            style={{ width: `${store.efficiency}%` }}
                          ></div>
                        </div>
                        <span className="text-sm font-medium text-gray-700">{store.efficiency}%</span>
                      </div>
                    </td>
                    <td className="py-3 px-4">
                      <span className="text-sm font-medium text-gray-900">{store.satisfaction}/5.0</span>
                    </td>
                    <td className="py-3 px-4">
                      <span className="text-sm font-medium text-gray-900">{store.throughput} units/day</span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}

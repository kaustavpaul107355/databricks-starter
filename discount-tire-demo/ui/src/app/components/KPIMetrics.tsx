import { useEffect, useState } from "react";
import { TrendingUp, TrendingDown, Star, Package, AlertTriangle } from "lucide-react";
import { formatCurrency, formatNumber } from "@/utils/formatting";

interface KPICardProps {
  title: string;
  value: string;
  trend?: number;
  icon: React.ReactNode;
  subtitle?: string;
  alert?: boolean;
}

function KPICard({ title, value, trend, icon, subtitle, alert }: KPICardProps) {
  return (
    <div className={`glass-panel rounded-xl p-6 ${alert ? 'border-amber-200 bg-amber-50/40' : ''}`}>
      <div className="flex items-start justify-between mb-4">
        <div className={`p-2 rounded-lg ${
          alert ? 'bg-amber-100' : 'bg-white/70'
        }`}>
          {icon}
        </div>
        {trend !== undefined && (
          <div className={`flex items-center gap-1 text-sm font-semibold ${
            trend > 0 ? 'text-green-600' : 'text-red-600'
          }`}>
            {trend > 0 ? (
              <TrendingUp className="w-4 h-4" />
            ) : (
              <TrendingDown className="w-4 h-4" />
            )}
            <span>{Math.abs(trend)}%</span>
          </div>
        )}
      </div>
      
      <div className="space-y-1">
        <p className="text-sm text-gray-600">{title}</p>
        <p className="text-3xl font-semibold text-gray-900">{value}</p>
        {subtitle && (
          <p className="text-xs text-gray-500">{subtitle}</p>
        )}
      </div>
    </div>
  );
}

export function KPIMetrics() {
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [metrics, setMetrics] = useState<{
    totalRevenue: number | null;
    revenueGrowth: number | null;
    avgSatisfaction: number | null;
    tireUnits: number | null;
    inventoryRisk: number | null;
    currentMonthLabel: string | null;
  }>({
    totalRevenue: null,
    revenueGrowth: null,
    avgSatisfaction: null,
    tireUnits: null,
    inventoryRisk: null,
    currentMonthLabel: null,
  });

  useEffect(() => {
    let mounted = true;
    setIsLoading(true);
    fetch("/api/dashboard/kpis")
      .then((response) => response.json())
      .then((payload) => {
        if (mounted) {
          setMetrics({
            totalRevenue: payload.totalRevenue ?? null,
            revenueGrowth: payload.revenueGrowth ?? null,
            avgSatisfaction: payload.avgSatisfaction ?? null,
            tireUnits: payload.tireUnits ?? null,
            inventoryRisk: payload.inventoryRisk ?? null,
            currentMonthLabel: payload.currentMonthLabel ?? null,
          });
          setLastUpdated(new Date());
          setIsLoading(false);
        }
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

  const formatPercent = (value: number | null) =>
    value === null ? "—" : `${(value * 100).toFixed(1)}%`;
  const formatScore = (value: number | null) =>
    value === null ? "—" : value.toFixed(1);

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-semibold text-gray-900">Key Metrics Snapshot</h2>
        <div className="text-xs text-gray-500">
          {isLoading ? "Loading live metrics..." : lastUpdated ? `Last updated ${lastUpdated.toLocaleTimeString()}` : ""}
        </div>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
        <KPICard
          title="Total Revenue"
          value={formatCurrency(metrics.totalRevenue ?? 0)}
          trend={metrics.revenueGrowth ? Math.round(metrics.revenueGrowth * 100) : undefined}
          subtitle={metrics.currentMonthLabel ? `This Month (${metrics.currentMonthLabel})` : "This Month"}
          icon={<TrendingUp className="w-5 h-5 text-gray-600" />}
        />
        
        <KPICard
          title="Revenue Growth"
          value={formatPercent(metrics.revenueGrowth)}
          subtitle="vs Last Month"
          icon={<TrendingUp className="w-5 h-5 text-green-600" />}
        />
        
        <KPICard
          title="Customer Satisfaction"
          value={formatScore(metrics.avgSatisfaction)}
          subtitle="Out of 5.0"
          icon={<Star className="w-5 h-5 text-amber-500" />}
        />
        
        <KPICard
          title="Tire Units Sold"
          value={formatNumber(metrics.tireUnits ?? 0)}
          subtitle={metrics.currentMonthLabel ? `This Month (${metrics.currentMonthLabel})` : "This Month"}
          icon={<Package className="w-5 h-5 text-blue-600" />}
        />
        
        <KPICard
          title="Inventory Risk"
          value={formatNumber(metrics.inventoryRisk ?? 0)}
          subtitle="Low Stock Items"
          alert={true}
          icon={<AlertTriangle className="w-5 h-5 text-amber-600" />}
        />
      </div>
    </div>
  );
}

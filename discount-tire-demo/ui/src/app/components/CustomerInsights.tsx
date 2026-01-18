import { useEffect, useState } from "react";
import { BarChart, Bar, LineChart, Line, PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from "recharts";
import { Star, Users, ThumbsUp, MessageSquare } from "lucide-react";

const COLORS = ["#3B82F6", "#10B981", "#F59E0B", "#8B5CF6", "#EC4899"];
const chartTickStyle = { fill: "#6b7280", fontSize: 11, fontFamily: "Inter, system-ui, sans-serif" };
const legendStyle = { fontSize: "12px", fontFamily: "Inter, system-ui, sans-serif" };
const tooltipStyle = {
  backgroundColor: "white",
  border: "1px solid #e5e7eb",
  borderRadius: "8px",
  fontSize: "12px",
  fontFamily: "Inter, system-ui, sans-serif",
};

interface InsightCardProps {
  title: string;
  value: string;
  subtitle: string;
  icon: React.ReactNode;
  trend?: string;
}

function InsightCard({ title, value, subtitle, icon, trend }: InsightCardProps) {
  return (
    <div className="glass-panel rounded-xl p-6">
      <div className="flex items-start justify-between mb-3">
        <div className="p-2 accent-pill rounded-lg">
          {icon}
        </div>
        {trend && (
          <span className="text-sm font-semibold text-green-600">{trend}</span>
        )}
      </div>
      <p className="text-sm text-gray-600 mb-1">{title}</p>
      <p className="text-3xl font-semibold text-gray-900 mb-1">{value}</p>
      <p className="text-xs text-gray-500">{subtitle}</p>
    </div>
  );
}

export function CustomerInsights() {
  const [satisfactionTrendData, setSatisfactionTrendData] = useState<
    Array<{ month: string; score: number; responses: number }>
  >([]);
  const [regionalSatisfactionData, setRegionalSatisfactionData] = useState<
    Array<{ region: string; score: number; surveys: number }>
  >([]);
  const [serviceBreakdownData, setServiceBreakdownData] = useState<
    Array<{ name: string; value: number; rating?: number }>
  >([]);
  const [npsData, setNpsData] = useState<Array<{ category: string; count: number; percentage: number }>>([]);
  const [feedbackTopicsData, setFeedbackTopicsData] = useState<
    Array<{ topic: string; mentions: number; sentiment: string }>
  >([]);
  const [metrics, setMetrics] = useState<{
    overallSatisfaction: number | null;
    totalSurveys: number | null;
    repeatRate: number | null;
    activeFeedback: number | null;
  }>({
    overallSatisfaction: null,
    totalSurveys: null,
    repeatRate: null,
    activeFeedback: null,
  });
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    let mounted = true;
    setIsLoading(true);
    fetch("/api/dashboard/customers")
      .then((response) => response.json())
      .then((payload) => {
        if (!mounted) {
          return;
        }
        setMetrics({
          overallSatisfaction: payload.metrics?.overallSatisfaction ?? null,
          totalSurveys: payload.metrics?.totalSurveys ?? null,
          repeatRate: payload.metrics?.repeatRate ?? null,
          activeFeedback: payload.metrics?.activeFeedback ?? null,
        });
        const trend = (payload.satisfactionTrend || []).map((row: any) => ({
          month: row.month ? new Date(row.month).toLocaleString("en-US", { month: "short" }) : "—",
          score: Number(row.score || 0),
          responses: Number(row.responses || 0),
        }));
        const regional = (payload.regionalSatisfaction || []).map((row: any) => ({
          region: row.region || "Unknown",
          score: Number(row.score || 0),
          surveys: Number(row.surveys || 0),
        }));
        const servicesRaw = (payload.serviceBreakdown || []).map((row: any) => ({
          name: row.name || "Other",
          value: Number(row.value || 0),
        }));
        const totalServices = servicesRaw.reduce((sum, row) => sum + row.value, 0) || 1;
        const services = servicesRaw.map((row) => ({
          ...row,
          value: Math.round((row.value / totalServices) * 100),
        }));
        const npsRaw = (payload.npsBreakdown || []).map((row: any) => ({
          category: row.category || "Passive",
          count: Number(row.count || 0),
        }));
        const npsTotal = npsRaw.reduce((sum, row) => sum + row.count, 0) || 1;
        const nps = npsRaw.map((row) => ({
          category: row.category === "Promoter" ? "Promoters" : row.category === "Detractor" ? "Detractors" : "Passives",
          count: row.count,
          percentage: Math.round((row.count / npsTotal) * 100),
        }));
        const feedbackMap: Record<string, { topic: string; mentions: number; sentiment: string }> = {};
        (payload.feedbackTopics || []).forEach((row: any) => {
          const topic = row.topic || "Other";
          const mentions = Number(row.mentions || 0);
          if (!feedbackMap[topic] || mentions > feedbackMap[topic].mentions) {
            feedbackMap[topic] = { topic, mentions, sentiment: row.sentiment || "neutral" };
          }
        });
        setSatisfactionTrendData(trend);
        setRegionalSatisfactionData(regional);
        setServiceBreakdownData(services);
        setNpsData(nps);
        setFeedbackTopicsData(Object.values(feedbackMap));
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

  const npsScore = npsData.reduce((score, row) => {
    if (row.category === "Promoters") {
      return score + row.percentage;
    }
    if (row.category === "Detractors") {
      return score - row.percentage;
    }
    return score;
  }, 0);

  const formatPercent = (value: number | null) =>
    value === null ? "—" : `${Math.round(value * 100)}%`;
  const formatNumber = (value: number | null) =>
    value === null ? "—" : value.toLocaleString(undefined, { maximumFractionDigits: 0 });

  return (
    <div className="space-y-8">
      <div>
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-2xl font-semibold text-gray-900">Customer Insights & Satisfaction</h2>
          <div className="text-xs text-gray-500">
            {isLoading ? "Loading live customer insights..." : lastUpdated ? `Last updated ${lastUpdated.toLocaleTimeString()}` : ""}
          </div>
        </div>
        
        {/* Customer Metrics */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
          <InsightCard
            title="Overall Satisfaction"
            value={metrics.overallSatisfaction ? `${metrics.overallSatisfaction.toFixed(1)}/5.0` : "—"}
            subtitle={
              metrics.totalSurveys ? `Based on ${formatNumber(metrics.totalSurveys)} surveys` : "Based on survey data"
            }
            icon={<Star className="w-5 h-5 text-yellow-500" />}
            trend="+3.5%"
          />
          <InsightCard
            title="Net Promoter Score"
            value={npsData.length ? `+${Math.round(npsScore)}` : "—"}
            subtitle="Based on recent surveys"
            icon={<ThumbsUp className="w-5 h-5 text-green-600" />}
            trend="+6 pts"
          />
          <InsightCard
            title="Repeat Customer Rate"
            value={formatPercent(metrics.repeatRate)}
            subtitle="Within 12 months"
            icon={<Users className="w-5 h-5 text-blue-600" />}
            trend="+5.2%"
          />
          <InsightCard
            title="Active Feedback"
            value={formatNumber(metrics.activeFeedback)}
            subtitle="This month"
            icon={<MessageSquare className="w-5 h-5 text-purple-600" />}
            trend="+11%"
          />
        </div>

        {/* Satisfaction Trend */}
        <div className="glass-panel rounded-xl p-6 mb-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Customer Satisfaction Trend</h3>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={satisfactionTrendData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis dataKey="month" tick={chartTickStyle} />
              <YAxis yAxisId="left" domain={[0, 5]} tick={chartTickStyle} />
              <YAxis yAxisId="right" orientation="right" tick={chartTickStyle} />
              <Tooltip contentStyle={tooltipStyle} />
              <Legend wrapperStyle={legendStyle} />
              <Line
                yAxisId="left"
                type="monotone"
                dataKey="score"
                stroke="#F59E0B"
                strokeWidth={3}
                dot={{ fill: "#F59E0B", r: 5 }}
                name="Satisfaction Score"
              />
              <Line
                yAxisId="right"
                type="monotone"
                dataKey="responses"
                stroke="#3B82F6"
                strokeWidth={2}
                dot={{ fill: "#3B82F6", r: 4 }}
                name="Survey Responses"
              />
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Regional Satisfaction & Service Breakdown */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
          <div className="glass-panel rounded-xl p-6">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Satisfaction by Region</h3>
            <ResponsiveContainer width="100%" height={300}>
            <BarChart data={regionalSatisfactionData} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                <XAxis type="number" domain={[0, 5]} tick={chartTickStyle} />
                <YAxis type="category" dataKey="region" tick={chartTickStyle} width={100} />
                <Tooltip contentStyle={tooltipStyle} />
                <Bar dataKey="score" fill="#10B981" radius={[0, 8, 8, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>

          <div className="glass-panel rounded-xl p-6">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Service Usage Distribution</h3>
            <div className="flex items-center justify-center">
              <ResponsiveContainer width="100%" height={300}>
                <PieChart>
                  <Pie
                    data={serviceBreakdownData}
                    cx="50%"
                    cy="50%"
                    labelLine={false}
                    label={({ name, value }) => `${name}: ${value}%`}
                    outerRadius={100}
                    fill="#8884d8"
                    dataKey="value"
                  >
                    {serviceBreakdownData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                    ))}
                  </Pie>
                  <Tooltip contentStyle={tooltipStyle} />
                </PieChart>
              </ResponsiveContainer>
            </div>
          </div>
        </div>

        {/* NPS Breakdown & Feedback Topics */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="glass-panel rounded-xl p-6">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Net Promoter Score Breakdown</h3>
            <div className="space-y-4">
                {npsData.map((item, index) => (
                <div key={index}>
                  <div className="flex justify-between text-sm mb-2">
                    <span className="font-medium text-gray-700">{item.category}</span>
                    <span className="text-gray-900 font-semibold">
                      {item.count.toLocaleString()} ({item.percentage}%)
                    </span>
                  </div>
                  <div className="w-full bg-gray-100 rounded-full h-3">
                    <div
                      className={`h-3 rounded-full transition-all ${
                        item.category === "Promoters"
                          ? "bg-green-500"
                          : item.category === "Passives"
                          ? "bg-yellow-500"
                          : "bg-red-500"
                      }`}
                      style={{ width: `${item.percentage}%` }}
                    ></div>
                  </div>
                </div>
              ))}
            </div>
            <div className="mt-6 p-4 bg-green-50 rounded-lg border border-green-200">
              <p className="text-sm text-gray-600 mb-1">Overall NPS</p>
              <p className="text-3xl font-bold text-green-700">+{npsScore}</p>
              <p className="text-xs text-gray-500 mt-1">Excellent score (Target: +40)</p>
            </div>
          </div>

          <div className="glass-panel rounded-xl p-6">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Top Feedback Topics</h3>
            <div className="space-y-3">
              {feedbackTopicsData.map((topic, index) => (
                <div
                  key={index}
                  className="p-3 bg-gray-50 rounded-lg border border-gray-200 hover:shadow-sm transition-shadow"
                >
                  <div className="flex justify-between items-center mb-2">
                    <span className="font-medium text-gray-900">{topic.topic}</span>
                    <span
                      className={`text-xs font-semibold px-2 py-1 rounded ${
                        topic.sentiment === "positive"
                          ? "bg-green-100 text-green-700"
                          : "bg-gray-100 text-gray-700"
                      }`}
                    >
                      {topic.sentiment}
                    </span>
                  </div>
                  <div className="flex items-center gap-2">
                    <div className="flex-1 bg-gray-200 rounded-full h-2">
                      <div
                        className="bg-blue-600 h-2 rounded-full"
                        style={{ width: `${(topic.mentions / 4520) * 100}%` }}
                      ></div>
                    </div>
                    <span className="text-xs text-gray-600">{topic.mentions.toLocaleString()}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

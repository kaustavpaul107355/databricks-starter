import { Home, TrendingUp, Package, Users, Map } from "lucide-react";

interface Tab {
  id: string;
  label: string;
  icon: React.ReactNode;
}

interface TabNavigationProps {
  activeTab: string;
  onTabChange: (tabId: string) => void;
}

const tabs: Tab[] = [
  { id: "home", label: "Executive Summary", icon: <Home className="w-4 h-4" /> },
  { id: "revenue", label: "Revenue Analytics", icon: <TrendingUp className="w-4 h-4" /> },
  { id: "operations", label: "Operations", icon: <Package className="w-4 h-4" /> },
  { id: "customers", label: "Customer Insights", icon: <Users className="w-4 h-4" /> },
  { id: "map", label: "Store Map", icon: <Map className="w-4 h-4" /> },
];

export function TabNavigation({ activeTab, onTabChange }: TabNavigationProps) {
  return (
    <div className="bg-white/80 backdrop-blur-sm border-b border-gray-200 shadow-sm">
      <div className="max-w-7xl mx-auto px-6">
        <div className="flex gap-1">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => onTabChange(tab.id)}
              className={`flex items-center gap-2 px-6 py-4 border-b-2 transition-all ${
                activeTab === tab.id
                  ? "border-blue-600 text-blue-700 bg-blue-50/50"
                  : "border-transparent text-gray-600 hover:text-gray-900 hover:bg-gray-50"
              }`}
            >
              {tab.icon}
              <span className="font-medium">{tab.label}</span>
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}

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

const tabColors = {
  home: { bg: "from-blue-400/90 to-blue-500/90", text: "text-white", shadow: "shadow-blue-300/50" },
  revenue: { bg: "from-green-400/90 to-green-500/90", text: "text-white", shadow: "shadow-green-300/50" },
  operations: { bg: "from-purple-400/90 to-purple-500/90", text: "text-white", shadow: "shadow-purple-300/50" },
  customers: { bg: "from-pink-400/90 to-pink-500/90", text: "text-white", shadow: "shadow-pink-300/50" },
  map: { bg: "from-teal-400/90 to-teal-500/90", text: "text-white", shadow: "shadow-teal-300/50" },
};

export function TabNavigation({ activeTab, onTabChange }: TabNavigationProps) {
  return (
    <div className="bg-gradient-to-b from-gray-50 to-white/80 backdrop-blur-sm border-b border-gray-200 shadow-sm">
      <div className="max-w-7xl mx-auto px-6 py-6">
        <div className="flex flex-wrap gap-4 justify-center">
          {tabs.map((tab) => {
            const colors = tabColors[tab.id as keyof typeof tabColors];
            const isActive = activeTab === tab.id;
            
            return (
              <button
                key={tab.id}
                onClick={() => onTabChange(tab.id)}
                className={`
                  group relative flex items-center gap-3 px-6 py-3.5 rounded-2xl
                  transition-all duration-300 transform
                  ${isActive 
                    ? `bg-gradient-to-br ${colors.bg} ${colors.text} shadow-lg ${colors.shadow} scale-105` 
                    : 'bg-white/70 backdrop-blur-sm text-gray-700 shadow-md hover:shadow-lg hover:scale-105 hover:bg-white border border-gray-200'
                  }
                `}
              >
                <div className={`transition-transform duration-300 ${isActive ? 'rotate-0' : 'group-hover:rotate-12'}`}>
                  {tab.icon}
                </div>
                <span className="font-semibold text-sm whitespace-nowrap">{tab.label}</span>
                {isActive && (
                  <div className="absolute inset-0 rounded-2xl bg-white/20 animate-pulse"></div>
                )}
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}

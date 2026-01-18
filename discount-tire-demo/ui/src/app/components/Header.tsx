import { User, Sparkles } from "lucide-react";
import dtLogo from "@/assets/DT_logo.svg";

export function Header() {
  return (
    <header className="bg-white border-b border-gray-200 shadow-sm">
      <div className="max-w-7xl mx-auto px-6 py-5">
        <div className="flex items-center justify-between">
          {/* Left: Discount Tire Logo and Title */}
          <div className="flex items-center gap-6">
            <div className="flex items-center gap-4">
              <div className="bg-gray-900 rounded-lg px-3 py-2 shadow-sm">
                <img src={dtLogo} alt="Discount Tire logo" className="h-10 w-auto" />
              </div>
              <div>
                <div className="text-xl font-bold text-gray-900 tracking-tight">
                  DISCOUNT TIRE
                </div>
                <div className="text-xs text-gray-500 font-medium">America's Largest Independent Tire Retailer</div>
              </div>
            </div>
            
            <div className="hidden lg:block h-10 w-px bg-gray-300"></div>
            
            <div className="hidden lg:block">
              <h1 className="text-lg font-semibold text-gray-900">Executive Business Brief</h1>
              <p className="text-xs text-gray-500">AI-Powered Analytics Dashboard</p>
            </div>
          </div>

          {/* Right: Status and User */}
          <div className="flex items-center gap-4">
            {/* AI Badge */}
            <div className="flex items-center gap-2 px-4 py-2 bg-gradient-to-r from-blue-50 to-indigo-50 border border-blue-200 rounded-full">
              <Sparkles className="w-4 h-4 text-blue-600" />
              <span className="text-xs font-semibold text-blue-700">Built on Databricks Data & AI Platform</span>
            </div>
            
            {/* Live Status */}
            <div className="hidden xl:flex items-center gap-2 px-3 py-2 bg-green-50 border border-green-200 rounded-full">
              <div className="w-2 h-2 rounded-full bg-green-500 animate-pulse"></div>
              <span className="text-xs font-medium text-green-700">Live Data</span>
            </div>
            
            {/* User Avatar */}
            <div className="flex items-center justify-center w-10 h-10 rounded-full bg-gradient-to-br from-gray-200 to-gray-300 border-2 border-white shadow-sm">
              <User className="w-5 h-5 text-gray-700" />
            </div>
          </div>
        </div>
      </div>
    </header>
  );
}
import { Lock, GitBranch, Building2, Database, Zap, Shield, Sparkles } from "lucide-react";
import dbxLogo from "@/assets/DBX_logo.svg";

export function GovernanceFooter() {
  return (
    <footer className="bg-gradient-to-br from-gray-900 via-slate-900 to-gray-900 text-gray-300 mt-16 border-t border-gray-800">
      <div className="max-w-7xl mx-auto px-6 py-12">
        {/* Databricks Branding Section */}
        <div className="text-center mb-10">
          <div className="inline-flex items-center gap-3 mb-4">
            <div className="flex items-center gap-3">
              <div className="bg-white rounded-md px-3 py-2 shadow-sm">
                <img src={dbxLogo} alt="Databricks logo" className="h-6 w-auto" />
              </div>
              <div className="text-left">
                <div className="text-xl font-bold text-white tracking-tight">Databricks</div>
                <div className="text-xs text-gray-400">Data Intelligence Platform</div>
              </div>
            </div>
          </div>
          <p className="text-sm text-gray-400 max-w-2xl mx-auto">
            Unifying data, analytics, and AI on one lakehouse platform. Built on Apache Spark™ and Delta Lake.
          </p>
        </div>

        {/* Platform Features Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-10">
          <div className="flex items-start gap-3 p-4 bg-white/5 rounded-lg border border-gray-800 hover:border-gray-700 transition-colors">
            <div className="p-2 bg-blue-500/10 rounded-lg">
              <Lock className="w-5 h-5 text-blue-400" />
            </div>
            <div>
              <h4 className="text-sm font-semibold text-white mb-1">Unity Catalog</h4>
              <p className="text-xs text-gray-400">Unified governance for all data and AI assets</p>
            </div>
          </div>

          <div className="flex items-start gap-3 p-4 bg-white/5 rounded-lg border border-gray-800 hover:border-gray-700 transition-colors">
            <div className="p-2 bg-green-500/10 rounded-lg">
              <Database className="w-5 h-5 text-green-400" />
            </div>
            <div>
              <h4 className="text-sm font-semibold text-white mb-1">Delta Lake</h4>
              <p className="text-xs text-gray-400">Reliable, performant data lakehouse storage</p>
            </div>
          </div>

          <div className="flex items-start gap-3 p-4 bg-white/5 rounded-lg border border-gray-800 hover:border-gray-700 transition-colors">
            <div className="p-2 bg-purple-500/10 rounded-lg">
              <Sparkles className="w-5 h-5 text-purple-400" />
            </div>
            <div>
              <h4 className="text-sm font-semibold text-white mb-1">AI/ML Workflows</h4>
              <p className="text-xs text-gray-400">End-to-end machine learning lifecycle</p>
            </div>
          </div>

          <div className="flex items-start gap-3 p-4 bg-white/5 rounded-lg border border-gray-800 hover:border-gray-700 transition-colors">
            <div className="p-2 bg-orange-500/10 rounded-lg">
              <Zap className="w-5 h-5 text-orange-400" />
            </div>
            <div>
              <h4 className="text-sm font-semibold text-white mb-1">Real-Time Analytics</h4>
              <p className="text-xs text-gray-400">Live insights from streaming data</p>
            </div>
          </div>
        </div>

        {/* Governance Features */}
        <div className="flex flex-col md:flex-row items-center justify-center gap-6 md:gap-8 text-sm mb-8 pb-8 border-b border-gray-800">
          <div className="flex items-center gap-2">
            <Shield className="w-4 h-4 text-green-400" />
            <span className="text-gray-300">Enterprise-grade security & compliance</span>
          </div>
          
          <div className="flex items-center gap-2">
            <GitBranch className="w-4 h-4 text-blue-400" />
            <span className="text-gray-300">Full data lineage & auditability</span>
          </div>
          
          <div className="flex items-center gap-2">
            <Building2 className="w-4 h-4 text-purple-400" />
            <span className="text-gray-300">Multi-cloud & hybrid deployment</span>
          </div>
        </div>
        
        {/* Bottom Info */}
        <div className="text-center space-y-2">
          <p className="text-xs text-gray-400">
            <span className="font-semibold text-gray-300">Built for Discount Tire Executive Leadership</span> • Powered by Databricks Lakehouse Platform
          </p>
          <p className="text-xs text-gray-500">
            © 2026 Databricks Inc. • Apache®, Apache Spark™, and Spark™ are trademarks of the Apache Software Foundation
          </p>
        </div>
      </div>
    </footer>
  );
}
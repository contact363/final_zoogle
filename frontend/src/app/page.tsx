"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { Search, Settings, Cog, Zap } from "lucide-react";

const MACHINE_CATEGORIES = [
  { label: "CNC Machining", icon: "⚙️", query: "CNC machining center" },
  { label: "Injection Molding", icon: "🔩", query: "injection molding machine" },
  { label: "Laser Cutting", icon: "⚡", query: "laser cutting machine" },
  { label: "Press Brakes", icon: "🔨", query: "press brake" },
  { label: "Industrial Lathes", icon: "🏭", query: "industrial lathe" },
  { label: "Grinders", icon: "💎", query: "grinding machine" },
  { label: "EDM Machines", icon: "🔬", query: "EDM machine" },
  { label: "Waterjet", icon: "💧", query: "waterjet cutting machine" },
];

export default function HomePage() {
  const [query, setQuery] = useState("");
  const router = useRouter();

  const handleSearch = (q?: string) => {
    const searchQuery = q || query;
    if (!searchQuery.trim()) return;
    router.push(`/search?q=${encodeURIComponent(searchQuery.trim())}`);
  };

  return (
    <div className="min-h-screen flex flex-col">
      {/* Header */}
      <header className="bg-white border-b border-steel-200 px-6 py-4 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <div className="w-8 h-8 bg-brand-600 rounded-lg flex items-center justify-center">
            <Zap className="w-5 h-5 text-white" />
          </div>
          <span className="text-xl font-bold text-steel-900">Zoogle</span>
        </div>
        <div className="flex items-center gap-3">
          <a href="/auth/login" className="btn-ghost text-sm">Sign In</a>
          <a href="/auth/register" className="btn-primary text-sm">Get Started</a>
        </div>
      </header>

      {/* Hero */}
      <main className="flex-1 flex flex-col items-center justify-center px-4 py-20">
        <div className="text-center mb-12">
          <div className="inline-flex items-center gap-2 bg-brand-50 text-brand-700 rounded-full px-4 py-1.5 text-sm font-medium mb-6">
            <Settings className="w-4 h-4" />
            250,000+ Industrial Machines Indexed
          </div>
          <h1 className="text-5xl font-bold text-steel-900 mb-4 leading-tight">
            Find Any Industrial
            <br />
            <span className="text-brand-600">Machine Worldwide</span>
          </h1>
          <p className="text-steel-500 text-lg max-w-xl mx-auto">
            Search across 500+ dealer websites for CNC machines, lathes, laser cutters,
            injection molding equipment and more.
          </p>
        </div>

        {/* Search bar */}
        <div className="w-full max-w-2xl">
          <div className="flex gap-3 card p-2">
            <div className="flex-1 flex items-center gap-3 px-3">
              <Search className="w-5 h-5 text-steel-400 shrink-0" />
              <input
                type="text"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && handleSearch()}
                placeholder='Search machines... e.g. "Haas VF2" or "Laser cutting machine"'
                className="flex-1 outline-none text-steel-900 placeholder-steel-400 bg-transparent text-base"
              />
            </div>
            <button
              onClick={() => handleSearch()}
              className="btn-primary rounded-lg px-6"
            >
              Search
            </button>
          </div>

          {/* Quick searches */}
          <div className="mt-4 flex flex-wrap gap-2 justify-center">
            {["Haas CNC", "DMG Mori", "Trumpf laser", "Fanuc", "Injection molding"].map((s) => (
              <button
                key={s}
                onClick={() => handleSearch(s)}
                className="text-sm text-brand-600 hover:text-brand-700 bg-brand-50 hover:bg-brand-100 px-3 py-1 rounded-full transition-colors"
              >
                {s}
              </button>
            ))}
          </div>
        </div>

        {/* Categories */}
        <div className="mt-20 w-full max-w-4xl">
          <h2 className="text-center text-steel-500 text-sm font-medium uppercase tracking-wider mb-6">
            Browse by Category
          </h2>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
            {MACHINE_CATEGORIES.map((cat) => (
              <button
                key={cat.label}
                onClick={() => handleSearch(cat.query)}
                className="card p-4 hover:border-brand-300 hover:shadow-md transition-all text-left group"
              >
                <div className="text-2xl mb-2">{cat.icon}</div>
                <div className="text-sm font-medium text-steel-700 group-hover:text-brand-600">
                  {cat.label}
                </div>
              </button>
            ))}
          </div>
        </div>
      </main>

      {/* Footer */}
      <footer className="bg-white border-t border-steel-200 py-6 text-center text-steel-400 text-sm">
        <p>Zoogle — Global Industrial Machine Search Engine</p>
        <div className="mt-2 flex justify-center gap-6">
          <a href="/admin" className="hover:text-steel-600">Admin Panel</a>
          <a href="/auth/login" className="hover:text-steel-600">Sign In</a>
        </div>
      </footer>
    </div>
  );
}

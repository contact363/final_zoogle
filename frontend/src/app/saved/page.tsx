"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { getSavedMachines } from "@/lib/api";
import { useAuthStore } from "@/lib/store";
import type { Machine } from "@/types";
import { Bookmark, MapPin, ExternalLink, Zap } from "lucide-react";
import toast from "react-hot-toast";

export default function SavedPage() {
  const { user } = useAuthStore();
  const router = useRouter();
  const [machines, setMachines] = useState<Machine[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!user) { router.push("/auth/login"); return; }
    getSavedMachines()
      .then(setMachines)
      .catch(() => toast.error("Failed to load saved machines"))
      .finally(() => setLoading(false));
  }, []);

  return (
    <div className="min-h-screen bg-steel-50">
      <header className="bg-white border-b border-steel-200 px-6 py-4 flex items-center gap-4">
        <a href="/" className="flex items-center gap-2">
          <div className="w-7 h-7 bg-brand-600 rounded-md flex items-center justify-center">
            <Zap className="w-4 h-4 text-white" />
          </div>
          <span className="font-bold text-steel-900">Zoogle</span>
        </a>
        <h1 className="text-lg font-semibold text-steel-900">Saved Machines</h1>
      </header>

      <div className="max-w-6xl mx-auto px-4 py-8">
        {loading ? (
          <div className="text-center text-steel-400 py-20">Loading...</div>
        ) : machines.length === 0 ? (
          <div className="text-center py-20 text-steel-400">
            <Bookmark className="w-12 h-12 mx-auto mb-4 opacity-30" />
            <p className="text-lg font-medium">No saved machines yet</p>
            <p className="text-sm mt-1">Save machines from search results to find them here</p>
            <a href="/" className="btn-primary inline-block mt-6 text-sm">Start Searching</a>
          </div>
        ) : (
          <>
            <p className="text-steel-500 text-sm mb-6">{machines.length} saved machine{machines.length !== 1 ? "s" : ""}</p>
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
              {machines.map((m) => (
                <a
                  key={m.id}
                  href={m.machine_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="card hover:shadow-md hover:border-brand-200 transition-all block"
                >
                  <div className="h-40 bg-steel-100 rounded-t-xl overflow-hidden">
                    {m.thumbnail_url ? (
                      <img src={m.thumbnail_url} alt="" className="w-full h-full object-cover" />
                    ) : (
                      <div className="w-full h-full flex items-center justify-center text-steel-300 text-4xl">⚙️</div>
                    )}
                  </div>
                  <div className="p-4">
                    {m.brand && <div className="text-xs font-semibold text-brand-600 mb-1">{m.brand}</div>}
                    <h3 className="text-sm font-semibold text-steel-900 mb-2 line-clamp-2">
                      {m.model || m.machine_type || "Industrial Machine"}
                    </h3>
                    <div className="text-base font-bold text-steel-900 mb-3">
                      {m.price
                        ? new Intl.NumberFormat("en-US", { style: "currency", currency: m.currency || "USD", maximumFractionDigits: 0 }).format(m.price)
                        : "Price on request"}
                    </div>
                    {m.location && (
                      <div className="flex items-center gap-1 text-xs text-steel-400">
                        <MapPin className="w-3 h-3" />
                        {m.location}
                      </div>
                    )}
                  </div>
                </a>
              ))}
            </div>
          </>
        )}
      </div>
    </div>
  );
}

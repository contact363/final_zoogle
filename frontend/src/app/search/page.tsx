"use client";

import { useEffect, useState, useCallback } from "react";
import { useSearchParams, useRouter } from "next/navigation";
import { Search, Filter, SortAsc, X, Zap } from "lucide-react";
import { searchMachines } from "@/lib/api";
import type { SearchResponse, SearchFilters } from "@/types";
import MachineCard from "@/components/search/MachineCard";
import FilterPanel from "@/components/search/FilterPanel";
import Pagination from "@/components/ui/Pagination";
import SearchBar from "@/components/search/SearchBar";

export default function SearchPage() {
  const searchParams = useSearchParams();
  const router = useRouter();

  const [results, setResults] = useState<SearchResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [showFilters, setShowFilters] = useState(false);
  const [filters, setFilters] = useState<SearchFilters>({
    query: searchParams.get("q") || "",
    machine_type: searchParams.get("type") || undefined,
    brand: searchParams.get("brand") || undefined,
    location: searchParams.get("location") || undefined,
    price_min: searchParams.get("price_min") ? Number(searchParams.get("price_min")) : undefined,
    price_max: searchParams.get("price_max") ? Number(searchParams.get("price_max")) : undefined,
    sort_by: searchParams.get("sort") || "relevance",
    page: Number(searchParams.get("page")) || 1,
    limit: 20,
  });

  const doSearch = useCallback(async (f: SearchFilters) => {
    if (!f.query.trim()) return;
    setLoading(true);
    try {
      const data = await searchMachines(f);
      setResults(data);
    } catch (err) {
      console.error("Search error", err);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    doSearch(filters);
  }, []);

  const handleSearch = (newFilters: Partial<SearchFilters>) => {
    const updated = { ...filters, ...newFilters, page: newFilters.page ?? 1 };
    setFilters(updated);

    // Update URL
    const params = new URLSearchParams();
    if (updated.query) params.set("q", updated.query);
    if (updated.machine_type) params.set("type", updated.machine_type);
    if (updated.brand) params.set("brand", updated.brand);
    if (updated.location) params.set("location", updated.location);
    if (updated.price_min) params.set("price_min", String(updated.price_min));
    if (updated.price_max) params.set("price_max", String(updated.price_max));
    if (updated.sort_by) params.set("sort", updated.sort_by);
    if (updated.page && updated.page > 1) params.set("page", String(updated.page));
    router.push(`/search?${params.toString()}`, { scroll: false });

    doSearch(updated);
  };

  return (
    <div className="min-h-screen bg-steel-50">
      {/* Top bar */}
      <header className="bg-white border-b border-steel-200 sticky top-0 z-30">
        <div className="max-w-7xl mx-auto px-4 py-3 flex items-center gap-4">
          <a href="/" className="flex items-center gap-2 shrink-0">
            <div className="w-7 h-7 bg-brand-600 rounded-md flex items-center justify-center">
              <Zap className="w-4 h-4 text-white" />
            </div>
            <span className="font-bold text-steel-900 hidden sm:block">Zoogle</span>
          </a>

          <div className="flex-1">
            <SearchBar
              defaultValue={filters.query}
              onSearch={(q) => handleSearch({ query: q })}
            />
          </div>

          <button
            onClick={() => setShowFilters(!showFilters)}
            className={`btn-ghost flex items-center gap-2 text-sm shrink-0 ${
              showFilters ? "bg-brand-50 text-brand-600" : ""
            }`}
          >
            <Filter className="w-4 h-4" />
            <span className="hidden sm:block">Filters</span>
          </button>
        </div>
      </header>

      <div className="max-w-7xl mx-auto px-4 py-6 flex gap-6">
        {/* Filter sidebar */}
        {showFilters && (
          <aside className="w-64 shrink-0">
            <FilterPanel filters={filters} onChange={handleSearch} />
          </aside>
        )}

        {/* Results */}
        <main className="flex-1 min-w-0">
          {/* Result header */}
          <div className="flex items-center justify-between mb-4">
            <div className="text-steel-500 text-sm">
              {loading ? (
                "Searching..."
              ) : results ? (
                <>
                  <span className="font-semibold text-steel-900">{results.total.toLocaleString()}</span>
                  {" machines found"} for{" "}
                  <span className="font-medium text-brand-600">"{results.query}"</span>
                </>
              ) : null}
            </div>

            {/* Sort */}
            <select
              value={filters.sort_by}
              onChange={(e) => handleSearch({ sort_by: e.target.value })}
              className="input w-auto text-sm py-1.5"
            >
              <option value="relevance">Most Relevant</option>
              <option value="newest">Newest First</option>
              <option value="price_asc">Price: Low to High</option>
              <option value="price_desc">Price: High to Low</option>
            </select>
          </div>

          {/* Loading skeleton */}
          {loading && (
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
              {Array.from({ length: 8 }).map((_, i) => (
                <div key={i} className="card p-4 animate-pulse">
                  <div className="bg-steel-200 h-40 rounded-lg mb-3" />
                  <div className="bg-steel-200 h-4 rounded mb-2 w-3/4" />
                  <div className="bg-steel-200 h-4 rounded w-1/2" />
                </div>
              ))}
            </div>
          )}

          {/* Results grid */}
          {!loading && results && (
            <>
              {results.results.length === 0 ? (
                <div className="text-center py-20 text-steel-400">
                  <Search className="w-12 h-12 mx-auto mb-4 opacity-30" />
                  <p className="text-lg font-medium">No machines found</p>
                  <p className="text-sm mt-1">Try different keywords or remove filters</p>
                </div>
              ) : (
                <>
                  <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
                    {results.results.map((machine) => (
                      <MachineCard key={machine.id} machine={machine} />
                    ))}
                  </div>

                  <Pagination
                    page={results.page}
                    pages={results.pages}
                    onPageChange={(p) => handleSearch({ page: p })}
                  />
                </>
              )}
            </>
          )}
        </main>
      </div>
    </div>
  );
}

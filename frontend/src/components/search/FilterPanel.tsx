"use client";

import { useState } from "react";
import type { SearchFilters } from "@/types";

const MACHINE_TYPES = [
  "CNC Machining Center",
  "CNC Lathe",
  "CNC Milling Machine",
  "Laser Cutting Machine",
  "Press Brake",
  "Industrial Lathe",
  "Injection Molding Machine",
  "Surface Grinder",
  "Cylindrical Grinder",
  "EDM Machine",
  "Wire EDM",
  "Punch Press",
  "Waterjet Cutting Machine",
  "Plasma Cutting Machine",
  "Boring Machine",
];

const SORT_OPTIONS = [
  { value: "relevance", label: "Most Relevant" },
  { value: "newest", label: "Newest" },
  { value: "price_asc", label: "Price ↑" },
  { value: "price_desc", label: "Price ↓" },
];

interface Props {
  filters: SearchFilters;
  onChange: (f: Partial<SearchFilters>) => void;
}

export default function FilterPanel({ filters, onChange }: Props) {
  const [priceMin, setPriceMin] = useState(filters.price_min?.toString() || "");
  const [priceMax, setPriceMax] = useState(filters.price_max?.toString() || "");

  const applyPrice = () => {
    onChange({
      price_min: priceMin ? Number(priceMin) : undefined,
      price_max: priceMax ? Number(priceMax) : undefined,
    });
  };

  return (
    <div className="card p-5 space-y-6">
      <h3 className="font-semibold text-steel-900">Filters</h3>

      {/* Machine Type */}
      <div>
        <label className="text-xs font-semibold text-steel-500 uppercase tracking-wider block mb-2">
          Machine Type
        </label>
        <div className="space-y-1 max-h-48 overflow-y-auto">
          <button
            onClick={() => onChange({ machine_type: undefined })}
            className={`w-full text-left text-sm px-2 py-1.5 rounded hover:bg-steel-100 transition-colors ${
              !filters.machine_type ? "text-brand-600 font-medium" : "text-steel-700"
            }`}
          >
            All Types
          </button>
          {MACHINE_TYPES.map((type) => (
            <button
              key={type}
              onClick={() =>
                onChange({ machine_type: filters.machine_type === type ? undefined : type })
              }
              className={`w-full text-left text-sm px-2 py-1.5 rounded hover:bg-steel-100 transition-colors ${
                filters.machine_type === type
                  ? "text-brand-600 font-medium bg-brand-50"
                  : "text-steel-700"
              }`}
            >
              {type}
            </button>
          ))}
        </div>
      </div>

      {/* Price Range */}
      <div>
        <label className="text-xs font-semibold text-steel-500 uppercase tracking-wider block mb-2">
          Price Range
        </label>
        <div className="flex gap-2 items-center">
          <input
            type="number"
            placeholder="Min"
            value={priceMin}
            onChange={(e) => setPriceMin(e.target.value)}
            onBlur={applyPrice}
            className="input text-sm py-1.5"
          />
          <span className="text-steel-400 text-sm">–</span>
          <input
            type="number"
            placeholder="Max"
            value={priceMax}
            onChange={(e) => setPriceMax(e.target.value)}
            onBlur={applyPrice}
            className="input text-sm py-1.5"
          />
        </div>
      </div>

      {/* Location */}
      <div>
        <label className="text-xs font-semibold text-steel-500 uppercase tracking-wider block mb-2">
          Location
        </label>
        <input
          type="text"
          placeholder="Country or city..."
          defaultValue={filters.location || ""}
          onBlur={(e) => onChange({ location: e.target.value || undefined })}
          className="input text-sm"
        />
      </div>

      {/* Brand */}
      <div>
        <label className="text-xs font-semibold text-steel-500 uppercase tracking-wider block mb-2">
          Brand
        </label>
        <input
          type="text"
          placeholder="e.g. Haas, DMG Mori..."
          defaultValue={filters.brand || ""}
          onBlur={(e) => onChange({ brand: e.target.value || undefined })}
          className="input text-sm"
        />
      </div>

      {/* Reset */}
      <button
        onClick={() =>
          onChange({
            machine_type: undefined,
            brand: undefined,
            location: undefined,
            price_min: undefined,
            price_max: undefined,
          })
        }
        className="btn-secondary w-full text-sm"
      >
        Clear Filters
      </button>
    </div>
  );
}

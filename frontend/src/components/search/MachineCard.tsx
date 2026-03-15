"use client";

import { MapPin, ExternalLink, Bookmark, Tag, DollarSign } from "lucide-react";
import type { SearchResultItem } from "@/types";
import { saveMachine } from "@/lib/api";
import { useAuthStore } from "@/lib/store";
import toast from "react-hot-toast";

interface Props {
  machine: SearchResultItem;
}

export default function MachineCard({ machine }: Props) {
  const { user } = useAuthStore();

  const formatPrice = (price: number | null, currency: string) => {
    if (!price) return "Price on request";
    try {
      return new Intl.NumberFormat("en-US", {
        style: "currency",
        currency: currency || "USD",
        maximumFractionDigits: 0,
      }).format(price);
    } catch {
      return `${currency} ${Number(price).toLocaleString()}`;
    }
  };

  const handleSave = async (e: React.MouseEvent) => {
    e.preventDefault();
    if (!user) {
      toast.error("Please sign in to save machines");
      return;
    }
    try {
      await saveMachine(machine.id);
      toast.success("Machine saved!");
    } catch {
      toast.error("Already saved or error occurred");
    }
  };

  const imageUrl = machine.thumbnail_url || "/placeholder-machine.svg";

  return (
    <a
      href={`/machine/${machine.id}`}
      className="card group hover:shadow-md hover:border-brand-200 transition-all block"
    >
      {/* Image */}
      <div className="relative h-44 bg-steel-100 rounded-t-xl overflow-hidden">
        <img
          src={imageUrl}
          alt={`${machine.brand ?? ""} ${machine.model ?? ""}`}
          className="w-full h-full object-cover group-hover:scale-105 transition-transform duration-300"
          onError={(e) => {
            (e.target as HTMLImageElement).src = "/placeholder-machine.svg";
          }}
        />
        {/* Save button */}
        <button
          onClick={handleSave}
          className="absolute top-2 right-2 p-1.5 bg-white/80 backdrop-blur-sm rounded-lg hover:bg-white transition-colors opacity-0 group-hover:opacity-100"
          title="Save machine"
        >
          <Bookmark className="w-4 h-4 text-steel-600" />
        </button>
      </div>

      {/* Content */}
      <div className="p-4 space-y-2">
        {/* Brand */}
        {machine.brand && (
          <div className="text-xs font-semibold text-brand-600 uppercase tracking-wide">
            {machine.brand}
          </div>
        )}

        {/* Model / Title */}
        <h3 className="text-sm font-semibold text-steel-900 leading-tight line-clamp-2">
          {machine.model || machine.machine_type || "Industrial Machine"}
        </h3>

        {/* Type row */}
        <div className="flex items-center gap-1.5">
          <Tag className="w-3 h-3 text-brand-500 shrink-0" />
          <span className="text-xs text-brand-700 font-medium truncate">
            {machine.machine_type || <span className="text-steel-400 font-normal">Type not specified</span>}
          </span>
        </div>

        {/* Price row */}
        <div className="flex items-center gap-1.5">
          <DollarSign className="w-3 h-3 text-green-600 shrink-0" />
          <span className={`text-sm font-bold ${machine.price ? "text-steel-900" : "text-steel-400 font-normal text-xs"}`}>
            {formatPrice(machine.price, machine.currency)}
          </span>
        </div>

        {/* Location row */}
        <div className="flex items-center gap-1.5">
          <MapPin className="w-3 h-3 text-steel-400 shrink-0" />
          <span className={`text-xs truncate ${machine.location ? "text-steel-500" : "text-steel-400"}`}>
            {machine.location || "Location not specified"}
          </span>
        </div>

        {/* Source */}
        {machine.website_source && (
          <div className="flex items-center gap-1 text-xs text-steel-300 pt-1 border-t border-steel-100">
            <ExternalLink className="w-3 h-3 shrink-0" />
            <span className="truncate">{machine.website_source}</span>
          </div>
        )}
      </div>
    </a>
  );
}

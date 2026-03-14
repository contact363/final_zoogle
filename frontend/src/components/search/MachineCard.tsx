"use client";

import Image from "next/image";
import { MapPin, ExternalLink, Bookmark, Tag } from "lucide-react";
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
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: currency || "USD",
      maximumFractionDigits: 0,
    }).format(price);
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
      href={machine.machine_url}
      target="_blank"
      rel="noopener noreferrer"
      className="card group hover:shadow-md hover:border-brand-200 transition-all block"
    >
      {/* Image */}
      <div className="relative h-44 bg-steel-100 rounded-t-xl overflow-hidden">
        <img
          src={imageUrl}
          alt={`${machine.brand} ${machine.model}`}
          className="w-full h-full object-cover group-hover:scale-105 transition-transform duration-300"
          onError={(e) => {
            (e.target as HTMLImageElement).src = "/placeholder-machine.svg";
          }}
        />
        {/* Machine type badge */}
        {machine.machine_type && (
          <div className="absolute top-2 left-2">
            <span className="badge-blue text-xs">{machine.machine_type}</span>
          </div>
        )}
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
      <div className="p-4">
        <div className="mb-1">
          {machine.brand && (
            <span className="text-xs font-semibold text-brand-600 uppercase tracking-wide">
              {machine.brand}
            </span>
          )}
        </div>

        <h3 className="text-sm font-semibold text-steel-900 leading-tight mb-2 line-clamp-2">
          {machine.model || machine.machine_type || "Industrial Machine"}
        </h3>

        <div className="text-base font-bold text-steel-900 mb-3">
          {formatPrice(machine.price, machine.currency)}
        </div>

        <div className="space-y-1 text-xs text-steel-400">
          {machine.location && (
            <div className="flex items-center gap-1">
              <MapPin className="w-3 h-3 shrink-0" />
              <span className="truncate">{machine.location}</span>
            </div>
          )}
          {machine.website_source && (
            <div className="flex items-center gap-1">
              <ExternalLink className="w-3 h-3 shrink-0" />
              <span className="truncate">{machine.website_source}</span>
            </div>
          )}
        </div>
      </div>
    </a>
  );
}

"use client";

import { useEffect, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import { ArrowLeft, MapPin, Tag, DollarSign, ExternalLink, ChevronLeft, ChevronRight } from "lucide-react";
import { getMachine } from "@/lib/api";
import type { Machine } from "@/types";

export default function MachinePage() {
  const { id } = useParams<{ id: string }>();
  const router = useRouter();
  const [machine, setMachine] = useState<Machine | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeImg, setActiveImg] = useState(0);

  useEffect(() => {
    getMachine(Number(id))
      .then(setMachine)
      .catch(() => setError("Machine not found"))
      .finally(() => setLoading(false));
  }, [id]);

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

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="animate-spin rounded-full h-10 w-10 border-b-2 border-brand-600" />
      </div>
    );
  }

  if (error || !machine) {
    return (
      <div className="min-h-screen flex flex-col items-center justify-center gap-4">
        <p className="text-steel-500 text-lg">{error || "Machine not found"}</p>
        <button onClick={() => router.back()} className="btn-secondary flex items-center gap-2">
          <ArrowLeft className="w-4 h-4" /> Go back
        </button>
      </div>
    );
  }

  // Build image list: prefer images array, fall back to thumbnail
  const images: string[] = machine.images?.length
    ? machine.images
        .slice()
        .sort((a, b) => (a.is_primary ? -1 : b.is_primary ? 1 : 0))
        .map((img) => img.image_url)
    : machine.thumbnail_url
    ? [machine.thumbnail_url]
    : ["/placeholder-machine.svg"];

  return (
    <div className="min-h-screen bg-steel-50">
      {/* Header */}
      <div className="bg-white border-b border-steel-200 sticky top-0 z-10">
        <div className="max-w-5xl mx-auto px-4 py-3 flex items-center gap-3">
          <button
            onClick={() => router.back()}
            className="p-2 rounded-lg hover:bg-steel-100 transition-colors"
          >
            <ArrowLeft className="w-5 h-5 text-steel-600" />
          </button>
          <span className="text-sm text-steel-500 truncate">
            {machine.brand && <span className="font-medium text-steel-900">{machine.brand}</span>}
            {machine.brand && machine.model && " · "}
            {machine.model}
          </span>
        </div>
      </div>

      <div className="max-w-5xl mx-auto px-4 py-8 grid grid-cols-1 md:grid-cols-2 gap-8">
        {/* Image gallery */}
        <div className="space-y-3">
          {/* Main image */}
          <div className="relative bg-steel-100 rounded-2xl overflow-hidden aspect-[4/3]">
            <img
              src={images[activeImg]}
              alt={`${machine.brand ?? ""} ${machine.model ?? ""}`}
              className="w-full h-full object-cover"
              onError={(e) => {
                (e.target as HTMLImageElement).src = "/placeholder-machine.svg";
              }}
            />
            {images.length > 1 && (
              <>
                <button
                  onClick={() => setActiveImg((i) => (i - 1 + images.length) % images.length)}
                  className="absolute left-2 top-1/2 -translate-y-1/2 p-1.5 bg-white/80 backdrop-blur-sm rounded-full hover:bg-white transition-colors"
                >
                  <ChevronLeft className="w-5 h-5 text-steel-700" />
                </button>
                <button
                  onClick={() => setActiveImg((i) => (i + 1) % images.length)}
                  className="absolute right-2 top-1/2 -translate-y-1/2 p-1.5 bg-white/80 backdrop-blur-sm rounded-full hover:bg-white transition-colors"
                >
                  <ChevronRight className="w-5 h-5 text-steel-700" />
                </button>
                <div className="absolute bottom-2 left-1/2 -translate-x-1/2 flex gap-1.5">
                  {images.map((_, i) => (
                    <button
                      key={i}
                      onClick={() => setActiveImg(i)}
                      className={`w-2 h-2 rounded-full transition-colors ${
                        i === activeImg ? "bg-white" : "bg-white/50"
                      }`}
                    />
                  ))}
                </div>
              </>
            )}
          </div>

          {/* Thumbnails */}
          {images.length > 1 && (
            <div className="flex gap-2 overflow-x-auto pb-1">
              {images.map((src, i) => (
                <button
                  key={i}
                  onClick={() => setActiveImg(i)}
                  className={`shrink-0 w-16 h-16 rounded-lg overflow-hidden border-2 transition-colors ${
                    i === activeImg ? "border-brand-500" : "border-transparent"
                  }`}
                >
                  <img
                    src={src}
                    alt=""
                    className="w-full h-full object-cover"
                    onError={(e) => {
                      (e.target as HTMLImageElement).src = "/placeholder-machine.svg";
                    }}
                  />
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Info panel */}
        <div className="space-y-5">
          {/* Brand + title */}
          {machine.brand && (
            <div className="text-sm font-semibold text-brand-600 uppercase tracking-wide">
              {machine.brand}
            </div>
          )}
          <h1 className="text-2xl font-bold text-steel-900 leading-tight">
            {machine.model || machine.machine_type || "Industrial Machine"}
          </h1>

          {/* Key details */}
          <div className="grid grid-cols-2 gap-3">
            <div className="bg-white rounded-xl p-3 border border-steel-200">
              <div className="flex items-center gap-1.5 mb-1">
                <DollarSign className="w-4 h-4 text-green-600" />
                <span className="text-xs text-steel-500 font-medium">Price</span>
              </div>
              <div className="text-sm font-bold text-steel-900">
                {formatPrice(machine.price, machine.currency)}
              </div>
            </div>

            <div className="bg-white rounded-xl p-3 border border-steel-200">
              <div className="flex items-center gap-1.5 mb-1">
                <Tag className="w-4 h-4 text-brand-500" />
                <span className="text-xs text-steel-500 font-medium">Type</span>
              </div>
              <div className="text-sm font-medium text-steel-900 truncate">
                {machine.machine_type || "—"}
              </div>
            </div>

            <div className="bg-white rounded-xl p-3 border border-steel-200 col-span-2">
              <div className="flex items-center gap-1.5 mb-1">
                <MapPin className="w-4 h-4 text-steel-400" />
                <span className="text-xs text-steel-500 font-medium">Location</span>
              </div>
              <div className="text-sm text-steel-900">{machine.location || "Not specified"}</div>
            </div>
          </div>

          {/* Description */}
          {machine.description && (
            <div className="bg-white rounded-xl p-4 border border-steel-200">
              <h2 className="text-xs font-semibold text-steel-500 uppercase tracking-wide mb-2">
                Description
              </h2>
              <p className="text-sm text-steel-700 leading-relaxed whitespace-pre-line">
                {machine.description}
              </p>
            </div>
          )}

          {/* Specs */}
          {machine.specs && machine.specs.length > 0 && (
            <div className="bg-white rounded-xl p-4 border border-steel-200">
              <h2 className="text-xs font-semibold text-steel-500 uppercase tracking-wide mb-3">
                Specifications
              </h2>
              <dl className="space-y-2">
                {machine.specs.map((spec, i) => (
                  <div key={i} className="flex justify-between gap-4 text-sm">
                    <dt className="text-steel-500 shrink-0 capitalize">
                      {spec.spec_key.replace(/_/g, " ")}
                    </dt>
                    <dd className="text-steel-900 font-medium text-right">
                      {spec.spec_value}
                      {spec.spec_unit && (
                        <span className="text-steel-400 ml-1">{spec.spec_unit}</span>
                      )}
                    </dd>
                  </div>
                ))}
              </dl>
            </div>
          )}

          {/* Source + external link */}
          <div className="flex flex-col gap-2">
            {machine.website_source && (
              <p className="text-xs text-steel-400">
                Source: <span className="font-medium">{machine.website_source}</span>
              </p>
            )}
            <a
              href={machine.machine_url}
              target="_blank"
              rel="noopener noreferrer"
              className="btn-primary flex items-center justify-center gap-2 w-full"
            >
              <ExternalLink className="w-4 h-4" />
              View on seller website
            </a>
          </div>
        </div>
      </div>
    </div>
  );
}

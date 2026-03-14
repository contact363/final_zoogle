"use client";

import { ChevronLeft, ChevronRight } from "lucide-react";

interface Props {
  page: number;
  pages: number;
  onPageChange: (page: number) => void;
}

export default function Pagination({ page, pages, onPageChange }: Props) {
  if (pages <= 1) return null;

  const pageNumbers = () => {
    const nums: (number | "...")[] = [];
    if (pages <= 7) {
      return Array.from({ length: pages }, (_, i) => i + 1);
    }
    nums.push(1);
    if (page > 3) nums.push("...");
    for (let i = Math.max(2, page - 1); i <= Math.min(pages - 1, page + 1); i++) {
      nums.push(i);
    }
    if (page < pages - 2) nums.push("...");
    nums.push(pages);
    return nums;
  };

  return (
    <div className="flex items-center justify-center gap-1 mt-8">
      <button
        onClick={() => onPageChange(page - 1)}
        disabled={page === 1}
        className="p-2 rounded-lg hover:bg-steel-100 disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
      >
        <ChevronLeft className="w-4 h-4" />
      </button>

      {pageNumbers().map((num, i) =>
        num === "..." ? (
          <span key={`dots-${i}`} className="px-2 text-steel-400">
            …
          </span>
        ) : (
          <button
            key={num}
            onClick={() => onPageChange(num as number)}
            className={`w-9 h-9 rounded-lg text-sm font-medium transition-colors ${
              num === page
                ? "bg-brand-600 text-white"
                : "hover:bg-steel-100 text-steel-700"
            }`}
          >
            {num}
          </button>
        )
      )}

      <button
        onClick={() => onPageChange(page + 1)}
        disabled={page === pages}
        className="p-2 rounded-lg hover:bg-steel-100 disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
      >
        <ChevronRight className="w-4 h-4" />
      </button>
    </div>
  );
}

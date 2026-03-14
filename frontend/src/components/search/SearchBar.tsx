"use client";

import { useState } from "react";
import { Search } from "lucide-react";

interface Props {
  defaultValue?: string;
  onSearch: (query: string) => void;
  placeholder?: string;
}

export default function SearchBar({
  defaultValue = "",
  onSearch,
  placeholder = 'Search machines, brands, models...',
}: Props) {
  const [value, setValue] = useState(defaultValue);

  return (
    <div className="flex items-center gap-2 bg-steel-100 rounded-lg px-3 py-2 border border-steel-200 focus-within:border-brand-400 focus-within:bg-white transition-colors">
      <Search className="w-4 h-4 text-steel-400 shrink-0" />
      <input
        type="text"
        value={value}
        onChange={(e) => setValue(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === "Enter") onSearch(value);
        }}
        placeholder={placeholder}
        className="flex-1 bg-transparent outline-none text-sm text-steel-900 placeholder-steel-400"
      />
      {value && (
        <button
          onClick={() => { setValue(""); onSearch(""); }}
          className="text-steel-400 hover:text-steel-600"
        >
          ×
        </button>
      )}
      <button
        onClick={() => onSearch(value)}
        className="bg-brand-600 text-white px-3 py-1 rounded text-sm font-medium hover:bg-brand-700 transition-colors"
      >
        Search
      </button>
    </div>
  );
}

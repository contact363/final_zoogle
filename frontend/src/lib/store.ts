import { create } from "zustand";
import { persist } from "zustand/middleware";
import type { User, SearchFilters } from "@/types";

interface AuthState {
  user: User | null;
  token: string | null;
  setAuth: (user: User, token: string) => void;
  logout: () => void;
}

export const useAuthStore = create<AuthState>()(
  persist(
    (set) => ({
      user: null,
      token: null,
      setAuth: (user, token) => {
        if (typeof window !== "undefined") {
          localStorage.setItem("zoogle_token", token);
        }
        set({ user, token });
      },
      logout: () => {
        if (typeof window !== "undefined") {
          localStorage.removeItem("zoogle_token");
        }
        set({ user: null, token: null });
      },
    }),
    { name: "zoogle-auth" }
  )
);

interface SearchState {
  filters: SearchFilters;
  setFilters: (f: Partial<SearchFilters>) => void;
  resetFilters: () => void;
}

const defaultFilters: SearchFilters = {
  query: "",
  sort_by: "relevance",
  page: 1,
  limit: 20,
};

export const useSearchStore = create<SearchState>((set) => ({
  filters: defaultFilters,
  setFilters: (f) =>
    set((state) => ({ filters: { ...state.filters, ...f, page: f.page ?? 1 } })),
  resetFilters: () => set({ filters: defaultFilters }),
}));

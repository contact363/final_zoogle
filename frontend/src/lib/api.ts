import axios from "axios";
import type { SearchFilters, SearchResponse, Machine, Website, AuthToken } from "@/types";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

export const api = axios.create({
  baseURL: API_BASE,
  headers: { "Content-Type": "application/json" },
});

// Auto-attach auth token
api.interceptors.request.use((config) => {
  if (typeof window !== "undefined") {
    const token = localStorage.getItem("zoogle_token");
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
  }
  return config;
});

// ── Search ──────────────────────────────────────────────────────────────────

export async function searchMachines(filters: SearchFilters): Promise<SearchResponse> {
  const { data } = await api.post("/api/search", filters);
  return data;
}

// ── Machine ─────────────────────────────────────────────────────────────────

export async function getMachine(id: number): Promise<Machine> {
  const { data } = await api.get(`/api/machines/${id}`);
  return data;
}

export async function saveMachine(id: number): Promise<void> {
  await api.post(`/api/users/me/saved/${id}`);
}

export async function unsaveMachine(id: number): Promise<void> {
  await api.delete(`/api/users/me/saved/${id}`);
}

export async function getSavedMachines(): Promise<Machine[]> {
  const { data } = await api.get("/api/users/me/saved");
  return data;
}

// ── Auth ─────────────────────────────────────────────────────────────────────

export async function login(email: string, password: string): Promise<AuthToken> {
  const { data } = await api.post("/api/auth/login", { email, password });
  return data;
}

export async function register(
  email: string,
  password: string,
  full_name?: string
): Promise<void> {
  await api.post("/api/auth/register", { email, password, full_name });
}

// ── Admin ─────────────────────────────────────────────────────────────────────

export async function getAdminStats() {
  const { data } = await api.get("/api/admin/stats");
  return data;
}

export async function listWebsites(): Promise<Website[]> {
  const { data } = await api.get("/api/admin/websites");
  return data;
}

export async function addWebsite(payload: {
  name: string;
  url: string;
  description?: string;
}): Promise<Website> {
  const { data } = await api.post("/api/admin/websites", payload);
  return data;
}

export async function deleteWebsite(id: number): Promise<void> {
  await api.delete(`/api/admin/websites/${id}`);
}

export async function startCrawl(websiteId: number) {
  const { data } = await api.post(`/api/admin/crawl/start/${websiteId}`);
  return data;
}

export async function startAllCrawls() {
  const { data } = await api.post("/api/admin/crawl/start-all");
  return data;
}

export async function getAdminMachines(params?: {
  skip?: number;
  limit?: number;
  website_id?: number;
}) {
  const { data } = await api.get("/api/admin/machines", { params });
  return data;
}

export async function deleteMachine(id: number): Promise<void> {
  await api.delete(`/api/admin/machines/${id}`);
}

export async function getCrawlLogs(params?: { skip?: number; limit?: number }) {
  const { data } = await api.get("/api/admin/crawl-logs", { params });
  return data;
}

export function exportMachinesExcelUrl() {
  return `${API_BASE}/api/admin/machines/export/excel`;
}

import axios from "axios";
import type { SearchFilters, SearchResponse, Machine, Website, AuthToken, TrainingRules, TrainingRulesForm } from "@/types";

// Hardcode production URL as fallback so this works even when
// NEXT_PUBLIC_API_URL is not set at Render build time.
const API_BASE =
  process.env.NEXT_PUBLIC_API_URL ||
  "https://final-zoogle-backend.onrender.com";

export const api = axios.create({
  baseURL: API_BASE,
  headers: { "Content-Type": "application/json" },
  timeout: 60000, // 60s — free Render tier cold-start can take 30-50s
});

// Surface backend error messages; detect cold-start network errors
api.interceptors.response.use(
  (res) => res,
  (err) => {
    const detail = err?.response?.data?.detail;
    if (detail) {
      err.message = Array.isArray(detail) ? detail[0]?.msg || "Request failed." : detail;
    } else if (err.code === "ERR_NETWORK" || err.message === "Network Error") {
      err.message = "Cannot reach server. If this is your first visit, the backend may be waking up — please wait 30 seconds and try again.";
    } else if (err.code === "ECONNABORTED") {
      err.message = "Request timed out. The server may be starting up — please try again.";
    }
    return Promise.reject(err);
  }
);

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

export async function updateWebsite(id: number, payload: {
  name?: string;
  description?: string;
  is_active?: boolean;
  crawl_enabled?: boolean;
}): Promise<Website> {
  const { data } = await api.patch(`/api/admin/websites/${id}`, payload);
  return data;
}

export async function deleteWebsite(id: number): Promise<void> {
  await api.delete(`/api/admin/websites/${id}`);
}

export async function recalculateMachineCounts(): Promise<void> {
  await api.post("/api/admin/websites/recalculate-counts");
}

export async function fixWebsiteNames(): Promise<{ fixed: number }> {
  const { data } = await api.post("/api/admin/websites/fix-names");
  return data;
}

export async function fillMachineTypes(): Promise<{ updated: number; total_checked: number }> {
  const { data } = await api.post("/api/admin/machines/fill-types");
  return data;
}

export async function discoverWebsite(websiteId: number) {
  const { data } = await api.post(`/api/admin/websites/${websiteId}/discover`);
  return data;
}

export async function collectUrlsWebsite(websiteId: number) {
  const { data } = await api.post(`/api/admin/websites/${websiteId}/collect-urls`);
  return data;
}

export async function startCrawl(websiteId: number) {
  const { data } = await api.post(`/api/admin/crawl/start/${websiteId}`);
  return data;
}

export async function startAllCrawls() {
  const { data } = await api.post("/api/admin/crawl/start-all");
  return data;
}

export async function fixStuckCrawls() {
  const { data } = await api.post("/api/admin/crawl/fix-stuck");
  return data;
}

export async function getAdminMachines(params?: {
  skip?: number;
  limit?: number;
  website_id?: number;
  machine_type?: string;
  brand?: string;
  q?: string;
  is_active?: boolean;
}) {
  const { data } = await api.get("/api/admin/machines", { params });
  return data;
}

export async function createMachine(payload: {
  website_id: number;
  machine_type?: string;
  brand?: string;
  model?: string;
  price?: number | null;
  currency?: string;
  location?: string;
  description?: string;
  machine_url?: string;
  is_active?: boolean;
}): Promise<any> {
  const { data } = await api.post("/api/admin/machines", payload);
  return data;
}

export async function updateMachine(id: number, payload: {
  machine_type?: string;
  brand?: string;
  model?: string;
  price?: number | null;
  location?: string;
  description?: string;
  is_active?: boolean;
}): Promise<any> {
  const { data } = await api.patch(`/api/admin/machines/${id}`, payload);
  return data;
}

export async function deleteMachine(id: number): Promise<void> {
  await api.delete(`/api/admin/machines/${id}`);
}

export async function getCrawlLogs(params?: { skip?: number; limit?: number }) {
  const { data } = await api.get("/api/admin/crawl-logs", { params });
  return data;
}

export async function diagnoseCrawl(websiteId: number) {
  const { data } = await api.get(`/api/admin/crawl/diagnose/${websiteId}`);
  return data;
}

export function exportMachinesExcelUrl() {
  return `${API_BASE}/api/admin/machines/export/excel`;
}

// ── Training Rules ─────────────────────────────────────────────────────────────

export async function getTrainingRules(websiteId: number): Promise<TrainingRules | null> {
  const { data } = await api.get(`/api/admin/websites/${websiteId}/training`);
  return data ?? null;
}

export async function saveTrainingRules(
  websiteId: number,
  payload: TrainingRulesForm,
): Promise<TrainingRules> {
  const { data } = await api.post(`/api/admin/websites/${websiteId}/training`, payload);
  return data;
}

export async function deleteTrainingRules(websiteId: number): Promise<void> {
  await api.delete(`/api/admin/websites/${websiteId}/training`);
}

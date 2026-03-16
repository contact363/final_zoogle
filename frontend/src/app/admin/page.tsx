"use client";

import { useEffect, useState, useCallback, useRef } from "react";
import {
  getAdminStats, listWebsites, addWebsite, updateWebsite, deleteWebsite,
  recalculateMachineCounts, fixWebsiteNames,
  startCrawl, startAllCrawls, fixStuckCrawls, getCrawlLogs,
  discoverWebsite,
  collectUrlsWebsite,
  getAdminMachines, updateMachine, deleteMachine, createMachine,
  exportMachinesExcelUrl, fillMachineTypes,
  getTrainingRules, saveTrainingRules, deleteTrainingRules,
} from "@/lib/api";
import type { TrainingRulesForm } from "@/types";
import { useAuthStore } from "@/lib/store";
import { useRouter } from "next/navigation";
import {
  Globe, Cpu, Users, Search, Play, Trash2, Download,
  BarChart3, FileText, Pencil, X, Check, RefreshCw, Wrench,
  ChevronDown, ChevronRight, ChevronLeft, Star, ExternalLink,
  Plus, Brain,
  SlidersHorizontal, Shield, ArrowUpDown, Image as ImageIcon,
} from "lucide-react";
import toast from "react-hot-toast";

type Section = "machines" | "dashboard" | "websites" | "logs";
type SortDir = "asc" | "desc";

// ── Sidebar nav config ─────────────────────────────────────────────────────────
const NAV_ITEMS: { key: Section; label: string; desc: string; icon: React.ReactNode }[] = [
  { key: "machines",  label: "Machines",    desc: "Approve, edit, manage listings",  icon: <Cpu className="w-5 h-5" /> },
  { key: "dashboard", label: "Dashboard",   desc: "Platform overview & stats",        icon: <BarChart3 className="w-5 h-5" /> },
  { key: "websites",  label: "Web Sources", desc: "Manage crawl sources",             icon: <Globe className="w-5 h-5" /> },
  { key: "logs",      label: "Crawl Logs",  desc: "View crawl history & errors",      icon: <FileText className="w-5 h-5" /> },
];


// ── Edit Website Modal ─────────────────────────────────────────────────────────
function EditWebsiteModal({ site, onClose, onSaved }: { site: any; onClose: () => void; onSaved: () => void }) {
  const [form, setForm] = useState({
    name: site.name ?? "",
    description: site.description ?? "",
    crawl_enabled: site.crawl_enabled ?? true,
    is_active: site.is_active ?? true,
  });
  const [saving, setSaving] = useState(false);

  const handleSave = async () => {
    setSaving(true);
    try {
      await updateWebsite(site.id, form);
      toast.success("Website updated");
      onSaved();
      onClose();
    } catch { toast.error("Failed to update website"); }
    finally { setSaving(false); }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-md p-6 space-y-4">
        <div className="flex items-center justify-between">
          <h3 className="text-lg font-semibold">Edit Website</h3>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-700"><X className="w-5 h-5" /></button>
        </div>
        <div className="space-y-3">
          <div>
            <label className="text-sm font-medium text-gray-700">Name</label>
            <input className="input w-full mt-1" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} />
          </div>
          <div>
            <label className="text-sm font-medium text-gray-700">Description</label>
            <textarea className="input w-full mt-1 h-20 resize-none" value={form.description} onChange={(e) => setForm({ ...form, description: e.target.value })} />
          </div>
          <div className="flex gap-6">
            <label className="flex items-center gap-2 text-sm cursor-pointer">
              <input type="checkbox" checked={form.is_active} onChange={(e) => setForm({ ...form, is_active: e.target.checked })} /> Active
            </label>
            <label className="flex items-center gap-2 text-sm cursor-pointer">
              <input type="checkbox" checked={form.crawl_enabled} onChange={(e) => setForm({ ...form, crawl_enabled: e.target.checked })} /> Crawl Enabled
            </label>
          </div>
        </div>
        <div className="flex justify-end gap-3 pt-2">
          <button onClick={onClose} className="btn-secondary">Cancel</button>
          <button onClick={handleSave} disabled={saving} className="btn-primary flex items-center gap-2">
            <Check className="w-4 h-4" />{saving ? "Saving..." : "Save"}
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Train Website Modal ────────────────────────────────────────────────────────
const EMPTY_RULES: TrainingRulesForm = {
  listing_selector:     null,
  title_selector:       null,
  url_selector:         null,
  description_selector: null,
  image_selector:       null,
  price_selector:       null,
  category_selector:    null,
  pagination_selector:  null,
};

const SELECTOR_FIELDS: { key: keyof TrainingRulesForm; label: string; hint: string }[] = [
  { key: "listing_selector",     label: "Listing Selector *",      hint: 'CSS selector for the repeating card container, e.g. ".product-card" or "li.machine-item"' },
  { key: "title_selector",       label: "Title Selector",          hint: 'Within each card, e.g. "h2.title::text" or ".product-name a::text"' },
  { key: "url_selector",         label: "URL / Link Selector",     hint: 'Within each card, e.g. "a.card-link::attr(href)" or "h3 a::attr(href)"' },
  { key: "price_selector",       label: "Price Selector",          hint: 'Within each card, e.g. ".price::text" or "span.asking-price::text"' },
  { key: "description_selector", label: "Description Selector",    hint: 'Within each card or detail page, e.g. ".description::text"' },
  { key: "image_selector",       label: "Image Selector",          hint: 'Within each card, e.g. "img::attr(src)" or "img::attr(data-src)"' },
  { key: "category_selector",    label: "Category Selector",       hint: 'Within each card, e.g. ".category::text"' },
  { key: "pagination_selector",  label: "Pagination Selector",     hint: 'Next-page link, e.g. "a[rel=\'next\']::attr(href)" or "a.next::attr(href)"' },
];

function TrainWebsiteModal({
  site,
  onClose,
  onSaved,
}: {
  site: any;
  onClose: () => void;
  onSaved: () => void;
}) {
  const [form, setForm] = useState<TrainingRulesForm>(EMPTY_RULES);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [hasRules, setHasRules] = useState(false);

  // Load existing rules on open
  useEffect(() => {
    getTrainingRules(site.id)
      .then((rules) => {
        if (rules) {
          const { listing_selector, title_selector, url_selector,
                  description_selector, image_selector, price_selector,
                  category_selector, pagination_selector } = rules;
          setForm({ listing_selector, title_selector, url_selector,
                    description_selector, image_selector, price_selector,
                    category_selector, pagination_selector });
          setHasRules(true);
        }
      })
      .catch(() => toast.error("Failed to load training rules"))
      .finally(() => setLoading(false));
  }, [site.id]);

  const handleSave = async () => {
    if (!form.listing_selector?.trim()) {
      toast.error("Listing Selector is required");
      return;
    }
    setSaving(true);
    try {
      // Normalize: convert empty strings to null
      const payload: TrainingRulesForm = Object.fromEntries(
        Object.entries(form).map(([k, v]) => [k, v?.trim() || null])
      ) as TrainingRulesForm;
      await saveTrainingRules(site.id, payload);
      toast.success("Training rules saved! Run a crawl to apply them.");
      onSaved();
      onClose();
    } catch { toast.error("Failed to save training rules"); }
    finally { setSaving(false); }
  };

  const handleDelete = async () => {
    if (!confirm("Remove all training rules for this website? The crawler will revert to auto-discovery.")) return;
    try {
      await deleteTrainingRules(site.id);
      toast.success("Training rules removed");
      onSaved();
      onClose();
    } catch { toast.error("Failed to remove training rules"); }
  };

  const set = (key: keyof TrainingRulesForm, val: string) =>
    setForm((f) => ({ ...f, [key]: val }));

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-2xl max-h-[90vh] flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200 shrink-0">
          <div>
            <h3 className="text-lg font-semibold text-gray-900 flex items-center gap-2">
              <Brain className="w-5 h-5 text-purple-600" />
              Train Scraper — {site.name}
            </h3>
            <p className="text-xs text-gray-500 mt-0.5">
              Configure CSS selectors so the crawler can reliably extract machines from this site.
              Leave a field blank to let the crawler auto-detect it.
            </p>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-700 ml-4">
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Body */}
        <div className="overflow-y-auto flex-1 px-6 py-4 space-y-4">
          {loading ? (
            <div className="flex justify-center py-8">
              <RefreshCw className="w-5 h-5 animate-spin text-gray-400" />
            </div>
          ) : (
            <>
              {hasRules && (
                <div className="flex items-center gap-2 px-3 py-2 bg-purple-50 border border-purple-200 rounded-lg text-sm text-purple-700">
                  <Check className="w-4 h-4 shrink-0" />
                  Training rules are active for this website. Edit below to update them.
                </div>
              )}
              {!hasRules && (
                <div className="flex items-center gap-2 px-3 py-2 bg-amber-50 border border-amber-200 rounded-lg text-sm text-amber-700">
                  <Brain className="w-4 h-4 shrink-0" />
                  No training rules yet — the crawler uses auto-discovery. Add a Listing Selector to enable trained extraction.
                </div>
              )}

              <div className="text-xs font-semibold text-gray-500 uppercase tracking-wide pt-1">
                CSS Selector Configuration
              </div>

              <div className="grid gap-3">
                {SELECTOR_FIELDS.map(({ key, label, hint }) => (
                  <div key={key}>
                    <label className="text-sm font-medium text-gray-700">
                      {label}
                    </label>
                    <input
                      className="input w-full mt-1 font-mono text-sm"
                      placeholder={hint}
                      value={form[key] ?? ""}
                      onChange={(e) => set(key, e.target.value)}
                    />
                  </div>
                ))}
              </div>

              {/* Quick reference */}
              <details className="text-xs text-gray-500 border border-gray-200 rounded-lg p-3">
                <summary className="cursor-pointer font-medium text-gray-600 select-none">
                  CSS selector quick reference
                </summary>
                <ul className="mt-2 space-y-1 list-disc list-inside">
                  <li><code className="bg-gray-100 px-1 rounded">.class-name</code> — element with a CSS class</li>
                  <li><code className="bg-gray-100 px-1 rounded">#id</code> — element with an ID</li>
                  <li><code className="bg-gray-100 px-1 rounded">div.card h2::text</code> — text inside h2 within div.card</li>
                  <li><code className="bg-gray-100 px-1 rounded">a::attr(href)</code> — href attribute of an anchor</li>
                  <li><code className="bg-gray-100 px-1 rounded">img::attr(src)</code> — src attribute of an image</li>
                  <li><code className="bg-gray-100 px-1 rounded">img::attr(data-src)</code> — lazy-loaded image src</li>
                  <li><code className="bg-gray-100 px-1 rounded">a[rel="next"]::attr(href)</code> — next-page link</li>
                </ul>
              </details>
            </>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between px-6 py-4 border-t border-gray-200 shrink-0">
          <div>
            {hasRules && (
              <button
                onClick={handleDelete}
                className="flex items-center gap-1.5 text-sm text-red-500 hover:text-red-700"
              >
                <Trash2 className="w-4 h-4" /> Remove rules
              </button>
            )}
          </div>
          <div className="flex gap-3">
            <button onClick={onClose} className="btn-secondary">Cancel</button>
            <button
              onClick={handleSave}
              disabled={saving || loading}
              className="btn-primary flex items-center gap-2"
            >
              <Brain className="w-4 h-4" />
              {saving ? "Saving..." : "Save Training Rules"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ── Machine Modal (create + edit) ──────────────────────────────────────────────
function MachineModal({
  machine, websites, onClose, onSaved,
}: { machine: any | null; websites: any[]; onClose: () => void; onSaved: () => void }) {
  const isNew = !machine;
  const [form, setForm] = useState({
    website_id: machine?.website_id ?? (websites[0]?.id ?? ""),
    machine_type: machine?.machine_type ?? "",
    brand: machine?.brand ?? "",
    model: machine?.model ?? "",
    price: machine?.price != null ? String(machine.price) : "",
    currency: machine?.currency ?? "USD",
    location: machine?.location ?? "",
    machine_url: machine?.machine_url ?? "",
    description: machine?.description ?? "",
    is_active: machine?.is_active ?? true,
  });
  const [saving, setSaving] = useState(false);

  const handleSave = async () => {
    setSaving(true);
    try {
      const priceVal = form.price !== "" ? parseFloat(form.price) : null;
      if (isNew) {
        await createMachine({ ...form, website_id: Number(form.website_id), price: priceVal });
        toast.success("Machine created");
      } else {
        await updateMachine(machine.id, {
          machine_type: form.machine_type || null,
          brand: form.brand || null,
          model: form.model || null,
          price: priceVal,
          location: form.location || null,
          description: form.description || null,
          is_active: form.is_active,
        });
        toast.success("Machine updated");
      }
      onSaved();
      onClose();
    } catch { toast.error(isNew ? "Failed to create machine" : "Failed to update machine"); }
    finally { setSaving(false); }
  };

  const Field = ({ label, field, type = "text" }: { label: string; field: keyof typeof form; type?: string }) => (
    <div>
      <label className="text-sm font-medium text-gray-700">{label}</label>
      <input
        type={type}
        className="input w-full mt-1"
        value={String(form[field])}
        onChange={(e) => setForm({ ...form, [field]: e.target.value })}
      />
    </div>
  );

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-lg p-6 space-y-4 max-h-[90vh] overflow-y-auto">
        <div className="flex items-center justify-between">
          <h3 className="text-lg font-semibold">{isNew ? "Add Machine" : "Edit Machine"}</h3>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-700"><X className="w-5 h-5" /></button>
        </div>

        {isNew && websites.length > 0 && (
          <div>
            <label className="text-sm font-medium text-gray-700">Source Website</label>
            <select
              className="input w-full mt-1"
              value={form.website_id}
              onChange={(e) => setForm({ ...form, website_id: e.target.value })}
            >
              {websites.map((w) => <option key={w.id} value={w.id}>{w.name}</option>)}
            </select>
          </div>
        )}

        <div className="grid grid-cols-2 gap-3">
          <Field label="Brand" field="brand" />
          <Field label="Model" field="model" />
          <Field label="Machine Type" field="machine_type" />
          <Field label="Price" field="price" type="number" />
          <Field label="Currency" field="currency" />
          <Field label="Location" field="location" />
          <div className="col-span-2">
            <Field label="Listing URL" field="machine_url" />
          </div>
          <div className="flex items-center gap-2 pt-4">
            <input
              type="checkbox"
              id="is_active_m"
              checked={form.is_active}
              onChange={(e) => setForm({ ...form, is_active: e.target.checked })}
            />
            <label htmlFor="is_active_m" className="text-sm cursor-pointer">Active</label>
          </div>
        </div>
        <div>
          <label className="text-sm font-medium text-gray-700">Description</label>
          <textarea className="input w-full mt-1 h-20 resize-none" value={form.description} onChange={(e) => setForm({ ...form, description: e.target.value })} />
        </div>

        <div className="flex justify-end gap-3 pt-2">
          <button onClick={onClose} className="btn-secondary">Cancel</button>
          <button onClick={handleSave} disabled={saving} className="btn-primary flex items-center gap-2">
            <Check className="w-4 h-4" />{saving ? "Saving..." : (isNew ? "Create" : "Save")}
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Status badge ───────────────────────────────────────────────────────────────
function StatusDot({ active }: { active: boolean }) {
  return (
    <span className={`inline-flex items-center gap-1.5 text-xs font-medium ${active ? "text-green-700" : "text-red-500"}`}>
      <span className={`w-2 h-2 rounded-full ${active ? "bg-green-500" : "bg-red-400"}`} />
      {active ? "Active" : "Inactive"}
    </span>
  );
}

function CrawlStatusBadge({ status }: { status: string }) {
  const map: Record<string, string> = {
    success: "bg-green-100 text-green-800",
    error:   "bg-red-100 text-red-800",
    running: "bg-blue-100 text-blue-800",
  };
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${map[status] ?? "bg-gray-100 text-gray-700"}`}>
      {status}
    </span>
  );
}

// ── Sort header ────────────────────────────────────────────────────────────────
function SortTh({
  label, field, sortField, sortDir, onSort,
}: { label: string; field: string; sortField: string | null; sortDir: SortDir; onSort: (f: string) => void }) {
  const active = sortField === field;
  return (
    <th
      className="px-3 py-3 text-left text-xs font-semibold text-gray-600 cursor-pointer select-none whitespace-nowrap hover:text-gray-900"
      onClick={() => onSort(field)}
    >
      <span className="inline-flex items-center gap-1">
        {label}
        <ArrowUpDown className={`w-3 h-3 ${active ? "text-blue-600" : "text-gray-400"}`} />
      </span>
    </th>
  );
}

// ── Main Admin Page ────────────────────────────────────────────────────────────
export default function AdminPage() {
  const { user } = useAuthStore();
  const router = useRouter();

  // Track whether the Zustand store has hydrated from localStorage.
  // On first render (SSR / before hydration) `user` is null even when the
  // token exists — without this guard the auth check fires immediately and
  // redirects to login on every refresh.
  const [hydrated, setHydrated] = useState(false);
  useEffect(() => { setHydrated(true); }, []);

  const [section, setSection] = useState<Section>("machines");
  const [collapsed, setCollapsed] = useState(false);

  // ── machines state ──
  const [machines, setMachines] = useState<any>({ total: 0, items: [] });
  const [machLoading, setMachLoading] = useState(false);
  const [machPage, setMachPage] = useState(1);
  const [machPerPage, setMachPerPage] = useState(50);
  const [machSearch, setMachSearch] = useState("");
  const [machStatus, setMachStatus] = useState<"all" | "active" | "inactive">("all");
  const [machType, setMachType] = useState("");
  const [machBrand, setMachBrand] = useState("");
  const [machSortField, setMachSortField] = useState<string | null>(null);
  const [machSortDir, setMachSortDir] = useState<SortDir>("asc");
  const [selectedIds, setSelectedIds] = useState<Set<number>>(new Set());
  const [editMachine, setEditMachine] = useState<any | null>(null);
  const [addMachineOpen, setAddMachineOpen] = useState(false);

  // ── other sections state ──
  const [stats, setStats] = useState<any>(null);
  const [websites, setWebsites] = useState<any[]>([]);
  const [logs, setLogs] = useState<any>({ total: 0, items: [] });
  const [loading, setLoading] = useState(false);
  const [newSite, setNewSite] = useState({ name: "", url: "", description: "" });
  const [editWebsite, setEditWebsite] = useState<any | null>(null);
  const [trainWebsite, setTrainWebsite] = useState<any | null>(null);
  const [expandedLog, setExpandedLog] = useState<number | null>(null);

  const searchDebounce = useRef<ReturnType<typeof setTimeout> | null>(null);

  // ── Auth guard — only runs after store has hydrated from localStorage ──
  useEffect(() => {
    if (!hydrated) return;
    if (!user?.is_admin) router.push("/auth/login");
  }, [hydrated, user]);

  // ── Load machines with debounce ──
  const loadMachines = useCallback(async (page = machPage, perPage = machPerPage) => {
    setMachLoading(true);
    try {
      const params: any = {
        skip: (page - 1) * perPage,
        limit: perPage,
      };
      if (machSearch.trim()) params.q = machSearch.trim();
      if (machType.trim()) params.machine_type = machType.trim();
      if (machBrand.trim()) params.brand = machBrand.trim();
      if (machStatus === "active") params.is_active = true;
      if (machStatus === "inactive") params.is_active = false;
      const res = await getAdminMachines(params);
      setMachines(res);
    } catch { toast.error("Failed to load machines"); }
    finally { setMachLoading(false); }
  }, [machPage, machPerPage, machSearch, machType, machBrand, machStatus]);

  // ── Load other sections ──
  const loadSection = useCallback(async () => {
    if (section === "machines") { loadMachines(machPage, machPerPage); return; }
    setLoading(true);
    try {
      if (section === "dashboard") setStats(await getAdminStats());
      if (section === "websites") setWebsites(await listWebsites());
      if (section === "logs") setLogs(await getCrawlLogs());
    } catch { toast.error("Failed to load data"); }
    finally { setLoading(false); }
  }, [section, machPage, machPerPage]);

  useEffect(() => {
    if (section !== "machines") loadSection();
    else loadMachines(1, machPerPage);
    setMachPage(1);
  }, [section]);

  // Reload machines when filters/page change
  useEffect(() => {
    if (section !== "machines") return;
    if (searchDebounce.current) clearTimeout(searchDebounce.current);
    searchDebounce.current = setTimeout(() => loadMachines(machPage, machPerPage), 300);
    return () => { if (searchDebounce.current) clearTimeout(searchDebounce.current); };
  }, [machSearch, machType, machBrand, machStatus, machPage, machPerPage]);

  const totalPages = Math.max(1, Math.ceil(machines.total / machPerPage));

  const handleSort = (field: string) => {
    if (machSortField === field) setMachSortDir(d => d === "asc" ? "desc" : "asc");
    else { setMachSortField(field); setMachSortDir("asc"); }
  };

  const sortedItems = [...(machines.items ?? [])].sort((a, b) => {
    if (!machSortField) return 0;
    const av = a[machSortField] ?? "";
    const bv = b[machSortField] ?? "";
    const cmp = String(av).localeCompare(String(bv), undefined, { numeric: true });
    return machSortDir === "asc" ? cmp : -cmp;
  });

  const toggleSelect = (id: number) => {
    setSelectedIds(prev => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });
  };
  const toggleAll = () => {
    if (selectedIds.size === sortedItems.length) setSelectedIds(new Set());
    else setSelectedIds(new Set(sortedItems.map((m: any) => m.id)));
  };

  const handleDeleteMachine = async (id: number) => {
    if (!confirm("Delete this machine?")) return;
    try {
      await deleteMachine(id);
      toast.success("Machine deleted");
      loadMachines(machPage, machPerPage);
    } catch { toast.error("Failed to delete"); }
  };

  const handleDeleteWebsite = async (id: number) => {
    if (!confirm("Delete website and all its machines?")) return;
    try {
      await deleteWebsite(id);
      toast.success("Website deleted");
      setWebsites(await listWebsites());
    } catch { toast.error("Failed to delete"); }
  };

  const handleStartCrawl = async (id: number) => {
    try {
      const res = await startCrawl(id);
      toast.success(`Crawl started (${res.task_id?.slice(0, 8)}...)`);
      setWebsites(await listWebsites());
    } catch { toast.error("Failed to start crawl"); }
  };

  const clearFilters = () => {
    setMachSearch(""); setMachStatus("all"); setMachType(""); setMachBrand("");
    setMachPage(1);
  };

  // ── Unique types/brands from current page (for dropdown hints) ──
  const uniqueTypes = Array.from(new Set(machines.items?.map((m: any) => m.machine_type).filter(Boolean))).slice(0, 30);
  const uniqueBrands = Array.from(new Set(machines.items?.map((m: any) => m.brand).filter(Boolean))).slice(0, 30);

  // Don't render anything until hydrated — prevents flash of login redirect
  if (!hydrated) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-100">
        <RefreshCw className="w-6 h-6 animate-spin text-gray-400" />
      </div>
    );
  }

  return (
    <div className="min-h-screen flex bg-gray-100">
      {/* Modals */}
      {editWebsite && (
        <EditWebsiteModal
          site={editWebsite}
          onClose={() => setEditWebsite(null)}
          onSaved={() => listWebsites().then(setWebsites)}
        />
      )}
      {trainWebsite && (
        <TrainWebsiteModal
          site={trainWebsite}
          onClose={() => setTrainWebsite(null)}
          onSaved={() => listWebsites().then(setWebsites)}
        />
      )}
      {(editMachine || addMachineOpen) && (
        <MachineModal
          machine={addMachineOpen ? null : editMachine}
          websites={websites.length ? websites : []}
          onClose={() => { setEditMachine(null); setAddMachineOpen(false); }}
          onSaved={() => loadMachines(machPage, machPerPage)}
        />
      )}

      {/* ── Sidebar ─────────────────────────────────────────────────────────── */}
      <aside
        className={`${collapsed ? "w-16" : "w-64"} bg-[#1c3344] text-white flex flex-col shrink-0 transition-all duration-200 min-h-screen`}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-4 py-5 border-b border-white/10">
          {!collapsed && (
            <div>
              <div className="font-bold text-base leading-tight">Admin Panel</div>
              <div className="text-xs text-white/50 mt-0.5">Manage your platform</div>
            </div>
          )}
          <button
            onClick={() => setCollapsed(c => !c)}
            className="p-1.5 rounded hover:bg-white/10 text-white/70 hover:text-white ml-auto"
          >
            {collapsed ? <ChevronRight className="w-4 h-4" /> : <ChevronLeft className="w-4 h-4" />}
          </button>
        </div>

        {/* Reset All Data */}
        {!collapsed && (
          <div className="px-4 py-3 border-b border-white/10">
            <button
              onClick={() => {
                if (!confirm("This will delete ALL machines and reset all data. Are you sure?")) return;
                toast.error("Reset not implemented — protect your data!");
              }}
              className="w-full flex items-center justify-center gap-2 bg-red-600 hover:bg-red-700 text-white text-sm font-semibold px-4 py-2 rounded-lg transition-colors"
            >
              <Trash2 className="w-4 h-4" /> Reset All Data
            </button>
          </div>
        )}

        {/* Nav */}
        <nav className="flex-1 px-2 py-4 space-y-1 overflow-y-auto">
          {!collapsed && (
            <div className="px-2 pb-2 text-xs font-semibold uppercase tracking-widest text-white/30">
              Management
            </div>
          )}

          {NAV_ITEMS.map((item) => {
            const active = section === item.key;
            return (
              <button
                key={item.key}
                onClick={() => setSection(item.key)}
                title={collapsed ? item.label : undefined}
                className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg transition-colors text-left ${
                  active
                    ? "bg-white/15 text-white border-l-2 border-white"
                    : "text-white/70 hover:bg-white/10 hover:text-white border-l-2 border-transparent"
                }`}
              >
                <span className="shrink-0">{item.icon}</span>
                {!collapsed && (
                  <div className="min-w-0">
                    <div className="text-sm font-medium leading-tight">{item.label}</div>
                    <div className="text-xs text-white/40 leading-tight truncate">{item.desc}</div>
                  </div>
                )}
              </button>
            );
          })}

        </nav>

        {/* User footer */}
        {!collapsed && (
          <div className="px-4 py-3 border-t border-white/10">
            <div className="text-xs text-white/40 truncate">{user?.email}</div>
            <a href="/" className="text-xs text-white/50 hover:text-white mt-0.5 inline-block">← Public Site</a>
          </div>
        )}
      </aside>

      {/* ── Main content ────────────────────────────────────────────────────── */}
      <main className="flex-1 flex flex-col min-w-0 overflow-hidden">

        {/* ══ MACHINES ══════════════════════════════════════════════════════ */}
        {section === "machines" && (
          <div className="flex flex-col h-full">
            {/* Top bar */}
            <div className="bg-white border-b border-gray-200 px-6 py-3 flex items-center justify-between gap-4 flex-wrap shrink-0">
              <div className="flex items-center gap-3 flex-wrap">
                <h2 className="text-base font-bold text-gray-900 whitespace-nowrap">
                  Machines ({machLoading ? "..." : machines.total?.toLocaleString()})
                </h2>
                {/* Show per page */}
                <div className="flex items-center gap-1 text-sm text-gray-500">
                  <span>Show:</span>
                  <select
                    value={machPerPage}
                    onChange={(e) => { setMachPerPage(Number(e.target.value)); setMachPage(1); }}
                    className="border border-gray-200 rounded px-2 py-1 text-sm bg-white focus:outline-none focus:ring-1 focus:ring-blue-500"
                  >
                    {[25, 50, 100].map(n => <option key={n} value={n}>{n}</option>)}
                  </select>
                </div>
                {/* Pagination */}
                <span className="text-sm text-gray-500 whitespace-nowrap">
                  Page {machPage} of {totalPages}
                </span>
                <button
                  disabled={machPage <= 1}
                  onClick={() => setMachPage(p => Math.max(1, p - 1))}
                  className="px-3 py-1 text-sm border border-gray-200 rounded hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  Prev
                </button>
                <button
                  disabled={machPage >= totalPages}
                  onClick={() => setMachPage(p => Math.min(totalPages, p + 1))}
                  className="px-3 py-1 text-sm border border-gray-200 rounded hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  Next
                </button>
              </div>

              {/* Action buttons */}
              <div className="flex items-center gap-2 flex-wrap">
                <button className="flex items-center gap-1.5 px-3 py-1.5 text-sm border border-gray-200 rounded-lg hover:bg-gray-50 text-gray-600">
                  <SlidersHorizontal className="w-4 h-4" /> Columns
                </button>
                <button
                  onClick={() => { if (!websites.length) listWebsites().then(setWebsites); setAddMachineOpen(true); }}
                  className="flex items-center gap-1.5 px-3 py-1.5 text-sm bg-gray-900 text-white rounded-lg hover:bg-gray-800 font-medium"
                >
                  <Plus className="w-4 h-4" /> Add Machine
                </button>
                <button
                  onClick={async () => {
                    try { const r = await fillMachineTypes(); toast.success(`Filled ${r.updated} types`); loadMachines(machPage, machPerPage); }
                    catch { toast.error("Failed"); }
                  }}
                  className="flex items-center gap-1.5 px-3 py-1.5 text-sm border border-gray-200 rounded-lg hover:bg-gray-50 text-gray-600"
                >
                  <Wrench className="w-4 h-4" /> Fill Types
                </button>
                <a
                  href={exportMachinesExcelUrl()} target="_blank" rel="noopener noreferrer"
                  className="flex items-center gap-1.5 px-3 py-1.5 text-sm border border-gray-200 rounded-lg hover:bg-gray-50 text-gray-600"
                >
                  <Download className="w-4 h-4" /> Export
                </a>
                <button onClick={clearFilters} className="px-3 py-1.5 text-sm border border-gray-200 rounded-lg hover:bg-gray-50 text-gray-600">
                  Clear
                </button>
              </div>
            </div>

            {/* Filter row */}
            <div className="bg-white border-b border-gray-200 px-6 py-2 flex items-center gap-2 flex-wrap shrink-0">
              {/* Search */}
              <div className="relative">
                <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-gray-400" />
                <input
                  value={machSearch}
                  onChange={(e) => { setMachSearch(e.target.value); setMachPage(1); }}
                  placeholder="SKU, model, brand..."
                  className="pl-8 pr-3 py-1.5 text-sm border border-gray-200 rounded-lg w-44 focus:outline-none focus:ring-1 focus:ring-blue-500 bg-white"
                />
              </div>

              {/* Status */}
              <select
                value={machStatus}
                onChange={(e) => { setMachStatus(e.target.value as any); setMachPage(1); }}
                className="px-3 py-1.5 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-1 focus:ring-blue-500 bg-white"
              >
                <option value="all">All Status</option>
                <option value="active">Active</option>
                <option value="inactive">Inactive</option>
              </select>

              {/* Type */}
              <select
                value={machType}
                onChange={(e) => { setMachType(e.target.value); setMachPage(1); }}
                className="px-3 py-1.5 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-1 focus:ring-blue-500 bg-white max-w-[180px]"
              >
                <option value="">All Types</option>
                {(uniqueTypes as string[]).map(t => <option key={t} value={t}>{t}</option>)}
              </select>

              {/* Brand */}
              <select
                value={machBrand}
                onChange={(e) => { setMachBrand(e.target.value); setMachPage(1); }}
                className="px-3 py-1.5 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-1 focus:ring-blue-500 bg-white max-w-[160px]"
              >
                <option value="">All Brands</option>
                {(uniqueBrands as string[]).map(b => <option key={b} value={b}>{b}</option>)}
              </select>

              {/* Refresh */}
              <button
                onClick={() => loadMachines(machPage, machPerPage)}
                className="p-1.5 border border-gray-200 rounded-lg hover:bg-gray-50 text-gray-500"
                title="Refresh"
              >
                <RefreshCw className={`w-3.5 h-3.5 ${machLoading ? "animate-spin" : ""}`} />
              </button>

              {selectedIds.size > 0 && (
                <span className="text-xs text-blue-600 font-medium">{selectedIds.size} selected</span>
              )}
            </div>

            {/* Table */}
            <div className="flex-1 overflow-auto">
              <table className="w-full text-sm border-collapse">
                <thead className="bg-gray-50 sticky top-0 z-10">
                  <tr className="border-b border-gray-200">
                    <th className="px-3 py-3 w-8">
                      <input
                        type="checkbox"
                        checked={sortedItems.length > 0 && selectedIds.size === sortedItems.length}
                        onChange={toggleAll}
                        className="rounded"
                      />
                    </th>
                    <th className="px-3 py-3 text-left text-xs font-semibold text-gray-600 w-16">Image</th>
                    <SortTh label="Model"    field="model"        sortField={machSortField} sortDir={machSortDir} onSort={handleSort} />
                    <SortTh label="Type"     field="machine_type" sortField={machSortField} sortDir={machSortDir} onSort={handleSort} />
                    <SortTh label="Brand"    field="brand"        sortField={machSortField} sortDir={machSortDir} onSort={handleSort} />
                    <SortTh label="Location" field="location"     sortField={machSortField} sortDir={machSortDir} onSort={handleSort} />
                    <SortTh label="Price"    field="price"        sortField={machSortField} sortDir={machSortDir} onSort={handleSort} />
                    <th className="px-3 py-3 text-left text-xs font-semibold text-gray-600">Premium</th>
                    <th className="px-3 py-3 text-left text-xs font-semibold text-gray-600">Status</th>
                    <th className="px-3 py-3 text-left text-xs font-semibold text-gray-600">E-URL</th>
                    <th className="px-3 py-3 text-left text-xs font-semibold text-gray-600">Actions</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {machLoading && (
                    <tr>
                      <td colSpan={11} className="px-4 py-12 text-center text-gray-400">
                        <RefreshCw className="w-5 h-5 animate-spin mx-auto mb-2" />
                        Loading machines...
                      </td>
                    </tr>
                  )}
                  {!machLoading && sortedItems.map((m: any) => (
                    <tr
                      key={m.id}
                      className={`hover:bg-blue-50/40 transition-colors ${!m.is_active ? "opacity-60" : ""} ${selectedIds.has(m.id) ? "bg-blue-50" : "bg-white"}`}
                    >
                      {/* Checkbox */}
                      <td className="px-3 py-2.5">
                        <input type="checkbox" checked={selectedIds.has(m.id)} onChange={() => toggleSelect(m.id)} className="rounded" />
                      </td>

                      {/* Thumbnail */}
                      <td className="px-3 py-2.5">
                        {m.thumbnail_url ? (
                          <img
                            src={m.thumbnail_url}
                            alt={m.model ?? "machine"}
                            className="w-10 h-10 object-cover rounded border border-gray-200"
                            onError={(e) => { (e.target as HTMLImageElement).style.display = "none"; }}
                          />
                        ) : (
                          <div className="w-10 h-10 rounded border border-gray-200 bg-gray-100 flex items-center justify-center">
                            <ImageIcon className="w-4 h-4 text-gray-300" />
                          </div>
                        )}
                      </td>

                      {/* Model */}
                      <td className="px-3 py-2.5">
                        <div className="font-medium text-gray-900 max-w-[180px] truncate" title={m.model ?? ""}>{m.model || "—"}</div>
                        <div className="text-xs text-gray-400 truncate max-w-[180px]">{m.website_source || ""}</div>
                      </td>

                      {/* Type */}
                      <td className="px-3 py-2.5">
                        {m.machine_type ? (
                          <span className="inline-block px-2 py-0.5 rounded-full bg-blue-50 text-blue-700 font-medium text-xs">{m.machine_type}</span>
                        ) : <span className="text-xs text-gray-300 italic">—</span>}
                      </td>

                      {/* Brand */}
                      <td className="px-3 py-2.5 text-gray-700 font-medium">{m.brand || <span className="text-gray-300 italic text-xs">—</span>}</td>

                      {/* Location */}
                      <td className="px-3 py-2.5 max-w-[120px] truncate" title={m.location ?? ""}>
                        {m.location ? (
                          <span className="text-gray-600 text-xs">{m.location}</span>
                        ) : <span className="text-xs text-gray-300 italic">—</span>}
                      </td>

                      {/* Price */}
                      <td className="px-3 py-2.5">
                        {m.price != null ? (
                          <span className="font-semibold text-gray-800 whitespace-nowrap">
                            {m.currency === "EUR" ? "€" : m.currency === "GBP" ? "£" : "$"}{Number(m.price).toLocaleString()}
                          </span>
                        ) : <span className="text-xs text-gray-300 italic">—</span>}
                      </td>

                      {/* Premium */}
                      <td className="px-3 py-2.5">
                        <Star className="w-4 h-4 text-gray-300" />
                      </td>

                      {/* Status */}
                      <td className="px-3 py-2.5">
                        <StatusDot active={m.is_active} />
                      </td>

                      {/* E-URL */}
                      <td className="px-3 py-2.5">
                        {m.machine_url ? (
                          <a href={m.machine_url} target="_blank" rel="noopener noreferrer" className="text-gray-400 hover:text-blue-600">
                            <ExternalLink className="w-4 h-4" />
                          </a>
                        ) : <span className="text-gray-200">—</span>}
                      </td>

                      {/* Actions */}
                      <td className="px-3 py-2.5">
                        <div className="flex items-center gap-1">
                          <button
                            onClick={() => setEditMachine(m)}
                            className="p-1 rounded hover:bg-blue-100 text-blue-500 hover:text-blue-700"
                            title="Edit"
                          >
                            <Pencil className="w-3.5 h-3.5" />
                          </button>
                          <button
                            onClick={() => handleDeleteMachine(m.id)}
                            className="p-1 rounded hover:bg-red-100 text-red-400 hover:text-red-600"
                            title="Delete"
                          >
                            <Trash2 className="w-3.5 h-3.5" />
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                  {!machLoading && sortedItems.length === 0 && (
                    <tr>
                      <td colSpan={11} className="px-4 py-12 text-center text-gray-400">No machines found.</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>

            {/* Bottom pagination bar */}
            <div className="bg-white border-t border-gray-200 px-6 py-3 flex items-center justify-between text-sm text-gray-500 shrink-0">
              <span>
                {machines.total > 0
                  ? `Showing ${((machPage - 1) * machPerPage) + 1}–${Math.min(machPage * machPerPage, machines.total)} of ${machines.total?.toLocaleString()} machines`
                  : "No machines"}
              </span>
              <div className="flex items-center gap-2">
                <button
                  disabled={machPage <= 1}
                  onClick={() => setMachPage(1)}
                  className="px-2 py-1 border border-gray-200 rounded hover:bg-gray-50 disabled:opacity-40"
                >«</button>
                <button
                  disabled={machPage <= 1}
                  onClick={() => setMachPage(p => p - 1)}
                  className="px-3 py-1 border border-gray-200 rounded hover:bg-gray-50 disabled:opacity-40"
                >Prev</button>
                <span className="px-3 py-1 bg-gray-900 text-white rounded text-xs font-medium">{machPage}</span>
                <button
                  disabled={machPage >= totalPages}
                  onClick={() => setMachPage(p => p + 1)}
                  className="px-3 py-1 border border-gray-200 rounded hover:bg-gray-50 disabled:opacity-40"
                >Next</button>
                <button
                  disabled={machPage >= totalPages}
                  onClick={() => setMachPage(totalPages)}
                  className="px-2 py-1 border border-gray-200 rounded hover:bg-gray-50 disabled:opacity-40"
                >»</button>
              </div>
            </div>
          </div>
        )}

        {/* ══ DASHBOARD ═════════════════════════════════════════════════════ */}
        {section === "dashboard" && (
          <div className="p-6 space-y-6 overflow-auto">
            <div className="flex items-center justify-between">
              <h2 className="text-xl font-bold text-gray-900">Dashboard</h2>
              <button onClick={loadSection} className="flex items-center gap-2 text-sm btn-secondary">
                <RefreshCw className="w-4 h-4" /> Refresh
              </button>
            </div>

            {loading && <div className="text-center py-12 text-gray-400"><RefreshCw className="w-5 h-5 animate-spin mx-auto" /></div>}

            {!loading && stats && (
              <>
                <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
                  {[
                    { label: "Total Machines",    value: stats.total_machines?.toLocaleString(),  icon: <Cpu className="w-5 h-5" />,    color: "text-blue-600 bg-blue-50" },
                    { label: "Indexed Websites",  value: stats.total_websites,                    icon: <Globe className="w-5 h-5" />,   color: "text-green-600 bg-green-50" },
                    { label: "Users",             value: stats.total_users,                       icon: <Users className="w-5 h-5" />,   color: "text-purple-600 bg-purple-50" },
                    { label: "Total Searches",    value: stats.total_searches?.toLocaleString(),  icon: <Search className="w-5 h-5" />,  color: "text-orange-600 bg-orange-50" },
                  ].map((s) => (
                    <div key={s.label} className="card p-5">
                      <div className={`inline-flex p-2 rounded-lg mb-3 ${s.color}`}>{s.icon}</div>
                      <div className="text-2xl font-bold text-gray-900">{s.value ?? "—"}</div>
                      <div className="text-sm text-gray-500 mt-1">{s.label}</div>
                    </div>
                  ))}
                </div>

                <div className="card p-5">
                  <h3 className="font-semibold text-gray-900 mb-4">Recent Crawls</h3>
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="text-gray-500 text-left border-b border-gray-100">
                        <th className="pb-2 font-medium">Website</th>
                        <th className="pb-2 font-medium">Status</th>
                        <th className="pb-2 font-medium">Found</th>
                        <th className="pb-2 font-medium">New</th>
                        <th className="pb-2 font-medium">Started</th>
                      </tr>
                    </thead>
                    <tbody>
                      {stats.recent_crawls?.map((c: any) => (
                        <tr key={c.id} className="border-b border-gray-50">
                          <td className="py-2 font-medium text-gray-900">{c.website_name}</td>
                          <td className="py-2"><CrawlStatusBadge status={c.status} /></td>
                          <td className="py-2 text-gray-700">{c.machines_found ?? 0}</td>
                          <td className="py-2 text-green-600 font-medium">{c.machines_new ?? 0}</td>
                          <td className="py-2 text-gray-400">{c.started_at ? new Date(c.started_at).toLocaleString() : "—"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            )}
          </div>
        )}

        {/* ══ WEBSITES ══════════════════════════════════════════════════════ */}
        {section === "websites" && (
          <div className="p-6 space-y-6 overflow-auto">
            <div className="flex items-center justify-between flex-wrap gap-3">
              <h2 className="text-xl font-bold text-gray-900">Web Sources ({websites.length})</h2>
              <div className="flex gap-2 flex-wrap">
                <button onClick={loadSection} className="btn-secondary flex items-center gap-2 text-sm"><RefreshCw className="w-4 h-4" /></button>
                <button
                  onClick={async () => {
                    try { const r = await fixWebsiteNames(); toast.success(`Fixed ${r.fixed} name(s)`); setWebsites(await listWebsites()); }
                    catch { toast.error("Failed"); }
                  }}
                  className="btn-secondary flex items-center gap-2 text-sm"
                >
                  <Wrench className="w-4 h-4" /> Fix Names
                </button>
                <button
                  onClick={async () => {
                    try { await recalculateMachineCounts(); toast.success("Counts updated"); setWebsites(await listWebsites()); }
                    catch { toast.error("Failed"); }
                  }}
                  className="btn-secondary flex items-center gap-2 text-sm"
                >
                  <RefreshCw className="w-4 h-4" /> Fix Counts
                </button>
                <button onClick={async () => { try { await startAllCrawls(); toast.success("All crawls queued"); } catch { toast.error("Failed"); } }} className="btn-primary flex items-center gap-2 text-sm">
                  <Play className="w-4 h-4" /> Crawl All
                </button>
              </div>
            </div>

            {loading && <div className="text-center py-12 text-gray-400"><RefreshCw className="w-5 h-5 animate-spin mx-auto" /></div>}

            {!loading && (
              <>
                {/* Add website */}
                <div className="card p-5">
                  <h3 className="font-semibold text-gray-900 mb-4">Add New Website</h3>
                  <form
                    onSubmit={async (e) => {
                      e.preventDefault();
                      try {
                        await addWebsite(newSite);
                        toast.success("Website added!");
                        setNewSite({ name: "", url: "", description: "" });
                        setWebsites(await listWebsites());
                      } catch { toast.error("Failed to add website"); }
                    }}
                    className="flex gap-3 flex-wrap"
                  >
                    <input required placeholder="Name" value={newSite.name} onChange={(e) => setNewSite({ ...newSite, name: e.target.value })} className="input flex-1 min-w-36" />
                    <input
                      required placeholder="URL (https://...)" value={newSite.url}
                      onChange={(e) => {
                        const url = e.target.value;
                        let autoName = newSite.name;
                        if (!newSite.name && url.includes(".")) {
                          try {
                            const domain = new URL(url.startsWith("http") ? url : `https://${url}`).hostname;
                            autoName = domain.replace(/^www\./, "").split(".")[0].replace(/[-_]/g, " ").replace(/\b\w/g, c => c.toUpperCase());
                          } catch {}
                        }
                        setNewSite({ ...newSite, url, name: autoName });
                      }}
                      className="input flex-1 min-w-52"
                    />
                    <input placeholder="Description (optional)" value={newSite.description} onChange={(e) => setNewSite({ ...newSite, description: e.target.value })} className="input flex-1 min-w-36" />
                    <button type="submit" className="btn-primary">Add Website</button>
                  </form>
                </div>

                {/* Websites table */}
                <div className="card overflow-hidden">
                  <table className="w-full text-sm">
                    <thead className="bg-gray-50 border-b border-gray-200">
                      <tr className="text-gray-500 text-left">
                        <th className="px-4 py-3 font-semibold">Name / URL</th>
                        <th className="px-4 py-3 font-semibold">Status</th>
                        <th className="px-4 py-3 font-semibold">① Discovered</th>
                        <th className="px-4 py-3 font-semibold">② URLs</th>
                        <th className="px-4 py-3 font-semibold">③ Extracted</th>
                        <th className="px-4 py-3 font-semibold">Last Crawl</th>
                        <th className="px-4 py-3 font-semibold">Actions</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100">
                      {websites.map((site) => (
                        <tr key={site.id} className="hover:bg-gray-50">
                          <td className="px-4 py-3">
                            <div className="font-medium text-gray-900">{site.name}</div>
                            <div className="text-gray-400 text-xs truncate max-w-xs">{site.url}</div>
                          </td>
                          <td className="px-4 py-3">
                            <CrawlStatusBadge status={site.crawl_status} />
                            {!site.is_active && <span className="ml-1 text-xs text-red-400">(inactive)</span>}
                          </td>
                          {/* ① Discovered column */}
                          <td className="px-4 py-3">
                            <div className="flex flex-col gap-1">
                              {site.discovery_status === "running" ? (
                                <span className="text-xs text-blue-500 flex items-center gap-1"><RefreshCw className="w-3 h-3 animate-spin" /> Scanning…</span>
                              ) : site.discovered_count != null ? (
                                <span className="text-sm font-bold text-indigo-600">{site.discovered_count.toLocaleString()}</span>
                              ) : (
                                <span className="text-xs text-gray-400">—</span>
                              )}
                              <button
                                onClick={async () => {
                                  try {
                                    await discoverWebsite(site.id);
                                    toast.success("Discovery started — URL collection will follow automatically");
                                    const poll = setInterval(async () => {
                                      const fresh = await listWebsites();
                                      setWebsites(fresh);
                                      const s = fresh.find((w: any) => w.id === site.id);
                                      if (s && s.discovery_status !== "running" && s.url_collection_status !== "running") {
                                        clearInterval(poll);
                                      }
                                    }, 3000);
                                    setTimeout(() => clearInterval(poll), 120000);
                                  } catch { toast.error("Discovery failed"); }
                                }}
                                disabled={site.discovery_status === "running" || site.url_collection_status === "running"}
                                className="text-xs text-indigo-500 hover:text-indigo-700 disabled:opacity-40 flex items-center gap-1 w-fit"
                                title="Phase 1: Discover count → Phase 2: Collect URLs (automatic)"
                              >
                                <Search className="w-3 h-3" /> Discover
                              </button>
                            </div>
                          </td>
                          {/* ② URLs collected column */}
                          <td className="px-4 py-3">
                            <div className="flex flex-col gap-1">
                              {site.url_collection_status === "running" ? (
                                <span className="text-xs text-blue-500 flex items-center gap-1"><RefreshCw className="w-3 h-3 animate-spin" /> Collecting…</span>
                              ) : site.urls_collected != null ? (
                                <span className={`text-sm font-bold ${
                                  site.discovered_count && site.urls_collected >= site.discovered_count * 0.9
                                    ? "text-green-600" : "text-yellow-600"
                                }`}>
                                  {site.urls_collected.toLocaleString()}
                                  {site.discovered_count ? ` / ${site.discovered_count.toLocaleString()}` : ""}
                                </span>
                              ) : (
                                <span className="text-xs text-gray-400">—</span>
                              )}
                              {site.discovery_status === "done" && site.url_collection_status !== "running" && (
                                <button
                                  onClick={async () => {
                                    try {
                                      await collectUrlsWebsite(site.id);
                                      toast.success("URL collection started!");
                                      setTimeout(async () => setWebsites(await listWebsites()), 3000);
                                    } catch { toast.error("URL collection failed"); }
                                  }}
                                  className="text-xs text-yellow-600 hover:text-yellow-800 flex items-center gap-1 w-fit"
                                  title="Phase 2: Collect all machine URLs"
                                >
                                  <RefreshCw className="w-3 h-3" /> Re-collect
                                </button>
                              )}
                            </div>
                          </td>
                          {/* ③ Extracted column */}
                          <td className="px-4 py-3">
                            <div className="flex flex-col gap-1">
                              <span className="text-sm font-medium text-gray-700">
                                {site.machine_count?.toLocaleString()}
                                {site.urls_collected != null && site.urls_collected > 0 && (
                                  <span className={`ml-1 text-xs font-semibold ${site.machine_count >= site.urls_collected * 0.9 ? "text-green-500" : "text-orange-400"}`}>
                                    / {site.urls_collected.toLocaleString()}
                                  </span>
                                )}
                              </span>
                              <button
                                onClick={() => handleStartCrawl(site.id)}
                                disabled={site.url_collection_status === "running" || site.crawl_status === "running"}
                                className="text-xs text-green-600 hover:text-green-800 disabled:opacity-40 flex items-center gap-1 w-fit"
                                title="Phase 3: Extract all machines from collected URLs"
                              >
                                <Play className="w-3 h-3" /> Extract
                              </button>
                            </div>
                          </td>
                          <td className="px-4 py-3 text-gray-400 text-xs">
                            {site.last_crawled_at ? new Date(site.last_crawled_at).toLocaleDateString() : "Never"}
                          </td>
                          <td className="px-4 py-3">
                            <div className="flex gap-1">
                              <button
                                onClick={() => setTrainWebsite(site)}
                                className="p-1.5 hover:bg-purple-50 rounded text-purple-500"
                                title="Train scraper selectors"
                              >
                                <Brain className="w-4 h-4" />
                              </button>
                              <button onClick={() => setEditWebsite(site)} className="p-1.5 hover:bg-blue-50 rounded text-blue-500" title="Edit website"><Pencil className="w-4 h-4" /></button>
                              <button onClick={() => handleDeleteWebsite(site.id)} className="p-1.5 hover:bg-red-50 rounded text-red-500" title="Delete website"><Trash2 className="w-4 h-4" /></button>
                            </div>
                          </td>
                        </tr>
                      ))}
                      {websites.length === 0 && (
                        <tr><td colSpan={7} className="px-4 py-10 text-center text-gray-400">No websites added yet.</td></tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </>
            )}
          </div>
        )}

        {/* ══ CRAWL LOGS ════════════════════════════════════════════════════ */}
        {section === "logs" && (
          <div className="p-6 space-y-4 overflow-auto">
            <div className="flex items-center justify-between flex-wrap gap-3">
              <h2 className="text-xl font-bold text-gray-900">Crawl Logs ({logs.total})</h2>
              <div className="flex gap-2">
                <button
                  onClick={async () => {
                    try { const r = await fixStuckCrawls(); toast.success(`Fixed ${r.fixed_crawl_logs} stuck crawl(s)`); loadSection(); }
                    catch { toast.error("Failed"); }
                  }}
                  className="btn-secondary flex items-center gap-2 text-sm"
                >
                  <Wrench className="w-4 h-4" /> Fix Stuck
                </button>
                <button onClick={loadSection} className="btn-secondary flex items-center gap-2 text-sm">
                  <RefreshCw className="w-4 h-4" /> Refresh
                </button>
              </div>
            </div>

            <p className="text-xs text-gray-400">Click any row to expand scrapy output / error details.</p>

            {loading && <div className="text-center py-12 text-gray-400"><RefreshCw className="w-5 h-5 animate-spin mx-auto" /></div>}

            {!loading && (
              <div className="card overflow-hidden">
                <table className="w-full text-sm">
                  <thead className="bg-gray-50 border-b border-gray-200">
                    <tr className="text-gray-500 text-left">
                      <th className="px-4 py-3 w-8" />
                      <th className="px-4 py-3 font-semibold">Website</th>
                      <th className="px-4 py-3 font-semibold">Type</th>
                      <th className="px-4 py-3 font-semibold">Status</th>
                      <th className="px-4 py-3 font-semibold">Found</th>
                      <th className="px-4 py-3 font-semibold">New</th>
                      <th className="px-4 py-3 font-semibold">Errors</th>
                      <th className="px-4 py-3 font-semibold">Started</th>
                      <th className="px-4 py-3 font-semibold">Duration</th>
                      <th className="px-4 py-3 font-semibold">Action</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100">
                    {logs.items?.map((log: any) => {
                      const duration = log.finished_at && log.started_at
                        ? Math.round((new Date(log.finished_at).getTime() - new Date(log.started_at).getTime()) / 1000) + "s"
                        : "Running...";
                      const isExpanded = expandedLog === log.id;
                      const hasDetail = log.error_details || log.log_output;
                      return (
                        <>
                          <tr
                            key={log.id}
                            className={`hover:bg-gray-50 ${log.log_type === "discovery" ? "bg-indigo-50/40" : log.log_type === "url_collection" ? "bg-yellow-50/40" : ""} ${hasDetail ? "cursor-pointer" : ""}`}
                            onClick={() => hasDetail && setExpandedLog(isExpanded ? null : log.id)}
                          >
                            <td className="px-4 py-3 text-gray-400">
                              {hasDetail ? (isExpanded ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />) : null}
                            </td>
                            <td className="px-4 py-3">
                              <div className="font-medium text-gray-900">{log.website_name}</div>
                              {log.website_url && <div className="text-xs text-gray-400 truncate max-w-[160px]">{log.website_url}</div>}
                            </td>
                            <td className="px-4 py-3">
                              {log.log_type === "discovery" ? (
                                <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-indigo-100 text-indigo-700">
                                  <Search className="w-3 h-3" /> Discovery
                                </span>
                              ) : log.log_type === "url_collection" ? (
                                <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-yellow-100 text-yellow-700">
                                  <RefreshCw className="w-3 h-3" /> URL Collect
                                </span>
                              ) : (
                                <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-700">
                                  <Play className="w-3 h-3" /> Crawl
                                </span>
                              )}
                            </td>
                            <td className="px-4 py-3"><CrawlStatusBadge status={log.status} /></td>
                            <td className="px-4 py-3 font-medium">
                              {log.log_type === "discovery" && log.status === "success" ? (
                                <span className="text-indigo-600 font-bold">{(log.machines_found ?? 0).toLocaleString()} on site</span>
                              ) : log.log_type === "url_collection" && log.status === "success" ? (
                                <span className="text-yellow-700 font-bold">{(log.machines_found ?? 0).toLocaleString()} URLs</span>
                              ) : (log.machines_found ?? 0).toLocaleString()}
                            </td>
                            <td className="px-4 py-3 text-green-600 font-medium">{(log.log_type === "discovery" || log.log_type === "url_collection") ? "—" : (log.machines_new ?? 0)}</td>
                            <td className="px-4 py-3 text-red-500">{log.errors_count ?? 0}</td>
                            <td className="px-4 py-3 text-gray-400 text-xs">{new Date(log.started_at).toLocaleString()}</td>
                            <td className="px-4 py-3 text-gray-400">{duration}</td>
                            <td className="px-4 py-3" onClick={(e) => e.stopPropagation()}>
                              {log.log_type === "discovery" && log.status === "success" && log.machines_found > 0 ? (
                                <button
                                  onClick={async () => {
                                    try {
                                      await startCrawl(log.website_id);
                                      toast.success(`Crawl started for ${log.website_name}!`);
                                    } catch { toast.error("Failed to start crawl"); }
                                  }}
                                  className="flex items-center gap-1 px-2 py-1 bg-green-600 hover:bg-green-700 text-white text-xs rounded font-medium"
                                  title={`Start full crawl to extract all ${log.machines_found} machines`}
                                >
                                  <Play className="w-3 h-3" /> Start Crawl
                                </button>
                              ) : null}
                            </td>
                          </tr>
                          {isExpanded && (
                            <tr key={`${log.id}-detail`}>
                              <td colSpan={10} className="p-0 bg-gray-900">
                                {log.error_details && (
                                  <div className="px-6 pt-3 pb-1">
                                    <p className="text-xs font-semibold text-red-400 mb-1">Error Summary</p>
                                    <pre className="text-xs text-red-300 whitespace-pre-wrap font-mono bg-black/30 rounded p-2 max-h-32 overflow-auto">{log.error_details}</pre>
                                  </div>
                                )}
                                {log.log_output && (
                                  <div className="px-6 pt-2 pb-3">
                                    <p className="text-xs font-semibold text-green-400 mb-1">{log.log_type === "discovery" ? "Discovery Output" : "Scrapy Output"}</p>
                                    <pre className="text-xs text-green-300 whitespace-pre-wrap font-mono bg-black/30 rounded p-3 max-h-64 overflow-auto">{log.log_output}</pre>
                                  </div>
                                )}
                              </td>
                            </tr>
                          )}
                        </>
                      );
                    })}
                    {logs.items?.length === 0 && (
                      <tr><td colSpan={10} className="px-4 py-10 text-center text-gray-400">No crawl logs yet.</td></tr>
                    )}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )}
      </main>
    </div>
  );
}

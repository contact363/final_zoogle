"use client";

import { useEffect, useState } from "react";
import {
  getAdminStats, listWebsites, addWebsite, updateWebsite, deleteWebsite,
  startCrawl, startAllCrawls, fixStuckCrawls, getCrawlLogs,
  getAdminMachines, updateMachine, deleteMachine,
  exportMachinesExcelUrl,
} from "@/lib/api";
import { useAuthStore } from "@/lib/store";
import { useRouter } from "next/navigation";
import {
  Globe, Cpu, Users, Search, Play, Trash2, Download,
  BarChart3, FileText, Pencil, X, Check, RefreshCw, Wrench, ChevronDown, ChevronRight,
} from "lucide-react";
import toast from "react-hot-toast";

type Tab = "dashboard" | "websites" | "machines" | "logs";

// ── Edit modal for Website ────────────────────────────────────────────────────
function EditWebsiteModal({
  site,
  onClose,
  onSaved,
}: {
  site: any;
  onClose: () => void;
  onSaved: () => void;
}) {
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
    } catch {
      toast.error("Failed to update website");
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
        <div className="flex items-center justify-between">
          <h3 className="text-lg font-semibold text-steel-900">Edit Website</h3>
          <button onClick={onClose} className="text-steel-400 hover:text-steel-700"><X className="w-5 h-5" /></button>
        </div>

        <div className="space-y-3">
          <div>
            <label className="text-sm font-medium text-steel-700">Name</label>
            <input
              className="input w-full mt-1"
              value={form.name}
              onChange={(e) => setForm({ ...form, name: e.target.value })}
            />
          </div>
          <div>
            <label className="text-sm font-medium text-steel-700">Description</label>
            <textarea
              className="input w-full mt-1 h-20 resize-none"
              value={form.description}
              onChange={(e) => setForm({ ...form, description: e.target.value })}
            />
          </div>
          <div className="flex items-center gap-6">
            <label className="flex items-center gap-2 text-sm text-steel-700 cursor-pointer">
              <input
                type="checkbox"
                checked={form.is_active}
                onChange={(e) => setForm({ ...form, is_active: e.target.checked })}
              />
              Active
            </label>
            <label className="flex items-center gap-2 text-sm text-steel-700 cursor-pointer">
              <input
                type="checkbox"
                checked={form.crawl_enabled}
                onChange={(e) => setForm({ ...form, crawl_enabled: e.target.checked })}
              />
              Crawl Enabled
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

// ── Edit modal for Machine ────────────────────────────────────────────────────
function EditMachineModal({
  machine,
  onClose,
  onSaved,
}: {
  machine: any;
  onClose: () => void;
  onSaved: () => void;
}) {
  const [form, setForm] = useState({
    machine_type: machine.machine_type ?? "",
    brand: machine.brand ?? "",
    model: machine.model ?? "",
    price: machine.price != null ? String(machine.price) : "",
    location: machine.location ?? "",
    description: machine.description ?? "",
    is_active: machine.is_active ?? true,
  });
  const [saving, setSaving] = useState(false);

  const handleSave = async () => {
    setSaving(true);
    try {
      const payload: any = {
        machine_type: form.machine_type || null,
        brand: form.brand || null,
        model: form.model || null,
        price: form.price !== "" ? parseFloat(form.price) : null,
        location: form.location || null,
        description: form.description || null,
        is_active: form.is_active,
      };
      await updateMachine(machine.id, payload);
      toast.success("Machine updated");
      onSaved();
      onClose();
    } catch {
      toast.error("Failed to update machine");
    } finally {
      setSaving(false);
    }
  };

  const Field = ({ label, field, type = "text" }: { label: string; field: keyof typeof form; type?: string }) => (
    <div>
      <label className="text-sm font-medium text-steel-700">{label}</label>
      <input
        type={type}
        className="input w-full mt-1"
        value={String(form[field])}
        onChange={(e) => setForm({ ...form, [field]: e.target.value })}
      />
    </div>
  );

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-xl shadow-xl w-full max-w-lg p-6 space-y-4 max-h-[90vh] overflow-y-auto">
        <div className="flex items-center justify-between">
          <h3 className="text-lg font-semibold text-steel-900">Edit Machine</h3>
          <button onClick={onClose} className="text-steel-400 hover:text-steel-700"><X className="w-5 h-5" /></button>
        </div>

        <div className="grid grid-cols-2 gap-3">
          <Field label="Brand" field="brand" />
          <Field label="Model" field="model" />
          <Field label="Machine Type" field="machine_type" />
          <Field label="Price (USD)" field="price" type="number" />
          <Field label="Location" field="location" />
          <div className="flex items-center gap-2 pt-5">
            <input
              type="checkbox"
              id="is_active"
              checked={form.is_active}
              onChange={(e) => setForm({ ...form, is_active: e.target.checked })}
            />
            <label htmlFor="is_active" className="text-sm text-steel-700 cursor-pointer">Active</label>
          </div>
        </div>
        <div>
          <label className="text-sm font-medium text-steel-700">Description</label>
          <textarea
            className="input w-full mt-1 h-24 resize-none"
            value={form.description}
            onChange={(e) => setForm({ ...form, description: e.target.value })}
          />
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

// ── Main Admin Page ────────────────────────────────────────────────────────────
export default function AdminPage() {
  const { user } = useAuthStore();
  const router = useRouter();
  const [tab, setTab] = useState<Tab>("dashboard");
  const [stats, setStats] = useState<any>(null);
  const [websites, setWebsites] = useState<any[]>([]);
  const [machines, setMachines] = useState<any>({ total: 0, items: [] });
  const [logs, setLogs] = useState<any>({ total: 0, items: [] });
  const [loading, setLoading] = useState(false);
  const [newSite, setNewSite] = useState({ name: "", url: "", description: "" });

  // Edit modal state
  const [editWebsite, setEditWebsite] = useState<any | null>(null);
  const [editMachine, setEditMachine] = useState<any | null>(null);
  const [expandedLog, setExpandedLog] = useState<number | null>(null);

  useEffect(() => {
    if (!user?.is_admin) {
      router.push("/auth/login");
      return;
    }
    loadData();
  }, [tab]);

  const loadData = async () => {
    setLoading(true);
    try {
      if (tab === "dashboard") setStats(await getAdminStats());
      if (tab === "websites") setWebsites(await listWebsites());
      if (tab === "machines") setMachines(await getAdminMachines({ limit: 100 }));
      if (tab === "logs") setLogs(await getCrawlLogs());
    } catch {
      toast.error("Failed to load data");
    } finally {
      setLoading(false);
    }
  };

  const handleAddWebsite = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      await addWebsite(newSite);
      toast.success("Website added!");
      setNewSite({ name: "", url: "", description: "" });
      setWebsites(await listWebsites());
    } catch {
      toast.error("Failed to add website");
    }
  };

  const handleStartCrawl = async (id: number) => {
    try {
      const res = await startCrawl(id);
      toast.success(`Crawl started (${res.task_id?.slice(0, 8)}...)`);
      setWebsites(await listWebsites());
    } catch { toast.error("Failed to start crawl"); }
  };

  const handleStartAll = async () => {
    try {
      await startAllCrawls();
      toast.success("All crawls queued");
    } catch { toast.error("Failed"); }
  };

  const handleDeleteMachine = async (id: number) => {
    if (!confirm("Delete this machine?")) return;
    try {
      await deleteMachine(id);
      toast.success("Machine deleted");
      setMachines(await getAdminMachines({ limit: 100 }));
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

  const TABS: { key: Tab; label: string; icon: React.ReactNode }[] = [
    { key: "dashboard", label: "Dashboard", icon: <BarChart3 className="w-4 h-4" /> },
    { key: "websites", label: "Websites", icon: <Globe className="w-4 h-4" /> },
    { key: "machines", label: "Machines", icon: <Cpu className="w-4 h-4" /> },
    { key: "logs", label: "Crawl Logs", icon: <FileText className="w-4 h-4" /> },
  ];

  const statusColor = (s: string) => {
    if (s === "success") return "badge-green";
    if (s === "error") return "badge-red";
    if (s === "running") return "badge-blue";
    return "badge-gray";
  };

  return (
    <div className="min-h-screen bg-steel-50">
      {/* Edit Modals */}
      {editWebsite && (
        <EditWebsiteModal
          site={editWebsite}
          onClose={() => setEditWebsite(null)}
          onSaved={() => listWebsites().then(setWebsites)}
        />
      )}
      {editMachine && (
        <EditMachineModal
          machine={editMachine}
          onClose={() => setEditMachine(null)}
          onSaved={() => getAdminMachines({ limit: 100 }).then(setMachines)}
        />
      )}

      {/* Admin Header */}
      <header className="bg-steel-900 text-white px-6 py-4 flex items-center justify-between">
        <div>
          <h1 className="text-lg font-bold">Zoogle Admin</h1>
          <p className="text-steel-400 text-xs">Industrial Machine Search Engine</p>
        </div>
        <div className="flex items-center gap-4">
          <span className="text-sm text-steel-300">{user?.email}</span>
          <a href="/" className="text-sm text-steel-400 hover:text-white">← Public Site</a>
        </div>
      </header>

      <div className="max-w-7xl mx-auto px-4 py-6 flex gap-6">
        {/* Sidebar */}
        <nav className="w-48 shrink-0">
          <div className="card p-2 space-y-1">
            {TABS.map((t) => (
              <button
                key={t.key}
                onClick={() => setTab(t.key)}
                className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-colors ${
                  tab === t.key ? "bg-brand-600 text-white" : "text-steel-700 hover:bg-steel-100"
                }`}
              >
                {t.icon}
                {t.label}
              </button>
            ))}
          </div>
        </nav>

        {/* Main content */}
        <main className="flex-1 min-w-0">
          {loading && <div className="text-center py-12 text-steel-400">Loading...</div>}

          {/* ── Dashboard ── */}
          {!loading && tab === "dashboard" && stats && (
            <div className="space-y-6">
              <div className="flex items-center justify-between">
                <h2 className="text-xl font-bold text-steel-900">Dashboard</h2>
                <button onClick={loadData} className="btn-secondary flex items-center gap-2 text-sm">
                  <RefreshCw className="w-4 h-4" /> Refresh
                </button>
              </div>

              <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
                {[
                  { label: "Total Machines", value: stats.total_machines?.toLocaleString(), icon: <Cpu className="w-5 h-5" />, color: "text-blue-600 bg-blue-50" },
                  { label: "Indexed Websites", value: stats.total_websites, icon: <Globe className="w-5 h-5" />, color: "text-green-600 bg-green-50" },
                  { label: "Users", value: stats.total_users, icon: <Users className="w-5 h-5" />, color: "text-purple-600 bg-purple-50" },
                  { label: "Total Searches", value: stats.total_searches?.toLocaleString(), icon: <Search className="w-5 h-5" />, color: "text-orange-600 bg-orange-50" },
                ].map((s) => (
                  <div key={s.label} className="card p-5">
                    <div className={`inline-flex p-2 rounded-lg mb-3 ${s.color}`}>{s.icon}</div>
                    <div className="text-2xl font-bold text-steel-900">{s.value ?? "—"}</div>
                    <div className="text-sm text-steel-500 mt-1">{s.label}</div>
                  </div>
                ))}
              </div>

              <div className="card p-5">
                <h3 className="font-semibold text-steel-900 mb-4">Recent Crawls</h3>
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-steel-500 text-left border-b border-steel-100">
                      <th className="pb-2">Website Name</th>
                      <th className="pb-2">Status</th>
                      <th className="pb-2">Found</th>
                      <th className="pb-2">New</th>
                      <th className="pb-2">Started</th>
                    </tr>
                  </thead>
                  <tbody>
                    {stats.recent_crawls?.map((c: any) => (
                      <tr key={c.id} className="border-b border-steel-50">
                        <td className="py-2 font-medium text-steel-900">{c.website_name}</td>
                        <td className="py-2"><span className={statusColor(c.status)}>{c.status}</span></td>
                        <td className="py-2 text-steel-700">{c.machines_found ?? 0}</td>
                        <td className="py-2 text-green-600 font-medium">{c.machines_new ?? 0}</td>
                        <td className="py-2 text-steel-400">{c.started_at ? new Date(c.started_at).toLocaleString() : "—"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* ── Websites ── */}
          {!loading && tab === "websites" && (
            <div className="space-y-6">
              <div className="flex items-center justify-between">
                <h2 className="text-xl font-bold text-steel-900">Websites ({websites.length})</h2>
                <div className="flex gap-2">
                  <button onClick={loadData} className="btn-secondary flex items-center gap-2 text-sm">
                    <RefreshCw className="w-4 h-4" />
                  </button>
                  <button onClick={handleStartAll} className="btn-primary flex items-center gap-2 text-sm">
                    <Play className="w-4 h-4" /> Crawl All
                  </button>
                </div>
              </div>

              {/* Add website form */}
              <div className="card p-5">
                <h3 className="font-semibold text-steel-900 mb-4">Add New Website</h3>
                <form onSubmit={handleAddWebsite} className="flex gap-3 flex-wrap">
                  <input
                    required
                    placeholder="Name (e.g. Machinio)"
                    value={newSite.name}
                    onChange={(e) => setNewSite({ ...newSite, name: e.target.value })}
                    className="input flex-1 min-w-40"
                  />
                  <input
                    required
                    placeholder="URL (e.g. https://machinio.com)"
                    value={newSite.url}
                    onChange={(e) => setNewSite({ ...newSite, url: e.target.value })}
                    className="input flex-1 min-w-60"
                  />
                  <input
                    placeholder="Description (optional)"
                    value={newSite.description}
                    onChange={(e) => setNewSite({ ...newSite, description: e.target.value })}
                    className="input flex-1 min-w-40"
                  />
                  <button type="submit" className="btn-primary">Add Website</button>
                </form>
              </div>

              {/* Website table */}
              <div className="card overflow-hidden">
                <table className="w-full text-sm">
                  <thead className="bg-steel-50 border-b border-steel-200">
                    <tr className="text-steel-500 text-left">
                      <th className="px-4 py-3">Name / URL</th>
                      <th className="px-4 py-3">Status</th>
                      <th className="px-4 py-3">Machines</th>
                      <th className="px-4 py-3">Last Crawl</th>
                      <th className="px-4 py-3">Crawl</th>
                      <th className="px-4 py-3">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {websites.map((site) => (
                      <tr key={site.id} className="border-b border-steel-50 hover:bg-steel-50">
                        <td className="px-4 py-3">
                          <div className="font-medium text-steel-900">{site.name}</div>
                          <div className="text-steel-400 text-xs truncate max-w-xs">{site.url}</div>
                        </td>
                        <td className="px-4 py-3">
                          <span className={statusColor(site.crawl_status)}>{site.crawl_status}</span>
                          {!site.is_active && <span className="ml-1 text-xs text-red-400">(inactive)</span>}
                        </td>
                        <td className="px-4 py-3">{site.machine_count}</td>
                        <td className="px-4 py-3 text-steel-400">
                          {site.last_crawled_at ? new Date(site.last_crawled_at).toLocaleDateString() : "Never"}
                        </td>
                        <td className="px-4 py-3">
                          <button
                            onClick={() => handleStartCrawl(site.id)}
                            className="p-1.5 hover:bg-green-50 rounded text-green-600"
                            title="Start crawl"
                          >
                            <Play className="w-4 h-4" />
                          </button>
                        </td>
                        <td className="px-4 py-3">
                          <div className="flex items-center gap-1">
                            <button
                              onClick={() => setEditWebsite(site)}
                              className="p-1.5 hover:bg-blue-50 rounded text-blue-500"
                              title="Edit"
                            >
                              <Pencil className="w-4 h-4" />
                            </button>
                            <button
                              onClick={() => handleDeleteWebsite(site.id)}
                              className="p-1.5 hover:bg-red-50 rounded text-red-500"
                              title="Delete"
                            >
                              <Trash2 className="w-4 h-4" />
                            </button>
                          </div>
                        </td>
                      </tr>
                    ))}
                    {websites.length === 0 && (
                      <tr>
                        <td colSpan={6} className="px-4 py-8 text-center text-steel-400">No websites added yet.</td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* ── Machines ── */}
          {!loading && tab === "machines" && (
            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <h2 className="text-xl font-bold text-steel-900">
                  Machines ({machines.total?.toLocaleString()})
                </h2>
                <div className="flex gap-2">
                  <button onClick={loadData} className="btn-secondary flex items-center gap-2 text-sm">
                    <RefreshCw className="w-4 h-4" />
                  </button>
                  <a
                    href={exportMachinesExcelUrl()}
                    className="btn-secondary flex items-center gap-2 text-sm"
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    <Download className="w-4 h-4" /> Export Excel
                  </a>
                </div>
              </div>

              <div className="card overflow-hidden">
                <table className="w-full text-sm">
                  <thead className="bg-steel-50 border-b border-steel-200">
                    <tr className="text-steel-500 text-left">
                      <th className="px-4 py-3">Machine</th>
                      <th className="px-4 py-3">Type</th>
                      <th className="px-4 py-3">Price</th>
                      <th className="px-4 py-3">Location</th>
                      <th className="px-4 py-3">Source</th>
                      <th className="px-4 py-3">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {machines.items?.map((m: any) => (
                      <tr key={m.id} className={`border-b border-steel-50 hover:bg-steel-50 ${!m.is_active ? "opacity-50" : ""}`}>
                        <td className="px-4 py-3">
                          <div className="font-medium text-steel-900">{m.brand} {m.model}</div>
                          <a
                            href={m.machine_url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="text-brand-600 text-xs hover:underline"
                          >
                            View listing
                          </a>
                        </td>
                        <td className="px-4 py-3 text-steel-500">{m.machine_type || "—"}</td>
                        <td className="px-4 py-3">
                          {m.price ? `$${Number(m.price).toLocaleString()}` : "—"}
                        </td>
                        <td className="px-4 py-3 text-steel-500">{m.location || "—"}</td>
                        <td className="px-4 py-3 text-steel-400 text-xs">{m.website_source}</td>
                        <td className="px-4 py-3">
                          <div className="flex items-center gap-1">
                            <button
                              onClick={() => setEditMachine(m)}
                              className="p-1.5 hover:bg-blue-50 rounded text-blue-500"
                              title="Edit"
                            >
                              <Pencil className="w-4 h-4" />
                            </button>
                            <button
                              onClick={() => handleDeleteMachine(m.id)}
                              className="p-1.5 hover:bg-red-50 rounded text-red-500"
                              title="Delete"
                            >
                              <Trash2 className="w-4 h-4" />
                            </button>
                          </div>
                        </td>
                      </tr>
                    ))}
                    {machines.items?.length === 0 && (
                      <tr>
                        <td colSpan={6} className="px-4 py-8 text-center text-steel-400">No machines found.</td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* ── Crawl Logs ── */}
          {!loading && tab === "logs" && (
            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <h2 className="text-xl font-bold text-steel-900">Crawl Logs ({logs.total})</h2>
                <div className="flex gap-2">
                  <button
                    onClick={async () => {
                      try {
                        const r = await fixStuckCrawls();
                        toast.success(`Fixed ${r.fixed_crawl_logs} stuck crawl(s)`);
                        loadData();
                      } catch { toast.error("Failed to fix stuck crawls"); }
                    }}
                    className="btn-secondary flex items-center gap-2 text-sm"
                  >
                    <Wrench className="w-4 h-4" /> Fix Stuck
                  </button>
                  <button onClick={loadData} className="btn-secondary flex items-center gap-2 text-sm">
                    <RefreshCw className="w-4 h-4" /> Refresh
                  </button>
                </div>
              </div>

              <p className="text-xs text-steel-400">Click any row to see full scrapy output / error details.</p>

              <div className="card overflow-hidden">
                <table className="w-full text-sm">
                  <thead className="bg-steel-50 border-b border-steel-200">
                    <tr className="text-steel-500 text-left">
                      <th className="px-4 py-3 w-8"></th>
                      <th className="px-4 py-3">Website Name</th>
                      <th className="px-4 py-3">Status</th>
                      <th className="px-4 py-3">Found</th>
                      <th className="px-4 py-3">New</th>
                      <th className="px-4 py-3">Errors</th>
                      <th className="px-4 py-3">Started</th>
                      <th className="px-4 py-3">Duration</th>
                    </tr>
                  </thead>
                  <tbody>
                    {logs.items?.map((log: any) => {
                      const duration =
                        log.finished_at && log.started_at
                          ? Math.round((new Date(log.finished_at).getTime() - new Date(log.started_at).getTime()) / 1000) + "s"
                          : "Running...";
                      const isExpanded = expandedLog === log.id;
                      const hasDetail = log.error_details || log.log_output;
                      return (
                        <>
                          <tr
                            key={log.id}
                            className={`border-b border-steel-50 hover:bg-steel-50 ${hasDetail ? "cursor-pointer" : ""}`}
                            onClick={() => hasDetail && setExpandedLog(isExpanded ? null : log.id)}
                          >
                            <td className="px-4 py-3 text-steel-400">
                              {hasDetail
                                ? (isExpanded ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />)
                                : null}
                            </td>
                            <td className="px-4 py-3">
                              <div className="font-medium text-steel-900">{log.website_name}</div>
                              {log.website_url && (
                                <div className="text-xs text-steel-400 truncate max-w-[180px]">{log.website_url}</div>
                              )}
                            </td>
                            <td className="px-4 py-3"><span className={statusColor(log.status)}>{log.status}</span></td>
                            <td className="px-4 py-3">{log.machines_found ?? 0}</td>
                            <td className="px-4 py-3 text-green-600 font-medium">{log.machines_new ?? 0}</td>
                            <td className="px-4 py-3 text-red-500">{log.errors_count ?? 0}</td>
                            <td className="px-4 py-3 text-steel-400">{new Date(log.started_at).toLocaleString()}</td>
                            <td className="px-4 py-3 text-steel-400">{duration}</td>
                          </tr>

                          {isExpanded && (
                            <tr key={`${log.id}-expanded`}>
                              <td colSpan={8} className="px-0 py-0 bg-steel-900">
                                {/* Error summary */}
                                {log.error_details && (
                                  <div className="px-6 pt-3 pb-1">
                                    <p className="text-xs font-semibold text-red-400 mb-1">Error Summary</p>
                                    <pre className="text-xs text-red-300 whitespace-pre-wrap font-mono bg-black/30 rounded p-2 max-h-32 overflow-auto">
                                      {log.error_details}
                                    </pre>
                                  </div>
                                )}
                                {/* Full log output */}
                                {log.log_output && (
                                  <div className="px-6 pt-2 pb-3">
                                    <p className="text-xs font-semibold text-green-400 mb-1">Full Scrapy Output (last 5000 chars)</p>
                                    <pre className="text-xs text-green-300 whitespace-pre-wrap font-mono bg-black/30 rounded p-3 max-h-64 overflow-auto">
                                      {log.log_output}
                                    </pre>
                                  </div>
                                )}
                              </td>
                            </tr>
                          )}
                        </>
                      );
                    })}
                    {logs.items?.length === 0 && (
                      <tr>
                        <td colSpan={8} className="px-4 py-8 text-center text-steel-400">No crawl logs yet.</td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </main>
      </div>
    </div>
  );
}

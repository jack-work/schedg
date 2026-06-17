import { useState, useEffect, useCallback, useRef, useMemo } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

// ── Types ────────────────────────────────────────────────────────

interface QueueEntry {
  name: string;
  driver: string;
  path: string;
  comparator: string;
}

interface TaskView {
  id: string;
  title: string;
  description?: string;
  priority: number;
  submitted?: string;
  attempts?: number;
  cancels?: number;
  reason?: string;
  leasedAt?: string;
  caller?: string;
}

interface LinksConfig {
  schedg_url?: string;
  angl_url?: string;
}

interface DepEdge {
  from: string;
  to: string;
}

interface QueueSnapshot {
  name: string;
  driver: string;
  ready: TaskView[];
  blocked: TaskView[];
  inflight: TaskView[];
  dead: TaskView[];
  completed: string[];
  deps: DepEdge[];
  blockedBy: Record<string, string[]>;
  counts: Record<string, number>;
  snapshotAt: string;
}

interface LogEntry {
  time: string;
  level: string;
  message: string;
}

type View = "list" | "queue" | "detail" | "graph" | "logs";
type DetailMode = "split" | "fullscreen";
type ListTab = "ready" | "blocked" | "inflight" | "dead" | "completed";

// ── Helpers ──────────────────────────────────────────────────────

function relativeTime(iso: string): string {
  if (!iso) return "";
  const d = Date.now() - new Date(iso).getTime();
  if (d < 60000) return `${Math.floor(d / 1000)}s ago`;
  if (d < 3600000) return `${Math.floor(d / 60000)}m ago`;
  if (d < 86400000) return `${Math.floor(d / 3600000)}h ago`;
  return `${Math.floor(d / 86400000)}d ago`;
}

function copyText(text: string) {
  navigator.clipboard.writeText(text);
}

function fuzzyMatch(query: string, text: string): boolean {
  if (!query) return true;
  const q = query.toLowerCase();
  const t = text.toLowerCase();
  let qi = 0;
  for (let i = 0; i < t.length && qi < q.length; i++) {
    if (t[i] === q[qi]) qi++;
  }
  return qi === q.length;
}

// ── Hooks ────────────────────────────────────────────────────────

function useSSE<T>(url: string | null, onMessage: (data: T) => void) {
  useEffect(() => {
    if (!url) return;
    const es = new EventSource(url);
    es.onmessage = (e) => {
      try {
        onMessage(JSON.parse(e.data));
      } catch {}
    };
    return () => es.close();
  }, [url]); // eslint-disable-line react-hooks/exhaustive-deps
}

// ── App ──────────────────────────────────────────────────────────

export function App() {
  const [queues, setQueues] = useState<QueueEntry[]>([]);
  const [links, setLinks] = useState<LinksConfig>({});
  const [selectedQueue, setSelectedQueue] = useState<string | null>(null);
  const [snapshot, setSnapshot] = useState<QueueSnapshot | null>(null);
  const [view, setView] = useState<View>("list");
  const [activeTab, setActiveTab] = useState<ListTab>("ready");
  const [search, setSearch] = useState("");
  const [selectedTask, setSelectedTask] = useState<TaskView | null>(null);
  const [detailMode, setDetailMode] = useState<DetailMode>("split");
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [helpVisible, setHelpVisible] = useState(false);
  const [focusedIdx, setFocusedIdx] = useState(-1);
  const searchRef = useRef<HTMLInputElement>(null);

  // Load queues and links on mount.
  useEffect(() => {
    fetch("/api/queues").then(r => r.json()).then(setQueues).catch(() => {});
    fetch("/api/links").then(r => r.json()).then(setLinks).catch(() => {});
  }, []);

  // Read queue name from URL hash on load.
  useEffect(() => {
    const hash = window.location.hash.slice(1);
    if (hash) {
      const parts = hash.split("/");
      setSelectedQueue(decodeURIComponent(parts[0]));
      setView("queue");
    }
  }, []);

  // SSE for queue events.
  const sseUrl = selectedQueue && view !== "list"
    ? `/api/queues/${encodeURIComponent(selectedQueue)}/events`
    : null;
  useSSE<QueueSnapshot>(sseUrl, setSnapshot);

  // SSE for logs.
  useSSE<LogEntry>(view === "logs" ? "/api/logs" : null, (entry) =>
    setLogs((prev) => [...prev.slice(-499), entry])
  );

  const selectQueue = useCallback((name: string) => {
    setSelectedQueue(name);
    setView("queue");
    setActiveTab("ready");
    setSearch("");
    setFocusedIdx(-1);
    window.history.replaceState(null, "", `#${encodeURIComponent(name)}`);
  }, []);

  const openDetail = useCallback((task: TaskView) => {
    setSelectedTask(task);
  }, []);

  const goBack = useCallback(() => {
    if (view === "queue" && selectedTask) {
      setSelectedTask(null);
      setDetailMode("split");
    } else if (view === "detail" || view === "graph") {
      setView("queue");
      setSelectedTask(null);
      setDetailMode("split");
    } else if (view === "queue") {
      setView("list");
      setSelectedQueue(null);
      setSnapshot(null);
      window.history.replaceState(null, "", "#");
    } else if (view === "logs") {
      setView(selectedQueue ? "queue" : "list");
    }
  }, [view, selectedQueue, selectedTask]);

  // Filtered task list for current tab.
  const currentTasks = useMemo(() => {
    if (!snapshot) return [];
    const lists: Record<ListTab, TaskView[]> = {
      ready: snapshot.ready,
      blocked: snapshot.blocked,
      inflight: snapshot.inflight,
      dead: snapshot.dead,
      completed: snapshot.completed.map((id) => ({
        id,
        title: `#${id}`,
        priority: 0,
      })),
    };
    const all = lists[activeTab] || [];
    if (!search) return all;
    return all.filter(
      (t) => fuzzyMatch(search, t.title) || fuzzyMatch(search, t.id)
    );
  }, [snapshot, activeTab, search]);

  // Keyboard shortcuts.
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      const inInput =
        (e.target as HTMLElement)?.tagName === "INPUT" ||
        (e.target as HTMLElement)?.tagName === "TEXTAREA";

      if (e.key === "?" && !inInput) {
        e.preventDefault();
        setHelpVisible((v) => !v);
        return;
      }
      if (e.key === "Escape") {
        if (helpVisible) { setHelpVisible(false); return; }
        if (view === "detail") { goBack(); return; }
        if (inInput) { (document.activeElement as HTMLElement)?.blur(); return; }
        return;
      }
      if ((e.ctrlKey || e.metaKey) && e.key === "l") {
        e.preventDefault();
        setView("logs");
        return;
      }
      if (e.key === "/" && !inInput) {
        e.preventDefault();
        searchRef.current?.focus();
        return;
      }
      if (e.key === "g" && !inInput && view === "queue") {
        e.preventDefault();
        setView("graph");
        return;
      }
      if (e.key === "f" && !inInput && (view === "detail" || (view === "queue" && selectedTask))) {
        e.preventDefault();
        setDetailMode((m) => (m === "split" ? "fullscreen" : "split"));
        return;
      }
      if (e.key === "Backspace" && !inInput) {
        e.preventDefault();
        if (detailMode === "fullscreen") { setDetailMode("split"); return; }
        goBack();
        return;
      }
      if (!inInput && (view === "queue" || view === "list")) {
        if (e.key === "j" || e.key === "ArrowDown") {
          e.preventDefault();
          const max = (view === "list" ? queues.length : currentTasks.length) - 1;
          setFocusedIdx((i) => i < 0 ? 0 : Math.min(i + 1, max));
          return;
        }
        if (e.key === "k" || e.key === "ArrowUp") {
          e.preventDefault();
          setFocusedIdx((i) => Math.max(i < 0 ? 0 : i - 1, 0));
          return;
        }
        if (e.key === "Enter" && focusedIdx >= 0) {
          e.preventDefault();
          if (view === "list" && queues[focusedIdx]) {
            selectQueue(queues[focusedIdx].name);
          } else if (view === "queue" && currentTasks[focusedIdx]) {
            openDetail(currentTasks[focusedIdx]);
          }
          return;
        }
        if (e.key === "c" && !inInput && view === "queue" && focusedIdx >= 0 && currentTasks[focusedIdx]) {
          copyText(currentTasks[focusedIdx].title || currentTasks[focusedIdx].id);
          return;
        }
      }
      if (!inInput && view === "queue") {
        const tabKeys: Record<string, ListTab> = {
          "1": "ready", "2": "blocked", "3": "inflight", "4": "dead", "5": "completed",
        };
        if (tabKeys[e.key]) {
          setActiveTab(tabKeys[e.key]);
          setFocusedIdx(-1);
          return;
        }
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [helpVisible, view, queues, currentTasks, focusedIdx, goBack, selectQueue, openDetail]);

  useEffect(() => setFocusedIdx(-1), [activeTab, search]);

  return (
    <div className="sg-root">
      <header className="sg-header">
        <div className="sg-header-left">
          <h1
            className="sg-logo"
            onClick={() => {
              setView("list");
              setSelectedQueue(null);
              window.history.replaceState(null, "", "#");
            }}
          >
            <img src="/ornaments/square2.png" className="sg-ornament-logo" alt="" />{" "}schedg
          </h1>
          {selectedQueue && view !== "list" && (
            <span className="sg-breadcrumb">
              <span className="sg-bc-sep">/</span>
              <span className="sg-bc-name" onClick={() => setView("queue")}>
                {selectedQueue}
              </span>
              {view === "detail" && selectedTask && (
                <>
                  <span className="sg-bc-sep">/</span>
                  <span className="sg-bc-id">#{selectedTask.id}</span>
                </>
              )}
              {view === "graph" && (
                <>
                  <span className="sg-bc-sep">/</span>
                  <span className="sg-bc-id">deps</span>
                </>
              )}
            </span>
          )}
        </div>
        <div className="sg-header-right">
          {snapshot && (
            <span className="sg-live-dot" title={`Last update: ${snapshot.snapshotAt}`}>
              <img src="/ornaments/square3.png" className="sg-ornament-live" alt="" /> LIVE
            </span>
          )}
          <button className="sg-hdr-btn" onClick={() => setView("logs")} title="Server logs (Ctrl+L)">
            logs
          </button>
          <span className="sg-help-hint" onClick={() => setHelpVisible(true)}>
            <kbd>?</kbd>
          </span>
        </div>
      </header>

      <main className="sg-main">
        {view === "list" && (
          <QueueList
            queues={queues}
            focusedIdx={focusedIdx}
            onSelect={selectQueue}
          />
        )}

        {view === "queue" && snapshot && !selectedTask && (
          <QueueView
            snapshot={snapshot}
            activeTab={activeTab}
            onTabChange={(t) => { setActiveTab(t); setFocusedIdx(-1); }}
            search={search}
            onSearchChange={setSearch}
            searchRef={searchRef}
            tasks={currentTasks}
            focusedIdx={focusedIdx}
            onSelect={openDetail}
            onGraphView={() => setView("graph")}
          />
        )}

        {view === "queue" && snapshot && selectedTask && (
          <div className="sg-split">
            <div className="sg-split-list">
              <QueueView
                snapshot={snapshot}
                activeTab={activeTab}
                onTabChange={(t) => { setActiveTab(t); setFocusedIdx(-1); }}
                search={search}
                onSearchChange={setSearch}
                searchRef={searchRef}
                tasks={currentTasks}
                focusedIdx={focusedIdx}
                onSelect={openDetail}
                onGraphView={() => setView("graph")}
              />
            </div>
            <div className={`sg-split-detail ${detailMode === "fullscreen" ? "sg-fullscreen" : ""}`}>
              <TaskDetail
                task={selectedTask}
                snapshot={snapshot}
                links={links}
                onBack={() => setSelectedTask(null)}
                onToggleFullscreen={() => setDetailMode((m) => m === "split" ? "fullscreen" : "split")}
                isFullscreen={detailMode === "fullscreen"}
              />
            </div>
          </div>
        )}

        {view === "detail" && selectedTask && snapshot && (
          <TaskDetail
            task={selectedTask}
            snapshot={snapshot}
            links={links}
            onBack={goBack}
            onToggleFullscreen={() => setDetailMode((m) => m === "split" ? "fullscreen" : "split")}
            isFullscreen={detailMode === "fullscreen"}
          />
        )}

        {view === "graph" && snapshot && (
          <DepGraph snapshot={snapshot} onSelectTask={openDetail} />
        )}

        {view === "logs" && <LogView logs={logs} />}
      </main>

      {helpVisible && (
        <div className="sg-overlay" onClick={() => setHelpVisible(false)}>
          <div className="sg-help" onClick={(e) => e.stopPropagation()}>
            <div className="sg-help-header">
              <img src="/ornaments/square3.png" className="sg-ornament-help" alt="" />
              <h2>Keyboard Shortcuts</h2>
            </div>
            <table>
              <tbody>
                {[
                  ["?", "Toggle help"],
                  ["j / k", "Navigate list"],
                  ["Enter", "Open selected"],
                  ["Backspace", "Go back"],
                  ["/", "Focus search"],
                  ["1-5", "Switch tab (ready/blocked/inflight/dead/completed)"],
                  ["g", "Dependency graph"],
                  ["c", "Copy focused task title"],
                  ["f", "Toggle fullscreen detail"],
                  ["Ctrl+L", "Server logs"],
                  ["Esc", "Close / unfocus"],
                ].map(([key, label]) => (
                  <tr key={key}>
                    <td><kbd>{key}</kbd></td>
                    <td>{label}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

// ── Queue List ───────────────────────────────────────────────────

function QueueList({
  queues,
  focusedIdx,
  onSelect,
}: {
  queues: QueueEntry[];
  focusedIdx: number;
  onSelect: (name: string) => void;
}) {
  return (
    <div className="sg-queue-list">
      <h2 className="sg-section-title">Registered Queues</h2>
      {queues.length === 0 && (
        <p className="sg-empty">No queues registered. Run <code>schedg init</code> to create one.</p>
      )}
      {queues.map((q, i) => (
        <div
          key={q.name}
          className={`sg-queue-card ${i === focusedIdx ? "sg-focused" : ""}`}
          onClick={() => onSelect(q.name)}
        >
          <div className="sg-qc-name"><img src="/ornaments/square1.png" className="sg-ornament-card" alt="" /> {q.name}</div>
          <div className="sg-qc-meta">
            <span className="sg-badge sg-badge-driver">{q.driver}</span>
            <span className="sg-qc-path">{q.path}</span>
          </div>
        </div>
      ))}
    </div>
  );
}

// ── Queue View ───────────────────────────────────────────────────

const TAB_LABELS: Record<ListTab, string> = {
  ready: "Ready",
  blocked: "Blocked",
  inflight: "In-Flight",
  dead: "Dead",
  completed: "Completed",
};
const TAB_ORDER: ListTab[] = ["ready", "blocked", "inflight", "dead", "completed"];

function QueueView({
  snapshot,
  activeTab,
  onTabChange,
  search,
  onSearchChange,
  searchRef,
  tasks,
  focusedIdx,
  onSelect,
  onGraphView,
}: {
  snapshot: QueueSnapshot;
  activeTab: ListTab;
  onTabChange: (tab: ListTab) => void;
  search: string;
  onSearchChange: (v: string) => void;
  searchRef: React.RefObject<HTMLInputElement | null>;
  tasks: TaskView[];
  focusedIdx: number;
  onSelect: (t: TaskView) => void;
  onGraphView: () => void;
}) {
  const listRef = useRef<HTMLDivElement>(null);

  // Auto-scroll focused item into view.
  useEffect(() => {
    const el = listRef.current?.querySelector(".sg-focused");
    el?.scrollIntoView({ block: "nearest" });
  }, [focusedIdx]);

  return (
    <div className="sg-queue-view">
      <div className="sg-counts-bar">
        {TAB_ORDER.map((tab, i) => (
          <button
            key={tab}
            className={`sg-count-btn ${activeTab === tab ? "sg-active" : ""}`}
            onClick={() => onTabChange(tab)}
          >
            <span className="sg-count-key">{i + 1}</span>
            <span className="sg-count-label">{TAB_LABELS[tab]}</span>
            <span className={`sg-count-num sg-count-${tab}`}>
              {snapshot.counts[tab] ?? 0}
            </span>
          </button>
        ))}
        <button className="sg-count-btn sg-graph-btn" onClick={onGraphView} title="Dependency graph (g)">
          <span className="sg-count-label">Deps</span>
          <span className="sg-count-num">{snapshot.deps.length}</span>
        </button>
      </div>

      <div className="sg-search-row">
        <input
          ref={searchRef}
          className="sg-search"
          type="text"
          placeholder="Filter tasks... ( / )"
          value={search}
          onChange={(e) => onSearchChange(e.target.value)}
          spellCheck={false}
        />
        <span className="sg-search-count">{tasks.length}</span>
      </div>

      <div className="sg-task-list" ref={listRef}>
        {tasks.length === 0 && (
          <p className="sg-empty">No tasks in {TAB_LABELS[activeTab].toLowerCase()}</p>
        )}
        {tasks.map((t, i) => (
          <div
            key={t.id}
            className={`sg-task-row ${i === focusedIdx ? "sg-focused" : ""} sg-state-${activeTab}`}
            onClick={() => onSelect(t)}
          >
            <span className="sg-task-id">#{t.id}</span>
            <span className={`sg-task-prio sg-p${Math.min(t.priority, 9)}`}>
              p{t.priority}
            </span>
            <span className="sg-task-title">{t.title || `Task #${t.id}`}</span>
            {t.leasedAt && (
              <span className="sg-task-leased" title={`Leased: ${t.leasedAt}`}>
                {relativeTime(t.leasedAt)}
              </span>
            )}
            {t.caller && (
              <span className="sg-task-caller" title={`Caller: ${t.caller}`}>{t.caller}</span>
            )}
            {(t.attempts ?? 0) > 0 && (
              <span className="sg-task-attempts">try {t.attempts}</span>
            )}
            {t.submitted && (
              <span className="sg-task-submitted">{relativeTime(t.submitted)}</span>
            )}
            <button
              className="sg-copy-btn"
              title="Copy title"
              onClick={(e) => {
                e.stopPropagation();
                copyText(t.title || t.id);
              }}
            >
              &#x2398;
            </button>
          </div>
        ))}
      </div>
    </div>
  );
}

// ── Task Detail ──────────────────────────────────────────────────

function TaskDetail({
  task,
  snapshot,
  links,
  onBack,
  onToggleFullscreen,
  isFullscreen,
}: {
  task: TaskView;
  snapshot: QueueSnapshot;
  links: LinksConfig;
  onBack: () => void;
  onToggleFullscreen?: () => void;
  isFullscreen?: boolean;
}) {
  const state = snapshot.inflight.find((t) => t.id === task.id)
    ? "inflight"
    : snapshot.dead.find((t) => t.id === task.id)
      ? "dead"
      : snapshot.blocked.find((t) => t.id === task.id)
        ? "blocked"
        : snapshot.completed.includes(task.id)
          ? "completed"
          : "ready";

  const blockedBy = snapshot.blockedBy?.[task.id] || [];

  return (
    <div className="sg-detail">
      <img src="/ornaments/half-left.png" className="sg-corner sg-corner-tl" alt="" />
      <img src="/ornaments/half-right.png" className="sg-corner sg-corner-tr" alt="" />
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
        <button className="sg-back-btn" onClick={onBack} style={{ margin: 0 }}>
          &larr; Back
        </button>
        {onToggleFullscreen && (
          <button className="sg-fullscreen-btn" onClick={onToggleFullscreen} title="Toggle fullscreen (f)">
            {isFullscreen ? "[=] Split" : "[ ] Fullscreen"}
          </button>
        )}
      </div>
      <div className="sg-detail-header">
        <span className="sg-detail-id">#{task.id}</span>
        <span className={`sg-badge sg-badge-${state}`}>{state}</span>
        <span className={`sg-task-prio sg-p${Math.min(task.priority, 9)}`}>
          p{task.priority}
        </span>
        <button
          className="sg-copy-btn"
          title="Copy all"
          onClick={() =>
            copyText(
              `#${task.id} ${task.title}\n\n${task.description || ""}`
            )
          }
        >
          &#x2398;
        </button>
      </div>
      <h2 className="sg-detail-title">{task.title}</h2>

      {task.description && (
        <div className="sg-detail-body">
          <div className="sg-detail-md">
            <ReactMarkdown remarkPlugins={[remarkGfm]}>{task.description}</ReactMarkdown>
          </div>
          <button
            className="sg-copy-btn sg-copy-body"
            title="Copy description"
            onClick={() => copyText(task.description || "")}
          >
            &#x2398; copy
          </button>
        </div>
      )}

      <div className="sg-detail-meta">
        {task.submitted && (
          <div className="sg-meta-row">
            <span className="sg-meta-key">Submitted</span>
            <span className="sg-meta-val">{task.submitted} ({relativeTime(task.submitted)})</span>
          </div>
        )}
        {task.leasedAt && (
          <div className="sg-meta-row">
            <span className="sg-meta-key">Leased At</span>
            <span className="sg-meta-val">{task.leasedAt} ({relativeTime(task.leasedAt)})</span>
          </div>
        )}
        {(task.attempts ?? 0) > 0 && (
          <div className="sg-meta-row">
            <span className="sg-meta-key">Attempts</span>
            <span className="sg-meta-val">{task.attempts}</span>
          </div>
        )}
        {(task.cancels ?? 0) > 0 && (
          <div className="sg-meta-row">
            <span className="sg-meta-key">Cancels</span>
            <span className="sg-meta-val">{task.cancels}</span>
          </div>
        )}
        {task.reason && (
          <div className="sg-meta-row">
            <span className="sg-meta-key">Reason</span>
            <span className="sg-meta-val sg-meta-reason">{task.reason}</span>
          </div>
        )}
        {task.caller && (
          <div className="sg-meta-row">
            <span className="sg-meta-key">Caller</span>
            <span className="sg-meta-val">
              {links.angl_url ? (
                <a className="sg-caller-link" href={`${links.angl_url}#${encodeURIComponent(task.caller)}`} target="_blank" rel="noopener">
                  {task.caller}
                </a>
              ) : task.caller}
            </span>
          </div>
        )}
        {blockedBy.length > 0 && (
          <div className="sg-meta-row">
            <span className="sg-meta-key">Blocked By</span>
            <span className="sg-meta-val">
              {blockedBy.map((id) => `#${id}`).join(", ")}
            </span>
          </div>
        )}
      </div>
    </div>
  );
}

// ── Dependency Graph (SVG) ───────────────────────────────────────

function DepGraph({
  snapshot,
  onSelectTask,
}: {
  snapshot: QueueSnapshot;
  onSelectTask: (t: TaskView) => void;
}) {
  const allTasks = useMemo(() => {
    const map = new Map<string, TaskView>();
    for (const t of [...snapshot.ready, ...snapshot.blocked, ...snapshot.inflight, ...snapshot.dead]) {
      map.set(t.id, t);
    }
    return map;
  }, [snapshot]);

  // Collect only tasks involved in deps.
  const nodeIds = useMemo(() => {
    const s = new Set<string>();
    for (const e of snapshot.deps) {
      s.add(e.from);
      s.add(e.to);
    }
    if (s.size === 0) {
      for (const t of snapshot.ready) s.add(t.id);
    }
    return Array.from(s);
  }, [snapshot]);

  // Simple layered layout.
  const positions = useMemo(() => {
    const pos = new Map<string, { x: number; y: number }>();
    // Compute in-degree for topological layering.
    const inDeg = new Map<string, number>();
    const outEdges = new Map<string, string[]>();
    for (const id of nodeIds) {
      inDeg.set(id, 0);
      outEdges.set(id, []);
    }
    for (const e of snapshot.deps) {
      inDeg.set(e.from, (inDeg.get(e.from) ?? 0) + 1);
      outEdges.get(e.to)?.push(e.from);
    }
    // BFS layering.
    const layers: string[][] = [];
    const visited = new Set<string>();
    let current = nodeIds.filter((id) => (inDeg.get(id) ?? 0) === 0);
    while (current.length > 0) {
      layers.push(current);
      current.forEach((id) => visited.add(id));
      const next: string[] = [];
      for (const id of current) {
        for (const child of outEdges.get(id) ?? []) {
          if (!visited.has(child)) {
            inDeg.set(child, (inDeg.get(child) ?? 0) - 1);
            if ((inDeg.get(child) ?? 0) <= 0) next.push(child);
          }
        }
      }
      current = next;
    }
    // Place remaining (cycles or isolated).
    const placed = new Set(layers.flat());
    const remaining = nodeIds.filter((id) => !placed.has(id));
    if (remaining.length > 0) layers.push(remaining);

    const colW = 220;
    const rowH = 60;
    for (let li = 0; li < layers.length; li++) {
      const layer = layers[li];
      for (let ni = 0; ni < layer.length; ni++) {
        pos.set(layer[ni], {
          x: 60 + li * colW,
          y: 40 + ni * rowH,
        });
      }
    }
    return pos;
  }, [nodeIds, snapshot.deps]);

  const svgW = Math.max(600, (positions.size > 0 ? Math.max(...Array.from(positions.values()).map((p) => p.x)) : 0) + 200);
  const svgH = Math.max(300, (positions.size > 0 ? Math.max(...Array.from(positions.values()).map((p) => p.y)) : 0) + 80);

  const stateOf = (id: string) => {
    if (snapshot.inflight.find((t) => t.id === id)) return "inflight";
    if (snapshot.dead.find((t) => t.id === id)) return "dead";
    if (snapshot.blocked.find((t) => t.id === id)) return "blocked";
    if (snapshot.completed.includes(id)) return "completed";
    return "ready";
  };

  return (
    <div className="sg-graph">
      <svg width={svgW} height={svgH}>
        <defs>
          <marker id="arrow" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse">
            <path d="M 0 0 L 10 5 L 0 10 z" fill="#484f58" />
          </marker>
        </defs>
        {snapshot.deps.map((e, i) => {
          const from = positions.get(e.from);
          const to = positions.get(e.to);
          if (!from || !to) return null;
          return (
            <line
              key={i}
              x1={to.x + 80}
              y1={to.y + 16}
              x2={from.x}
              y2={from.y + 16}
              stroke="#484f58"
              strokeWidth={1.5}
              markerEnd="url(#arrow)"
            />
          );
        })}
        {nodeIds.map((id) => {
          const p = positions.get(id);
          if (!p) return null;
          const task = allTasks.get(id);
          const state = stateOf(id);
          return (
            <g
              key={id}
              transform={`translate(${p.x},${p.y})`}
              className={`sg-graph-node sg-gn-${state}`}
              onClick={() => task && onSelectTask(task)}
              style={{ cursor: task ? "pointer" : "default" }}
            >
              <rect width={160} height={32} rx={4} />
              <text x={8} y={20} className="sg-graph-label">
                #{id} {task?.title ? task.title.slice(0, 18) : ""}
              </text>
            </g>
          );
        })}
      </svg>
    </div>
  );
}

// ── Log View ─────────────────────────────────────────────────────

function LogView({ logs }: { logs: LogEntry[] }) {
  const endRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [logs.length]);

  return (
    <div className="sg-logs">
      <h2 className="sg-section-title">Server Logs</h2>
      <div className="sg-log-list">
        {logs.map((entry, i) => (
          <div key={i} className={`sg-log-entry sg-log-${entry.level}`}>
            <span className="sg-log-time">
              {new Date(entry.time).toLocaleTimeString()}
            </span>
            <span className={`sg-log-level`}>{entry.level}</span>
            <span className="sg-log-msg">{entry.message}</span>
          </div>
        ))}
        <div ref={endRef} />
      </div>
    </div>
  );
}

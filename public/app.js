const STORAGE_PREFIX = "binance-pages-worker-v3";

const windowOptions = [
  { key: "1d", label: "1天" },
  { key: "3d", label: "3天" },
  { key: "7d", label: "7天" },
];

const WINDOW_CONFIG = {
  "1d": {
    label: "1天",
    ttlMs: 60_000,
  },
  "3d": {
    label: "3天",
    ttlMs: 5 * 60_000,
  },
  "7d": {
    label: "7天",
    ttlMs: 5 * 60_000,
  },
};

const runtimeCache = new Map();
const DEMO_SNAPSHOTS_PATH = "/demo-snapshots.json";
const LOCAL_PREVIEW_HOSTNAMES = new Set(["127.0.0.1", "localhost", "::1"]);
const NOTE_KIND_META = {
  exact: {
    className: "is-exact",
    label: "精确",
  },
  approx: {
    className: "is-approx",
    label: "近似",
  },
  info: {
    className: "is-info",
    label: "说明",
  },
  warn: {
    className: "is-warn",
    label: "提醒",
  },
};

const state = {
  selectedWindow: "1d",
  selectedSegment: "all",
  leaderboardLimit: 20,
  searchText: "",
  payload: null,
  loading: false,
  loadId: 0,
  refreshTimer: null,
};

const elements = {
  windowTabs: document.querySelector("#windowTabs"),
  segmentFilter: document.querySelector("#segmentFilter"),
  leaderboardLimit: document.querySelector("#leaderboardLimit"),
  searchInput: document.querySelector("#searchInput"),
  refreshButton: document.querySelector("#refreshButton"),
  statusText: document.querySelector("#statusText"),
  notesList: document.querySelector("#notesList"),
  generatedAt: document.querySelector("#generatedAt"),
  countTotal: document.querySelector("#countTotal"),
  countSpot: document.querySelector("#countSpot"),
  countUsdm: document.querySelector("#countUsdm"),
  dataSource: document.querySelector("#dataSource"),
  heroWindowLabel: document.querySelector("#heroWindowLabel"),
  gainersList: document.querySelector("#gainersList"),
  losersList: document.querySelector("#losersList"),
  activityList: document.querySelector("#activityList"),
};

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function readStoredEntry(key) {
  try {
    const raw = localStorage.getItem(`${STORAGE_PREFIX}:${key}`);
    if (!raw) {
      return null;
    }

    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") {
      return null;
    }

    return parsed;
  } catch {
    return null;
  }
}

function writeStoredEntry(key, entry) {
  try {
    localStorage.setItem(`${STORAGE_PREFIX}:${key}`, JSON.stringify(entry));
  } catch {
    // Ignore storage quota failures.
  }
}

function isLocalPreview() {
  return LOCAL_PREVIEW_HOSTNAMES.has(window.location.hostname);
}

async function withCache(key, ttlMs, loader, options = {}) {
  const force = options.force === true;
  const now = Date.now();
  const cached = runtimeCache.get(key);

  if (!force && cached?.value && cached.expiresAt > now) {
    return cached.value;
  }

  if (!force) {
    const stored = readStoredEntry(key);
    if (stored?.value && stored.expiresAt > now) {
      runtimeCache.set(key, stored);
      return stored.value;
    }
  }

  if (!force && cached?.promise) {
    return cached.promise;
  }

  const promise = (async () => {
    const value = await loader();
    const effectiveTtlMs = value?.backend?.isStale ? 15_000 : ttlMs;
    const entry = {
      value,
      expiresAt: Date.now() + effectiveTtlMs,
    };

    runtimeCache.set(key, entry);
    writeStoredEntry(key, entry);
    return value;
  })();

  runtimeCache.set(key, {
    value: cached?.value,
    expiresAt: cached?.expiresAt ?? 0,
    promise,
  });

  try {
    return await promise;
  } catch (error) {
    if (cached) {
      runtimeCache.set(key, cached);
    } else {
      runtimeCache.delete(key);
    }
    throw error;
  }
}

async function fetchJson(url, options = {}) {
  const {
    timeoutMs = 20_000,
    retries = 1,
    label = url.toString(),
    retryDelayMs = 500,
  } = options;

  let lastError = null;

  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetch(url, {
        signal: controller.signal,
        headers: {
          Accept: "application/json",
        },
      });

      const rawText = await response.text();
      let payload = null;

      try {
        payload = rawText ? JSON.parse(rawText) : null;
      } catch {
        throw new Error(`${label} returned invalid JSON`);
      }

      if (!response.ok) {
        throw new Error(payload?.message || payload?.error || `${label} failed with ${response.status}`);
      }

      return payload;
    } catch (error) {
      lastError = error;
      const shouldRetry =
        attempt < retries &&
        (error.name === "AbortError" || String(error.message ?? "").includes("fetch"));

      if (shouldRetry) {
        await new Promise((resolve) => setTimeout(resolve, retryDelayMs * (attempt + 1)));
      } else {
        break;
      }
    } finally {
      clearTimeout(timeout);
    }
  }

  throw lastError ?? new Error(`${label} failed`);
}

async function fetchSnapshotFromApi(windowKey, options = {}) {
  const url = new URL("/api/snapshot", window.location.origin);
  url.searchParams.set("window", windowKey);

  if (options.force === true) {
    url.searchParams.set("refresh", "1");
  }

  return fetchJson(url, {
    label: `Snapshot ${windowKey}`,
    retries: 1,
    timeoutMs: 90_000,
  });
}

function shouldUseDemoFallback(error) {
  if (!isLocalPreview()) {
    return false;
  }

  const message = String(error?.message ?? "");
  return message.includes("invalid JSON") || message.includes("fetch") || error?.name === "AbortError";
}

async function loadDemoSnapshot(windowKey, reason = "") {
  const url = new URL(DEMO_SNAPSHOTS_PATH, window.location.origin);
  const bundle = await fetchJson(url, {
    label: "Local demo snapshots",
    retries: 0,
    timeoutMs: 8_000,
  });

  const payload = bundle?.[windowKey];
  if (!payload || typeof payload !== "object") {
    throw new Error(`Local demo snapshot is missing for ${windowKey}`);
  }

  const notes = Array.isArray(payload.notes) ? [...payload.notes] : [];
  notes.unshift({
    kind: "info",
    text: "当前展示的是本地演示快照，用于静态预览前端界面；切到带 Pages Functions 的 Wrangler 预览后会自动改为真实接口数据。",
  });

  if (reason) {
    notes.push({
      kind: "warn",
      text: `已自动回退到演示数据，原因：${reason}`,
    });
  }

  return {
    ...payload,
    notes,
    backend: {
      cacheStatus: "demo-local",
      storage: "demo",
      hasD1: false,
      isDemo: true,
      isStale: false,
      servedAt: new Date().toISOString(),
      fallbackReason: reason,
    },
  };
}

async function getSnapshot(windowKey, options = {}, setStage = () => {}) {
  setStage("正在请求服务端榜单...");

  return withCache(
    `snapshot:${windowKey}`,
    WINDOW_CONFIG[windowKey].ttlMs,
    async () => {
      try {
        return await fetchSnapshotFromApi(windowKey, options);
      } catch (error) {
        if (!shouldUseDemoFallback(error)) {
          throw error;
        }

        setStage("接口不可用，正在切换本地演示快照...");
        return loadDemoSnapshot(windowKey, String(error.message ?? ""));
      }
    },
    options,
  );
}

function updateWindowLabels() {
  const label = WINDOW_CONFIG[state.selectedWindow]?.label ?? state.selectedWindow;
  elements.heroWindowLabel.textContent = label;
}

function renderWindowTabs() {
  elements.windowTabs.innerHTML = windowOptions
    .map(
      (option) => `
        <button
          class="tab-button ${option.key === state.selectedWindow ? "is-active" : ""}"
          type="button"
          data-window="${option.key}"
        >
          ${option.label}
        </button>
      `,
    )
    .join("");

  elements.windowTabs.querySelectorAll("[data-window]").forEach((button) => {
    button.addEventListener("click", () => {
      const nextWindow = button.getAttribute("data-window");
      if (nextWindow === state.selectedWindow) {
        return;
      }

      state.selectedWindow = nextWindow;
      renderWindowTabs();
      updateWindowLabels();
      loadSnapshot();
    });
  });
}

function formatPercent(value) {
  if (!Number.isFinite(value)) {
    return "-";
  }

  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(2)}%`;
}

function formatUnsignedPercent(value) {
  if (!Number.isFinite(value)) {
    return "-";
  }

  return `${value.toFixed(2)}%`;
}

function formatPrice(value) {
  if (!Number.isFinite(value)) {
    return "-";
  }

  if (value >= 1000) {
    return value.toLocaleString("en-US", { maximumFractionDigits: 2 });
  }

  if (value >= 1) {
    return value.toLocaleString("en-US", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 6,
    });
  }

  if (value >= 0.01) {
    return value.toLocaleString("en-US", {
      minimumFractionDigits: 4,
      maximumFractionDigits: 8,
    });
  }

  return value.toLocaleString("en-US", {
    minimumFractionDigits: 6,
    maximumFractionDigits: 10,
  });
}

function formatTime(value) {
  if (!value) {
    return "-";
  }

  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(value));
}

function getValueClass(value) {
  if (!Number.isFinite(value) || value === 0) {
    return "value-neutral";
  }

  return value > 0 ? "value-positive" : "value-negative";
}

function getFilteredItems() {
  const items = Array.isArray(state.payload?.items) ? state.payload.items : [];

  return items.filter((item) => {
    if (state.selectedSegment !== "all" && item.segment !== state.selectedSegment) {
      return false;
    }

    if (!state.searchText) {
      return true;
    }

    const keyword = state.searchText.toLowerCase();
    const displaySymbol = String(item.displaySymbol ?? "").toLowerCase();
    const segmentLabel = String(item.segmentLabel ?? "").toLowerCase();
    return (
      item.symbol.toLowerCase().includes(keyword) ||
      displaySymbol.includes(keyword) ||
      segmentLabel.includes(keyword)
    );
  });
}

function sortItems(items, mode) {
  const copy = [...items];

  copy.sort((left, right) => {
    if (mode === "asc") {
      return (left.changePercent ?? Infinity) - (right.changePercent ?? Infinity);
    }

    return (right.changePercent ?? -Infinity) - (left.changePercent ?? -Infinity);
  });

  return copy;
}

function sortByActivity(items) {
  const copy = [...items];

  copy.sort((left, right) => {
    const rangeDelta = (right.rangePercent ?? -Infinity) - (left.rangePercent ?? -Infinity);
    if (rangeDelta !== 0) {
      return rangeDelta;
    }

    return Math.abs(right.changePercent ?? 0) - Math.abs(left.changePercent ?? 0);
  });

  return copy;
}

function getWindowLabel() {
  return WINDOW_CONFIG[state.selectedWindow]?.label ?? state.selectedWindow;
}

function createInfoChip(label, options = {}) {
  const className = options.className ? ` ${options.className}` : "";
  const segmentAttr = options.segment ? ` data-segment="${escapeHtml(options.segment)}"` : "";
  return `<span class="info-chip${className}"${segmentAttr}>${escapeHtml(label)}</span>`;
}

function createDetailItem(label, value, className = "") {
  const toneClass = className ? ` ${className}` : "";
  return `
    <div class="detail-item${toneClass}">
      <span>${escapeHtml(label)}</span>
      <strong>${escapeHtml(value)}</strong>
    </div>
  `;
}

function getAccuracyMeta(item) {
  if (item.dataStatus === "history-failed") {
    return {
      className: "is-missing",
      label: "缺失",
    };
  }

  if (item.dataStatus === "history-short") {
    return {
      className: "is-missing",
      label: "历史不足",
    };
  }

  return {
    className: item.isExact ? "is-exact" : "is-approx",
    label: item.isExact ? "精确" : "近似",
  };
}

function createCardChips(item, options = {}) {
  const accuracy = getAccuracyMeta(item);
  const chips = [
    createInfoChip(item.segmentLabel, {
      className: "segment-chip",
      segment: item.segment,
    }),
    createInfoChip(accuracy.label, {
      className: `accuracy-chip ${accuracy.className}`,
    }),
    createInfoChip(`最新价 ${formatPrice(item.lastPrice)}`, {
      className: "metric-chip",
    }),
    createInfoChip(`窗口 ${getWindowLabel()}`, {
      className: "window-chip",
    }),
  ];

  if (options.includeChange) {
    chips.push(
      createInfoChip(`${getWindowLabel()}涨跌 ${formatPercent(item.changePercent)}`, {
        className: getValueClass(item.changePercent),
      }),
    );
  }

  if (item.dataIssue) {
    chips.push(
      createInfoChip(item.dataIssue, {
        className: "warn-chip",
      }),
    );
  }

  return chips.join("");
}

function createBoardCard(item, rank, direction) {
  const displayName = escapeHtml(item.displaySymbol || item.symbol);
  const rawSymbol = escapeHtml(item.symbol);
  const directionLabel = direction === "loser" ? `${getWindowLabel()}跌幅` : `${getWindowLabel()}涨幅`;

  return `
    <article class="market-card market-card-${direction}">
      <div class="market-card-top">
        <div class="market-title-row">
          <span class="market-rank">#${rank}</span>
          <div class="market-card-copy">
            <h3>${displayName}</h3>
            <p>${rawSymbol}</p>
            ${item.dataIssue ? `<span class="market-inline-note">${escapeHtml(item.dataIssue)}</span>` : ""}
          </div>
        </div>
        <div class="market-primary ${getValueClass(item.changePercent)}">
          <span>${escapeHtml(directionLabel)}</span>
          <strong>${escapeHtml(formatPercent(item.changePercent))}</strong>
        </div>
      </div>
      <div class="market-chip-row">
        ${createCardChips(item)}
      </div>
    </article>
  `;
}

function createActivityCard(item, rank) {
  return `
    <article class="market-card market-card-activity">
      <div class="market-card-top">
        <div class="market-title-row">
          <span class="market-rank">#${rank}</span>
          <div class="market-card-copy">
            <h3>${escapeHtml(item.displaySymbol || item.symbol)}</h3>
            <p>${escapeHtml(item.symbol)}</p>
            ${item.dataIssue ? `<span class="market-inline-note">${escapeHtml(item.dataIssue)}</span>` : ""}
          </div>
        </div>
        <div class="market-primary ${getValueClass(item.rangePercent)}">
          <span>${escapeHtml(`${getWindowLabel()}异动`)}</span>
          <strong>${escapeHtml(formatUnsignedPercent(item.rangePercent))}</strong>
        </div>
      </div>
      <div class="market-chip-row">
        ${createCardChips(item, { includeChange: true })}
      </div>
      <div class="detail-grid">
        ${createDetailItem("最新价", formatPrice(item.lastPrice))}
        ${createDetailItem(`${getWindowLabel()}涨跌`, formatPercent(item.changePercent), getValueClass(item.changePercent))}
        ${createDetailItem("周期最高", formatPrice(item.windowHigh))}
        ${createDetailItem("周期最低", formatPrice(item.windowLow))}
      </div>
    </article>
  `;
}

function renderEmptyState(target, message) {
  target.innerHTML = `<div class="empty-state">${escapeHtml(message)}</div>`;
}

function renderNotes() {
  const notes = Array.isArray(state.payload?.notes) ? state.payload.notes : [];
  if (notes.length === 0) {
    renderEmptyState(elements.notesList, "当前窗口没有额外说明。");
    return;
  }

  elements.notesList.innerHTML = notes
    .map((note) => {
      const meta = NOTE_KIND_META[note.kind] ?? NOTE_KIND_META.info;
      return `
        <div class="note-row">
          <span class="note-tag ${meta.className}">${meta.label}</span>
          <p>${escapeHtml(note.text)}</p>
        </div>
      `;
    })
    .join("");
}

function getDataSourceLabel(payload) {
  if (payload?.backend?.isDemo) {
    return "Local Demo / 静态预览";
  }

  if (!payload?.backend?.hasD1) {
    return "Pages / 未配置 D1";
  }

  if (payload?.backend?.isStale) {
    return "Pages / D1 旧快照";
  }

  return "Pages / D1 实时";
}

function renderMeta() {
  elements.generatedAt.textContent = state.payload?.generatedAt
    ? formatTime(state.payload.generatedAt)
    : "-";

  elements.countTotal.textContent = state.payload?.counts?.total ?? "-";
  elements.countSpot.textContent = state.payload?.counts?.spot ?? "-";
  elements.countUsdm.textContent = state.payload?.counts?.usdm ?? "-";
  elements.dataSource.textContent = getDataSourceLabel(state.payload);
}

function renderTables() {
  const filtered = getFilteredItems();
  const gainers = sortItems(filtered, "desc").slice(0, state.leaderboardLimit);
  const losers = sortItems(filtered, "asc").slice(0, state.leaderboardLimit);
  const activityItems = sortByActivity(filtered).slice(0, state.leaderboardLimit);

  if (gainers.length === 0) {
    renderEmptyState(elements.gainersList, "当前筛选条件下没有数据。");
  } else {
    elements.gainersList.innerHTML = gainers
      .map((item, index) => createBoardCard(item, index + 1, "gainer"))
      .join("");
  }

  if (losers.length === 0) {
    renderEmptyState(elements.losersList, "当前筛选条件下没有数据。");
  } else {
    elements.losersList.innerHTML = losers
      .map((item, index) => createBoardCard(item, index + 1, "loser"))
      .join("");
  }

  if (activityItems.length === 0) {
    renderEmptyState(elements.activityList, "当前筛选条件下没有数据。");
  } else {
    elements.activityList.innerHTML = activityItems
      .map((item, index) => createActivityCard(item, index + 1))
      .join("");
  }
}

function renderAll() {
  renderMeta();
  renderNotes();
  renderTables();
}

function setStatus(message) {
  elements.statusText.textContent = message;
}

function buildLoadedStatus(payload) {
  const missingCount =
    (payload.diagnostics?.usdmHistoryFailures?.length ?? 0) +
    (payload.diagnostics?.usdmInsufficientHistory?.length ?? 0);

  const statusParts = [`已载入 ${payload.windowLabel} 榜单`, `共 ${payload.counts.total} 个标的`];

  if (payload.backend?.isDemo) {
    statusParts.push("当前为本地演示数据");
  }

  if (payload.backend?.isStale) {
    statusParts.push("当前展示的是旧快照");
  }

  if (payload.window !== "1d" && missingCount > 0) {
    statusParts.push(`${missingCount} 个永续标的历史仍在积累`);
  }

  return `${statusParts.join("，")}。`;
}

async function loadSnapshot(options = {}) {
  const force = options.force === true;
  const currentLoadId = state.loadId + 1;
  state.loadId = currentLoadId;
  state.loading = true;

  try {
    const payload = await getSnapshot(
      state.selectedWindow,
      { force },
      (message) => {
        if (state.loadId === currentLoadId) {
          setStatus(message);
        }
      },
    );

    if (state.loadId !== currentLoadId) {
      return;
    }

    state.payload = payload;
    renderAll();
    setStatus(buildLoadedStatus(payload));
  } catch (error) {
    if (state.loadId !== currentLoadId) {
      return;
    }

    if (state.payload) {
      setStatus("刷新失败，当前继续展示本地缓存/旧快照。请检查 D1 绑定或定时同步是否正常。");
      return;
    }

    setStatus(`加载失败：${error.message ? `(${error.message})` : "请检查 /api/snapshot 或先运行同步任务"}`);
    renderNotes();
    renderEmptyState(elements.gainersList, "未能获取数据。");
    renderEmptyState(elements.losersList, "未能获取数据。");
    renderEmptyState(elements.activityList, "未能获取数据。");
  } finally {
    if (state.loadId === currentLoadId) {
      state.loading = false;
    }
  }
}

function scheduleRefresh() {
  if (state.refreshTimer) {
    clearInterval(state.refreshTimer);
  }

  state.refreshTimer = setInterval(() => {
    if (!document.hidden) {
      loadSnapshot();
    }
  }, 5 * 60_000);
}

function bindControls() {
  elements.segmentFilter.addEventListener("change", (event) => {
    state.selectedSegment = event.target.value;
    renderTables();
  });

  elements.leaderboardLimit.addEventListener("change", (event) => {
    state.leaderboardLimit = Number(event.target.value);
    renderTables();
  });

  elements.searchInput.addEventListener("input", (event) => {
    state.searchText = event.target.value.trim();
    renderTables();
  });

  elements.refreshButton.addEventListener("click", () => {
    loadSnapshot({ force: true });
  });
}

function bootstrap() {
  renderWindowTabs();
  updateWindowLabels();
  bindControls();
  scheduleRefresh();
  loadSnapshot();
}

bootstrap();

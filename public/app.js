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
  gainerWindowHead: document.querySelector("#gainerWindowHead"),
  loserWindowHead: document.querySelector("#loserWindowHead"),
  activityChangeHead: document.querySelector("#activityChangeHead"),
  activityRangeHead: document.querySelector("#activityRangeHead"),
  gainersBody: document.querySelector("#gainersBody"),
  losersBody: document.querySelector("#losersBody"),
  activityBody: document.querySelector("#activityBody"),
};

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

async function getSnapshot(windowKey, options = {}, setStage = () => {}) {
  setStage("正在请求服务端榜单...");

  return withCache(
    `snapshot:${windowKey}`,
    WINDOW_CONFIG[windowKey].ttlMs,
    () => fetchSnapshotFromApi(windowKey, options),
    options,
  );
}

function updateWindowLabels() {
  const label = WINDOW_CONFIG[state.selectedWindow]?.label ?? state.selectedWindow;
  elements.heroWindowLabel.textContent = label;
  elements.gainerWindowHead.textContent = `${label}涨跌幅`;
  elements.loserWindowHead.textContent = `${label}涨跌幅`;
  elements.activityChangeHead.textContent = `${label}涨跌幅`;
  elements.activityRangeHead.textContent = `${label}异动幅度`;
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
    return (
      item.symbol.toLowerCase().includes(keyword) ||
      item.displaySymbol.toLowerCase().includes(keyword) ||
      item.segmentLabel.toLowerCase().includes(keyword)
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

function createSymbolCell(item) {
  return `
    <div class="symbol-cell">
      <span class="symbol-main">${item.displaySymbol || item.symbol}</span>
      <span class="symbol-sub">${item.symbol}</span>
      ${item.dataIssue ? `<span class="symbol-issue">${item.dataIssue}</span>` : ""}
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

function createBoardRow(item, rank) {
  return `
    <tr>
      <td>${rank}</td>
      <td>${createSymbolCell(item)}</td>
      <td><span class="segment-badge" data-segment="${item.segment}">${item.segmentLabel}</span></td>
      <td>${formatPrice(item.lastPrice)}</td>
      <td class="${getValueClass(item.changePercent)}">${formatPercent(item.changePercent)}</td>
    </tr>
  `;
}

function createActivityRow(item, rank) {
  const accuracy = getAccuracyMeta(item);
  return `
    <tr>
      <td>${rank}</td>
      <td>${createSymbolCell(item)}</td>
      <td><span class="segment-badge" data-segment="${item.segment}">${item.segmentLabel}</span></td>
      <td>${formatPrice(item.lastPrice)}</td>
      <td class="${getValueClass(item.changePercent)}">${formatPercent(item.changePercent)}</td>
      <td>${formatPrice(item.windowHigh)}</td>
      <td>${formatPrice(item.windowLow)}</td>
      <td class="${getValueClass(item.rangePercent)}">${formatUnsignedPercent(item.rangePercent)}</td>
      <td>
        <span class="accuracy-badge ${accuracy.className}">
          ${accuracy.label}
        </span>
      </td>
    </tr>
  `;
}

function renderEmptyState(target, message, colspan) {
  target.innerHTML = `<tr><td class="empty-state" colspan="${colspan}">${message}</td></tr>`;
}

function renderNotes() {
  const notes = Array.isArray(state.payload?.notes) ? state.payload.notes : [];
  elements.notesList.innerHTML = notes
    .map(
      (note) => `
        <div class="note-row">
          <span class="note-tag is-${note.kind}">${note.kind}</span>
          <p>${note.text}</p>
        </div>
      `,
    )
    .join("");
}

function getDataSourceLabel(payload) {
  if (!payload?.backend?.hasD1) {
    return "Pages Function / 未配置 D1";
  }

  if (payload?.backend?.isStale) {
    return "Pages Function / D1 旧快照";
  }

  return "Pages Function / D1";
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
    renderEmptyState(elements.gainersBody, "当前筛选条件下没有数据。", 5);
  } else {
    elements.gainersBody.innerHTML = gainers.map((item, index) => createBoardRow(item, index + 1)).join("");
  }

  if (losers.length === 0) {
    renderEmptyState(elements.losersBody, "当前筛选条件下没有数据。", 5);
  } else {
    elements.losersBody.innerHTML = losers.map((item, index) => createBoardRow(item, index + 1)).join("");
  }

  if (activityItems.length === 0) {
    renderEmptyState(elements.activityBody, "当前筛选条件下没有数据。", 9);
  } else {
    elements.activityBody.innerHTML = activityItems
      .map((item, index) => createActivityRow(item, index + 1))
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

  let suffix = "";
  if (payload.backend?.isStale) {
    suffix += " 当前展示的是旧快照，等待下一次定时同步。";
  }

  if (payload.window !== "1d" && missingCount > 0) {
    suffix += ` 其中 ${missingCount} 个永续标的历史仍在积累。`;
  }

  return `已载入 ${payload.windowLabel} 榜单，共 ${payload.counts.total} 个标的。${suffix}`;
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
    renderEmptyState(elements.gainersBody, "未能获取数据。", 5);
    renderEmptyState(elements.losersBody, "未能获取数据。", 5);
    renderEmptyState(elements.activityBody, "未能获取数据。", 9);
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

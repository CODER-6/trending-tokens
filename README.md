# Binance USDT Movers

一个适合小流量免费部署的版本：

- 前端：Cloudflare Pages
- 公共 API：Pages Functions / Workers runtime
- 数据库：D1
- 定时采集：GitHub Actions

页面和公开 API 不再在用户请求时直连 Binance。GitHub Actions 定时拉取 `spot + U 本位永续` 数据，写入 `POST /api/internal/ingest`，再由 Worker 落到 D1。前端 `/api/snapshot` 只读取 D1 快照。

## 当前实现

页面只保留：

- 现货 `USDT` 交易对
- U 本位永续 `USDT` 交易对
- `1d / 3d / 7d` 三个周期

页面包含：

- 涨幅榜
- 跌幅榜
- 异动榜

异动榜按 `(周期最高价 - 周期最低价) / 窗口起点价` 排序。

## 架构说明

### 1. 前端

- Pages 直接托管 `public/`
- 页面请求 `/api/snapshot?window=1d|3d|7d`

### 2. 公共 API

- `functions/api/snapshot.js`
- 只从 D1 读取已入库快照
- 如果最新快照超过约 90 分钟没更新，接口仍返回旧数据，但 `backend.isStale=true`

### 3. 内部写入 API

- `functions/api/internal/ingest.js`
- 只接受 `POST`
- 需要 `Authorization: Bearer <INGEST_SHARED_SECRET>` 或 `x-ingest-secret`

### 4. D1 的用途

D1 里有两张表：

- `snapshot_cache`：缓存 `1d / 3d / 7d` 完整榜单
- `usdm_daily_stats`：累计 U 本位永续每日快照，用来近似计算 `3d / 7d`

这是一个重要取舍：

- 现货 `1d / 3d / 7d` 仍然走 Binance Spot rolling window，属于精确窗口值
- U 本位永续 `1d` 仍然走 Binance 24h ticker，属于精确窗口值
- U 本位永续 `3d / 7d` 使用 D1 中累积的日快照做近似计算

## 本地开发

### 1. 启动 Pages 本地服务

先给本地 Worker 一个共享密钥。当前仓库的 `.dev.vars` 可以这样写：

```text
INGEST_SHARED_SECRET=local-dev-secret
```

然后启动：

```bash
npm start
```

### 2. 本地跑一次采集并写入

如果你需要代理访问 Binance，可以使用你现在这组环境变量：

```bash
export NODE_OPTIONS=--use-env-proxy
export https_proxy=http://127.0.0.1:7897
export http_proxy=http://127.0.0.1:7897
export all_proxy=socks5://127.0.0.1:7897
export INGEST_BASE_URL=http://127.0.0.1:8788
export INGEST_SHARED_SECRET=local-dev-secret
npm run sync:push
```

完成后打开：

- `http://localhost:8788/`
- `http://localhost:8788/api/snapshot?window=1d`

如果是首次运行：

- `1d` 会立刻有数据
- `3d / 7d` 的永续部分需要先累计到对应天数

## D1 初始化

### 1. 创建数据库

```bash
npx wrangler d1 create crypto-hot-coin
```

### 2. 把返回的 ID 写进 `wrangler.jsonc`

绑定名必须是 `DB`：

```jsonc
"d1_databases": [
  {
    "binding": "DB",
    "database_name": "crypto-hot-coin",
    "database_id": "replace-with-your-d1-database-id",
    "preview_database_id": "DB"
  }
]
```

### 3. 执行 schema

```bash
npx wrangler d1 execute crypto-hot-coin --remote --file=./migrations/0001_init.sql
```

## 部署到 Cloudflare Pages

### 1. Pages 项目

把仓库导入 Cloudflare Pages：

- `Framework preset`: `None`
- `Build command`: `exit 0`
- `Build output directory`: `public`

### 2. D1 绑定

在 Pages 项目里绑定 D1：

- Variable name: `DB`
- Database: `crypto-hot-coin`

### 3. Worker 环境变量

在 Pages 项目里增加：

```text
INGEST_SHARED_SECRET=一串足够长的随机字符串
```

这个值要和 GitHub Secrets 里的 `INGEST_SHARED_SECRET` 保持一致。

## GitHub Actions 定时采集

仓库已经带了：

- `.github/workflows/sync.yml`
- `scripts/collect-and-push.mjs`

默认每小时第 `17` 分钟执行一次，也支持手动触发。

你需要在 GitHub 仓库的 `Settings -> Secrets and variables -> Actions` 里配置两个 Secrets：

```text
INGEST_BASE_URL=https://你的-pages-域名
INGEST_SHARED_SECRET=和 Cloudflare 里一致的密钥
```

建议：

- 如果你用 `pages.dev`，值写成 `https://your-project.pages.dev`
- 首次配置完成后，先手动运行一次 `Sync Binance Snapshots`

## 为什么这版更适合免费方案

- 用户请求只读 D1，不依赖 Cloudflare 运行时直接访问 Binance Futures
- GitHub Actions 不需要额外服务器
- 一小时同步一次足够满足小流量榜单站点
- 就算偶发漏跑，页面仍能显示上一次成功快照

## 文件结构

```text
public/
  index.html
  app.js
  styles.css
functions/
  api/
    snapshot.js
    internal/
      ingest.js
  _lib/
    snapshot-service.js
migrations/
  0001_init.sql
scripts/
  collect-and-push.mjs
.github/
  workflows/
    sync.yml
wrangler.jsonc
```

## 检查

```bash
npm run check
```

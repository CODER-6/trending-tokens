# Binance USDT Movers

一个适合部署到 Cloudflare Pages 的纯静态前端页面。浏览器会直接请求 Binance 公共市场数据，不依赖自建后端。

当前只保留：

- 现货 `USDT` 交易对
- U 本位永续 `USDT` 交易对
- `1d / 3d / 7d` 三个周期

页面包含：

- 涨幅榜
- 跌幅榜
- 异动榜

异动榜按 `(周期最高价 - 周期最低价) / 窗口起点价` 排序。

## 本地预览

```bash
npm start
```

默认地址：

```text
http://127.0.0.1:8124
```

这只是本地静态文件预览，不承担任何 API 聚合逻辑。

## 部署到 Cloudflare Pages

1. 把仓库导入 Cloudflare Pages。
2. `Framework preset` 选 `None`。
3. `Build command` 留空。
4. `Build output directory` 设为 `public`。
5. 部署即可。

## 数据逻辑

- 现货 `1d / 3d / 7d`：直接调用 Binance Spot rolling window 接口，属于精确窗口值。
- U 本位永续 `1d`：直接调用 Binance Futures `24hr ticker`，属于精确值。
- U 本位永续 `3d / 7d`：使用 `1d` K 线近似计算，因此和真正滚动窗口可能有轻微偏差。

## 重要说明

- 这是纯前端版本，数据请求从用户浏览器直接发往 Binance。
- 如果用户当前网络无法访问 Binance，页面就无法正常加载数据。
- 首次打开 `3d / 7d` 会慢一些，因为浏览器需要逐个拉取永续 K 线并在本地计算。
- 页面会在浏览器本地做短缓存，所以短时间内重复切换同一窗口会快很多。

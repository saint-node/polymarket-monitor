# Polymarket Monitor v6 — Runbook

短 runbook: 记录当前部署拓扑、启停、常见故障排查。代码仅 `monitor_v6.py` 一个入口。

## 1. 运行拓扑 (当前生产)

```
Telegram servers
      │  HTTPS
      ▼
monitor.saint-node.com:8443   ← Nginx 终止 TLS (Let's Encrypt)
      │  proxy_pass
      ▼
127.0.0.1:18080/webhook       ← monitor_v6.py (aiohttp)
      │
      ├─► Telegram Bot A / Bot B (sendMessage, answerCallbackQuery)
      ├─► Supabase REST (markets / spikes / articles / human_clicks)
      ├─► OpenRouter (Sonar 检索 + LLM 研判)
      └─► Polymarket Gamma API (行情 + 结算)
```

- 公网入口: `https://monitor.saint-node.com:8443/webhook`
- 应用监听: `127.0.0.1:18080` (HTTP, 只对本机)
- 443 端口由其他代理服务占用, webhook 走 8443 与其共存

## 2. 配置位置

- 环境变量: [.env](.env) (与 `monitor_v6.py` 同目录, 已 gitignore)
  - `OPENROUTER_API_KEY`
  - `TELEGRAM_BOT_A_TOKEN` / `TELEGRAM_BOT_B_TOKEN` / `TELEGRAM_CHAT_ID`
  - `SUPABASE_URL` / `SUPABASE_SERVICE_ROLE_KEY`
  - `WEBHOOK_URL` (必须以 `https://monitor.saint-node.com:8443/webhook` 形式)
  - `WEBHOOK_SECRET_TOKEN` (与 `setWebhook` 的 secret 一致)
- 端口/阈值: 代码常量 (`monitor_v6.py` §2)
- Nginx: `/etc/nginx/sites-enabled/default` 内 `listen 8443 ssl` server 块

## 3. 启停 (systemd)

首次安装:

```bash
sudo cp /root/polymarket-monitor/deploy/polymarket-monitor.service \
        /etc/systemd/system/polymarket-monitor.service
sudo systemctl daemon-reload
sudo systemctl enable polymarket-monitor
sudo systemctl start polymarket-monitor
```

从 nohup 迁移到 systemd (一次性):

```bash
pkill -f 'python3 monitor_v6.py' || true
sudo systemctl start polymarket-monitor
sudo systemctl status polymarket-monitor
```

日常操作:

```bash
sudo systemctl restart polymarket-monitor
sudo systemctl stop polymarket-monitor
sudo systemctl status polymarket-monitor
tail -f /root/polymarket-monitor/monitor.log
```

## 4. 验收 (任意一次真实点击后应出现)

`tail -n 80 /root/polymarket-monitor/monitor.log` 期望顺序:

1. `[sonar] ok ...` / `[sonar] empty ...`
2. `[llm] ok spike=... judgment=...`
3. `POST .../markets` + `POST .../spikes` + (可选)`POST .../articles` 返回 201
4. `[bot_a] sent spike=... message_id=...`
5. (用户点击后) `[webhook] click spike=... judgment=...`
6. `aiohttp.access 127.0.0.1 ... POST /webhook ... 200`
7. `[bot_b] sent spike=... human=... llm=...`
8. `POST .../human_clicks ... 201 Created`

Supabase 可直接查 `human_clicks` 表确认落库。

## 5. 常见故障排查

### 5.1 点击按钮一直 loading / Bot B 不发

可能原因 (按概率排序):
- webhook 未到达程序 -> 查 nginx 8443 是否监听 + 查 `last_error_message`
- secret 不一致 -> 查 `.env` 与 `setWebhook` 注册值
- 程序未在跑 -> `systemctl status polymarket-monitor`

快速检查:

```bash
curl "https://api.telegram.org/bot<BOT_A_TOKEN>/getWebhookInfo"
ss -tlnp | grep -E ':8443|:18080'
```

重点字段: `url` 是否为 8443、`pending_update_count` 是否涨、`last_error_message` 是否最新。

### 5.2 Nginx 证书错误 (certificate verify failed)

- 确认 443 / 8443 的实际 TLS 返回者: `echo | openssl s_client -connect monitor.saint-node.com:8443 -servername monitor.saint-node.com 2>/dev/null | openssl x509 -noout -subject -issuer -dates`
- `subject` / `issuer` 不是 Let's Encrypt -> 有其他进程占端口, 检查 `ss -lntp | grep :<port>`

### 5.3 scan 抓到过期市场

- v6 起 `passes_mg` 会过滤 `days_left < 0`; 如仍出现过期市场, 检查 Gamma 返回的 `endDate` 解析是否失败 (解析失败会跳过该市场)

### 5.4 Telegram 报端口非法

- Telegram webhook 只允许 `443/80/88/8443`; 切换公网端口时必须从这四个里选, 否则 `setWebhook` 会 400

### 5.5 Supabase 401 / 权限错

- 确认使用 `service_role` key (非 anon key); 检查 `.env` 中 `SUPABASE_SERVICE_ROLE_KEY`

## 6. 手工重注册 webhook (兜底)

启动时的 `set_webhook` 为 best-effort, 失败不阻断. 可人工兜底:

```bash
curl -X POST "https://api.telegram.org/bot<BOT_A_TOKEN>/setWebhook" \
     -H "Content-Type: application/json" \
     -d '{"url":"https://monitor.saint-node.com:8443/webhook","secret_token":"<WEBHOOK_SECRET_TOKEN>"}'
```

删除 webhook (调试时):

```bash
curl "https://api.telegram.org/bot<BOT_A_TOKEN>/deleteWebhook"
```

## 7. 关键不变量

- WebHook 公网端口必须是 `443/80/88/8443` 之一 (Telegram 硬约束)
- `WEBHOOK_URL` path 必须与代码内 `WEBHOOK_PATH` (由 URL path 派生) 一致
- `WEBHOOK_SECRET_TOKEN` 两端必须一致 (header 不匹配直接 401)
- `.env` 缺任一必填 key, 启动阶段 KeyError 快速失败 (设计约定, 不做静默降级)
- 时间字段统一策略: 存储层统一 UTC (`Z` / `+00:00`), 展示层再转东八区或相对时间
- Sonar `published_at` 仅作为外部输入; 入库前必须做 UTC 标准化, 无法解析或未来值写 `NULL`
- Bot B 顶部 `triggered_at` 显示按东八区格式化 (`YYYY-MM-DD HH:MM:SS UTC+8`), 底层存储仍保持 UTC

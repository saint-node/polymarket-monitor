# Polymarket Monitor — Systemd + Git Quickstart

这份文件是日常速查手册：你不用翻聊天记录，只看这里就能完成服务管理和 Git 收口。

## 1) 首次安装服务（只做一次）

```bash
sudo cp /root/polymarket-monitor/deploy/polymarket-monitor.service /etc/systemd/system/polymarket-monitor.service
sudo systemctl daemon-reload
sudo systemctl enable polymarket-monitor
sudo systemctl start polymarket-monitor
sudo systemctl status polymarket-monitor
```

- 作用：把服务注册到 systemd，开启开机自启并立即启动。

## 2) 日常常用命令

```bash
sudo systemctl start polymarket-monitor
sudo systemctl stop polymarket-monitor
sudo systemctl restart polymarket-monitor
sudo systemctl status polymarket-monitor
```

- 作用：启动/停止/重启/查看当前运行状态。

## 3) 日志查看

```bash
tail -n 100 /root/polymarket-monitor/monitor.log
tail -f /root/polymarket-monitor/monitor.log
```

- 第一条看最近 100 行，第二条实时追踪日志。
- 注意：`Ctrl+C` 只会退出 `tail -f`，不会停止 systemd 服务。

## 4) 改代码后怎么生效

```bash
sudo systemctl restart polymarket-monitor
sudo systemctl status polymarket-monitor
tail -n 80 /root/polymarket-monitor/monitor.log
```

- 作用：让新代码生效，并马上看是否正常启动。

## 5) 改了 service 文件后怎么生效

```bash
sudo systemctl daemon-reload
sudo systemctl restart polymarket-monitor
sudo systemctl status polymarket-monitor
```

- 作用：先让 systemd 重新加载 unit 配置，再重启服务。

## 6) 一键排查顺序（服务异常时）

```bash
sudo systemctl status polymarket-monitor
tail -n 120 /root/polymarket-monitor/monitor.log
ss -tlnp | grep -E ':8443|:18080'
```

- 顺序建议：先看服务状态 -> 再看日志 -> 最后看端口监听。

## 7) Git 最小闭环（避免代码不可追溯）

### 7.1 先看当前状态

```bash
git -C /root/polymarket-monitor status --short --branch
git -C /root/polymarket-monitor log --oneline -n 10
git -C /root/polymarket-monitor remote -v
```

- 作用：确认你在哪个分支、有哪些改动、远程仓库是否配置正确。

### 7.2 敏感/运行时文件不要提交

建议忽略（或保持不提交）：

- `.env`
- `monitor.log`
- `__pycache__/`
- `.DS_Store`

原则：真实密钥和本机运行垃圾文件不进 Git。

### 7.3 提交策略（建议）

- 代码改动和文档改动分开提交，回滚更干净。
- 每次提交前先看一眼变更范围，避免把不相关文件带进去。

## 8) 从本地改动到服务器生效（最小部署清单）

```bash
# 1) 代码变更后重启服务
sudo systemctl restart polymarket-monitor

# 2) 看服务状态
sudo systemctl status polymarket-monitor

# 3) 看关键日志
tail -n 80 /root/polymarket-monitor/monitor.log
```

- 验收关键字（示例）：
  - `[set_webhook] registered`
  - `[webhook] listening`
  - 点击后出现 `[webhook] click` / `[bot_b] sent`

## 9) 快速记忆版

- 改代码：`restart + status + tail`
- 改 service：`daemon-reload + restart + status`
- 日志追踪：`tail -f`
- `Ctrl+C` 不会停服务（只退出日志跟随）

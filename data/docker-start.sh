#!/bin/sh
# 容器入口。
# 前台：python3 server.py（平台靠它做健康检查，退出即触发重启）
# 后台：cloudflared Quick Tunnel（免费、无账号、无域名、无备案），
#       和你现在 Termux 里那条 trycloudflare.com 链路完全一样，
#       微信内置 WebView 能打开。
# 注意：data/ 必须挂「持久卷」（容器里路径 /app/data），留言 / 计数才不会因重新部署而丢。

set -u
# 按脚本自身所在目录定位（容器里就是 /app），不写死路径
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$ROOT" || exit 1
mkdir -p "$ROOT/data"

PORT="${PORT:-8080}"

# ---- 1) 后台：免费公网隧道（断了自动重连，日志同时进 stdout 和持久卷） ----
if command -v cloudflared >/dev/null 2>&1; then
  (
    while true; do
      echo "[tunnel] 启动 cloudflared quick tunnel → http://127.0.0.1:${PORT}"
      cloudflared tunnel --no-autoupdate --protocol http2 \
        --url "http://127.0.0.1:${PORT}" 2>&1 | tee -a "$ROOT/data/tunnel.log"
      echo "[tunnel] 断开，10 秒后重连（网址可能会变）"
      sleep 10
    done
  ) &

  # ---- 2) 后台：把最新公网网址写进持久卷，方便随时查看 ----
  (
    while true; do
      u=$(grep -oE 'https://[a-zA-Z0-9._-]*trycloudflare\.com' \
            "$ROOT/data/tunnel.log" 2>/dev/null | tail -n 1)
      if [ -n "$u" ]; then
        printf '%s\n' "$u" > "$ROOT/data/tunnel-url.txt"
      fi
      sleep 5
    done
  ) &
else
  echo "[tunnel] 没找到 cloudflared，跳过；仍可用平台自带的固定域名访问" >&2
fi

# ---- 3) 前台：网站本体（server.py 一行未改） ----
echo "[site] IdleSite backend on 0.0.0.0:${PORT}  db=$ROOT/data/site.db"
exec python3 "$ROOT/server.py" "$PORT"

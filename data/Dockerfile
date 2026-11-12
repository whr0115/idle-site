# 免费上云镜像：站内 server.py / db.py / 前端文件 一行不改
# 只在外面套一层「常驻 Python + cloudflared 免费隧道」的启动壳。
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=Asia/Shanghai

WORKDIR /app
COPY . /app

# cloudflared 只用于生成免费的 trycloudflare.com 公网网址（不需要账号、域名、备案）。
# 容器退出/重连时网址会变，docker-start.sh 会把最新网址写进 /app/data/tunnel-url.txt。
RUN set -eu; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl ca-certificates; \
    case "$(uname -m)" in \
      x86_64|amd64) A=amd64 ;; \
      aarch64|arm64) A=arm64 ;; \
      *) A=amd64 ;; \
    esac; \
    curl -fL --retry 3 \
      "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-${A}" \
      -o /usr/local/bin/cloudflared; \
    chmod +x /usr/local/bin/cloudflared; \
    apt-get purge -y curl; \
    apt-get autoremove -y; \
    rm -rf /var/lib/apt/lists/*; \
    mkdir -p /app/data; \
    chmod +x /app/docker-start.sh

# 平台健康检查打这个端口；若平台强制注入别的 PORT，docker-start.sh 会自动跟随
EXPOSE 8080

CMD ["/app/docker-start.sh"]

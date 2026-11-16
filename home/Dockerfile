# Home bridge image: xray (one vless node as a local SOCKS5 proxy) + ssh/nc for the reverse tunnels.
# Build context is the repo root (it reuses scraper/vpn.py to turn the vless:// link into an Xray config).
FROM python:3.12-slim

ARG XRAY_VERSION=v1.8.24
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl unzip openssh-client netcat-openbsd \
    && case "$(dpkg --print-architecture)" in arm64) A=arm64-v8a ;; *) A=64 ;; esac \
    && curl -fsSL -o /tmp/xray.zip "https://github.com/XTLS/Xray-core/releases/download/${XRAY_VERSION}/Xray-linux-${A}.zip" \
    && unzip -o /tmp/xray.zip xray -d /usr/local/bin \
    && chmod +x /usr/local/bin/xray \
    && rm -f /tmp/xray.zip \
    && apt-get purge -y curl unzip && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY scraper/__init__.py scraper/constants.py scraper/vpn.py /app/scraper/
COPY home/bridge.py /app/bridge.py

CMD ["python3", "bridge.py"]

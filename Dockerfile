FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Xray (free vless lists) and sing-box (own subscription with vless + hysteria2): local SOCKS inbounds for the vless:// nodes used by url-check (scraper/vpn.py).
ARG XRAY_VERSION=v1.8.24
ARG SINGBOX_VERSION=1.14.4
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl unzip \
    && case "$(dpkg --print-architecture)" in arm64) A=arm64-v8a ;; *) A=64 ;; esac \
    && curl -fsSL -o /tmp/xray.zip "https://github.com/XTLS/Xray-core/releases/download/${XRAY_VERSION}/Xray-linux-${A}.zip" \
    && unzip -o /tmp/xray.zip xray -d /usr/local/bin \
    && chmod +x /usr/local/bin/xray \
    && rm -f /tmp/xray.zip \
    && case "$(dpkg --print-architecture)" in arm64) SA=arm64 ;; *) SA=amd64 ;; esac \
    && for name in "sing-box-${SINGBOX_VERSION}-linux-${SA}.tar.gz" "sing-box-${SINGBOX_VERSION}-linux-${SA}-glibc.tar.gz"; do \
         curl -fsSL -o /tmp/sb.tgz "https://github.com/SagerNet/sing-box/releases/download/v${SINGBOX_VERSION}/${name}" && break; \
       done \
    && tar -xzf /tmp/sb.tgz -C /tmp \
    && install -m 755 "$(find /tmp -name sing-box -type f | head -1)" /usr/local/bin/sing-box \
    && rm -rf /tmp/sb.tgz /tmp/sing-box-* \
    && apt-get purge -y curl unzip && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

COPY . .

CMD ["python3", "main.py"]

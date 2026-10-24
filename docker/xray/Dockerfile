FROM python:3.12-alpine

ARG XRAY_VERSION=1.8.24

RUN apk add --no-cache curl unzip ca-certificates \
    && curl -fsSL -o /tmp/xray.zip \
       "https://github.com/XTLS/Xray-core/releases/download/v${XRAY_VERSION}/Xray-linux-64.zip" \
    && unzip -o /tmp/xray.zip -d /usr/local/bin xray \
    && rm -f /tmp/xray.zip \
    && chmod +x /usr/local/bin/xray \
    && apk del unzip

COPY render_config.py /opt/render_config.py
COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh /opt/render_config.py

ENTRYPOINT ["/opt/entrypoint.sh"]

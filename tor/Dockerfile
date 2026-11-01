FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends tor ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# No ports are published: only the other containers of the compose network can reach the SOCKS ports.
ENTRYPOINT ["/entrypoint.sh"]

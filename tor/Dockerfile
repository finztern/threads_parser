FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends tor ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Runs with network_mode: host (see docker-compose.yml); SocksPolicy limits clients to local/docker addresses.
ENTRYPOINT ["/entrypoint.sh"]

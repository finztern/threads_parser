FROM mcr.microsoft.com/playwright/python:v1.47.0-jammy

# Non-interactive: tzdata would otherwise block the build on a timezone prompt.
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC

RUN apt-get update && apt-get install -y --no-install-recommends \
        xvfb x11vnc novnc websockify fluxbox \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/interactive_login.py /app/scripts/interactive_login.py
COPY docker/login/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV DISPLAY=:99
EXPOSE 6080

ENTRYPOINT ["/entrypoint.sh"]

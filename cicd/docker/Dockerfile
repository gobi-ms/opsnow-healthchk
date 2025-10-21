# syntax=docker/dockerfile:1

FROM python:3.9-slim

# Prevent Python from writing .pyc files and force stdout/stderr unbuffered
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install apt dependencies needed for Chrome and general tooling
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      wget \
      gnupg \
      unzip \
      xvfb \
      build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Google Chrome (stable)
RUN wget -O /usr/share/keyrings/google-linux-signing-keyring.gpg \
       https://dl.google.com/linux/linux_signing_key.pub \
 && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux-signing-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" \
       > /etc/apt/sources.list.d/google-chrome.list \
 && apt-get update && apt-get install -y --no-install-recommends \
       google-chrome-stable \
 && rm -rf /var/lib/apt/lists/*

# Create an unprivileged user for running the app
RUN useradd --create-home --shell /bin/bash appuser

WORKDIR /app

# Copy requirements and install. Keep requirements path relative to repo: src/requirements.txt
COPY src/requirements.txt /app/src/requirements.txt
RUN pip install --no-cache-dir -r /app/src/requirements.txt

# Copy the application source into /app/src
COPY src/ /app/src/

# Ensure correct ownership for the non-root user
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Set working directory to where the entrypoint expects to run
WORKDIR /app/src

# Default command (can be overridden by docker run arguments)
CMD ["python", "global_monitor.py"]

# Optionally add a healthcheck (uncomment and adjust if your app exposes a local HTTP health endpoint)
# HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
#   CMD curl -f http://localhost:8000/health || exit 1

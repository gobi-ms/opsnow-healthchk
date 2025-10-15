FROM python:3.9-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install base tools and add Google’s keyring/repo the modern way
RUN apt-get update && apt-get install -y --no-install-recommends \
      wget \
      gnupg \
      ca-certificates \
      unzip \
      xvfb \
    && wget -O /usr/share/keyrings/google-linux-signing-keyring.gpg \
         https://dl.google.com/linux/linux_signing_key.pub \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux-signing-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" \
         > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update && apt-get install -y --no-install-recommends \
         google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY src/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/global_monitor.py .
COPY src/global_config.yaml .

CMD ["python", "global_monitor.py"]

# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Install system dependencies required for Chrome and webdriver
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    xvfb \
    --no-install-recommends

# Install Google Chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update && apt-get install -y google-chrome-stable

# Set up the working directory
WORKDIR /app

# Copy source files first
COPY src/requirements.txt .
COPY src/global_monitor.py .
COPY src/global_config.yaml .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Command to run the application
CMD ["python", "global_monitor.py"]

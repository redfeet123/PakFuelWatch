FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/opt/dagster/app \
    DAGSTER_HOME=/opt/dagster/dagster_home

WORKDIR /opt/dagster/app

# Install base system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy package manifests for efficient layer caching
COPY requirements.txt pyproject.toml ./

# Upgrade pip and install all Python dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Install Patchright Chromium browser and its system OS dependencies
RUN apt-get update && \
    patchright install --with-deps chromium && \
    rm -rf /var/lib/apt/lists/*

# Prepare Dagster home and compute logs directories
RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/dagster_home/compute_logs

# Copy application code into container
COPY . /opt/dagster/app

WORKDIR /opt/dagster/app

FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV PATH=/root/.local/bin:$PATH

# Copy requirements first for Docker layer caching
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . /app

EXPOSE 8080

# Configure FastMCP to bind to all interfaces on port 8080
ENV MCP_HOST=0.0.0.0
ENV MCP_PORT=8080

# Signal headless environment so Cloud Portal auth fails fast
# instead of blocking for 2 minutes waiting for a browser callback
ENV HEADLESS=true

# Persist the Cloud Portal token cache in the mounted volume
# so it survives container restarts
ENV TOKEN_CACHE_PATH=/app/data/.incorta_cloud_token.json
RUN mkdir -p /app/data

CMD ["python", "server.py"]

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

# Persist HuggingFace / sentence-transformers model cache inside the image so
# the embedding model does not get downloaded at request time (which would
# block the asyncio event loop and starve MCP initialize handshakes).
ENV HF_HOME=/opt/hf-cache \
    SENTENCE_TRANSFORMERS_HOME=/opt/hf-cache
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('BAAI/bge-base-en-v1.5', device='cpu')"

# Copy application code
COPY . /app

EXPOSE 8080

# Bind to all interfaces
ENV MCP_HOST=0.0.0.0
ENV MCP_PORT=8080

# Public URL for OAuth callbacks — set this to your persistent domain
# e.g. https://7196.devopsportal.incortaops.com
ENV MCP_PUBLIC_URL=""

# Per-user Cloud Portal JWT storage directory
ENV TOKENS_DIR=/app/data/tokens
RUN mkdir -p /app/data/tokens

CMD ["python", "server.py"]
# force rebuild Sun Apr  5 16:20:07 EET 2026

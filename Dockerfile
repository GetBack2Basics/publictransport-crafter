FROM python:3.11-slim

LABEL maintainer="OWL DevOps"
LABEL description="PublicTransport Crafter Dashboard"

# Prevent interactive prompts during build
ENV DEBIAN_FRONTEND=noninteractive
ENV WHEROBOTS_ENV=dev

# Install system-level dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create working directory
WORKDIR /app

# Install Python dependencies
RUN pip install --no-cache-dir \
    websocket-client==1.8.0

# Copy project source
COPY . /app/

# Expose the dashboard port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080', timeout=3)" || exit 1

# Run the dashboard server
CMD ["python", "src/Dashboard/dashboard_server.py"]

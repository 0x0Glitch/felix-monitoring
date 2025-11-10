FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY pyproject.toml .
COPY uv.lock* .

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install uv && \
    uv pip install --system -r pyproject.toml

# Copy application code
COPY . .

# Create data directory
RUN mkdir -p /app/data

# Default command
CMD ["python", "-m", "user_monitoring.main"]

# Use Python 3.11 slim image for optimal size and performance
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PORT=8008

# Create non-root user for security
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Install system dependencies required for pandas and numpy
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Set work directory
WORKDIR /app

# Copy requirements first for better Docker layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .
COPY recommender.py .
COPY fast_ai_score.py .

# Copy data directory if it contains required files
COPY data/ ./data/

# Copy Google Cloud credentials if present
COPY aiml-*.json ./

# Create cache directory for pandas and other temp files
RUN mkdir -p /tmp/cache && \
    chown -R appuser:appuser /app /tmp/cache

# Switch to non-root user
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=60s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8008/', timeout=10)" || exit 1

# Expose port
EXPOSE 8008

# Start command with proper error handling
CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8008", "--workers", "1"]
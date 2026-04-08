# Pi runtime container — lightweight edge image
# Built for linux/arm64 (Raspberry Pi 5)
# Runs OpenClaw + the daily trading pipeline

FROM python:3.11-slim

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (layer cached unless requirements change)
COPY requirements/base.txt requirements/base.txt
COPY requirements/pi.txt requirements/pi.txt
RUN pip install --no-cache-dir -r requirements/pi.txt

# Copy source — only what the Pi runtime needs
COPY core/ core/
COPY services/ services/
COPY app/pi/ app/pi/
COPY config/ config/

# PYTHONPATH so `from core.contracts import schemas` works
ENV PYTHONPATH=/app

# Default entrypoint — OpenClaw or the daily run script
# Override via docker run or compose as needed
CMD ["python", "app/pi/run_daily.py"]

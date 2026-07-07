FROM python:3.12-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt .
# --timeout/--retries: requirements.txt pulls a multi-hundred-MB CUDA/torch
# stack; pip's default 15s socket timeout can trip on a single slow chunk
# near the end of a huge wheel (observed: torch-2.11.0, 530MB) and aborts
# the whole layer with no partial credit. Longer timeout + retries make
# that transient case retry instead of failing the entire install.
RUN pip install --no-cache-dir --timeout 120 --retries 5 -r requirements.txt

# App source
COPY . .

EXPOSE 9000
# Default: run API server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "9000", "--workers", "2"]

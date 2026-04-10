FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies
COPY src/kafka/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy full src folder
COPY src /app/src

RUN mkdir -p /app/logs /app/secrets

CMD ["python", "src/kafka/producer.py"]
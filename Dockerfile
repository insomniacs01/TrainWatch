FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends openssh-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY static ./static
COPY docs ./docs
COPY config.empty.yaml ./config.empty.yaml
COPY config.example.yaml ./config.example.yaml
COPY config.mock.yaml ./config.mock.yaml
COPY run.py ./run.py
COPY README.md ./README.md

RUN mkdir -p /app/data

EXPOSE 8420

CMD ["python", "run.py", "--config", "config.empty.yaml", "--host", "0.0.0.0", "--port", "8420"]

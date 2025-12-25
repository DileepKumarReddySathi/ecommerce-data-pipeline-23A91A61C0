FROM python:3.11-slim-bullseye

ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=on

WORKDIR /app

# ✅ REQUIRED: postgresql-client
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
        postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .
RUN chmod +x setup.sh

CMD ["bash", "setup.sh"]

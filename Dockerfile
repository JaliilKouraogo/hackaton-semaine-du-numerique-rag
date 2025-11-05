FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_VIRTUALENVS_CREATE=false

WORKDIR /app

COPY requirements.txt ./
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y --auto-remove build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY . .

ENV QDRANT_URL=http://qdrant:6333 \
    QDRANT_COLLECTION=burkina_corpus \
    EMBED_MODEL=BAAI/bge-m3 \
    OLLAMA_URL=http://ollama:11434 \
    OLLAMA_MODEL=gemma3:1b

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

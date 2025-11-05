#!/usr/bin/env python3
"""FastAPI service exposing the Qdrant + Ollama QA pipeline."""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import List, Optional

import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from qdrant_client import QdrantClient, models
from sentence_transformers import SentenceTransformer

DEFAULT_COLLECTION = os.getenv("QDRANT_COLLECTION", "burkina_corpus")
DEFAULT_QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
DEFAULT_OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
DEFAULT_EMBED_MODEL = os.getenv("EMBED_MODEL", "BAAI/bge-m3")
DEFAULT_OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:7b-instruct")
HAS_QUERY_VECTOR = hasattr(models, "QueryVector")

app = FastAPI(title="Burkina QA API", version="1.0.0")

FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"

allowed_origins = [
    origin.strip()
    for origin in os.getenv(
        "ALLOWED_ORIGINS",
        "http://localhost:5500,http://127.0.0.1:5500",
    ).split(",")
    if origin.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if FRONTEND_DIR.exists():
    app.mount("/frontend", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")


class AskRequest(BaseModel):
    question: str = Field(..., description="Question en français.")
    top_k: int = Field(4, ge=1, le=20, description="Nombre de documents récupérés.")
    score_threshold: Optional[float] = Field(
        None, description="Score minimal (cosine). Ignoré si None."
    )
    normalize: bool = Field(True, description="Normaliser l'embedding de la requête.")
    timeout: int = Field(180, ge=10, le=600, description="Timeout Ollama (secondes).")
    ollama_model: Optional[str] = Field(
        None, description="Modèle Ollama à utiliser (sinon défaut)."
    )


class SourceHit(BaseModel):
    source: str
    score: float
    payload: dict


class AskResponse(BaseModel):
    answer: str
    sources: List[SourceHit]


@lru_cache(maxsize=1)
def get_model() -> SentenceTransformer:
    return SentenceTransformer(DEFAULT_EMBED_MODEL)


@lru_cache(maxsize=1)
def get_client() -> QdrantClient:
    return QdrantClient(url=DEFAULT_QDRANT_URL, api_key=os.getenv("QDRANT_API_KEY"), timeout=30)


def build_prompt(question: str, hits: List[SourceHit]) -> str:
    snippets = []
    for idx, hit in enumerate(hits, 1):
        payload = hit.payload or {}
        src = payload.get("url") or payload.get("file_name") or payload.get("orig_file") or "source inconnue"
        text = payload.get("text") or ""
        snippets.append(
            f"### Document {idx}\nSource: {src}\nTexte:\n{text}".strip()
        )
    ctx = "\n\n".join(snippets)
    return (
        "Tu es un assistant qui répond en français sur les démarches administratives au Burkina Faso.\n"
        "Utilise uniquement les extraits ci-dessous. Mentionne les sources pertinentes.\n\n"
        f"### Extraits\n{ctx}\n\n### Question\n{question}\n\n### Réponse attendue\n"
    )


def call_ollama(prompt: str, timeout: int, model_name: Optional[str]) -> str:
    mdl = model_name or DEFAULT_OLLAMA_MODEL
    resp = requests.post(
        f"{DEFAULT_OLLAMA_URL}/api/generate",
        json={"model": mdl, "prompt": prompt, "stream": False},
        timeout=timeout,
    )
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Ollama error: {resp.text}")
    data = resp.json()
    return data.get("response", "").strip()


def qdrant_search(query_vector: List[float], top_k: int, score_threshold: Optional[float]) -> List[SourceHit]:
    client = get_client()
    threshold = score_threshold if score_threshold is not None else None
    try:
        if not HAS_QUERY_VECTOR:
            raise AttributeError("QueryVector not available")
        results = client.query_points(
            collection_name=DEFAULT_COLLECTION,
            query=models.Query(
                query=models.QueryVector(vector=query_vector),
            ),
            limit=top_k,
            with_payload=True,
            score_threshold=threshold,
        )
    except Exception:
        results = client.search(
            collection_name=DEFAULT_COLLECTION,
            query_vector=query_vector,
            limit=top_k,
            with_payload=True,
            score_threshold=threshold,
        )
    hits: List[SourceHit] = []
    for hit in results:
        payload = hit.payload or {}
        if "text" not in payload:
            payload["text"] = payload.get("text", "")
        hits.append(SourceHit(source=payload.get("url") or payload.get("file_name") or "source inconnue", score=hit.score, payload=payload))
    return hits


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "collection": DEFAULT_COLLECTION}


@app.get("/", include_in_schema=False)
def serve_frontend_root():
    if FRONTEND_DIR.exists():
        return FileResponse(FRONTEND_DIR / "index.html")
    return {"message": "Burkina QA API is running. Frontend directory missing."}


@app.post("/ask", response_model=AskResponse)
def ask(req: AskRequest) -> AskResponse:
    model = get_model()
    vector = model.encode(req.question, normalize_embeddings=req.normalize).tolist()
    hits = qdrant_search(vector, top_k=req.top_k, score_threshold=req.score_threshold)
    if not hits:
        raise HTTPException(status_code=404, detail="Aucun document pertinent trouvé.")
    prompt = build_prompt(req.question, hits)
    answer = call_ollama(prompt, timeout=req.timeout, model_name=req.ollama_model)
    return AskResponse(answer=answer, sources=hits)

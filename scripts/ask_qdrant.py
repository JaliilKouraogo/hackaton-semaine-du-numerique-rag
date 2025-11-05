#!/usr/bin/env python3
"""Quick CLI to query Qdrant with BGE-M3 embeddings and get an answer from Ollama.

Example:
  python scripts/ask_qdrant.py "Quelles sont les étapes pour obtenir le RCCM ?"
"""
from __future__ import annotations

import argparse
import json
import os
from textwrap import dedent

import requests
from qdrant_client import QdrantClient
from qdrant_client.models import SearchParams
from sentence_transformers import SentenceTransformer

DEFAULT_MODEL = "BAAI/bge-m3"
DEFAULT_COLLECTION = "burkina_corpus"
DEFAULT_OLLAMA_MODEL = "qwen2.5:7b-instruct"
DEFAULT_TOP_K = 4
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")


def build_prompt(question: str, contexts: list[dict]) -> str:
    snippets = []
    for idx, hit in enumerate(contexts, 1):
        payload = hit.payload or {}
        src = payload.get("url") or payload.get("file_name") or payload.get("orig_file") or "source inconnue"
        snippets.append(
            dedent(
                f"""
                ### Document {idx}
                Source: {src}
                Texte:
                {payload.get("text", "")}
                """
            ).strip()
        )
    joined = "\n\n".join(snippets)
    return dedent(
        f"""
        Tu es un assistant qui répond en français à des questions sur l'entrepreneuriat au Burkina Faso.
        Appuie-toi exclusivement sur les extraits suivants (ne pas inventer). Cite les sources si possible.

        ### Extraits
        {joined}

        ### Question
        {question}

        ### Réponse attendue
        """
    ).strip()


def call_ollama(model: str, prompt: str, timeout: int) -> str:
    resp = requests.post(
        f"{OLLAMA_URL}/api/generate",
        json={"model": model, "prompt": prompt, "stream": False},
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("response", "").strip()


def main() -> None:
    parser = argparse.ArgumentParser(description="Pose une question et récupère une réponse via Qdrant + Ollama.")
    parser.add_argument("question", help="Question en français.")
    parser.add_argument("--collection", default=DEFAULT_COLLECTION)
    parser.add_argument("--top-k", type=int, default=DEFAULT_TOP_K)
    parser.add_argument("--embed-model", default=DEFAULT_MODEL)
    parser.add_argument("--ollama-model", default=DEFAULT_OLLAMA_MODEL)
    parser.add_argument("--normalize", action="store_true", help="Normalise les embeddings avant la recherche (recommandé).")
    parser.add_argument("--qdrant-url", default="http://localhost:6333")
    parser.add_argument("--qdrant-key", default=None)
    parser.add_argument("--timeout", type=int, default=180, help="Timeout (s) pour l'appel Ollama")
    parser.add_argument("--score-threshold", type=float, default=0.0, help="Score minimal des documents conservés")
    parser.add_argument("--vector-name", default=None, help="Nom du vecteur si la collection est multi-vecteurs (ex: 'text')")
    args = parser.parse_args()

    # 1) Embedding
    model = SentenceTransformer(args.embed_model)
    query_vec = model.encode(args.question, normalize_embeddings=args.normalize)

    # 2) Qdrant client
    client = QdrantClient(url=args.qdrant_url, api_key=args.qdrant_key, timeout=30)

    # 3) Recherche (API moderne: client.search)
    #    - query_vector peut être soit la liste, soit un tuple (nom, liste) pour vecteurs nommés
    query_vector = (args.vector_name, query_vec.tolist()) if args.vector_name else query_vec.tolist()

    hits = client.search(
        collection_name=args.collection,
        query_vector=query_vector,
        limit=args.top_k,
        with_payload=True,
        with_vectors=False,
        score_threshold=args.score_threshold if args.score_threshold > 0 else None,
        search_params=SearchParams(  # tu peux ajuster au besoin
            hnsw_ef=128,
            exact=False,
        ),
    )

    if not hits:
        print("Aucun document retrouvé dans Qdrant.")
        return

    # 4) Construit le prompt + appelle Ollama
    prompt = build_prompt(args.question, hits)
    answer = call_ollama(args.ollama_model, prompt, timeout=args.timeout)

    # 5) Affichage
    print("Réponse :\n")
    print(answer)
    print("\n---\nSources :")
    for idx, hit in enumerate(hits, 1):
        payload = hit.payload or {}
        src = payload.get("url") or payload.get("file_name") or payload.get("orig_file") or "source inconnue"
        print(f"{idx}. {src} (score={hit.score:.3f})")


if __name__ == "__main__":
    main()

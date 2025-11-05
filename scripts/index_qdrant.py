#!/usr/bin/env python3
"""Embed a JSONL corpus with BGE-M3 and index it into Qdrant.

Example:
  python scripts/index_qdrant.py \
      --input data/corpus_cleaned.jsonl \
      --collection burkina_corpus \
      --qdrant-url http://localhost:6333 \
      --batch-size 64 \
      --recreate
"""
from __future__ import annotations

import os
import argparse
import json
import logging
import uuid
from pathlib import Path
from typing import Iterable, Iterator, List, Optional

from qdrant_client import QdrantClient, models
from sentence_transformers import SentenceTransformer

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover - tqdm is optional
    tqdm = None

Log = logging.getLogger("index_qdrant")


def iter_jsonl(path: Path) -> Iterator[dict]:
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as err:
                Log.warning("Skipping invalid JSON (line %d): %s", line_no, err)


def prepare_payload(rec: dict) -> dict:
    payload = {
        "id": rec.get("id"),
        "orig_id": rec.get("orig_id"),
        "chunk_index": rec.get("chunk_index"),
        "title": rec.get("title"),
        "url": rec.get("url"),
        "date": rec.get("date"),
        "source": rec.get("source"),
        "file_name": rec.get("file_name"),
        "orig_file": rec.get("orig_file"),
        "word_count": rec.get("word_count"),
    }
    # Drop None fields to keep payload lean
    return {k: v for k, v in payload.items() if v is not None}


def make_point_id(raw_id: str) -> str:
    """Generate a stable UUID for Qdrant from any string identifier."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, raw_id))


def batched(it: Iterable, size: int) -> Iterator[list]:
    batch: list = []
    for item in it:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def ensure_collection(
    client: QdrantClient,
    name: str,
    dim: int,
    recreate: bool,
    distance: models.Distance,
) -> None:
    if recreate:
        Log.info("Recreating collection %s (dim=%d)", name, dim)
        client.recreate_collection(
            collection_name=name,
            vectors_config=models.VectorParams(size=dim, distance=distance),
        )
        return

    existing = None
    try:
        collections = client.get_collections()
        if any(col.name == name for col in collections.collections):
            existing = client.get_collection(name)
    except Exception:
        existing = None
    if existing:
        Log.info(
            "Using existing collection %s (vectors: %s)",
            name,
            existing.vectors_count,
        )
        return

    Log.info("Creating collection %s (dim=%d)", name, dim)
    try:
        client.create_collection(
            collection_name=name,
            vectors_config=models.VectorParams(size=dim, distance=distance),
        )
    except Exception as exc:
        Log.warning("Collection %s already exists or cannot be created (%s). Continuing.", name, exc)


def main() -> None:
    parser = argparse.ArgumentParser(description="Embed JSONL corpus with BGE-M3 into Qdrant.")
    parser.add_argument("--input", default="data/corpus_cleaned.jsonl", help="JSONL corpus with a `text` field.")
    parser.add_argument("--collection", default="burkina_corpus", help="Qdrant collection name.")
    parser.add_argument("--model", default="BAAI/bge-m3", help="SentenceTransformer model name or local path.")
    parser.add_argument("--batch-size", type=int, default=64, help="Batch size for embedding.")
    parser.add_argument("--qdrant-url", default="http://localhost:6333", help="Qdrant endpoint.")
    parser.add_argument("--qdrant-api-key", default=None, help="Qdrant API key (for Cloud).")
    parser.add_argument("--distance", default="cosine", choices=["cosine", "dot", "euclid"], help="Vector distance metric.")
    parser.add_argument("--recreate", action="store_true", help="Drop and recreate the collection before indexing.")
    parser.add_argument("--max-records", type=int, default=0, help="Limit number of records (0 = all).")
    parser.add_argument("--normalize", action="store_true", help="L2-normalize embeddings before upload.")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    Log.info("Loading model %s", args.model)
    try:
        model = SentenceTransformer(args.model)
    except OSError as err:
        Log.warning("Could not load with default settings (%s). Retrying without safetensors-only guard.", err)
        os.environ.pop("TRANSFORMERS_PYTORCH_SAFETENSORS_ONLY", None)
        model = SentenceTransformer(args.model, model_kwargs={"trust_remote_code": True})
    vector_size = model.get_sentence_embedding_dimension()
    Log.info("Model dimension: %d", vector_size)

    distance = {
        "cosine": models.Distance.COSINE,
        "dot": models.Distance.DOT,
        "euclid": models.Distance.EUCLID,
    }[args.distance]

    client = QdrantClient(
        url=args.qdrant_url,
        api_key=args.qdrant_api_key,
        timeout=60,
    )
    ensure_collection(client, args.collection, vector_size, args.recreate, distance)

    iterator = iter_jsonl(input_path)
    if args.max_records > 0:
        iterator = (rec for idx, rec in enumerate(iterator, 1) if idx <= args.max_records)
    progress_total = None if args.max_records == 0 else args.max_records
    iterator = tqdm(iterator, total=progress_total, desc="Indexing") if tqdm else iterator

    total_points = 0
    buffer_texts: List[str] = []
    buffer_ids: List[str] = []
    buffer_payloads: List[dict] = []

    for rec in iterator:
        text = (rec.get("text") or "").strip()
        if not text:
            continue

        raw_id = rec.get("id") or rec.get("orig_id")
        if not raw_id:
            Log.warning("Skipping record without id: %s", rec)
            continue

        point_id = make_point_id(str(raw_id))
        buffer_ids.append(point_id)
        buffer_texts.append(text)
        buffer_payloads.append(prepare_payload(rec))

        if len(buffer_ids) >= args.batch_size:
            vectors = model.encode(
                buffer_texts,
                batch_size=args.batch_size,
                normalize_embeddings=args.normalize,
                show_progress_bar=False,
            )
            client.upsert(
                collection_name=args.collection,
                points=models.Batch(ids=buffer_ids, vectors=vectors.tolist(), payloads=buffer_payloads),
            )
            total_points += len(buffer_ids)
            buffer_ids, buffer_texts, buffer_payloads = [], [], []

    if buffer_ids:
        vectors = model.encode(
            buffer_texts,
            batch_size=args.batch_size,
            normalize_embeddings=args.normalize,
            show_progress_bar=False,
        )
        client.upsert(
            collection_name=args.collection,
            points=models.Batch(ids=buffer_ids, vectors=vectors.tolist(), payloads=buffer_payloads),
        )
        total_points += len(buffer_ids)

    Log.info("Finished. Total points indexed: %d", total_points)


if __name__ == "__main__":
    main()

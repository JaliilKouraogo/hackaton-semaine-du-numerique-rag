#!/usr/bin/env python3
"""Normalize and merge HTML and PDF extracted JSONL into a staging corpus.

Reads `data/html.jsonl` and `data/pdfs.jsonl` (if present) and writes
`data/corpus_stage.jsonl` with unified fields:
 - id, url, title, source (html|pdf), orig_file, file_name, text, word_count
"""
from __future__ import annotations
import json
import argparse
from pathlib import Path


def normalize_text(s: str) -> str:
    if s is None:
        return ""
    return " ".join(s.split()).strip()


def process_file(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            text = normalize_text(rec.get("text") or "")
            out = {
                "id": rec.get("id"),
                "url": rec.get("url"),
                "title": rec.get("title"),
                "source": rec.get("file") and ("html") or ("pdf" if rec.get("orig_file") else "unknown"),
                "orig_file": rec.get("file") or rec.get("orig_file"),
                "file_name": rec.get("file_name") or (Path(rec.get("file") or "").name if rec.get("file") else None),
                "text": text,
                "word_count": len(text.split())
            }
            yield out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--html", default="data/html.jsonl")
    p.add_argument("--pdfs", default="data/pdfs.jsonl")
    p.add_argument("--out", default="data/corpus_stage.jsonl")
    args = p.parse_args()

    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)

    with outp.open("w", encoding="utf-8") as outf:
        # HTML
        h = Path(args.html)
        if h.exists():
            for r in process_file(h):
                outf.write(json.dumps(r, ensure_ascii=False) + "\n")

        # PDFs
        ppath = Path(args.pdfs)
        if ppath.exists():
            for r in process_file(ppath):
                outf.write(json.dumps(r, ensure_ascii=False) + "\n")

    print("Wrote", outp)


if __name__ == "__main__":
    main()

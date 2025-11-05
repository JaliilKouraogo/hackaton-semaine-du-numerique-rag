#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extraction de texte depuis PDF en dossier → JSONL

Fonctions:
 - parcourt un dossier (option --recursive)
 - extrait texte (pdfplumber si dispo, fallback PyPDF2)
 - calcule sha256, taille et page_count
 - optionnellement découpe en chunks par nombre de mots (--chunk-words)
 - streaming JSONL en sortie (une ligne par document / chunk)

Exemples:
  python scripts/pdf_extract.py --pdf-dir data/raw_pdfs --out data/corpus_pdfs.jsonl
  python scripts/pdf_extract.py --pdf-dir data/raw_pdfs --out data/corpus_pdfs.jsonl --recursive --pattern "*.pdf" --chunk-words 300
  python scripts/pdf_extract.py --pdf-dir data/raw_pdfs --out data/corpus_pdfs.jsonl --skip-empty
"""
from __future__ import annotations
import argparse
import os
import json
import hashlib
from typing import Iterator, List, Optional
from pathlib import Path

# optional libs
try:
    import pdfplumber
except Exception:
    pdfplumber = None

try:
    from PyPDF2 import PdfReader
except Exception:
    PdfReader = None

try:
    from tqdm import tqdm
except Exception:
    tqdm = lambda x, **_: x  # fallback

# ---------- utilitaires ----------
def sha256_of_file(path: str, block_size: int = 65536) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(block_size), b""):
            h.update(block)
    return h.hexdigest()


def list_pdf_files(root: str, recursive: bool = False, pattern: str = "*.pdf") -> Iterator[str]:
    p = Path(root)
    if not p.exists():
        return
    if recursive:
        for f in p.rglob(pattern):
            if f.is_file():
                yield str(f)
    else:
        for f in p.glob(pattern):
            if f.is_file():
                yield str(f)


def normalize_whitespace(s: str) -> str:
    return " ".join(s.split()).strip()


def chunk_text_by_words(text: str, max_words: int, overlap: int = 0) -> List[str]:
    if not text:
        return []
    words = text.split()
    if max_words <= 0 or len(words) <= max_words:
        return [" ".join(words)]
    chunks = []
    i = 0
    step = max_words - overlap if (max_words - overlap) > 0 else max_words
    while i < len(words):
        chunk = words[i:i + max_words]
        chunks.append(" ".join(chunk))
        i += step
    return chunks


# ---------- extraction ----------
def extract_text_pdfplumber(path: str) -> (str, int):
    """Retourne (text, page_count) en utilisant pdfplumber"""
    texts = []
    page_count = 0
    with pdfplumber.open(path) as pdf:
        page_count = len(pdf.pages)
        for p in pdf.pages:
            try:
                t = p.extract_text() or ""
            except Exception:
                t = ""
            texts.append(t)
    return ("\n\n".join(texts)).strip(), page_count


def extract_text_pypdf2(path: str) -> (str, int):
    """Fallback PyPDF2: retourne (text, page_count)"""
    if PdfReader is None:
        raise RuntimeError("PyPDF2 n'est pas disponible")
    reader = PdfReader(path)
    texts = []
    page_count = len(reader.pages)
    for p in reader.pages:
        try:
            t = p.extract_text() or ""
        except Exception:
            t = ""
        texts.append(t)
    return ("\n\n".join(texts)).strip(), page_count


def extract_text_from_pdf(path: str) -> (str, int):
    """Choisit la meilleure méthode disponible pour extraire le texte."""
    if pdfplumber is not None:
        try:
            return extract_text_pdfplumber(path)
        except Exception:
            # fallback silencieux
            pass
    if PdfReader is not None:
        return extract_text_pypdf2(path)
    raise RuntimeError("Aucun extracteur PDF disponible (installer pdfplumber ou PyPDF2)")


# ---------- pipeline principal ----------
def process_pdf_file(path: str, chunk_words: Optional[int], overlap: int, skip_empty: bool) -> Iterator[dict]:
    """Itère sur 1 ou plusieurs enregistrements dérivés d'un PDF."""
    stat = os.stat(path)
    size = stat.st_size
    sha = sha256_of_file(path)
    try:
        text, page_count = extract_text_from_pdf(path)
    except Exception as e:
        yield {
            "id": f"err_{sha[:10]}",
            "orig_file": path,
            "error": str(e),
            "page_count": None,
            "size_bytes": size,
            "sha256": sha,
            "text": None,
        }
        return

    text = (text or "").strip()
    if skip_empty and not text:
        return

    if chunk_words and chunk_words > 0:
        chunks = chunk_text_by_words(normalize_whitespace(text), chunk_words, overlap)
        for idx, ch in enumerate(chunks):
            rec = {
                "id": f"{sha[:12]}_chunk_{idx}",
                "orig_file": path,
                "file_name": os.path.basename(path),
                "page_count": page_count,
                "size_bytes": size,
                "sha256": sha,
                "chunk_index": idx,
                "text": ch,
            }
            yield rec
    else:
        rec = {
            "id": sha[:20],
            "orig_file": path,
            "file_name": os.path.basename(path),
            "page_count": page_count,
            "size_bytes": size,
            "sha256": sha,
            "text": text,
        }
        yield rec


def main(argv=None):
    parser = argparse.ArgumentParser(description="Extract PDFs from a directory to a JSONL file (with optional chunking).")
    parser.add_argument("--pdf-dir", required=True, help="Directory containing PDFs")
    parser.add_argument("--out", required=True, help="Output JSONL file")
    parser.add_argument("--recursive", action="store_true", help="Scan subdirectories")
    parser.add_argument("--pattern", default="*.pdf", help="Glob pattern (default: *.pdf)")
    parser.add_argument("--skip-empty", action="store_true", help="Skip PDFs with empty extracted text")
    parser.add_argument("--min-pages", type=int, default=0, help="Ignore PDFs with fewer than this many pages")
    parser.add_argument("--chunk-words", type=int, default=0, help="If >0, split each PDF into chunks of N words")
    parser.add_argument("--overlap", type=int, default=50, help="Overlap in words between chunks (when --chunk-words used)")
    parser.add_argument("--show-progress", action="store_true", help="Show progress bar if tqdm available")
    args = parser.parse_args(argv)

    pdf_files = list(list_pdf_files(args.pdf_dir, recursive=args.recursive, pattern=args.pattern))
    if not pdf_files:
        print("Aucun fichier PDF trouvé dans", args.pdf_dir)
        return

    # prepare output
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    total_processed = 0
    with open(args.out, "w", encoding="utf-8") as out_f:
        iterator = tqdm(pdf_files, desc="PDFs") if args.show_progress else pdf_files
        for path in iterator:
            try:
                # quick page count filter using pdfplumber or PyPDF2 without full extraction if min-pages > 0
                if args.min_pages > 0:
                    try:
                        if pdfplumber is not None:
                            with pdfplumber.open(path) as pdf:
                                pages = len(pdf.pages)
                        elif PdfReader is not None:
                            pages = len(PdfReader(path).pages)
                        else:
                            pages = 0
                    except Exception:
                        pages = 0
                    if pages < args.min_pages:
                        continue

                for rec in process_pdf_file(path, chunk_words=args.chunk_words if args.chunk_words > 0 else None, overlap=args.overlap, skip_empty=args.skip_empty):
                    out_f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                total_processed += 1
            except Exception as e:
                # never crash the whole pipeline on single file error
                err_rec = {"id": f"err_{hashlib.sha256(path.encode()).hexdigest()[:10]}", "orig_file": path, "error": str(e)}
                out_f.write(json.dumps(err_rec, ensure_ascii=False) + "\n")

    print(f"Terminé. Fichiers PDF traités: {total_processed}. Sortie: {args.out}")


if __name__ == "__main__":
    main()

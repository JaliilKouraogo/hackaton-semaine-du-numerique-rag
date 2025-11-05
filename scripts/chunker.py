#!/usr/bin/env python3
"""
Chunker amélioré pour JSONL.

Fonctionnalités :
- Supporte chunking par mots (recommandé) ou par caractères.
- Respecte autant que possible les limites de phrases (utilise nltk.sent_tokenize si disponible).
- Overlap configurable (en mots) entre chunks.
- Préserve les métadonnées d'origine et génère des chunk_id stables.
- Lecture / écriture streaming JSONL (mémoire contrôlée).
- Compatibilité CLI: --in, --out, --max-words, --max-chars, --overlap, --mode.

Exemples :
    python scripts/chunker.py --in data/corpus_stage.jsonl --out data/corpus_chunked.jsonl --max-words 300 --overlap 50
    python scripts/chunker.py --in data/corpus_stage.jsonl --out data/corpus_chunked.jsonl --mode chars --max-chars 1000
"""
from __future__ import annotations
import argparse
import json
import sys
import uuid
from typing import List

# Optional dependencies
try:
    from tqdm import tqdm
except Exception:
    tqdm = lambda x, **_: x  # fallback: identity

# Try to use nltk sentence tokenizer for nicer splits (allows language selection)
USE_NLTK = True
try:
    import nltk

    try:
        nltk.data.find("tokenizers/punkt")
    except LookupError:
        nltk.download("punkt", quiet=True)
    from nltk.tokenize import sent_tokenize as _nltk_sent_tokenize

    def sentence_tokenize(text: str, lang: str = "french") -> List[str]:
        try:
            return _nltk_sent_tokenize(text, language=lang)
        except Exception:
            # fallback to simple splitter if requested language model missing
            import re
            s = re.split(r'(?<=[\.\?\!]\s)', text)
            return [p.strip() for p in s if p.strip()]
except Exception:
    USE_NLTK = False
    def sentence_tokenize(text: str, lang: str = "french") -> List[str]:
        # very simple fallback: split on punctuation-ish boundaries
        import re
        s = re.split(r'(?<=[\.\?\!]\s)', text)
        return [p.strip() for p in s if p.strip()]


def chunk_by_chars(text: str, max_chars: int) -> List[str]:
    """Simple découpe par caractères (fallback)."""
    if not text:
        return []
    chunks = []
    start = 0
    L = len(text)
    while start < L:
        chunks.append(text[start:start + max_chars])
        start += max_chars
    return chunks


def chunk_by_words(text: str, max_words: int, overlap: int, lang: str = "french") -> List[str]:
    """
    Découpe en chunks basés sur le nombre de mots en essayant de respecter les phrases.
    - overlap: nombre de mots à recouvrir entre chunks (0 = pas d'overlap)
    """
    if not text:
        return []

    # sentence split (better readability); fallback uses simple split
    sentences = sentence_tokenize(text, lang=lang)

    # Convert sentences to lists of words
    sent_word_lists = [s.split() for s in sentences if s and s.strip()]

    chunks = []
    current_words: List[str] = []

    def flush_chunk():
        if not current_words:
            return None
        chunk_text = " ".join(current_words).strip()
        if chunk_text:
            chunks.append(chunk_text)

    for sent_words in sent_word_lists:
        # If adding the entire sentence stays within limit, append
        if len(current_words) + len(sent_words) <= max_words:
            current_words.extend(sent_words)
            continue

        # If current is empty but sentence itself is larger than max_words -> split sentence by words
        if not current_words and len(sent_words) > max_words:
            # split sentence into word-based subchunks
            i = 0
            while i < len(sent_words):
                sub = sent_words[i:i + max_words]
                chunks.append(" ".join(sub))
                i += max_words - overlap if max_words - overlap > 0 else max_words
            current_words = []
            continue

        # Otherwise flush current chunk and start new chunk (with overlap)
        flush_chunk()
        # Prepare overlap: keep last `overlap` words from previously flushed chunk
        if overlap > 0:
            last_words = (chunks[-1].split()[-overlap:]) if chunks else []
            current_words = last_words.copy()
        else:
            current_words = []

        # Now try to add the sentence to the (possibly overlapped) current chunk.
        # If still too big (i.e., overlap+sentence > max_words), we may need to split sentence.
        if len(current_words) + len(sent_words) <= max_words:
            current_words.extend(sent_words)
        else:
            # split sentence into smaller pieces that fit
            i = 0
            remaining = sent_words
            while i < len(remaining):
                room = max_words - len(current_words)
                if room <= 0:
                    flush_chunk()
                    # set overlap for next chunk
                    last_words = (chunks[-1].split()[-overlap:]) if chunks else []
                    current_words = last_words.copy() if overlap > 0 else []
                    room = max_words - len(current_words)
                take = remaining[i:i + room]
                current_words.extend(take)
                i += room
    # flush final chunk
    flush_chunk()
    return chunks


def make_chunk_records(original: dict, chunks: List[str]) -> List[dict]:
    """Génère des enregistrements JSON pour chaque chunk en préservant les métadonnées."""
    base_id = original.get("id") or str(uuid.uuid4())
    records = []
    for i, ch in enumerate(chunks):
        rec = {
            "id": f"{base_id}_chunk_{i}",
            "orig_id": base_id,
            "chunk_index": i,
            "title": original.get("title"),
            "url": original.get("url"),
            "date": original.get("date"),
            "text": ch,
        }
        # copy any other useful metadata present
        for k in original:
            if k not in rec and k not in ("text", "id"):
                rec[k] = original.get(k)
        records.append(rec)
    return records


def process_stream(in_path: str, out_path: str, mode: str, max_words: int, max_chars: int, overlap: int, lang: str = "french"):
    total_in = 0
    total_out = 0
    with open(in_path, "r", encoding="utf-8") as inf, open(out_path, "w", encoding="utf-8") as outf:
        for line in tqdm(inf):
            total_in += 1
            line = line.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)
            except Exception as e:
                print(f"Warning: skipped invalid JSON line #{total_in}: {e}", file=sys.stderr)
                continue
            text = (doc.get("text") or "").strip()
            if not text:
                # skip or write empty? we skip, but could write doc with empty text
                continue

            if mode == "chars":
                chunks = chunk_by_chars(text, max_chars)
            else:
                chunks = chunk_by_words(text, max_words, overlap, lang=lang)

            records = make_chunk_records(doc, chunks)
            for r in records:
                outf.write(json.dumps(r, ensure_ascii=False) + "\n")
                total_out += 1

    print(f"Finished. Input docs: {total_in}, Output chunks: {total_out}")


def positive_int(value: str) -> int:
    try:
        iv = int(value)
    except Exception:
        raise argparse.ArgumentTypeError(f"{value} is not an integer")
    if iv < 1:
        raise argparse.ArgumentTypeError(f"{value} must be >= 1")
    return iv


def main(argv=None):
    parser = argparse.ArgumentParser(description="Chunk JSONL file into passages (words or chars).")
    parser.add_argument("--in", dest="infile", required=True, help="Input JSONL file")
    parser.add_argument("--out", required=True, help="Output JSONL file")
    parser.add_argument("--mode", choices=("words", "chars"), default="words", help="Chunking mode (default: words)")
    parser.add_argument("--max-words", type=positive_int, default=300, help="Max words per chunk (words mode)")
    parser.add_argument("--max-chars", type=positive_int, default=1000, help="Max chars per chunk (chars mode)")
    parser.add_argument("--overlap", type=int, default=50, help="Overlap in words between consecutive chunks (words mode)")
    parser.add_argument("--lang", default="french", help="Sentence tokenizer language (default: french)")
    args = parser.parse_args(argv)

    if args.mode == "words":
        print(f"Chunking by words: max_words={args.max_words}, overlap={args.overlap}, lang={args.lang}")
        process_stream(args.infile, args.out, mode="words", max_words=args.max_words, max_chars=0, overlap=args.overlap, lang=args.lang)
    else:
        print(f"Chunking by chars: max_chars={args.max_chars}")
        process_stream(args.infile, args.out, mode="chars", max_words=0, max_chars=args.max_chars, overlap=0, lang=args.lang)


if __name__ == "__main__":
    main()

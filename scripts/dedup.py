#!/usr/bin/env python3
"""
Deduplicate JSONL by hashing a text field, with optional normalization and fuzzy matching.

Examples:
  # basic (exact) dedup on 'text' field
  python scripts/dedup.py --in data/corpus_chunked.jsonl --out data/corpus_dedup.jsonl

  # use sha1 and only first 1000 chars for hashing (faster, less memory)
  python scripts/dedup.py --in data/corpus_chunked.jsonl --out data/corpus_dedup.jsonl --hash sha1 --prefix 1000

  # enable normalization and fuzzy matching (compare to last 500 kept items)
  python scripts/dedup.py --in data/corpus_chunked.jsonl --out data/corpus_dedup.jsonl --normalize --fuzzy 0.90 --max-fuzzy-store 500

Notes:
 - Fuzzy mode uses difflib.SequenceMatcher and can be slow on large corpora; limit max-fuzzy-store to control memory/time.
 - Normalization: lowercasing + collapse whitespace. You can add more normalization if needed.
"""
from __future__ import annotations
import argparse
import json
import hashlib
import sys
from typing import Optional, Set, List

try:
    from tqdm import tqdm
except Exception:
    tqdm = lambda x, **kwargs: x  # fallback if tqdm missing

import difflib
import re

SUPPORTED_HASHES = {"md5", "sha1", "sha256"}


def normalize_text(s: str) -> str:
    """Basic normalization: lowercase, collapse whitespace."""
    if s is None:
        return ""
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s.lower()


def compute_hash(s: str, algo: str = "sha256", prefix: Optional[int] = None) -> str:
    if prefix is not None and prefix > 0:
        s = s[:prefix]
    if algo == "md5":
        return hashlib.md5(s.encode("utf-8")).hexdigest()
    if algo == "sha1":
        return hashlib.sha1(s.encode("utf-8")).hexdigest()
    # default sha256
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def is_similar(a: str, b: str, threshold: float) -> bool:
    """Return True if similarity(a,b) >= threshold using difflib ratio."""
    if not a or not b:
        return False
    try:
        ratio = difflib.SequenceMatcher(None, a, b).ratio()
        return ratio >= threshold
    except Exception:
        return False


def main(argv=None):
    parser = argparse.ArgumentParser(description="Deduplicate JSONL by hashing a text field (with optional fuzzy match).")
    parser.add_argument("--in", dest="inp", required=True, help="Input JSONL file")
    parser.add_argument("--out", required=True, help="Output JSONL file (deduplicated)")
    parser.add_argument("--field", default="text", help="JSON field to use for dedup (default: text)")
    parser.add_argument("--hash", dest="hash_algo", choices=list(SUPPORTED_HASHES), default="sha256", help="Hash algorithm")
    parser.add_argument("--prefix", type=int, default=0, help="Only hash the first N characters of the field (0 = full text)")
    parser.add_argument("--normalize", action="store_true", help="Normalize text before hashing (lowercase + collapse whitespace)")
    parser.add_argument("--min-length", type=int, default=1, help="Skip (drop) records whose field length < min-length")
    parser.add_argument("--fuzzy", type=float, default=0.0, help="Enable fuzzy dedup: similarity threshold [0..1]. 0 disables fuzzy.")
    parser.add_argument("--max-fuzzy-store", type=int, default=500, help="When fuzzy enabled, compare only to last N kept items (controls memory/time)")
    parser.add_argument("--show-progress", action="store_true", help="Show progress bar if tqdm available")
    args = parser.parse_args(argv)

    if args.hash_algo not in SUPPORTED_HASHES:
        print(f"Unsupported hash algorithm: {args.hash_algo}", file=sys.stderr)
        sys.exit(2)
    if args.fuzzy and not (0.0 < args.fuzzy <= 1.0):
        print("--fuzzy must be in (0.0, 1.0]", file=sys.stderr)
        sys.exit(2)

    seen_hashes: Set[str] = set()
    kept_texts_for_fuzzy: List[str] = []  # circular buffer of recent kept texts (for fuzzy)
    kept_count = 0
    skipped_count = 0
    invalid_lines = 0
    total_lines = 0

    try:
        inf = open(args.inp, "r", encoding="utf-8")
    except Exception as e:
        print(f"Cannot open input file {args.inp}: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        outf = open(args.out, "w", encoding="utf-8")
    except Exception as e:
        print(f"Cannot open output file {args.out}: {e}", file=sys.stderr)
        inf.close()
        sys.exit(1)

    iterator = inf
    if args.show_progress:
        # attempt to get file length for tqdm; fallback to simple iterator
        try:
            total = sum(1 for _ in open(args.inp, "r", encoding="utf-8"))
            inf.seek(0)
            iterator = tqdm(inf, total=total, desc="Dedup")
        except Exception:
            inf.seek(0)
            iterator = tqdm(inf, desc="Dedup")

    for line in iterator:
        total_lines += 1
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except Exception:
            invalid_lines += 1
            continue

        txt = rec.get(args.field, "")
        if not isinstance(txt, str):
            # try to coerce non-string (e.g., numbers)
            try:
                txt = str(txt)
            except Exception:
                txt = ""

        if args.normalize:
            txt_proc = normalize_text(txt)
        else:
            txt_proc = txt.strip()

        if len(txt_proc) < args.min_length:
            skipped_count += 1
            continue

        # compute hash on prefix or full processed text
        prefix = args.prefix if args.prefix and args.prefix > 0 else None
        h = compute_hash(txt_proc, algo=args.hash_algo, prefix=prefix)

        duplicate = False

        # fast exact check by hash
        if h in seen_hashes:
            duplicate = True
        else:
            # if fuzzy enabled, compare to recent kept_texts
            if args.fuzzy and kept_texts_for_fuzzy:
                # iterate recent kept texts (most recent first)
                for recent in reversed(kept_texts_for_fuzzy):
                    if is_similar(txt_proc, recent, args.fuzzy):
                        duplicate = True
                        break

        if duplicate:
            skipped_count += 1
            continue

        # keep this record
        outf.write(json.dumps(rec, ensure_ascii=False) + "\n")
        kept_count += 1
        seen_hashes.add(h)
        if args.fuzzy:
            kept_texts_for_fuzzy.append(txt_proc)
            # trim buffer to max size
            if len(kept_texts_for_fuzzy) > args.max_fuzzy_store:
                # remove oldest
                kept_texts_for_fuzzy.pop(0)

    inf.close()
    outf.close()

    # summary
    print("Dedup completed.")
    print(f"Total lines read: {total_lines}")
    print(f"Invalid JSON lines skipped: {invalid_lines}")
    print(f"Records kept: {kept_count}")
    print(f"Records skipped (duplicates/short): {skipped_count}")

    # exit code: 0 normal
    sys.exit(0)


if __name__ == "__main__":
    main()

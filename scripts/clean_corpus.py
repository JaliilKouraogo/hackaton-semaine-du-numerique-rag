#!/usr/bin/env python3
"""Clean corpus JSONL by removing short/empty/noisy texts.

Outputs a cleaned JSONL and a JSON report with counts.

Usage:
  python scripts/clean_corpus.py --in data/corpus_dedup.jsonl --out data/corpus_cleaned.jsonl --min-words 50 --require-accent
"""
from __future__ import annotations
import argparse
import json
import re
from pathlib import Path

ACCENT_RE = re.compile(r"[àâäéèêëîïôöùûüçÀÂÄÉÈÊËÎÏÔÖÙÛÜÇ]")
NON_LETTER = re.compile(r"[^A-Za-zÀ-ÖØ-öø-ÿ\s]")  # Updated to use LETTER_RE fallback

# Simple unicode-aware letter matcher (covers Latin + common accented letters)
LETTER_RE = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿ]")


def is_noisy(text: str, max_non_letter_ratio: float) -> bool:
    if not text:
        return True
    # compute ratio of non-letter-like chars
    total = len(text)
    if total == 0:
        return True
    non_letters = sum(1 for ch in text if not LETTER_RE.search(ch))
    ratio = non_letters / total
    return ratio > max_non_letter_ratio


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="inp", default="data/corpus_dedup.jsonl")
    p.add_argument("--out", default="data/corpus_cleaned.jsonl")
    p.add_argument("--min-words", type=int, default=50, help="Minimum word count to keep (default 50)")
    p.add_argument("--max-words", type=int, default=10000, help="Maximum word count to keep (default 10000)")
    p.add_argument("--max-non-letter-ratio", type=float, default=0.25, help="If fraction of non-letter chars exceeds this, drop record (default 0.25)")
    p.add_argument("--require-accent", action="store_true", help="Require at least one accented character (heuristic to prefer French)")
    p.add_argument("--report", default="data/clean_report.json", help="JSON report output")
    args = p.parse_args(argv)

    inp = Path(args.inp)
    outp = Path(args.out)
    rptp = Path(args.report)
    outp.parent.mkdir(parents=True, exist_ok=True)
    rptp.parent.mkdir(parents=True, exist_ok=True)

    totals = {
        "in_lines": 0,
        "kept": 0,
        "dropped_short": 0,
        "dropped_noisy": 0,
        "dropped_accent": 0,
        "dropped_toolarge": 0,
    }

    with inp.open("r", encoding="utf-8") as inf, outp.open("w", encoding="utf-8") as outf:
        for line in inf:
            totals["in_lines"] += 1
            line = line.strip()
            if not line:
                totals["dropped_short"] += 1
                continue
            try:
                rec = json.loads(line)
            except Exception:
                totals["dropped_noisy"] += 1
                continue
            text = (rec.get("text") or "").strip()
            wc = len(text.split())
            if wc < args.min_words:
                totals["dropped_short"] += 1
                continue
            if wc > args.max_words:
                totals["dropped_toolarge"] += 1
                continue
            if args.require_accent and not ACCENT_RE.search(text):
                totals["dropped_accent"] += 1
                continue
            if is_noisy(text, args.max_non_letter_ratio):
                totals["dropped_noisy"] += 1
                continue
            # keep
            outf.write(json.dumps(rec, ensure_ascii=False) + "\n")
            totals["kept"] += 1

    with rptp.open("w", encoding="utf-8") as rf:
        json.dump(totals, rf, ensure_ascii=False, indent=2)

    print("Cleaning finished.", outp, rptp)


if __name__ == "__main__":
    main()

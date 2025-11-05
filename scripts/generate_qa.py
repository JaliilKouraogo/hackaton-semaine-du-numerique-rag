#!/usr/bin/env python3
"""Generate a simple QA report (counts, lengths, samples) from a JSONL corpus.

Writes a markdown report to data/qa_report.md by default.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import random


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="inp", default="data/corpus_dedup.jsonl")
    p.add_argument("--out", default="data/qa_report.md")
    p.add_argument("--samples", type=int, default=5)
    args = p.parse_args()

    inp = Path(args.inp)
    outp = Path(args.out)
    if not inp.exists():
        print("Input not found:", inp)
        return

    docs = []
    total_words = 0
    empty = 0
    lengths = []
    with inp.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
            except Exception:
                continue
            text = (rec.get("text") or "").strip()
            wc = len(text.split())
            total_words += wc
            lengths.append(wc)
            if wc == 0:
                empty += 1
            docs.append(rec)

    n = len(docs)
    avg = total_words / n if n else 0
    med = sorted(lengths)[len(lengths)//2] if lengths else 0

    samples = random.sample(docs, min(args.samples, len(docs))) if docs else []

    outp.parent.mkdir(parents=True, exist_ok=True)
    with outp.open("w", encoding="utf-8") as f:
        f.write(f"# QA Report\n\n")
        f.write(f"Total records: {n}  \n")
        f.write(f"Empty texts: {empty}  \n")
        f.write(f"Total words: {total_words}  \n")
        f.write(f"Average words per record: {avg:.1f}  \n")
        f.write(f"Median words: {med}  \n\n")
        f.write("## Sample records\n\n")
        for i, s in enumerate(samples, 1):
            txt = (s.get("text") or "").strip()
            f.write(f"### Sample {i}\n")
            f.write(f"- id: {s.get('id')}\n")
            f.write(f"- url: {s.get('url')}\n")
            f.write(f"- file: {s.get('orig_file') or s.get('file')}\n")
            f.write(f"- words: {len(txt.split())}\n\n")
            f.write("```\n")
            f.write((s.get('text') or '') + "\n")
            f.write("```\n\n")

    print("Wrote", outp)


if __name__ == "__main__":
    main()

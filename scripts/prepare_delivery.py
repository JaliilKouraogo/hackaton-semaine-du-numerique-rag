#!/usr/bin/env python3
"""Prepare final delivery artifacts: corpus.jsonl, sources.csv, README_data.md

Reads `data/corpus_dedup.jsonl` (or another input) and produces:
 - data/corpus.jsonl (copy)
 - data/sources.csv (url, file, source, words)
 - data/README_data.md (short metadata)
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="inp", default="data/corpus_dedup.jsonl")
    p.add_argument("--out", default="data/corpus.jsonl")
    args = p.parse_args()

    inp = Path(args.inp)
    outp = Path(args.out)
    if not inp.exists():
        print("Input not found:", inp)
        return

    sources = []
    outp.parent.mkdir(parents=True, exist_ok=True)
    # copy to corpus.jsonl
    with inp.open("r", encoding="utf-8") as inf, outp.open("w", encoding="utf-8") as outf:
        for line in inf:
            outf.write(line)
            try:
                rec = json.loads(line)
            except Exception:
                continue
            sources.append((rec.get("url"), rec.get("orig_file") or rec.get("file"), rec.get("source"), rec.get("word_count")))

    # write sources.csv
    csvp = outp.parent / "sources.csv"
    with csvp.open("w", encoding="utf-8") as cf:
        cf.write("url,orig_file,source,word_count\n")
        for row in sources:
            url, fpath, src, wc = row
            # simple CSV escaping
            url = (url or "").replace('"', '""')
            fpath = (fpath or "").replace('"', '""')
            src = (src or "").replace('"', '""')
            cf.write(f'"{url}","{fpath}","{src}",{wc or 0}\n')

    # README
    readme = outp.parent / "README_data.md"
    with readme.open("w", encoding="utf-8") as rf:
        rf.write("# Data package\n\n")
        rf.write("This folder contains the processed corpus and metadata prepared for embedding and upsert.\n\n")
        rf.write("Files:\n\n")
        rf.write("- `corpus.jsonl`: final corpus (one JSON object per line). Fields: id, url, title, source, orig_file, file_name, text, word_count.\n")
        rf.write("- `sources.csv`: CSV mapping of source URL -> original file and word counts.\n")
        rf.write("- `qa_report.md`: QA report with samples and stats.\n")
        rf.write("- `robots_report.jsonl`: robots.txt checks for seeds used during crawl.\n")

    print("Wrote:", outp, csvp, readme)


if __name__ == "__main__":
    main()

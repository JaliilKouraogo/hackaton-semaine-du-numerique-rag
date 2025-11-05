#!/usr/bin/env python3
"""Extract readable text from saved HTML files using crawl_report.jsonl as index.

Outputs one JSON object per HTML page into data/html.jsonl with fields:
 - id (sha256 of file)
 - url
 -title
 -file
 -text
"""
from __future__ import annotations
import json
import argparse
from pathlib import Path
import hashlib

try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None


def sha256_of_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for block in iter(lambda: f.read(65536), b""):
            h.update(block)
    return h.hexdigest()


def extract_text(html: str) -> str:
    if BeautifulSoup is None:
        # naive fallback
        return "\n".join(line.strip() for line in html.splitlines() if line.strip())
    soup = BeautifulSoup(html, "lxml")
    # prefer article tag
    article = soup.find("article")
    if article:
        paragraphs = [p.get_text(strip=True) for p in article.find_all("p")]
        if paragraphs:
            return "\n\n".join(paragraphs)
    paragraphs = [p.get_text(strip=True) for p in soup.find_all("p")]
    if paragraphs:
        return "\n\n".join(paragraphs)
    # fallback to visible text
    return soup.get_text(separator="\n").strip()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--crawl-report", default="data/raw_html/crawl_report.jsonl")
    p.add_argument("--out", default="data/html.jsonl")
    args = p.parse_args()

    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)

    with open(args.crawl_report, "r", encoding="utf-8") as ri, outp.open("w", encoding="utf-8") as outf:
        for line in ri:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            url = rec.get("url")
            saved = rec.get("saved_raw")
            ctype = rec.get("content_type") or ""
            if not saved:
                continue
            # only process HTML-like saved files
            if not str(saved).lower().endswith(".html") and "html" not in (ctype or ""):
                continue
            path = Path(saved)
            if not path.exists():
                # try relative to repository
                path = Path(".") / saved
                if not path.exists():
                    continue
            try:
                txt = path.read_text(encoding="utf-8", errors="replace")
            except Exception:
                continue
            text = extract_text(txt)
            doc = {
                "id": sha256_of_file(path)[:20],
                "url": url,
                "title": rec.get("title"),
                "file": str(path),
                "text": text,
            }
            outf.write(json.dumps(doc, ensure_ascii=False) + "\n")

    print("Wrote", outp)


if __name__ == "__main__":
    main()

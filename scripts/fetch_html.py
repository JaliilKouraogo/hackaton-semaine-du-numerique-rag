#!/usr/bin/env python3
"""Télécharge une page (stream) et la sauvegarde localement.

Caractéristiques :
- session requests avec retry
- streaming pour gros contenus + limite optionnelle (--max-bytes)
- mode de sauvegarde: binary (par défaut) ou text (essaie d'utiliser l'encodage HTTP)
- option pour extraire le texte lisible du HTML (--save-text)
- User-Agent configurable et overwrite control

Exemples :
  python scripts/fetch_html.py --url "https://example.com" --out data/raw_html/example.html
  python scripts/fetch_html.py --url "https://example.com" --out data/raw_html/example.html --save-text
  python scripts/fetch_html.py --url "https://example.com/doc.pdf" --out data/raw_pdfs/doc.pdf --max-bytes 5000000
"""
from __future__ import annotations
import argparse
import os
import sys
import shutil
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Optional libs for better HTML->text extraction
try:
    from newspaper import Article  # newspaper3k
except Exception:
    Article = None

try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None


DEFAULT_USER_AGENT = "DataCollectorBot/1.0 (+mailto:ton.email@example.com)"
DEFAULT_TIMEOUT = 20
CHUNK_SIZE = 32 * 1024  # 32KB chunks when streaming


def build_session(retries: int = 3, backoff: float = 0.3, timeout: int = DEFAULT_TIMEOUT) -> (requests.Session, int):
    s = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s, timeout


def ensure_parent_dir(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def save_stream(resp: requests.Response, out_path: str, max_bytes: Optional[int] = None, overwrite: bool = False) -> int:
    """Enregistre la réponse en streaming. Retourne le nombre d'octets écrits."""
    if os.path.exists(out_path) and not overwrite:
        raise FileExistsError(f"Fichier existe déjà : {out_path} (passer --overwrite pour écraser)")

    ensure_parent_dir(out_path)
    total_written = 0
    with open(out_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
            if not chunk:
                continue
            chunk_len = len(chunk)
            # enforce max_bytes if set
            if max_bytes is not None and total_written + chunk_len > max_bytes:
                # write only allowed part then stop
                allowed = max_bytes - total_written
                if allowed > 0:
                    f.write(chunk[:allowed])
                    total_written += allowed
                # remove partially written file to avoid corrupted artifacts? we keep it and signal via exception
                raise IOError(f"Max bytes exceeded ({max_bytes}). Partial file saved at {out_path}")
            f.write(chunk)
            total_written += chunk_len
    return total_written


def extract_text_from_html_with_newspaper(url: str) -> Optional[str]:
    if Article is None:
        return None
    try:
        a = Article(url)
        a.download()
        a.parse()
        return a.text or None
    except Exception:
        return None


def extract_text_from_html_with_bs(html: str) -> Optional[str]:
    if BeautifulSoup is None:
        return None
    try:
        soup = BeautifulSoup(html, "lxml")
        # prefer <article>
        article = soup.find("article")
        if article:
            paragraphs = [p.get_text(strip=True) for p in article.find_all("p") if p.get_text(strip=True)]
            if paragraphs:
                return "\n\n".join(paragraphs)
        # fallback all <p>
        paragraphs = [p.get_text(strip=True) for p in soup.find_all("p") if p.get_text(strip=True)]
        if paragraphs:
            return "\n\n".join(paragraphs)
        # fallback full visible text
        text = soup.get_text(separator="\n").strip()
        return text if text else None
    except Exception:
        return None


def fetch_html(url: str, out_path: str, user_agent: str = DEFAULT_USER_AGENT, timeout: int = DEFAULT_TIMEOUT,
               overwrite: bool = False, max_bytes: Optional[int] = None, mode: str = "binary",
               save_text: bool = False, show_headers: bool = False) -> int:
    """Télécharge et sauvegarde. Retourne le nombre d'octets écrits (binary mode) ou caractères (text mode)."""
    session, _timeout = build_session()
    headers = {"User-Agent": user_agent, "Accept": "*/*"}
    # stream=True for large content
    resp = session.get(url, headers=headers, timeout=timeout, stream=True)
    resp.raise_for_status()

    if show_headers:
        print("Response headers:")
        for k, v in resp.headers.items():
            print(f"  {k}: {v}")

    content_type = resp.headers.get("Content-Type", "")
    # binary mode: write bytes stream
    if mode == "binary":
        written = save_stream(resp, out_path, max_bytes=max_bytes, overwrite=overwrite)
        # optionally extract text if requested and content is HTML
        if save_text and "html" in content_type.lower():
            # we need the full HTML as text; read from saved file
            try:
                with open(out_path, "rb") as f:
                    raw = f.read()
                # try to decode using response apparent encoding or utf-8 fallback
                enc = resp.encoding or resp.apparent_encoding or "utf-8"
                html = raw.decode(enc, errors="replace")
            except Exception:
                html = None
            text = None
            if html:
                # try newspaper first (needs internet? it already has html via download; Article can accept html if you set it)
                if Article:
                    try:
                        a = Article(url)
                        a.set_html(html)
                        a.parse()
                        text = a.text
                    except Exception:
                        text = None
                if not text and BeautifulSoup:
                    text = extract_text_from_html_with_bs(html)
            if text:
                txt_path = os.path.splitext(out_path)[0] + ".txt"
                with open(txt_path, "w", encoding="utf-8") as tf:
                    tf.write(text)
        return written
    else:
        # text mode: decode and write text
        enc = resp.encoding or resp.apparent_encoding or "utf-8"
        text_chunks = []
        total_chars = 0
        for chunk in resp.iter_content(chunk_size=CHUNK_SIZE, decode_unicode=True):
            if not chunk:
                continue
            text_chunks.append(chunk)
            total_chars += len(chunk)
            if max_bytes is not None and total_chars > max_bytes:
                raise IOError(f"Max chars exceeded ({max_bytes}). Partial text saved.")
        full_text = "".join(text_chunks)
        ensure_parent_dir(out_path)
        if os.path.exists(out_path) and not overwrite:
            raise FileExistsError(f"Fichier existe déjà : {out_path} (passer --overwrite pour écraser)")
        with open(out_path, "w", encoding=enc, errors="replace") as f:
            f.write(full_text)
        return total_chars


def main(argv=None):
    parser = argparse.ArgumentParser(description="Fetch a URL and save it locally (streaming, robust).")
    parser.add_argument("--url", required=True, help="URL to download")
    parser.add_argument("--out", required=True, help="Output file path")
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing file")
    parser.add_argument("--max-bytes", type=int, default=0, help="Maximum bytes to download (0 = unlimited)")
    parser.add_argument("--mode", choices=("binary", "text"), default="binary", help="Save as binary (bytes) or decoded text")
    parser.add_argument("--save-text", action="store_true", help="If HTML, also save extracted readable text (.txt)")
    parser.add_argument("--show-headers", action="store_true", help="Print response headers")
    args = parser.parse_args(argv)

    max_bytes = args.max_bytes if args.max_bytes and args.max_bytes > 0 else None

    try:
        written = fetch_html(
            url=args.url,
            out_path=args.out,
            user_agent=args.user_agent,
            timeout=args.timeout,
            overwrite=args.overwrite,
            max_bytes=max_bytes,
            mode=args.mode,
            save_text=args.save_text,
            show_headers=args.show_headers,
        )
        print(f"Saved {written} {'bytes' if args.mode=='binary' else 'chars'} -> {args.out}")
        sys.exit(0)
    except requests.HTTPError as he:
        print(f"HTTP Error: {he}", file=sys.stderr)
        sys.exit(3)
    except FileExistsError as fe:
        print(f"Exists: {fe}", file=sys.stderr)
        sys.exit(4)
    except IOError as ioe:
        print(f"I/O Error: {ioe}", file=sys.stderr)
        sys.exit(5)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()

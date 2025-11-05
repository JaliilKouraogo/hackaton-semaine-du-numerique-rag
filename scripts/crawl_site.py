#!/usr/bin/env python3
"""
Crawler respectueux amélioré.

Usage examples:
  python scripts/crawl_site.py --start-url https://example.com --out-dir data/raw_html --max-pages 100
  python scripts/crawl_site.py --start-url https://dgi.bf --out-dir data/raw_html --max-pages 50 --extract text --user-agent "MoovHackathonBot"
  python scripts/crawl_site.py --start-url https://example.com --out-dir data/raw_html --ignore-robots

Fonctions clés:
 - respect robots.txt (can_fetch + crawl-delay) par défaut
 - session requests avec retry
 - sauvegarde raw + optionally extracted text
 - report JSONL (crawl_report.jsonl) avec métadonnées
"""
from __future__ import annotations
import argparse
import json
import os
import time
import hashlib
import sys
from typing import Optional, Tuple
from urllib.parse import urljoin, urlparse, urldefrag

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from urllib import robotparser

# Optional progress bar if tqdm installed
try:
    from tqdm import tqdm
except Exception:
    tqdm = lambda x, **_: x

DEFAULT_USER_AGENT = "DataCollectorBot/1.0 (+mailto:ton.email@example.com)"
DEFAULT_DELAY = 0.5
DEFAULT_TIMEOUT = 15


def build_session(retries: int = 3, backoff: float = 0.5, timeout: int = DEFAULT_TIMEOUT) -> Tuple[requests.Session, int]:
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session, timeout


def canonicalize(url: str) -> Optional[str]:
    """Normalize URL: remove fragments, ensure scheme present, return None for non-http(s)."""
    if not url:
        return None
    url = url.strip()
    url, _frag = urldefrag(url)  # remove fragment
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        # ignore mailto:, javascript:, ftp: etc.
        return None
    # normalize: rebuild minimal canonical
    if not parsed.netloc:
        return None
    return parsed.geturl() if hasattr(parsed, "geturl") else url


def safe_filename_for_url(url: str, ext: str) -> str:
    parsed = urlparse(url)
    base = parsed.path.strip("/").replace("/", "_") or "root"
    h = hashlib.sha1(url.encode("utf-8")).hexdigest()[:10]
    # keep small but unique filename
    name = f"{base}_{h}.{ext}"
    # avoid overly long names
    if len(name) > 200:
        name = f"{h}.{ext}"
    return name


def load_robots(start_url: str, session: requests.Session, timeout: int = 5) -> Tuple[Optional[robotparser.RobotFileParser], str]:
    parsed = urlparse(start_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    robots_url = urljoin(base, "/robots.txt")
    rp = robotparser.RobotFileParser()
    try:
        r = session.get(robots_url, timeout=timeout, headers={"User-Agent": DEFAULT_USER_AGENT})
        if r.status_code == 200 and r.text:
            rp.parse(r.text.splitlines())
            return rp, robots_url
        else:
            return None, robots_url
    except Exception:
        return None, robots_url


def extract_text_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    # prefer article tag
    article = soup.select_one("article")
    if article:
        text = "\n\n".join(p.get_text(strip=True) for p in article.find_all("p"))
        if text.strip():
            return text.strip()
    # fallback: all <p>
    paragraphs = soup.find_all("p")
    if paragraphs:
        return "\n\n".join(p.get_text(strip=True) for p in paragraphs).strip()
    # last fallback: whole visible text
    return soup.get_text(separator="\n").strip()


def get_crawl_delay(rp: robotparser.RobotFileParser, user_agent: str) -> Optional[float]:
    try:
        d = rp.crawl_delay(user_agent)
        if d is None:
            d = rp.crawl_delay("*")
        return float(d) if d is not None else None
    except Exception:
        return None


def is_same_domain(base_netloc: str, candidate_url: str, include_subdomains: bool = False) -> bool:
    parsed = urlparse(candidate_url)
    if not parsed.netloc:
        return False
    if include_subdomains:
        # allow subdomains of base_netloc
        return parsed.netloc.endswith(base_netloc)
    else:
        return parsed.netloc == base_netloc


def determine_ext_from_content_type(content_type: Optional[str]) -> str:
    if not content_type:
        return "bin"
    ct = content_type.split(";")[0].strip().lower()
    if ct in ("text/html", "application/xhtml+xml"):
        return "html"
    if ct == "application/pdf":
        return "pdf"
    if ct.startswith("text/"):
        return "txt"
    # common image types
    if ct.startswith("image/"):
        return ct.split("/")[1]
    return "bin"


def crawl(
    start_url: str,
    out_dir: str,
    max_pages: int = 50,
    user_agent: str = DEFAULT_USER_AGENT,
    default_delay: float = DEFAULT_DELAY,
    timeout: int = DEFAULT_TIMEOUT,
    include_subdomains: bool = False,
    extract: str = "none",  # choices: none | text | html
    ignore_robots: bool = False,
    max_depth: int = 3,
):
    session, _timeout = build_session()
    session.headers.update({"User-Agent": user_agent})

    os.makedirs(out_dir, exist_ok=True)
    raw_dir = os.path.join(out_dir, "raw")
    txt_dir = os.path.join(out_dir, "text")
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(txt_dir, exist_ok=True)

    rp, robots_url = (None, "")
    if not ignore_robots:
        rp, robots_url = load_robots(start_url, session, timeout=timeout)
        if rp:
            print(f"Loaded robots.txt: {robots_url}")
        else:
            print(f"No robots.txt parsed at {robots_url} (proceeding permissively)")

    # BFS queue: list of tuples (url, depth)
    queue = []
    start_canon = canonicalize(start_url)
    if not start_canon:
        raise ValueError("start-url must be a valid http(s) URL")
    queue.append((start_canon, 0))
    seen = set()
    base_netloc = urlparse(start_canon).netloc

    report_path = os.path.join(out_dir, "crawl_report.jsonl")
    report_f = open(report_path, "w", encoding="utf-8")

    pages = 0
    try:
        for url, depth in tqdm(queue):
            # queue iteration will be manual to allow adding items while iterating
            break
        # Use manual loop
        idx = 0
        while queue and pages < max_pages:
            url, depth = queue.pop(0)
            idx += 1
            if url in seen:
                continue
            if depth > max_depth:
                continue

            # robots check
            if not ignore_robots and rp is not None:
                try:
                    allowed = rp.can_fetch(user_agent, url)
                except Exception:
                    allowed = True
                if not allowed:
                    print(f"[robots] Skipping (disallowed): {url}")
                    seen.add(url)
                    # write report entry
                    report_f.write(json.dumps({"url": url, "status": "disallowed_by_robots"}) + "\n")
                    continue

            # fetch
            try:
                resp = session.get(url, timeout=timeout, allow_redirects=True)
            except Exception as e:
                print(f"[error] Failed to GET {url}: {e}")
                report_f.write(json.dumps({"url": url, "status": "error", "error": str(e)}) + "\n")
                seen.add(url)
                continue

            status = resp.status_code
            content_type = resp.headers.get("Content-Type", "")
            ext = determine_ext_from_content_type(content_type)
            filename = safe_filename_for_url(url, ext)
            raw_path = os.path.join(raw_dir, filename)
            try:
                with open(raw_path, "wb") as f:
                    f.write(resp.content)
            except Exception as e:
                print(f"[error] Can't write file {raw_path}: {e}")

            title = None
            extracted = None
            if "html" in ext:
                try:
                    soup = BeautifulSoup(resp.text, "lxml")
                    title_tag = soup.title.string.strip() if soup.title and soup.title.string else None
                    title = title_tag
                    if extract in ("text", "html"):
                        if extract == "text":
                            extracted = extract_text_from_html(resp.text)
                            if extracted:
                                txt_fname = filename.rsplit(".", 1)[0] + ".txt"
                                txt_path = os.path.join(txt_dir, txt_fname)
                                with open(txt_path, "w", encoding="utf-8") as tf:
                                    tf.write(extracted)
                        else:
                            # save pretty html text (same as raw but ensuring .html ext)
                            html_fname = filename.rsplit(".", 1)[0] + ".html"
                            html_path = os.path.join(txt_dir, html_fname)
                            with open(html_path, "w", encoding="utf-8") as hf:
                                hf.write(resp.text)
                except Exception as e:
                    print(f"[warn] HTML parse/extract failed for {url}: {e}")

            # write report entry
            report = {
                "url": url,
                "status_code": status,
                "content_type": content_type,
                "saved_raw": raw_path,
                "saved_text": None if extracted is None else os.path.join(txt_dir, filename.rsplit(".", 1)[0] + ".txt"),
                "title": title,
                "depth": depth,
            }
            report_f.write(json.dumps(report, ensure_ascii=False) + "\n")
            pages += 1
            seen.add(url)
            print(f"[{pages}/{max_pages}] Fetched {url} (status={status}, type={content_type})")

            # parse links and queue same-domain urls
            if "html" in ext and depth < max_depth:
                try:
                    soup = BeautifulSoup(resp.text, "lxml")
                    for a in soup.find_all("a", href=True):
                        raw_link = a["href"]
                        joined = urljoin(url, raw_link)
                        canon = canonicalize(joined)
                        if not canon:
                            continue
                        if canon in seen:
                            continue
                        if not is_same_domain(base_netloc, canon, include_subdomains=include_subdomains):
                            continue
                        # robots check before enqueue
                        if not ignore_robots and rp is not None:
                            try:
                                if not rp.can_fetch(user_agent, canon):
                                    # skip enqueueing if disallowed
                                    continue
                            except Exception:
                                pass
                        queue.append((canon, depth + 1))
                except Exception as e:
                    print(f"[warn] link extraction error for {url}: {e}")

            # obey crawl-delay if present
            delay = default_delay
            if not ignore_robots and rp is not None:
                d = get_crawl_delay(rp, user_agent)
                if d:
                    delay = d
            time.sleep(delay)
    finally:
        report_f.close()

    print(f"Done. Pages fetched: {pages}. Report: {report_path}")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Small respectful site crawler")
    parser.add_argument("--start-url", required=True, help="Starting URL (http/https)")
    parser.add_argument("--out-dir", required=True, help="Directory where raw pages and report will be saved")
    parser.add_argument("--max-pages", type=int, default=50)
    parser.add_argument("--max-depth", type=int, default=3, help="Max link depth from start URL")
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Default delay in seconds between requests")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--include-subdomains", action="store_true", help="Allow crawling subdomains of the start netloc")
    parser.add_argument("--extract", choices=("none", "text", "html"), default="none", help="Extract readable text from HTML and save to out-dir/text")
    parser.add_argument("--ignore-robots", action="store_true", help="Ignore robots.txt (for testing only)")
    args = parser.parse_args(argv)

    try:
        crawl(
            start_url=args.start_url,
            out_dir=args.out_dir,
            max_pages=args.max_pages,
            user_agent=args.user_agent,
            default_delay=args.delay,
            timeout=args.timeout,
            include_subdomains=args.include_subdomains,
            extract=args.extract,
            ignore_robots=args.ignore_robots,
            max_depth=args.max_depth,
        )
    except KeyboardInterrupt:
        print("Interrupted by user", file=sys.stderr)
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

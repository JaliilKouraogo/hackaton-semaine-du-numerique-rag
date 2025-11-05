#!/usr/bin/env python3
"""Vérifie et analyse le fichier robots.txt d'un domaine.

Exemples :
    python scripts/check_robots.py --url https://example.com
    python scripts/check_robots.py --url https://example.com --path /some/page --user-agent "MoovHackBot" --json
    python scripts/check_robots.py --url https://example.com --out data/robots_example.txt
"""
from __future__ import annotations
import argparse
import json
import sys
from typing import Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib import robotparser


DEFAULT_USER_AGENT = "MoovHackathonBot/1.0 (+mailto:ton.email@example.com)"


def build_session(retries: int = 3, backoff: float = 0.5, timeout: float = 10.0) -> Tuple[requests.Session, float]:
    """Crée une session requests avec retry."""
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


def fetch_robots_txt(session: requests.Session, base_url: str, timeout: float, user_agent: str) -> Tuple[Optional[str], int, Optional[str]]:
    """
    Récupère robots.txt et retourne (content, status_code, error_message).
    - base_url: ex. "https://example.com"
    """
    robots_url = urljoin(base_url, "/robots.txt")
    headers = {"User-Agent": user_agent}
    try:
        resp = session.get(robots_url, headers=headers, timeout=timeout)
        return resp.text if resp.status_code == 200 else (None if resp.status_code != 200 else resp.text), resp.status_code, None
    except requests.RequestException as e:
        return None, -1, str(e)


def parse_robots(content: str) -> robotparser.RobotFileParser:
    """Parse le texte robots.txt dans un RobotFileParser (sans fetch réseau)."""
    rp = robotparser.RobotFileParser()
    # robotparser.parse attend un itérable de lignes
    rp.parse(content.splitlines())
    return rp


def analyze(base_url: str, path: str, user_agent: str, retries: int = 3, backoff: float = 0.5, timeout: float = 10.0):
    """Récupère et analyse robots.txt et vérifie la possibilité d'accès à path."""
    session, timeout = build_session(retries=retries, backoff=backoff, timeout=timeout)
    content, status_code, err = fetch_robots_txt(session, base_url, timeout, user_agent)
    result = {
        "base_url": base_url,
        "robots_url": urljoin(base_url, "/robots.txt"),
        "fetched": False,
        "status_code": status_code,
        "error": err,
        "user_agent": user_agent,
        "tested_path": path,
        "allowed": None,
        "note": None,
        "raw": None,
    }

    if err:
        result["note"] = f"Erreur récupération robots.txt: {err}"
        return result

    if status_code == 200 and content is not None:
        result["fetched"] = True
        result["raw"] = content
        try:
            rp = parse_robots(content)
            # rp.can_fetch attend une URL (ou path relatif) : on passe l'URL complète du path
            test_url = urljoin(base_url, path)
            result["allowed"] = rp.can_fetch(user_agent, test_url)
            result["note"] = "Parsed robots.txt avec succès."
        except Exception as e:
            result["note"] = f"Erreur parsing robots.txt: {e}"
            result["allowed"] = None
    elif status_code == 404:
        # pas de robots.txt : par convention, on considère que tout est autorisé par défaut,
        # mais on note l'absence pour prudence
        result["fetched"] = False
        result["note"] = "robots.txt non trouvé (404). Par défaut, cela n'interdit rien mais vérifier la politique du site."
        result["allowed"] = True
    else:
        result["note"] = f"robots.txt inaccessible (status: {status_code})."
        result["allowed"] = None

    return result


def canonical_base(url: str) -> str:
    """Retourne la base canonique scheme://netloc d'une URL."""
    parsed = urlparse(url)
    if not parsed.scheme:
        raise ValueError("URL must include a scheme (http:// or https://)")
    return f"{parsed.scheme}://{parsed.netloc}"


def main():
    parser = argparse.ArgumentParser(description="Vérifier et analyser robots.txt d'un domaine.")
    parser.add_argument("--url", required=True, help="Une URL sur le site (ex: https://example.com/page) ou base.")
    parser.add_argument("--path", default="/", help="Chemin à tester /page (par défaut '/')")
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT, help="User-Agent à tester dans robots.txt")
    parser.add_argument("--out", help="Fichier de sortie (texte brut si --json absent, sinon JSON)")
    parser.add_argument("--json", action="store_true", help="Afficher la sortie en JSON structuré")
    parser.add_argument("--retries", type=int, default=3, help="Nombre de retries pour la récupération")
    parser.add_argument("--backoff", type=float, default=0.5, help="Backoff factor pour retries")
    parser.add_argument("--timeout", type=float, default=10.0, help="Timeout en secondes pour les requêtes")
    args = parser.parse_args()

    try:
        base = canonical_base(args.url)
    except Exception as e:
        print(f"URL invalide: {e}", file=sys.stderr)
        sys.exit(2)

    res = analyze(base, args.path, args.user_agent, retries=args.retries, backoff=args.backoff, timeout=args.timeout)

    if args.json:
        out_text = json.dumps(res, ensure_ascii=False, indent=2)
    else:
        # format lisible
        lines = []
        lines.append(f"Base: {res['base_url']}")
        lines.append(f"Robots URL: {res['robots_url']}")
        lines.append(f"Status: {res['status_code']}")
        if res.get("note"):
            lines.append(f"Note: {res['note']}")
        lines.append(f"User-Agent testé: {res['user_agent']}")
        lines.append(f"Chemin testé: {res['tested_path']}")
        allowed = res['allowed']
        if allowed is True:
            lines.append("Résultat: AUTORISÉ (can_fetch = True)")
        elif allowed is False:
            lines.append("Résultat: INTERDIT (can_fetch = False)")
        else:
            lines.append("Résultat: INCONNU / robots.txt absent ou non interprétable")
        if res.get("raw") and len(res["raw"]) > 0:
            lines.append("\n--- Début robots.txt ---")
            lines.append(res["raw"][:2000])  # n'affiche pas tout si très long
            lines.append("--- Fin robots.txt ---")
        out_text = "\n".join(lines)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(out_text)
        print(f"Écrit dans {args.out}")
    else:
        print(out_text)

    # exit code: 0 si autorisé (ou robots absent), 3 si explicitement interdit, 4 si erreur de fetch/parsing
    if res["allowed"] is True:
        sys.exit(0)
    if res["allowed"] is False:
        sys.exit(3)
    sys.exit(4)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Utilitaire amélioré pour copier / déplacer / fusionner / valider un ou plusieurs fichiers JSONL.

Fonctionnalités principales :
 - Accepte un ou plusieurs fichiers d'entrée (chemins ou motifs glob)
 - Validation optionnelle JSON line-by-line (--validate)
 - Option de déduplication légère (--dedup) basée sur hash de la ligne
 - Écriture atomique (écrit dans un fichier temporaire puis remplace)
 - Option pour déplacer plutôt que copier (--move)
 - Option pour compresser la sortie en gzip (--gzip)
 - Option append pour ajouter au fichier de sortie existant (--append)
 - Progress bar via tqdm si installé

Exemples :
  # simple copie unique
  python scripts/save_jsonl.py --in data/corpus_stage.jsonl --out data/corpus.jsonl

  # fusion de plusieurs fichiers via glob, validation + dedup
  python scripts/save_jsonl.py --in "data/stage/*.jsonl" --out data/corpus.jsonl --validate --dedup

  # merge, déplacer fichiers sources après fusion, compresser la sortie
  python scripts/save_jsonl.py --in "data/stage/*.jsonl" --out data/corpus.jsonl.gz --move --gzip --validate

Return codes:
 0 = success
 1 = runtime error (IO, permission...)
 2 = invalid args
 3 = JSON validation failed (if --validate specified) - file produced but non-zero exit
"""
from __future__ import annotations
import argparse
import glob
import gzip
import json
import os
import shutil
import sys
import tempfile
import hashlib
from pathlib import Path
from typing import Iterable, List, Set

try:
    from tqdm import tqdm
except Exception:
    tqdm = lambda x, **kwargs: x  # fallback if tqdm missing


def expand_inputs(patterns: Iterable[str]) -> List[Path]:
    paths: List[Path] = []
    for p in patterns:
        # allow both glob patterns and direct paths
        matched = list(map(Path, glob.glob(p, recursive=True)))
        if not matched:
            # try literal path
            sp = Path(p)
            if sp.exists():
                matched = [sp]
        for m in matched:
            if m.is_file():
                paths.append(m)
    # keep stable order and remove duplicates while preserving order
    seen = set()
    uniq = []
    for p in paths:
        rp = str(p.resolve())
        if rp not in seen:
            seen.add(rp)
            uniq.append(p)
    return uniq


def open_out(path: Path, gzip_out: bool, append: bool):
    # ensure parent dir
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "ab" if append else "wb"
    if gzip_out:
        return gzip.open(path, mode)
    else:
        return open(path, mode)


def compute_line_hash(line: bytes) -> str:
    return hashlib.sha256(line).hexdigest()


def process_files(
    inputs: List[Path],
    out_path: Path,
    gzip_out: bool = False,
    append: bool = False,
    validate: bool = False,
    dedup: bool = False,
    move_sources: bool = False,
):
    """Concatène/valide/dedup les fichiers d'entrée vers out_path atomiquement."""
    # If append is True and out_path already exists and not gzip mismatch, we'll open out in append mode.
    # Otherwise write to a temp file and atomically replace at the end.
    temp_file = None
    out_is_existing = out_path.exists() and append

    # If append and file exists, we write directly to it (atomic replace not used for append)
    if out_is_existing:
        out_f = open_out(out_path, gzip_out, append=True)
        temp_file_path = None
    else:
        # create temporary file in same directory for atomic replace
        fd, temp_file_path = tempfile.mkstemp(prefix=".tmp_merge_", dir=str(out_path.parent))
        os.close(fd)
        # but we want to open with gzip if requested
        if gzip_out:
            # gzip expects binary file object — open underlying file and then wrap
            out_f = gzip.open(temp_file_path, "wb")
        else:
            out_f = open(temp_file_path, "wb")

    seen_hashes: Set[str] = set()
    total_in = 0
    total_out = 0
    validation_errors = 0

    try:
        for src in tqdm(inputs, desc="Inputs", unit="file"):
            try:
                # open input (support gzipped inputs transparently)
                if src.suffix.lower() in (".gz", ".gzip"):
                    in_f = gzip.open(src, "rb")
                else:
                    in_f = open(src, "rb")
            except Exception as e:
                print(f"[warn] cannot open {src}: {e}", file=sys.stderr)
                continue

            with in_f:
                for raw_line in in_f:
                    total_in += 1
                    # normalize line endings; keep it as bytes
                    if not raw_line.strip():
                        continue
                    # optional validation
                    if validate:
                        try:
                            # decode as utf-8 with replace to avoid crashes on bad bytes, then json loads
                            decoded = raw_line.decode("utf-8")
                            json.loads(decoded)
                        except Exception:
                            # try latin1 fallback
                            try:
                                decoded = raw_line.decode("latin1")
                                json.loads(decoded)
                            except Exception:
                                validation_errors += 1
                                # keep going but don't write invalid lines
                                print(f"[validate] invalid JSON line in {src} at input #{total_in}", file=sys.stderr)
                                continue

                    # dedup check (hash of raw bytes)
                    if dedup:
                        h = compute_line_hash(raw_line)
                        if h in seen_hashes:
                            continue
                        seen_hashes.add(h)

                    # write to output
                    out_f.write(raw_line)
                    # ensure newline at end
                    if not raw_line.endswith(b"\n"):
                        out_f.write(b"\n")
                    total_out += 1

            # optionally move the source file (after successful read)
            if move_sources:
                try:
                    # move to same dir with .moved extension to avoid accidental overwrite
                    moved = src.with_suffix(src.suffix + ".moved")
                    shutil.move(str(src), str(moved))
                except Exception as e:
                    print(f"[warn] failed to move {src}: {e}", file=sys.stderr)

    finally:
        out_f.flush()
        out_f.close()

    # If we wrote into a temp file, move it atomically to final destination
    if temp_file_path:
        try:
            # if out_path exists and not append, remove or backup depending on policy (we overwrite)
            if out_path.exists():
                out_path.unlink()
            # move/rename temp -> out
            shutil.move(temp_file_path, str(out_path))
        except Exception as e:
            print(f"[error] failed to move temp file to {out_path}: {e}", file=sys.stderr)
            return {"total_in": total_in, "total_out": total_out, "validation_errors": validation_errors, "ok": False}

    return {"total_in": total_in, "total_out": total_out, "validation_errors": validation_errors, "ok": True}


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Merge/copy/move JSONL files with validation and optional dedup.")
    p.add_argument("--in", dest="inputs", required=True, nargs="+", help="Input files or glob patterns (quote globs).")
    p.add_argument("--out", required=True, help="Output file path (can end with .gz if --gzip).")
    p.add_argument("--move", action="store_true", help="Move source files instead of copying (rename with .moved).")
    p.add_argument("--validate", action="store_true", help="Validate each line is valid JSON (skip invalid).")
    p.add_argument("--dedup", action="store_true", help="Deduplicate identical lines (sha256 of line).")
    p.add_argument("--gzip", action="store_true", help="Compress output with gzip.")
    p.add_argument("--append", action="store_true", help="Append to existing output file if present.")
    p.add_argument("--overwrite", action="store_true", help="Overwrite output file if it exists (default: error).")
    p.add_argument("--show-progress", action="store_true", help="Show progress bar (requires tqdm).")
    args = p.parse_args(argv)
    return args


def main(argv=None):
    args = parse_args(argv)
    inputs = expand_inputs(args.inputs)
    if not inputs:
        print("No input files found for patterns:", args.inputs, file=sys.stderr)
        return 2

    out_path = Path(args.out)
    gzip_out = args.gzip or out_path.suffix.lower() in (".gz", ".gzip")

    # handle existing output
    if out_path.exists() and not args.append:
        if not args.overwrite:
            print(f"Output {out_path} exists. Use --overwrite to replace or --append to append.", file=sys.stderr)
            return 2
        else:
            try:
                out_path.unlink()
            except Exception as e:
                print(f"Cannot remove existing output {out_path}: {e}", file=sys.stderr)
                return 1

    # set tqdm behavior
    global tqdm
    if not args.show_progress:
        tqdm = lambda x, **kwargs: x

    result = process_files(
        inputs=inputs,
        out_path=out_path,
        gzip_out=gzip_out,
        append=args.append,
        validate=args.validate,
        dedup=args.dedup,
        move_sources=args.move,
    )

    if not result["ok"]:
        print("Operation failed.", file=sys.stderr)
        return 1

    print(f"Done. Lines read: {result['total_in']}, lines written: {result['total_out']}, validation_errors: {result['validation_errors']}")
    # if validation errors occurred and user asked for validation, return non-zero to signal caution
    if args.validate and result["validation_errors"] > 0:
        return 3

    return 0


if __name__ == "__main__":
    sys.exit(main())

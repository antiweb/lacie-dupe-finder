#!/usr/bin/env python3
"""
Duplicate file finder for LaCie-2-Big-16Tb  — v5 (parallel reads for RAID 0)

Run from Terminal with LOW priority so macOS stays responsive:
    nice -n 20 python3 /Volumes/LaCie-2-Big-16Tb/find_duplicates.py

Progress is logged to:  ~/Desktop/LaCie_dupes_progress.log
Final report saved to:  ~/Desktop/LaCie_duplicates_report.txt

If interrupted, re-run the same command — it will resume from the checkpoint.

Speed strategy for a 2-drive RAID 0:
  • Inode sorting is skipped — data is striped, so physical location is not
    predictable from inode number.
  • TWO parallel reader threads — matches the two physical drives, so both
    drives stay busy simultaneously. More than 2 workers rarely helps on
    RAID 0 and can cause contention.
  • Two-pass hashing: 64 KB quick pass first, full hash only on survivors.
  • Python threads are fine here — hashing is I/O-bound, so the GIL releases
    during reads and both threads run truly in parallel.
"""

import os
import hashlib
import json
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
DRIVE      = "/Volumes/LaCie-2-Big-16Tb"
REPORT     = os.path.expanduser("~/Desktop/LaCie_duplicates_report.txt")
LOG        = os.path.expanduser("~/Desktop/LaCie_dupes_progress.log")
CHECKPOINT = os.path.expanduser("~/Desktop/LaCie_dupes_checkpoint.json")

WORKERS       = 2            # match number of physical drives in the RAID 0
MIN_SIZE      = 10_000       # skip files under 10 KB
PARTIAL_BYTES = 64 * 1024    # quick-hash reads only this much per file

SKIP_DIR_NAMES = {
    '.Spotlight-V100', '.fseventsd', '.Trashes', '.TemporaryItems',
    '__MACOSX', '.DocumentRevisions-V100', '.MobileBackups',
    'node_modules', '.git', '.svn',
    'fakedlls', 'wine', 'wswine.bundle',
}

SKIP_PATH_SUBSTRINGS = [
    '.app/Contents/',
    'Wineskin',
    '/wine/lib/',
    'node_modules/',
]

SKIP_FILES = {'.DS_Store'}

# ── Helpers ───────────────────────────────────────────────────────────────────
_log_lock = __import__('threading').Lock()

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    with _log_lock:
        print(line, flush=True)
        with open(LOG, "a") as f:
            f.write(line + "\n")

def human(n):
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"

def should_skip_path(path):
    return any(s in path for s in SKIP_PATH_SUBSTRINGS)

def hash_file(path, partial=False):
    """Return (path, md5_hex) or (path, None) on error."""
    h = hashlib.md5()
    try:
        with open(path, "rb") as f:
            if partial:
                buf = f.read(PARTIAL_BYTES)
                if not buf:
                    return path, None
                h.update(buf)
            else:
                while True:
                    buf = f.read(4 * 1024 * 1024)
                    if not buf:
                        break
                    h.update(buf)
        return path, h.hexdigest()
    except Exception:
        return path, None

def parallel_hash(paths, partial, label):
    """Hash a list of paths in parallel, returning {path: hex}. Shows progress."""
    results = {}
    errors  = 0
    done    = 0
    total   = len(paths)
    last_log = time.time()

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(hash_file, p, partial): p for p in paths}
        for fut in as_completed(futures):
            path, digest = fut.result()
            if digest:
                results[path] = digest
            else:
                errors += 1
            done += 1
            if time.time() - last_log >= 30:   # log every 30 s
                log(f"  {label}: {done:,} / {total:,} ({done/total*100:.1f}%)")
                last_log = time.time()

    log(f"  {label}: {done:,} / {total:,} done — {errors} errors")
    return results

# ── Load checkpoint ───────────────────────────────────────────────────────────
checkpoint_data = {}
if os.path.exists(CHECKPOINT):
    try:
        with open(CHECKPOINT) as f:
            checkpoint_data = json.load(f)
        log(f"Resuming from checkpoint (phase: {checkpoint_data.get('phase','?')})")
    except Exception:
        checkpoint_data = {}

# ── Phase 1: walk tree, collect file sizes ────────────────────────────────────
if checkpoint_data.get("phase") in ("hashing", "done"):
    log("Phase 1: skipped (loaded from checkpoint)")
    size_map    = {int(k): v for k, v in checkpoint_data["size_map"].items()}
    total_files = checkpoint_data["total_files"]
    skipped     = checkpoint_data["skipped"]
else:
    log("Phase 1: walking directory tree (stat only — no file reads)...")
    size_map    = defaultdict(list)
    total_files = 0
    skipped     = 0

    for root, dirs, files in os.walk(DRIVE):
        dirs[:] = [
            d for d in dirs
            if d not in SKIP_DIR_NAMES
            and not should_skip_path(os.path.join(root, d))
        ]
        if should_skip_path(root):
            skipped += len(files)
            continue

        for fname in files:
            if fname.startswith('._') or fname in SKIP_FILES:
                skipped += 1
                continue
            fpath = os.path.join(root, fname)
            try:
                size = os.path.getsize(fpath)
                if size < MIN_SIZE:
                    skipped += 1
                    continue
                size_map[size].append(fpath)
                total_files += 1
            except OSError:
                skipped += 1

        if total_files % 20000 == 0 and total_files > 0:
            log(f"  ...{total_files:,} files indexed")

    log(f"Phase 1 done: {total_files:,} files, {skipped:,} skipped")

    with open(CHECKPOINT, "w") as f:
        json.dump({
            "phase":       "hashing",
            "total_files": total_files,
            "skipped":     skipped,
            "size_map":    {str(k): v for k, v in size_map.items()},
        }, f)
    log("Checkpoint saved.")

# ── Phase 2: identify size-collision candidates ───────────────────────────────
candidates = {sz: paths for sz, paths in size_map.items() if len(paths) > 1}
candidate_paths = [p for paths in candidates.values() for p in paths]
candidate_files = len(candidate_paths)
candidate_bytes = sum(sz * len(p) for sz, p in candidates.items())
log(f"Phase 2: {candidate_files:,} candidate files ({human(candidate_bytes)}) across "
    f"{len(candidates):,} size groups — reading with {WORKERS} parallel threads")

# ── Phase 3: two-pass parallel hashing ───────────────────────────────────────
if checkpoint_data.get("phase") == "done":
    log("Phase 3: skipped (loaded from checkpoint)")
    hash_map = {k: [tuple(x) for x in v] for k, v in checkpoint_data["hash_map"].items()}
    hashed   = checkpoint_data["hashed"]
    errors   = checkpoint_data["errors"]
else:
    # Pass 3a — quick 64 KB hash of all candidates (2 threads in parallel)
    log(f"Phase 3a: quick hash (64 KB × {candidate_files:,} files, {WORKERS} threads)...")
    partial_results = parallel_hash(candidate_paths, partial=True, label="quick-hash")

    # Group by partial hash; keep only groups where 2+ files match
    partial_map = defaultdict(list)
    path_to_size = {p: sz for sz, paths in candidates.items() for p in paths}
    for path, digest in partial_results.items():
        partial_map[digest].append((path, path_to_size[path]))

    survivors      = {h: g for h, g in partial_map.items() if len(g) > 1}
    survivor_paths = [p for g in survivors.values() for p, _ in g]
    survivor_files = len(survivor_paths)
    eliminated     = candidate_files - survivor_files
    log(f"Phase 3a done: {eliminated:,} eliminated — {survivor_files:,} survivors")

    # Pass 3b — full hash of survivors only (2 threads in parallel)
    log(f"Phase 3b: full hash ({survivor_files:,} files, {WORKERS} threads)...")
    full_results = parallel_hash(survivor_paths, partial=False, label="full-hash")

    # Group by full hash
    hash_map = defaultdict(list)
    for path, digest in full_results.items():
        hash_map[digest].append((path, path_to_size[path]))

    hashed = len(full_results)
    errors = survivor_files - hashed

    # Save checkpoint
    with open(CHECKPOINT, "w") as f:
        json.dump({
            "phase":       "done",
            "total_files": total_files,
            "skipped":     skipped,
            "size_map":    {str(k): v for k, v in size_map.items()},
            "hash_map":    dict(hash_map),
            "hashed":      hashed,
            "errors":      errors,
        }, f)
    log(f"Phase 3 done: {hashed:,} full-hashed, {errors} errors")

# ── Phase 4: report ───────────────────────────────────────────────────────────
duplicates = {h: g for h, g in hash_map.items() if len(g) > 1}
dup_groups = len(duplicates)
dup_files  = sum(len(g) for g in duplicates.values())
wasted     = sum((len(g) - 1) * g[0][1] for g in duplicates.values())

log(f"Phase 4: {dup_groups:,} groups | {dup_files:,} files | {human(wasted)} recoverable")
log(f"Writing report → {REPORT}")

with open(REPORT, "w") as out:
    out.write("=" * 72 + "\n")
    out.write("DUPLICATE FILE REPORT — LaCie-2-Big-16Tb\n")
    out.write(f"Generated : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    out.write("=" * 72 + "\n\n")
    out.write(f"Files scanned    : {total_files:,}\n")
    out.write(f"Duplicate groups : {dup_groups:,}\n")
    out.write(f"Duplicate files  : {dup_files:,}\n")
    out.write(f"Space to recover : {human(wasted)}\n\n")
    out.write("Tip: review before deleting — some duplicates may be intentional backups.\n\n")
    out.write("=" * 72 + "\n")
    out.write("GROUPS (largest space waste first)\n")
    out.write("=" * 72 + "\n\n")

    for i, group in enumerate(
        sorted(duplicates.values(), key=lambda g: (len(g)-1)*g[0][1], reverse=True), 1
    ):
        size  = group[0][1]
        waste = (len(group) - 1) * size
        out.write(f"--- Group {i}  |  {len(group)} copies  |  {human(size)} each  |  {human(waste)} wasted ---\n")
        for path, _ in sorted(group, key=lambda x: x[0]):
            out.write(f"  {path}\n")
        out.write("\n")

if os.path.exists(CHECKPOINT):
    os.remove(CHECKPOINT)

log(f"✓ Complete!  open \"{REPORT}\"")

# LaCie Dupe Finder

A Python script for finding duplicate files on large drives, optimised specifically for **RAID 0 arrays** (two physical drives striped together). Built and tested on a 16 TB LaCie 2big RAID 0 with ~7.6 TB of data across hundreds of thousands of files.

---

## How It Works

Naively scanning for duplicates on a large drive means reading every file — on a 7+ TB drive that takes days. This script uses a four-phase strategy to reduce the actual I/O to a fraction of that:

### Phase 1 — Metadata Walk (no file reads)
The script walks the entire directory tree using `os.stat()`, collecting each file's path and size. No file contents are read at this stage — it's pure filesystem metadata, which is fast regardless of drive size.

Files are skipped if they:
- Are under 10 KB (thumbnails, icons, cache files — not worth checking)
- Live inside `.app` bundles, `wine`/`Wineskin` directories, or other non-user-content folders
- Are macOS system files (`.DS_Store`, `._*` resource forks, `.Spotlight-V100`, etc.)

### Phase 2 — Size Collision Filtering
Two files can only be duplicates if they are exactly the same size. All files with a unique size are discarded immediately — no hashing needed. Only files that share a size with at least one other file proceed to hashing.

On a typical archive drive this eliminates the large majority of files before a single byte of content is read.

### Phase 3a — Quick Hash (64 KB per file, parallel)
For each size-collision candidate, only the **first 64 KB** is read and hashed. Files that differ in their opening 64 KB cannot be duplicates and are eliminated. This is extremely fast compared to reading full files.

Two parallel reader threads run simultaneously — one per physical drive in the RAID 0 array — keeping both drives saturated at once.

### Phase 3b — Full Hash (survivors only, parallel)
Only files that matched on both size *and* the 64 KB partial hash are fully hashed. In practice, this is a small fraction of the original candidate set. Full hashes confirm true byte-for-byte duplicates beyond any doubt.

Again, two parallel threads are used to maximise RAID 0 throughput.

### Phase 4 — Report
Results are written to `~/Desktop/LaCie_duplicates_report.txt`, listing every duplicate group sorted by wasted space (largest first), so the biggest wins are immediately visible at the top.

---

## Why Two Threads?

This script is optimised for **RAID 0**, which stripes data across two physical drives. A single sequential reader can only saturate one drive at a time. Two parallel threads keep both drives busy simultaneously, roughly doubling throughput. More than two workers would cause contention rather than improvement.

On a single spinning HDD, inode-sorted sequential reads would be the better strategy (the drive head sweeps in one direction). On RAID 0, inode ordering is meaningless because data is striped, making parallelism the right approach instead.

Python threads are used rather than processes. This works well here because file hashing is I/O-bound — the GIL is released during disk reads, so both threads run truly in parallel.

---

## Requirements

- Python 3.6 or later (uses f-strings and `concurrent.futures`)
- No third-party libraries — standard library only
- macOS (paths and volume mounting assumed; easily adapted for Linux/Windows)

---

## Usage

```bash
nice -n 20 python3 /Volumes/LaCie-2-Big-16Tb/find_duplicates.py
```

The `nice -n 20` prefix runs the script at the lowest CPU/IO priority, which keeps macOS responsive while the scan runs in the background. Without it, sustained disk activity can starve the OS and cause UI freezes.

---

## Output Files

| File | Description |
|------|-------------|
| `~/Desktop/LaCie_duplicates_report.txt` | Final report — duplicate groups sorted by wasted space |
| `~/Desktop/LaCie_dupes_progress.log` | Live progress log, updated every 30 seconds |
| `~/Desktop/LaCie_dupes_checkpoint.json` | Resumable checkpoint (deleted automatically on completion) |

---

## Resuming After Interruption

If the script is interrupted (crash, power loss, manual Ctrl+C), just re-run the same command. It detects the checkpoint file and skips straight to whichever phase it was in. The checkpoint is saved after Phase 1 (metadata walk) and again every 5 minutes during hashing.

On successful completion, the checkpoint file is automatically deleted.

---

## Configuration

All tuneable settings are at the top of the script:

```python
DRIVE         = "/Volumes/LaCie-2-Big-16Tb"  # path to the drive to scan
WORKERS       = 2            # parallel threads — set to number of physical drives
MIN_SIZE      = 10_000       # skip files smaller than this (bytes)
PARTIAL_BYTES = 64 * 1024    # how much to read in the quick-hash pass
```

**Adjusting `WORKERS`:**
- RAID 0 with 2 drives → `2` (default)
- RAID 0 with 4 drives → try `4`
- Single HDD → `1` (parallelism hurts on a single spindle)
- SSD → try `4`–`8` (SSDs handle parallel reads well)

**Adjusting `MIN_SIZE`:**
Raising this (e.g. to `100_000` for 100 KB) speeds up Phase 1 and reduces candidates, but means smaller duplicate files will not be detected. Lower it to `0` to check everything including tiny files.

---

## Skipped Directories

The following folder names and path patterns are excluded from scanning. They contain app internals, Wine/Wineskin DLL stubs, or system metadata — not user content, and scanning them causes excessive I/O with no useful results:

**Folder names:**
`.Spotlight-V100`, `.fseventsd`, `.Trashes`, `.TemporaryItems`, `__MACOSX`, `.DocumentRevisions-V100`, `.MobileBackups`, `node_modules`, `.git`, `.svn`, `fakedlls`, `wine`, `wswine.bundle`

**Path substrings:**
`.app/Contents/`, `Wineskin`, `/wine/lib/`, `node_modules/`

To add your own exclusions, edit `SKIP_DIR_NAMES` or `SKIP_PATH_SUBSTRINGS` at the top of the script.

---

## Reading the Report

The report lists every confirmed duplicate group, largest wasted space first:

```
--- Group 1  |  3 copies  |  2.1 GB each  |  4.2 GB wasted ---
  /Volumes/LaCie-2-Big-16Tb/Backups/project.zip
  /Volumes/LaCie-2-Big-16Tb/OldDrive/project.zip
  /Volumes/LaCie-2-Big-16Tb/project.zip

--- Group 2  |  2 copies  |  850.0 MB each  |  850.0 MB wasted ---
  /Volumes/LaCie-2-Big-16Tb/Archive/footage.mov
  /Volumes/LaCie-2-Big-16Tb/Exports/footage.mov
```

Before deleting anything, review carefully — files in `Backups/` or `Archive/` folders may be intentional duplicates. The script tells you what is duplicated; deciding what to keep is up to you.

---

## Performance Notes

- **Phase 1** (metadata walk) completes in a few minutes even on 7+ TB drives
- **Phase 3a** (quick hash) is the fastest hashing phase — 64 KB reads across thousands of files
- **Phase 3b** (full hash) duration depends entirely on how many files survive Phase 3a
- On a 7.6 TB RAID 0 with ~150,000 files, total runtime is typically a few hours
- Monitor progress live with: `tail -f ~/Desktop/LaCie_dupes_progress.log`

---

## Background

This script was built iteratively to handle a real-world problem: a 16 TB LaCie 2big RAID 0 drive with 7.6 TB of accumulated data, multiple old machine backups, and years of copied folders. Earlier approaches caused macOS to freeze due to unsorted small-file I/O hammering the drive. The current version was designed specifically to avoid that while keeping both physical drives in the RAID 0 array productive.

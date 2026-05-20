# Fast Parallel Rsync Wrapper (Heavily optimized by Gemini AI)

A high-performance, non-blocking parallel wrapper for `rsync`, written in Python. Designed to synchronize massive directory structures (100M+ files, 100k+ directories) on enterprise storage arrays (NFS, GPFS, NetApp) by eliminating standard `rsync` I/O bottlenecks.

## 🚀 How It Works
Standard `rsync` struggles with massive file trees due to sequential metadata scanning. This script splits the workload into highly optimized, concurrent phases:

1. **O(1) Memory Source Scan:** Uses Python's low-level `os.scandir` to map the source directory hierarchy instantly. It ignores individual files and heavy metadata, drastically reducing RAM and I/O overhead.
2. **Producer-Consumer Sync (Phase 1):** A thread pool pulls mapped directories from the scanner and executes standard `rsync` processes in parallel, saturating network and disk throughput.
3. **Immediate Orphan Cleanup (Phase 2):** A secondary thread pool concurrently scans the destination. It immediately identifies and deletes orphaned directory trees (structures that no longer exist in the source) without waiting for the entire synchronization to finish, preventing race conditions natively.

## ⚡ Performance
In real-world production benchmarks on **143+ million items** across 175,000 directories, second synchronization time was reduced from **~8 hours** to **~20 minutes**.

## 📋 Requirements
* Linux / Unix-like OS
* Python 3.8+ (No external libraries required)
* Standard `rsync` binary accessible in `$PATH`

## 🛠️ Usage

Basic execution syntax:
```bash
python3 dsync.py -w NUMBER /path/to/source/ /path/to/destination/

Full help:

usage: dsync.py [-h] [-w WORKERS] [--options OPTIONS] [-d] [-n] [-l LOG_CHANGED] src dst

Non-blocking parallel rsync wrapper with directory-level granularity.

positional arguments:
  src                   Source directory
  dst                   Destination directory

options:
  -h, --help            show this help message and exit
  -w WORKERS, --workers WORKERS
                        Number of parallel workers for sync and cleanup (default: 8)
  --options OPTIONS     Parameters for rsync. Default: '-lptgoD --delete'.
  -d, --debug           Print detailed debug logging
  -n, --dry-run         Simulation run without writing data to disk
  -l LOG_CHANGED, --log-changed LOG_CHANGED
                        Append paths of modified or deleted directories to log file

# dsync: Parallel Rsync Wrapper & Time Machine for Massive Filesystems

dsync is a high-performance, Python-based toolkit designed to back up and restore massive filesystems (hundreds of millions of files, e.g., GPFS, Ceph, NFS) where standard single-threaded rsync creates severe bottlenecks.

## It implements a multithreaded reverse-incremental backup strategy and a highly optimized Point-in-Time recovery (Time Machine) system.
🚀 Key Features

* **Massive Parallelization without IOPS Hotspotting:** Directory traversal uses a multithreaded architecture. Unlike standard Depth-First Search (DFS) which can hammer a single storage node, dsync uses a randomized PriorityQueue. This distributes read/write IOPS evenly across the entire clustered filesystem.

*    **Reverse-Incremental Backups:** Keeps the main mirror always up-to-date (1:1 with production) while moving modified or deleted files to timestamped diffs/ directories.

*    Smart Inode Cleanup: Automatically prunes empty directory shells from incremental backups, saving millions of unnecessary inodes.

*    **Multithreaded Point-in-Time Restore:** Reconstructs the exact state of any directory or file to a specific historical point. Restores are highly parallelized and avoid slow recursive tree copying.

*    In-Place & Out-of-Place Recovery: Safely restore data directly to production or build the reconstructed directory tree in an isolated destination.

## 🛠️ The Toolkit

The project consists of two primary tools:
### 1. dsync_backup.py (The Synchronizer)

A non-blocking parallel rsync wrapper. It scans the source directory tree, queues subdirectories with randomized priority, and dispatches them to a pool of rsync worker threads.

### 2. dsync_restore.py (The Time Machine)

Analyzes the main mirror and the incremental diffs to build an execution plan in memory. It can reverse-patch data back to a specific timestamp using parallelized copy operations.

#### ⚙️ Architecture details

Immutable History: The restore script treats the backup history as strictly immutable. Its baseline is the --mirror (the frozen state after the last sync), completely independent of current live production data.

Execution Plans: Before any restore operation, dsync_restore.py traverses the necessary diffs and generates an optimized atomized task list in RAM. This prevents the script from stalling on deep directory nesting during the actual I/O operations.

#### 📋 Requirements

    Python 3.6+
    rsync installed on the host
    No external Python dependencies required (uses pure standard library).

## Exmaple
```
# backup/rsync
dsync_backup.py  /tmp/dsync_test/prod/   /tmp/dsync_test/mirror/  --backup-dir /tmp/dsync_test/history/  -w 2

# list history
dsync_restore.py --prod /tmp/dsync_test/prod/ --mirror /tmp/dsync_test/mirror/ --backup-dir /tmp/dsync_test/history/ --target "AAA/file.txt"  --history

# restore
dsync_restore.py --prod /tmp/dsync_test/prod/ --mirror /tmp/dsync_test/mirror/ --backup-dir /tmp/dsync_test/history/ --target "AAA/file.txt" --restore-to VASE_STRE_ID --dest /tmp/dsync_test/obnoveno_z_minulosti/ -w 2
```
## Help
```
dsync_backup.py --help
usage: dsync_backup.py [-h] [-w WORKERS] [--options OPTIONS] [-d] [-n] [-l LOG_CHANGED] [-b BACKUP_DIR] src dst

Non-blocking parallel rsync wrapper with reverse-incremental backup support.

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
  -b BACKUP_DIR, --backup-dir BACKUP_DIR
                        Root directory for reverse-incremental backups (diffs and logs)


dsync_restore.py --help
usage: dsync_restore.py [-h] --prod PROD --mirror MIRROR --backup-dir BACKUP_DIR --target TARGET [--dest DEST] [--history] [--restore-to RESTORE_TO] [-w WORKERS]

Time Machine Restore for dsync

options:
  -h, --help            show this help message and exit
  --prod PROD           Production directory (e.g., /mnt/data/tmp/)
  --mirror MIRROR       Main backup mirror directory (rsync destination, e.g., tmp2)
  --backup-dir BACKUP_DIR
                        Root directory for reverse-incremental backups (containing timestamps)
  --target TARGET       Relative path to the target item (file or directory) to process
  --dest DEST           OPTIONAL: Alternative destination directory for out-of-place restore
  --history             Only print the available point-in-time recovery timeline for the target and exit
  --restore-to RESTORE_TO
                        Target timestamp for recovery in YYYYMMDD_HHMMSS format, or 'now'
  -w WORKERS, --workers WORKERS
                        Number of concurrent worker threads for copy operations```

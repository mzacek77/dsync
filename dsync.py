#!/usr/bin/env python3
"""
Parallel and Asynchronous Rsync Wrapper for Massive Directory Structures.

This script is designed for extremely fast synchronization of large storage arrays
containing tens or hundreds of millions of files. It bypasses the sequential limitations
of standard rsync by splitting the workload into three concurrent concepts:
1. Scanner (Producer): Scans the source directory using low-level os.scandir.
2. Rsync Workers (Consumers): Synchronize individual directories independently.
3. Cleanup Workers: Concurrently scan the destination and immediately delete orphaned directories.
"""

import os
import subprocess
import argparse
import shutil
import threading
import queue
import shlex
import time
import re
import sys
from pathlib import Path
from datetime import datetime

def print_debug(msg: str, debug: bool):
    """Helper function to print detailed debug logs if the -d flag is active."""
    if debug:
        print(f"[DEBUG] {msg}")

def format_time(seconds: float) -> str:
    """Converts elapsed time in seconds to a human-readable format (hours, minutes, seconds)."""
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h > 0: return f"{h}h {m}m {s}s"
    if m > 0: return f"{m}m {s}s"
    return f"{s}s"

def format_bytes(size: float) -> str:
    """Dynamically formats sizes in bytes into the most appropriate unit (KB, MB, GB, etc.)."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"

def parse_rsync_stats(stdout: str) -> dict:
    """
    Parses the text output of the --stats flag from the rsync command.
    Uses regular expressions to extract exact counts of transferred, created,
    and deleted files, as well as the total volume of data sent.
    """
    stats = {'files': 0, 'created_total': 0, 'created_reg': 0, 'deleted': 0, 'transferred': 0, 'bytes': 0}
    for line in stdout.splitlines():
        if "Number of files:" in line:
            m = re.search(r'Number of files:\s*([0-9\., ]+)', line)
            if m: stats['files'] = int(re.sub(r'\D', '', m.group(1)))
        elif "Number of created files:" in line:
            m_tot = re.search(r'Number of created files:\s*([0-9\., ]+)', line)
            if m_tot:
                stats['created_total'] = int(re.sub(r'\D', '', m_tot.group(1)))
            m_reg = re.search(r'reg:\s*([0-9\., ]+)', line)
            if m_reg:
                stats['created_reg'] = int(re.sub(r'\D', '', m_reg.group(1)))
            else:
                stats['created_reg'] = stats.get('created_total', 0)
        elif "Number of deleted files:" in line:
            m = re.search(r'Number of deleted files:\s*([0-9\., ]+)', line)
            if m: stats['deleted'] = int(re.sub(r'\D', '', m.group(1)))
        elif "Number of regular files transferred:" in line:
            m = re.search(r'Number of regular files transferred:\s*([0-9\., ]+)', line)
            if m: stats['transferred'] = int(re.sub(r'\D', '', m.group(1)))
        elif "Total bytes sent:" in line:
            m = re.search(r'Total bytes sent:\s*([0-9\., ]+)', line)
            if m: stats['bytes'] = int(re.sub(r'\D', '', m.group(1)))
    return stats

def walk_and_produce(base_path: Path, path_queue: queue.Queue, dir_set: set,
                     dir_set_lock: threading.Lock, num_workers: int, debug: bool):
    """
    Phase 1 (Producer): Fast scan of the source directory.
    Runs in its own thread and populates the path_queue with discovered subdirectories.
    
    Uses os.scandir, which is an extremely efficient low-level system call. It ignores files
    and avoids loading their metadata, significantly reducing I/O overhead and RAM usage
    on massive directory trees.
    """
    def scan_dirs_recursive(current_path: Path):
        try:
            with os.scandir(current_path) as it:
                # Sorting is useful only for readable debug logs; disabled in production for performance
                entries = sorted(it, key=lambda e: e.name) if debug else it
                for entry in entries:
                    if entry.is_dir(follow_symlinks=False):
                        full_dir = Path(entry.path)
                        rel_dir = full_dir.relative_to(base_path)
                        
                        # Push directory to the queue for rsync workers
                        path_queue.put(rel_dir)
                        # Sets in Python are not thread-safe; writing must be protected by a lock
                        with dir_set_lock:
                            dir_set.add(rel_dir)
                            
                        # Recurse deeper into the tree
                        scan_dirs_recursive(full_dir)
        except OSError as e:
            # Catches issues like "Permission denied"
            print_debug(f"Scan error at {current_path}: {e}", debug)

    # Populate the root directory initially
    path_queue.put(Path("."))
    with dir_set_lock:
        dir_set.add(Path("."))

    # Trigger the recursive deep scan
    scan_dirs_recursive(base_path)

    # Insert "poison pills" (sentinels). Each worker consumes one None to know
    # that scanning is complete and it should exit its execution loop.
    for _ in range(num_workers):
        path_queue.put(None)

def sync_worker(worker_id: int, q: queue.Queue, src_base: Path, dst_base: Path,
                rsync_options: list, results: list, results_lock: threading.Lock,
                debug: bool, dry_run: bool):
    """
    Phase 1 (Consumer): Rsync Worker.
    Pulls paths from the queue populated by the producer and runs synchronization.
    Runs across N parallel threads to maximize network and disk throughput.
    """
    # Setting the environment locale to 'C' optimizes text and shell operations in subprocesses
    env = os.environ.copy()
    env["LC_ALL"] = "C"

    while True:
        rel_dir = q.get()
        if rel_dir is None:
            # Worker encountered a sentinel, terminating work loop
            q.task_done()
            break

        src_dir = src_base / rel_dir
        dst_dir = dst_base / rel_dir

        if not dry_run:
            # Rsync with --dirs does not always reliably create the entire parent directory
            # structure automatically, so we explicitly create it ahead of time via Python
            dst_dir.mkdir(parents=True, exist_ok=True)

        # DRY-RUN Logic: Simulating execution on a non-existent target would cause rsync to fail.
        # Python instead scans the directory to generate estimated statistics so reports remain accurate.
        if dry_run and not dst_dir.exists():
            files = created_total = created_reg = bytes_sent = 0
            try:
                for item in src_dir.iterdir():
                    files += 1
                    created_total += 1
                    if item.is_file() and not item.is_symlink():
                        created_reg += 1
                        bytes_sent += item.stat().st_size
            except Exception as e:
                with results_lock:
                    results.append((rel_dir, 1, {}, f"Read error (simulation): {e}"))
                q.task_done()
                continue

            stats = {
                'files': files, 'created_total': created_total, 'created_reg': created_reg,
                'deleted': 0, 'transferred': created_reg, 'bytes': bytes_sent
            }
            with results_lock:
                results.append((rel_dir, 0, stats, "Simulated by script (target missing)"))
            q.task_done()
            continue

        # Build the command array for subprocess
        # --filter=P */ is critical: it protects nested subdirectories at the destination from being deleted,
        # because rsync runs without recursion (--no-recursive) and would otherwise treat them as foreign entities.
        cmd = ["rsync"] + rsync_options + [
            "--dirs", "--no-recursive", "--filter=P */", "--stats"
        ]

        if dry_run:
            cmd.append("--dry-run")

        # Rsync requires a trailing slash, otherwise it will copy the directory INSIDE the destination directory
        cmd += [f"{src_dir}/", f"{dst_dir}/"]

        try:
            # Blocks this thread until the specific rsync execution completes
            res = subprocess.run(cmd, capture_output=True, text=True, env=env)
            stats = parse_rsync_stats(res.stdout) if res.returncode == 0 else {}
            # Results list must be updated under lock since all worker threads append to it concurrently
            with results_lock:
                results.append((rel_dir, res.returncode, stats, res.stderr))
        except Exception as e:
            with results_lock:
                results.append((rel_dir, 1, {}, str(e)))
        finally:
            q.task_done()

def cleanup_worker(worker_id: int, q: queue.Queue, dst_base: Path, src_dirs_set: set,
                   deleted_log: list, log_lock: threading.Lock, dry_run: bool, debug: bool):
    """
    Phase 2: Concurrent scanning and IMMEDIATE cleanup of orphaned directories.
    
    This worker traverses only known, valid directories derived from Phase 1. If it discovers
    a directory inside a valid destination path that does not exist within the source set (src_dirs_set),
    it flags it as an orphan and immediately calls shutil.rmtree to wipe it permanently.
    
    By querying only valid paths, we guarantee that we only catch the top-level root of any
    orphaned tree structure, naturally preventing race conditions (e.g., separate threads trying
    to delete A/B and A/B/C concurrently). Shutil.rmtree cleanly drops the entire subtree.
    """
    while True:
        rel_dir = q.get()
        if rel_dir is None:
            q.task_done()
            break
            
        dst_dir = dst_base / rel_dir
        if dst_dir.exists():
            local_orphans = []
            try:
                # Utilizing the highly optimized C-level os.scandir iterator
                with os.scandir(dst_dir) as it:
                    for entry in it:
                        # We only care about directories (files are handled natively by rsync's --delete flag)
                        if entry.is_dir(follow_symlinks=False):
                            child_rel = rel_dir / entry.name
                            # Querying a Python Set is an O(1) operation, making this verification instant
                            if child_rel not in src_dirs_set:
                                local_orphans.append((child_rel, Path(entry.path)))
            except OSError:
                pass
                
            # Immediate parallel removal of discovered orphaned trees
            for child_rel, full_path in local_orphans:
                if not dry_run:
                    # ignore_errors=True prevents the worker from crashing if a file inside is locked or lacks permissions
                    shutil.rmtree(full_path, ignore_errors=True)
                
                # Log the deletion safely under lock
                with log_lock:
                    deleted_log.append(child_rel)
                
                if dry_run or debug:
                    action = "WOULD BE DELETED" if dry_run else "Deleted"
                    print(f"[{action}] tree: {full_path}")
                    
        q.task_done()

def main():
    """
    Main orchestration function.
    Parses command-line arguments, initializes thread queues, controls synchronization (Phase 1)
    and concurrent cleanup scheduling (Phase 2), aggregates execution statistics, and prints
    the formatted performance summary.
    """
    parser = argparse.ArgumentParser(
        description="Non-blocking parallel rsync wrapper with directory-level granularity."
    )
    parser.add_argument("src", help="Source directory")
    parser.add_argument("dst", help="Destination directory")
    parser.add_argument("-w", "--workers", type=int, default=8,
                        help="Number of parallel workers for sync and cleanup (default: 8)")
    parser.add_argument("--options", type=str,
                        default="-lptgoD --delete",
                        help="Parameters for rsync. Default: '-lptgoD --delete'.")
    parser.add_argument("-d", "--debug", action="store_true",
                        help="Print detailed debug logging")
    parser.add_argument("-n", "--dry-run", action="store_true",
                        help="Simulation run without writing data to disk")
    parser.add_argument("-l", "--log-changed", type=str,
                        help="Append paths of modified or deleted directories to log file")

    args = parser.parse_args()
    
    # resolve() enforces absolute paths and evaluates any symlinks in root parameters
    src_base = Path(args.src).resolve()
    dst_base = Path(args.dst).resolve()
    # shlex cleanly breaks down custom options while respecting internal quoting/escaping
    rsync_options = shlex.split(args.options)

    # Validate source existence and permissions
    try:
        src_ok = src_base.is_dir()
    except PermissionError:
        src_ok = False
    if not src_ok:
        print(f"Critical error: Source {src_base} does not exist or is not a directory.")
        sys.exit(1)

    # Validate or initialize destination base
    if not args.dry_run:
        try:
            dst_base.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            print(f"Critical error: Cannot create or write to destination {dst_base}.")
            sys.exit(1)

    start_time = time.time()
    if args.dry_run:
        print("
!!! SCRIPT RUNNING IN DRY-RUN MODE - NO DATA WILL BE MODIFIED !!!
")

    print("[INFO] Phase 1: Scanning source and starting Rsync workers...")
    
    # Queue size is bound to 500,000 to prevent runaway memory usage if the producer
    # maps the drive orders of magnitude faster than consumers can clear sync jobs.
    task_queue = queue.Queue(maxsize=500000)
    src_dirs_set = set()
    src_dirs_lock = threading.Lock()
    results = []
    results_lock = threading.Lock()

    # Launch the Producer thread (maps source and populates task_queue)
    t_src_walker = threading.Thread(
        target=walk_and_produce,
        args=(src_base, task_queue, src_dirs_set, src_dirs_lock, args.workers, args.debug)
    )
    t_src_walker.start()

    # Launch Consumer threads (execute parallel rsync instances)
    worker_threads = []
    for i in range(args.workers):
        t = threading.Thread(
            target=sync_worker,
            args=(i, task_queue, src_base, dst_base, rsync_options,
                  results, results_lock, args.debug, args.dry_run)
        )
        t.start()
        worker_threads.append(t)

    # Wait ONLY for the source scanning Producer thread to conclude mapping
    t_src_walker.join()
    if not args.debug:
        print(f"[INFO] Source scan completed. Found {len(src_dirs_set)} directories. `src_dirs_set` is locked.")

    # With src_dirs_set finalized and locked, we can safely boot Phase 2 cleanup tasks
    print("[INFO] Phase 2: Concurrent scan and IMMEDIATE cleanup of orphaned directories...")
    cleanup_queue = queue.Queue()
    deleted_log = []
    log_lock = threading.Lock()

    # Populate the cleanup scheduler queue with the finished directory list
    for rel_dir in src_dirs_set:
        cleanup_queue.put(rel_dir)
    # Append poison pills for cleanup workers
    for _ in range(args.workers):
        cleanup_queue.put(None)

    cleanup_threads = []
    for i in range(args.workers):
        t = threading.Thread(
            target=cleanup_worker,
            args=(i, cleanup_queue, dst_base, src_dirs_set, deleted_log, log_lock, args.dry_run, args.debug)
        )
        t.start()
        cleanup_threads.append(t)

    # Barrier Synchronization: Wait for both data sync and target cleanup loops to finish completely
    for t in worker_threads:
        t.join()
        
    for t in cleanup_threads:
        t.join()

    # --- Metrics Processing & Reporting ---
    success_count = error_count = 0
    tot_files = tot_created_total = tot_created_reg = tot_deleted = tot_transferred = tot_bytes = 0
    changed_directories_log = []
    simulated_count = 0

    for rel_dir, code, stats, err in results:
        if code == 0:
            success_count += 1
            tot_files += stats.get('files', 0)
            tot_created_total += stats.get('created_total', 0)
            tot_created_reg += stats.get('created_reg', 0)
            tot_deleted += stats.get('deleted', 0)
            tot_transferred += stats.get('transferred', 0)
            tot_bytes += stats.get('bytes', 0)

            is_simulated = "Simulated" in err
            if is_simulated:
                simulated_count += 1

            # Log directory metadata alterations only if modifications actually occurred
            if stats.get('transferred', 0) > 0 or stats.get('deleted', 0) > 0 or stats.get('created_total', 0) > 0:
                tag = "MODIFIED (simulated)" if is_simulated else "MODIFIED"
                changed_directories_log.append(f"{tag}: {rel_dir}")
        else:
            error_count += 1
            print(f"[ERROR] Directory '{rel_dir}': {err.strip()}")

    # Compute updated files (total transfers minus newly created files)
    tot_updated = max(0, tot_transferred - tot_created_reg)

    # Append Phase 2 deleted directory trees to the main change log
    deleted_dirs_count = len(deleted_log)
    for deleted_rel in deleted_log:
        changed_directories_log.append(f"DELETED (tree): {deleted_rel}")

    # Commit the modification history log to disk if a file path was specified
    if args.log_changed and changed_directories_log:
        try:
            with open(args.log_changed, 'a', encoding='utf-8') as log_file:
                run_type = " (DRY-RUN)" if args.dry_run else ""
                log_file.write(f"
--- Sync run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{run_type} ---
")
                for entry in changed_directories_log:
                    log_file.write(f"{entry}
")
            print(f"
[INFO] Log of changed directories appended to: {args.log_changed}")
        except IOError as e:
            print(f"
[ERROR] Failed to write to log file '{args.log_changed}': {e}")

    # Output execution stats summary
    elapsed = time.time() - start_time
    header = "--- SYNCHRONIZATION STATS (DRY-RUN SIMULATION) ---" if args.dry_run else "--- SYNCHRONIZATION STATS ---"

    print(f"
" + "=" * len(header))
    print(header)
    print(f"Duration (total):      {format_time(elapsed)}")
    print(f"Directories success:   {success_count}")
    if args.dry_run and simulated_count > 0:
        print(f"  of which simulated:  {simulated_count} (target dir missing, stats estimated)")
    print(f"Directories with err:  {error_count}")
    print(f"Total items checked:   {tot_files} (files, links, dirs)")
    print("-" * len(header))
    print(f"New items total:       {tot_created_total} (of which {tot_created_reg} are regular files)")
    print(f"Modified files:        {tot_updated} (updated content)")
    print(f"Deleted items:         {tot_deleted} (handled by rsync)")
    print(f"Deleted full folders:  {deleted_dirs_count} (handled immediately by script)")
    print("-" * len(header))
    print(f"Total transferred:     {format_bytes(tot_bytes)}")
    print("=" * len(header))

if __name__ == "__main__":
    main()

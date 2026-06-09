#!/usr/bin/env python3
import os
import subprocess
import argparse
import shutil
import threading
import queue
import random
import shlex
import time
import re
import sys
from pathlib import Path
from datetime import datetime

def print_debug(msg: str, debug: bool):
    if debug:
        print(f"[DEBUG] {msg}")

def format_time(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h > 0: return f"{h}h {m}m {s}s"
    if m > 0: return f"{m}m {s}s"
    return f"{s}s"

def format_bytes(size: float) -> str:
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"

def parse_rsync_stats(stdout: str) -> dict:
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
    def scan_dirs_recursive(current_path: Path):
        try:
            with os.scandir(current_path) as it:
                entries = sorted(it, key=lambda e: e.name) if debug else it
                for entry in entries:
                    if entry.is_dir(follow_symlinks=False):
                        full_dir = Path(entry.path)
                        rel_dir = full_dir.relative_to(base_path)
                        
                        path_queue.put((random.random(), rel_dir))
                        with dir_set_lock:
                            dir_set.add(rel_dir)
                            
                        scan_dirs_recursive(full_dir)
        except OSError as e:
            print_debug(f"Scan error at {current_path}: {e}", debug)

    path_queue.put((random.random(), Path(".")))
    with dir_set_lock:
        dir_set.add(Path("."))

    scan_dirs_recursive(base_path)

    for _ in range(num_workers):
        path_queue.put((2.0, None))

def sync_worker(worker_id: int, q: queue.Queue, src_base: Path, dst_base: Path,
                rsync_options: list, results: list, results_lock: threading.Lock,
                debug: bool, dry_run: bool, backup_diffs: Path):
    env = os.environ.copy()
    env["LC_ALL"] = "C"

    while True:
        _, rel_dir = q.get()
        if rel_dir is None:
            q.task_done()
            break

        src_dir = src_base / rel_dir
        dst_dir = dst_base / rel_dir

        if not dry_run:
            dst_dir.mkdir(parents=True, exist_ok=True)

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
                    results.append((rel_dir, 1, {}, f"Read error (simulation): {e}", []))
                q.task_done()
                continue

            stats = {
                'files': files, 'created_total': created_total, 'created_reg': created_reg,
                'deleted': 0, 'transferred': created_reg, 'bytes': bytes_sent
            }
            with results_lock:
                results.append((rel_dir, 0, stats, "Simulated by script (target missing)", []))
            q.task_done()
            continue

        cmd = ["rsync"] + rsync_options + [
            "--dirs", "--no-recursive", "--filter=P */", "--stats", "-8"
        ]

        if backup_diffs:
            b_dir = backup_diffs / rel_dir
            if not dry_run:
                b_dir.mkdir(parents=True, exist_ok=True)
            cmd += ["--backup", f"--backup-dir={b_dir}", "--itemize-changes"]

        if dry_run:
            cmd.append("--dry-run")

        cmd += [f"{src_dir}/", f"{dst_dir}/"]

        try:
            res = subprocess.run(cmd, capture_output=True, text=True, env=env)
            stats = parse_rsync_stats(res.stdout) if res.returncode == 0 else {}
            
            created_items = []
            if backup_diffs and res.returncode == 0:
                # Parsujeme výstup --itemize-changes
                for line in res.stdout.splitlines():
                    if "+++++++" in line:
                        parts = line.split(maxsplit=1)
                        if len(parts) == 2:
                            flags, filename = parts
                            # >f (file), cd (dir), >l (symlink), cL (hardlink)
                            if flags[0] in ('>', 'c') and "+++++++" in flags:
                                created_items.append(str(rel_dir / filename))

            with results_lock:
                results.append((rel_dir, res.returncode, stats, res.stderr, created_items))
        except Exception as e:
            with results_lock:
                results.append((rel_dir, 1, {}, str(e), []))
        finally:
            q.task_done()

def cleanup_worker(worker_id: int, q: queue.Queue, dst_base: Path, src_dirs_set: set,
                   deleted_log: list, log_lock: threading.Lock, dry_run: bool, debug: bool, backup_diffs: Path):
    while True:
        rel_dir = q.get()
        if rel_dir is None:
            q.task_done()
            break
            
        dst_dir = dst_base / rel_dir
        if dst_dir.exists():
            local_orphans = []
            try:
                with os.scandir(dst_dir) as it:
                    for entry in it:
                        if entry.is_dir(follow_symlinks=False):
                            child_rel = rel_dir / entry.name
                            if child_rel not in src_dirs_set:
                                local_orphans.append((child_rel, Path(entry.path)))
            except OSError:
                pass
                
            for child_rel, full_path in local_orphans:
                if not dry_run:
                    if backup_diffs:
                        # Pokud je aktivní backup_dir, nesmíme strom smazat, ale přesunout do zálohy
                        target_orphan = backup_diffs / child_rel
                        target_orphan.parent.mkdir(parents=True, exist_ok=True)
                        try:
                            shutil.move(str(full_path), str(target_orphan))
                        except Exception as e:
                            print_debug(f"Failed to move orphan to backup: {e}", debug)
                    else:
                        shutil.rmtree(full_path, ignore_errors=True)
                
                with log_lock:
                    deleted_log.append(child_rel)
                
                if dry_run or debug:
                    if backup_diffs:
                        action = "WOULD BE MOVED TO BACKUP" if dry_run else "Moved to backup"
                    else:
                        action = "WOULD BE DELETED" if dry_run else "Deleted"
                    print(f"[{action}] tree: {full_path}")
                    
        q.task_done()

def main():
    parser = argparse.ArgumentParser(
        description="Non-blocking parallel rsync wrapper with reverse-incremental backup support."
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
    parser.add_argument("-b", "--backup-dir", type=str,
                        help="Root directory for reverse-incremental backups (diffs and logs)")

    args = parser.parse_args()
    src_base = Path(args.src).resolve()
    dst_base = Path(args.dst).resolve()
    rsync_options = shlex.split(args.options)

    backup_diffs = None
    backup_log_file = None
    if args.backup_dir:
        run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_root = Path(args.backup_dir).resolve() / run_timestamp
        backup_diffs = backup_root / "diffs"
        backup_log_file = backup_root / "created.list"
        if not args.dry_run:
            backup_diffs.mkdir(parents=True, exist_ok=True)
        print(f"[INFO] Reverse-incremental backup enabled. Diffs and logs in: {backup_root}")

    try:
        src_ok = src_base.is_dir()
    except PermissionError:
        src_ok = False
    if not src_ok:
        print(f"Critical error: Source {src_base} does not exist or is not a directory.")
        sys.exit(1)

    if not args.dry_run:
        try:
            dst_base.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            print(f"Critical error: Cannot create or write to destination {dst_base}.")
            sys.exit(1)

    start_time = time.time()
    if args.dry_run:
        print("\n!!! SCRIPT RUNNING IN DRY-RUN MODE - NO DATA WILL BE MODIFIED !!!\n")

    print("[INFO] Phase 1: Scanning source and starting Rsync workers...")
    
    task_queue = queue.PriorityQueue(maxsize=1500000)
    src_dirs_set = set()
    src_dirs_lock = threading.Lock()
    results = []
    results_lock = threading.Lock()

    t_src_walker = threading.Thread(
        target=walk_and_produce,
        args=(src_base, task_queue, src_dirs_set, src_dirs_lock, args.workers, args.debug)
    )
    t_src_walker.start()

    worker_threads = []
    for i in range(args.workers):
        t = threading.Thread(
            target=sync_worker,
            args=(i, task_queue, src_base, dst_base, rsync_options,
                  results, results_lock, args.debug, args.dry_run, backup_diffs)
        )
        t.start()
        worker_threads.append(t)

    t_src_walker.join()
    if not args.debug:
        print(f"[INFO] Source scan completed. Found {len(src_dirs_set)} directories. `src_dirs_set` is locked.")

    print("[INFO] Phase 2: Concurrent scan and cleanup of orphaned directories...")
    cleanup_queue = queue.Queue()
    deleted_log = []
    log_lock = threading.Lock()

    for rel_dir in src_dirs_set:
        cleanup_queue.put(rel_dir)
    for _ in range(args.workers):
        cleanup_queue.put(None)

    cleanup_threads = []
    for i in range(args.workers):
        t = threading.Thread(
            target=cleanup_worker,
            args=(i, cleanup_queue, dst_base, src_dirs_set, deleted_log, log_lock, args.dry_run, args.debug, backup_diffs)
        )
        t.start()
        cleanup_threads.append(t)

    for t in worker_threads:
        t.join()
        
    for t in cleanup_threads:
        t.join()

    # --- Zpracování výsledků ---
    success_count = error_count = 0
    tot_files = tot_created_total = tot_created_reg = tot_deleted = tot_transferred = tot_bytes = 0
    changed_directories_log = []
    simulated_count = 0
    all_created_items = []

    for rel_dir, code, stats, err, created_items in results:
        if code == 0:
            success_count += 1
            tot_files += stats.get('files', 0)
            tot_created_total += stats.get('created_total', 0)
            tot_created_reg += stats.get('created_reg', 0)
            tot_deleted += stats.get('deleted', 0)
            tot_transferred += stats.get('transferred', 0)
            tot_bytes += stats.get('bytes', 0)
            all_created_items.extend(created_items)

            is_simulated = "Simulated" in err
            if is_simulated:
                simulated_count += 1

            if stats.get('transferred', 0) > 0 or stats.get('deleted', 0) > 0 or stats.get('created_total', 0) > 0:
                tag = "MODIFIED (simulated)" if is_simulated else "MODIFIED"
                changed_directories_log.append(f"{tag}: {rel_dir}")
        else:
            error_count += 1
            print(f"[ERROR] Directory '{rel_dir}': {err.strip()}")

    tot_updated = max(0, tot_transferred - tot_created_reg)

    deleted_dirs_count = len(deleted_log)
    for deleted_rel in deleted_log:
        changed_directories_log.append(f"DELETED (tree): {deleted_rel}")

    if backup_log_file and not args.dry_run and all_created_items:
        try:
            with open(backup_log_file, 'w', encoding='utf-8') as f:
                for item in sorted(all_created_items):
                    f.write(f"{item}\n")
            print(f"\n[INFO] Wrote {len(all_created_items)} created items to {backup_log_file}")
        except IOError as e:
            print(f"\n[ERROR] Failed to write created log '{backup_log_file}': {e}")

    if args.log_changed and changed_directories_log:
        try:
            with open(args.log_changed, 'a', encoding='utf-8') as log_file:
                run_type = " (DRY-RUN)" if args.dry_run else ""
                log_file.write(f"\n--- Sync run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{run_type} ---\n")
                for entry in changed_directories_log:
                    log_file.write(f"{entry}\n")
            print(f"[INFO] Log of changed directories appended to: {args.log_changed}")
        except IOError as e:
            print(f"[ERROR] Failed to write to log file '{args.log_changed}': {e}")

    elapsed = time.time() - start_time
    header = "--- SYNCHRONIZATION STATS (DRY-RUN SIMULATION) ---" if args.dry_run else "--- SYNCHRONIZATION STATS ---"

    print(f"\n" + "=" * len(header))
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

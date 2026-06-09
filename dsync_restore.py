#!/usr/bin/env python3
import os
import argparse
import shutil
from pathlib import Path
from datetime import datetime
import concurrent.futures

def has_content(p: Path) -> bool:
    if not p.exists(): return False
    if p.is_file() or p.is_symlink(): return True
    for root, dirs, files in os.walk(p):
        if files: return True
    return False

def get_backup_runs(backup_dir: Path):
    runs = []
    if not backup_dir.exists(): return runs
    for entry in backup_dir.iterdir():
        if entry.is_dir():
            try:
                dt = datetime.strptime(entry.name, '%Y%m%d_%H%M%S')
                runs.append((dt, entry))
            except ValueError: continue
    return sorted(runs, key=lambda x: x[0], reverse=True)

def show_history(target_rel_path: str, backup_dir: Path, mirror_base: Path):
    runs = get_backup_runs(backup_dir)
    target = Path(target_rel_path)
    print(f"\n--- Dostupné body obnovy pro: {target_rel_path} ---")
    if not runs:
        print("Nenalezeny žádné zálohy v zadaném adresáři.")
        return

    mirror_target = mirror_base / target
    exists_in_timeline = mirror_target.exists()
    history_events = []

    for dt, run_path in runs:
        timestamp_str = dt.strftime('%Y-%m-%d %H:%M:%S')
        restore_arg = dt.strftime('%Y%m%d_%H%M%S')
        diff_path = run_path / "diffs" / target
        created_log = run_path / "created.list"

        changed = False
        msg = "✓ Beze změn (Stabilní stav)"
        is_created = False

        if created_log.exists():
            with open(created_log, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip() == str(target):
                        is_created = True
                        break

        if is_created:
            msg = "➕ VZNIK (Položka byla nově vytvořena)"
            changed = True
            exists_in_timeline = False
        elif has_content(diff_path):
            item_type = "Adresář" if diff_path.is_dir() else "Soubor"
            if exists_in_timeline:
                msg = f"📝 MODIFIKACE ({item_type} byl upraven)"
            else:
                msg = f"❌ SMAZÁNÍ ({item_type} byl odstraněn)"
                exists_in_timeline = True
            changed = True

        history_events.append({
            'dt_str': timestamp_str, 'id': restore_arg,
            'changed': changed, 'msg': msg
        })

    for i, event in enumerate(history_events):
        if event['changed']:
            print(f"[{event['dt_str']}] [ID: {event['id']}] {event['msg']}")
        else:
            is_last = (i == len(history_events) - 1)
            next_is_change = not is_last and history_events[i+1]['changed']
            if is_last or next_is_change:
                print(f"[{event['dt_str']}] [ID: {event['id']}] {event['msg']}")
    print("-" * 60 + "\n")

def build_execution_plan(target_rel_path: str, backup_dir: Path, target_dt: datetime) -> dict:
    runs = get_backup_runs(backup_dir)
    plan = {}
    target = Path(target_rel_path)
    runs_to_apply = [(dt, p) for dt, p in runs if dt > target_dt]

    for dt, run_path in runs_to_apply:
        diff_base = run_path / "diffs"
        created_log = run_path / "created.list"

        diff_target = diff_base / target
        if diff_target.exists():
            if diff_target.is_dir():
                for root, _, files in os.walk(diff_target):
                    root_path = Path(root)
                    for f in files:
                        full_diff_path = root_path / f
                        rel_to_prod = full_diff_path.relative_to(diff_base)
                        plan[rel_to_prod] = {"action": "COPY", "src": full_diff_path}
            else:
                plan[target] = {"action": "COPY", "src": diff_target}

        if created_log.exists():
            with open(created_log, 'r', encoding='utf-8') as f:
                for line in f:
                    logged_path = Path(line.strip())
                    if logged_path == target or target in logged_path.parents:
                        plan[logged_path] = {"action": "DELETE"}
    return plan

def execute_restoration(plan: dict, prod_base: Path, mirror_base: Path, dest_base: Path, target_rel: str, workers: int):
    target_mirror = mirror_base / target_rel
    target_dest = dest_base if dest_base else prod_base
    
    deletes = set(p for p, act in plan.items() if act["action"] == "DELETE")
    copies = {p: act["src"] for p, act in plan.items() if act["action"] == "COPY"}

    def copy_worker(task):
        src, dst = task
        dst.parent.mkdir(parents=True, exist_ok=True)
        # Optimalizace pro in-place obnovu
        if dst.exists():
            try:
                s_stat = src.stat()
                d_stat = dst.stat()
                if s_stat.st_size == d_stat.st_size and int(s_stat.st_mtime) == int(d_stat.st_mtime):
                    return
            except OSError: pass
        shutil.copy2(src, dst)

    tasks = []
    if dest_base:
        print(f"[INFO] Režim: OUT-OF-PLACE obnova do {dest_base}")
    else:
        print(f"[INFO] Režim: IN-PLACE obnova přímo do produkce ({prod_base})")
        print(f"[INFO] Provádím DELETE operací: {len(deletes)}")
        for rel_path in deletes:
            del_path = prod_base / rel_path
            if del_path.exists():
                if del_path.is_dir(): shutil.rmtree(del_path, ignore_errors=True)
                else: del_path.unlink(missing_ok=True)

    # Natažení dat z hlavního zrcadla (Vše co nebylo smazáno nebo přepsáno v diffech)
    if target_mirror.exists():
        if target_mirror.is_dir():
            for root, _, files in os.walk(target_mirror):
                root_path = Path(root)
                for f in files:
                    mirror_file = root_path / f
                    rel_path = mirror_file.relative_to(mirror_base)
                    if rel_path in deletes or rel_path in copies: continue
                    tasks.append((mirror_file, target_dest / rel_path))
        else:
            rel_path = target_mirror.relative_to(mirror_base)
            if rel_path not in deletes and rel_path not in copies:
                tasks.append((target_mirror, target_dest / rel_path))

    # Aplikace reverzních záplat z diffů
    for rel_path, src_path in copies.items():
        tasks.append((src_path, target_dest / rel_path))

    print(f"[INFO] Připravuji zdrojová data z MIRROR: {mirror_base}")
    print(f"[INFO] Provádím COPY operací: {len(tasks)} (ve {workers} vláknech)")
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        executor.map(copy_worker, tasks)

def main():
    parser = argparse.ArgumentParser(description="Time Machine Restore for dsync")
    parser.add_argument("--prod", required=True, help="Production directory (e.g., /mnt/data/tmp/)")
    parser.add_argument("--mirror", required=True, help="Main backup mirror directory (rsync destination, e.g., tmp2)")
    parser.add_argument("--backup-dir", required=True, help="Root directory for reverse-incremental backups (containing timestamps)")
    parser.add_argument("--target", required=True, help="Relative path to the target item (file or directory) to process")
    parser.add_argument("--dest", type=str, help="OPTIONAL: Alternative destination directory for out-of-place restore")
    parser.add_argument("--history", action="store_true", help="Only print the available point-in-time recovery timeline for the target and exit")
    parser.add_argument("--restore-to", type=str, help="Target timestamp for recovery in YYYYMMDD_HHMMSS format, or 'now'")
    parser.add_argument("-w", "--workers", type=int, default=8, help="Number of concurrent worker threads for copy operations")
    args = parser.parse_args()
    prod_base = Path(args.prod).resolve()
    mirror_base = Path(args.mirror).resolve()
    backup_base = Path(args.backup_dir).resolve()
    dest_base = Path(args.dest).resolve() if args.dest else None
    
    target_rel = args.target.lstrip('/')

    if args.history:
        show_history(target_rel, backup_base, mirror_base)
        return

    if not args.restore_to:
        print("[CHYBA] Pro obnovu musíte zadat parametr --restore-to (YYYYMMDD_HHMMSS) nebo použít --history.")
        return

    if args.restore_to.lower() == "now":
        target_dt = datetime.now()
    else:
        try: target_dt = datetime.strptime(args.restore_to, '%Y%m%d_%H%M%S')
        except ValueError:
            print("[CHYBA] Neplatný formát času. Použijte 'now' nebo YYYYMMDD_HHMMSS.")
            return

    print(f"\n[INFO] Sestavuji exekuční plán pro obnovu '{target_rel}' do stavu k {target_dt}...")
    plan = build_execution_plan(target_rel, backup_base, target_dt)

    execute_restoration(plan, prod_base, mirror_base, dest_base, target_rel, args.workers)
    print("\n[OK] Obnova úspěšně dokončena.")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Run the ignored local test repository by component or source diff.

The mapping is explicit in test_groups.toml. Unknown backend paths select the
full suite: a missing map must cost time, never silently skip protection.
"""

from __future__ import annotations

import argparse
import ast
import fnmatch
import json
import random
import re
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import cache
from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parents[1]
TESTS = ROOT / "tests"
SOURCE = ROOT / "src"
MANIFEST = tomllib.loads((ROOT / "scripts/test_groups.toml").read_text())
GROUPS = MANIFEST["groups"]
FULL_SUITE_PATHS = MANIFEST["policy"]["full_suite_paths"]


def test_files() -> list[Path]:
    return sorted(TESTS.glob("test_*.py"))


def matches(patterns: list[str], name: str) -> bool:
    return any(fnmatch.fnmatchcase(name, pattern) for pattern in patterns)


def tests_for(groups: set[str]) -> list[Path]:
    return [path for path in test_files() if any(matches(GROUPS[group]["tests"], path.name) for group in groups)]


def imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    modules = {
        alias.name for node in ast.walk(tree) if isinstance(node, ast.Import)
        for alias in node.names
    }
    modules.update(
        node.module for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    )
    return modules


def missing_alma_imports() -> list[str]:
    """Catch stale test imports before a costly pytest collection/run."""
    @cache
    def module_exists(module: str) -> bool:
        relative = Path(*module.split("."))
        package = SOURCE / relative
        return (SOURCE / relative.with_suffix(".py")).is_file() or (
            package.is_dir() and any(package.rglob("*.py"))
        )

    @cache
    def exported_names(module: str) -> set[str] | None:
        relative = SOURCE / Path(*module.split("."))
        path = relative.with_suffix(".py") if relative.with_suffix(".py").is_file() else relative / "__init__.py"
        if not path.is_file():
            return None  # Namespace package: submodules are checked as paths.
        tree = ast.parse(path.read_text(), filename=str(path))
        if any(isinstance(node, ast.FunctionDef) and node.name == "__getattr__" for node in tree.body):
            return None  # Dynamic exports cannot be checked from syntax alone.
        names = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store)}
        names.update(
            node.name for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
        )
        names.update(
            alias.asname or alias.name.split(".")[0]
            for node in ast.walk(tree) if isinstance(node, (ast.Import, ast.ImportFrom))
            for alias in node.names
        )
        return names

    misses: list[str] = []
    for path in sorted(TESTS.rglob("*.py")):
        for module in sorted(imported_modules(path)):
            if module != "alma" and not module.startswith("alma."):
                continue
            if not module_exists(module):
                misses.append(f"{path.name}: {module}")
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom) or node.level or not node.module:
                continue
            if node.module != "alma" and not node.module.startswith("alma."):
                continue
            exports = exported_names(node.module)
            if exports is None:
                continue
            parent = SOURCE / Path(*node.module.split("."))
            for alias in node.names:
                if alias.name == "*" or alias.name in exports:
                    continue
                if (parent / alias.name).is_dir() or (parent / f"{alias.name}.py").is_file():
                    continue
                misses.append(f"{path.name}: {node.module}.{alias.name}")
    return misses


def git_paths(*args: str, cwd: Path = ROOT) -> set[str]:
    result = subprocess.run(["git", *args], cwd=cwd, text=True, capture_output=True, check=True)
    return {line for line in result.stdout.splitlines() if line}


def source_branch_to_test_branch(source_branch: str) -> str:
    return "main" if source_branch == "main" else "source-" + source_branch.replace("/", "-")


def branch_name(cwd: Path) -> str:
    return subprocess.run(
        ["git", "branch", "--show-current"], cwd=cwd,
        text=True, capture_output=True, check=True,
    ).stdout.strip()


def check_branch_alignment() -> str | None:
    source_branch = branch_name(ROOT)
    test_branch = branch_name(TESTS)
    if not source_branch or not test_branch:
        return "source and local test repositories must both have a checked-out branch"
    expected = source_branch_to_test_branch(source_branch)
    if test_branch != expected:
        return f"source branch {source_branch} expects local test branch {expected}; found {test_branch}"
    return None


def changed_paths(base: str) -> set[str]:
    committed = git_paths("diff", "--name-only", f"{base}...HEAD")
    working = git_paths("diff", "--name-only", "HEAD")
    untracked = git_paths("ls-files", "--others", "--exclude-standard")
    return committed | working | untracked


def shards_for(paths: list[Path], workers: int) -> list[list[Path]]:
    """Put large test files on the least-loaded process first."""
    shards: list[list[Path]] = [[] for _ in range(min(workers, len(paths)))]
    loads = [0] * len(shards)
    for path in sorted(paths, key=lambda p: (-p.stat().st_size, p.name)):
        index = min(range(len(shards)), key=lambda i: (loads[i], i))
        shards[index].append(path)
        loads[index] += path.stat().st_size
    return shards


def run_pytest(paths: list[Path], *, dry_run: bool, workers: int, shuffle_seed: int | None) -> int:
    if not paths:
        print("No backend tests selected.")
        return 0
    print(f"Selected {len(paths)} test files across {min(workers, len(paths))} processes.", flush=True)
    if dry_run:
        print("\n".join(str(path.relative_to(ROOT)) for path in paths))
        return 0
    shards = shards_for(paths, workers)
    if shuffle_seed is not None:
        for index, files in enumerate(shards):
            random.Random(shuffle_seed + index).shuffle(files)
    log_dir = Path(tempfile.mkdtemp(prefix="alma-test-run-"))
    started = time.perf_counter()

    def run_shard(index: int, files: list[Path]) -> tuple[int, str, float]:
        command = [
            str(ROOT / ".venv/bin/python"), "-m", "pytest", "-q", "-m", "not network",
            "--durations=20", *map(str, files),
        ]
        shard_started = time.perf_counter()
        result = subprocess.run(command, cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        (log_dir / f"worker-{index + 1}.log").write_text(result.stdout)
        return result.returncode, result.stdout, time.perf_counter() - shard_started

    results: list[tuple[int, str, float] | None] = [None] * len(shards)
    with ThreadPoolExecutor(max_workers=len(shards)) as pool:
        futures = {pool.submit(run_shard, index, files): index for index, files in enumerate(shards)}
        for future in as_completed(futures):
            index = futures[future]
            results[index] = future.result()
            print(f"Worker {index + 1}/{len(shards)} finished ({len(shards[index])} files).", flush=True)
    failed = False
    for index, result in enumerate(results, 1):
        assert result is not None
        code, output, elapsed = result
        lines = output.splitlines()
        if code:
            failed = True
            print(f"\nWorker {index}/{len(shards)} FAILED ({len(shards[index - 1])} files, {elapsed:.1f}s):")
            print("\n".join(lines[-50:]))
            print(f"Full output: {log_dir / f'worker-{index}.log'}")
        else:
            summary = next(
                (line for line in reversed(lines) if re.search(r"\b\d+ passed\b.*\bin \d", line)),
                lines[-1] if lines else "no pytest output",
            )
            print(f"Worker {index}/{len(shards)} ({len(shards[index - 1])} files, {elapsed:.1f}s): {summary}")
    print(f"Total wall time: {time.perf_counter() - started:.1f}s; logs: {log_dir}")
    return int(failed)


def run_frontend(*, dry_run: bool) -> int:
    """The frontend suite is short enough to keep whole, including source guards."""
    commands = [["npm", "run", "typecheck"], ["npm", "run", "test"]]
    for command in commands:
        print(f"Frontend: {' '.join(command)}", flush=True)
        if not dry_run and subprocess.call(command, cwd=ROOT / "frontend"):
            return 1
    return 0


def print_graph(fmt: str) -> None:
    graph = {
        name: {
            "sources": group["sources"],
            "tests": [p.name for p in tests_for({name})],
        }
        for name, group in GROUPS.items()
    }
    if fmt == "json":
        direct_imports = {
            path.name: sorted(
                module for module in imported_modules(path)
                if module == "alma" or module.startswith(("alma.", "tests."))
            )
            for path in test_files()
        }
        print(json.dumps({"full_suite_paths": FULL_SUITE_PATHS, "groups": graph, "direct_imports": direct_imports}, indent=2))
        return
    print("digraph alma_tests {")
    print('  rankdir=LR; node [fontname="sans-serif"];')
    print('  "full suite" [shape=box, style=filled, fillcolor="#ffe6c8"];')
    for pattern in FULL_SUITE_PATHS:
        print(f"  {json.dumps('source:' + pattern)} -> \"full suite\";")
    for name, group in graph.items():
        print(f'  "group:{name}" [shape=box, style=filled, fillcolor="#e5edf5", label="{name}"];')
        for pattern in group["sources"]:
            print(f"  {json.dumps('source:' + pattern)} -> {json.dumps('group:' + name)};")
        for filename in group["tests"]:
            print(f"  {json.dumps('group:' + name)} -> {json.dumps('test:' + filename)};")
    print("}")


def check_layout() -> bool:
    unmatched = [p.name for p in test_files() if not any(matches(g["tests"], p.name) for g in GROUPS.values())]
    stale_imports = missing_alma_imports()
    for name in unmatched:
        print(f"UNMAPPED {name}")
    for name in stale_imports:
        print(f"MISSING IMPORT {name}")
    print(f"{len(test_files()) - len(unmatched)}/{len(test_files())} test files mapped")
    return bool(unmatched or stale_imports)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("list", help="show component groups")
    sub.add_parser("check", help="fail on ungrouped files or stale alma imports")
    graph_parser = sub.add_parser("graph", help="print the source -> group -> test dependency graph")
    graph_parser.add_argument("--format", choices=("json", "dot"), default="json")
    group_parser = sub.add_parser("group", help="run one or more component groups")
    group_parser.add_argument("groups", nargs="+", choices=sorted(GROUPS))
    group_parser.add_argument("--dry-run", action="store_true")
    group_parser.add_argument("--workers", type=int, default=4)
    group_parser.add_argument("--shuffle-seed", type=int, help="shuffle file order within each worker")
    changed_parser = sub.add_parser("changed", help="run groups affected by source changes")
    changed_parser.add_argument("--base", default="main", help="Git base branch or ref (default: main)")
    changed_parser.add_argument("--dry-run", action="store_true")
    changed_parser.add_argument("--workers", type=int, default=4)
    changed_parser.add_argument("--shuffle-seed", type=int, help="shuffle file order within each worker")
    full_parser = sub.add_parser("full", help="run the complete non-network suite")
    full_parser.add_argument("--dry-run", action="store_true")
    full_parser.add_argument("--workers", type=int, default=4)
    full_parser.add_argument("--shuffle-seed", type=int, help="shuffle file order within each worker")
    link_parser = sub.add_parser("link", help="create an independent local-test worktree for another source worktree")
    link_parser.add_argument("worktree", type=Path)
    args = parser.parse_args()

    if args.command == "list":
        for name in GROUPS:
            print(f"{name:12} {len(tests_for({name})):3} files")
        return 0
    if args.command == "graph":
        print_graph(args.format)
        return 0
    if args.command == "link":
        target = args.worktree.resolve() / "tests"
        if target.exists() or target.is_symlink():
            parser.error(f"{target} already exists; inspect it before creating a test worktree")
        if not (target.parent / "pyproject.toml").exists():
            parser.error(f"{target.parent} does not look like an ALMa worktree")
        source_branch = branch_name(target.parent)
        if not source_branch:
            parser.error("the source worktree needs a branch before it can own a test branch")
        test_branch = source_branch_to_test_branch(source_branch)
        exists = subprocess.run(
            ["git", "show-ref", "--verify", "--quiet", f"refs/heads/{test_branch}"],
            cwd=TESTS,
        ).returncode == 0
        command = ["git", "worktree", "add"]
        if not exists:
            command += ["-b", test_branch]
        command += [str(target)]
        if exists:
            command += [test_branch]
        else:
            command += ["main"]
        subprocess.run(command, cwd=TESTS, check=True)
        print(f"Local test branch {test_branch} checked out at {target}")
        return 0
    if not test_files():
        parser.error("tests/ is empty; restore or link the local test repository")
    alignment_error = check_branch_alignment()
    if alignment_error:
        parser.error(alignment_error)
    if args.command in {"group", "changed", "full"} and args.workers < 1:
        parser.error("--workers must be at least 1")
    if args.command == "check":
        return int(check_layout())
    if check_layout():
        return 1
    if args.command == "group":
        return run_pytest(tests_for(set(args.groups)), dry_run=args.dry_run, workers=args.workers, shuffle_seed=args.shuffle_seed)
    if args.command == "full":
        return run_pytest(test_files(), dry_run=args.dry_run, workers=args.workers, shuffle_seed=args.shuffle_seed)

    changed = changed_paths(args.base)
    frontend_changed = any(p.startswith("frontend/") for p in changed)
    backend = {p for p in changed if p.startswith("src/alma/") or p.startswith("tests/")}
    backend.update(p for p in changed if matches(FULL_SUITE_PATHS, p))
    if (TESTS / ".git").exists():
        backend.update(f"tests/{p}" for p in git_paths("diff", "--name-only", "main...HEAD", cwd=TESTS))
        backend.update(f"tests/{p}" for p in git_paths("diff", "--name-only", "HEAD", cwd=TESTS))
        backend.update(f"tests/{p}" for p in git_paths("ls-files", "--others", "--exclude-standard", cwd=TESTS))
    direct = {TESTS / p.removeprefix("tests/") for p in backend if p.startswith("tests/") and p.endswith(".py")}
    if frontend_changed and run_frontend(dry_run=args.dry_run):
        return 1
    unknown = {p for p in backend if not p.startswith("tests/") and not any(matches(g["sources"], p) for g in GROUPS.values())}
    shared_tests = {p for p in backend if p.startswith("tests/") and not p.removeprefix("tests/").startswith("test_")}
    full_suite = unknown | shared_tests | {p for p in backend if matches(FULL_SUITE_PATHS, p)}
    if full_suite:
        print("Shared or unmapped changes select the full suite:")
        print("\n".join(sorted(full_suite)))
        return run_pytest(test_files(), dry_run=args.dry_run, workers=args.workers, shuffle_seed=args.shuffle_seed)
    selected_groups = {name for name, group in GROUPS.items() if any(matches(group["sources"], p) for p in backend)}
    selected = sorted(set(tests_for(selected_groups)) | {p for p in direct if p.exists()})
    print("Groups:", ", ".join(sorted(selected_groups)) or "none")
    return run_pytest(selected, dry_run=args.dry_run, workers=args.workers, shuffle_seed=args.shuffle_seed)


if __name__ == "__main__":
    sys.exit(main())

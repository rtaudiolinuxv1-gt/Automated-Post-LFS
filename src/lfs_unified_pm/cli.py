from __future__ import annotations

import argparse
import json
import os
import sys

from .app import PackageManagerApp
from .build import BuildExecutor
from .build_scripts import BuildScriptExporter
from .git_source import DEFAULT_T2_GIT_URL
from .tui import run_tui


def build_parser():
    parser = argparse.ArgumentParser(prog="lfs-pm")
    parser.add_argument("--root", default="./filesystem_mountpoint", help="target filesystem root")
    subparsers = parser.add_subparsers(dest="command", required=True)

    sync = subparsers.add_parser("sync", help="import package metadata")
    sync.add_argument("--base-catalog", default="")
    sync.add_argument("--base-override", action="append", default=[])
    sync.add_argument("--blfs-xml", action="append", default=[])
    sync.add_argument("--t2-tree", action="append", default=[])
    sync.add_argument("--arch-json", action="append", default=[])
    sync.add_argument("--arch-repos", default="")
    sync.add_argument("--custom", action="append", default=[])

    sync_t2 = subparsers.add_parser("sync-t2-git", help="refresh and import T2 packages from git")
    sync_t2.add_argument("--t2-git-dir", default="./t2sde")
    sync_t2.add_argument("--t2-git-url", default="")
    sync_t2.add_argument("--t2-git-branch", default="")

    subparsers.add_parser("scan-root", help="scan target filesystem")

    search = subparsers.add_parser("search", help="search packages")
    search.add_argument("query")

    info = subparsers.add_parser("info", help="show package details")
    info.add_argument("name")

    deps = subparsers.add_parser("deps", help="show dependency plan")
    deps.add_argument("name")
    deps.add_argument("--include-recommends", action="store_true")
    deps.add_argument("--auto-optional", action="store_true")

    rdeps = subparsers.add_parser("rdeps", help="show reverse dependencies")
    rdeps.add_argument("name")

    build = subparsers.add_parser("build", help="build and optionally install packages")
    build.add_argument("packages", nargs="+")
    build.add_argument("--include-recommends", action="store_true")
    build.add_argument("--auto-optional", action="store_true")
    build.add_argument("--package-format", default="")
    build.add_argument("--build-process", choices=["python", "scripts"], default="python")
    build.add_argument("--build-mode", choices=["native", "chroot"], default="native")
    build.add_argument("--chroot-root", default="")
    build.add_argument("--no-install", action="store_true")
    build.add_argument("--allow-la-removal", action="store_true")

    subparsers.add_parser("list-installed", help="list installed packages")

    history = subparsers.add_parser("history", help="show recent transactions")
    history.add_argument("--limit", type=int, default=20)

    verify = subparsers.add_parser("verify", help="verify installed files exist")

    subparsers.add_parser("tui", help="run the curses package browser")
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    app = PackageManagerApp(args.root)
    try:
        return _dispatch(app, args)
    finally:
        app.close()


def _dispatch(app, args):
    if args.command == "sync":
        imported, report = app.sync_with_report(
            base_catalog=args.base_catalog,
            base_override=args.base_override,
            blfs_xml=args.blfs_xml,
            t2_tree=args.t2_tree,
            arch_json=args.arch_json,
            arch_repos=args.arch_repos,
            custom=args.custom,
        )
        by_source = {}
        for package in imported:
            by_source[package.source_origin] = by_source.get(package.source_origin, 0) + 1
        detail = ", ".join("%s=%d" % (key, by_source[key]) for key in sorted(by_source))
        print("Imported %d package records" % len(imported))
        if detail:
            print(detail)
        _print_change_report(report)
        return 0

    if args.command == "sync-t2-git":
        imported, report = app.sync_t2_git(
            repo_dir=args.t2_git_dir,
            repo_url=args.t2_git_url,
            branch=args.t2_git_branch,
        )
        git_report = report.get("git", {})
        print("Imported %d T2 package records" % len([item for item in imported if item.source_origin == "t2"]))
        print(
            "T2 git repo: %s (%s -> %s)"
            % (
                git_report.get("repo_url", "") or DEFAULT_T2_GIT_URL,
                git_report.get("previous_head", "") or "new-clone",
                git_report.get("current_head", ""),
            )
        )
        changed_packages = git_report.get("changed_packages", [])
        if changed_packages:
            print("Git changed package dirs: %d" % len(changed_packages))
        if git_report.get("warning"):
            print("Git warning: %s" % git_report["warning"])
        _print_change_report({"t2": report.get("t2", {}), "removed": report.get("removed", {})})
        return 0

    if args.command == "scan-root":
        report = app.scan_root()
        print(json.dumps(report.__dict__, indent=2, sort_keys=True))
        return 0

    if args.command == "search":
        for package in app.catalog().search(args.query):
            print("%-24s %-10s %-10s %s" % (package.name, package.version, package.source_origin, package.summary))
        return 0

    if args.command == "info":
        package = app.catalog().resolve(args.name)
        if not package:
            print("Package not found: %s" % args.name, file=sys.stderr)
            return 1
        print(json.dumps(package.__dict__, indent=2, sort_keys=True))
        return 0

    if args.command == "deps":
        return _print_plan(
            app.plan(
                [args.name],
                include_recommends=args.include_recommends,
                auto_optional=args.auto_optional,
            )
        )

    if args.command == "rdeps":
        for package in app.catalog().reverse_dependencies(args.name):
            print("%-24s %-10s %-10s" % (package.name, package.version, package.source_origin))
        return 0

    if args.command == "build":
        plan = app.plan(
            args.packages,
            include_recommends=args.include_recommends,
            auto_optional=args.auto_optional,
        )
        status = _print_plan(plan)
        if status != 0:
            return status
        if args.build_process == "scripts":
            exporter = BuildScriptExporter(app.config, app.store)
            base_dir, master_path, scripts = exporter.export_queue(
                [step.package for step in plan.ordered_steps],
                package_format=args.package_format or "none",
                install_after_build=not args.no_install,
                update_tracking=True,
                build_mode=args.build_mode,
                chroot_root=args.chroot_root,
            )
            print("Exported %d package script(s) to %s" % (len(scripts), base_dir))
            print("Master script: %s" % master_path)
            return 0
        executor = BuildExecutor(app.config, app.store)
        artifacts = executor.execute_plan(
            plan,
            build_mode=args.build_mode,
            package_format=args.package_format,
            install=not args.no_install,
            allow_la_removal=args.allow_la_removal,
            chroot_root=args.chroot_root,
        )
        for name, artifact in artifacts:
            line = name
            if artifact:
                line += " -> " + artifact
            print(line)
        return 0

    if args.command == "list-installed":
        for item in app.store.list_installed():
            provider = item.metadata.get("build_provider", "")
            suffix = " via %s" % provider if provider else ""
            print("%-24s %-10s %-10s %s%s" % (item.name, item.version, item.source_origin, item.install_reason, suffix))
        return 0

    if args.command == "history":
        for row in app.store.history(limit=args.limit):
            print(
                "%s %-12s %-24s %-10s %-8s %s"
                % (
                    row["created_at"],
                    row["action"],
                    row["package_name"],
                    row["version"],
                    row["status"],
                    _format_history_detail(row["detail"]),
                )
            )
        return 0

    if args.command == "verify":
        missing = []
        for item in app.store.list_installed():
            for path in item.files:
                full = os.path.join(app.config.root, path.lstrip("/"))
                if not os.path.exists(full):
                    missing.append((item.name, path))
        for name, path in missing:
            print("%s missing %s" % (name, path))
        return 1 if missing else 0

    if args.command == "tui":
        return run_tui(app)

    parser = build_parser()
    parser.print_help()
    return 1


def _print_plan(plan):
    if plan.unresolved:
        print("Unresolved dependencies: %s" % ", ".join(plan.unresolved), file=sys.stderr)
        return 1
    if plan.conflicts:
        for conflict in plan.conflicts:
            print("Conflict: %s" % conflict, file=sys.stderr)
        return 1
    for step in plan.ordered_steps:
        package = step.package
        print("%s %s [%s]" % (package.name, package.version, package.source_origin))
        if step.missing_recommends:
            print("  missing recommended: %s" % ", ".join(step.missing_recommends))
        if step.missing_optional:
            print("  missing optional: %s" % ", ".join(step.missing_optional))
    return 0


def _print_change_report(report):
    removed = report.get("removed", {})
    for source in sorted(key for key in report.keys() if key != "removed" and key != "git"):
        details = report[source]
        print(
            "%s: added=%d version_changed=%d recipe_changed=%d unchanged=%d"
            % (
                source,
                len(details.get("added", [])),
                len(details.get("version_changed", [])),
                len(details.get("recipe_changed", [])),
                len(details.get("unchanged", [])),
            )
        )
    for source in sorted(removed):
        if removed[source]:
            print("%s: removed=%d" % (source, len(removed[source])))


def _format_history_detail(detail):
    if not detail:
        return ""
    try:
        payload = json.loads(detail)
    except ValueError:
        return detail
    parts = []
    if payload.get("build_provider"):
        parts.append("provider=%s[%s]" % (payload["build_provider"], payload.get("provider_source", "")))
    if payload.get("artifact"):
        parts.append("artifact=%s" % payload["artifact"])
    if payload.get("provider_members"):
        parts.append("members=%s" % ",".join(payload["provider_members"][:6]))
    if payload.get("handling"):
        parts.append("handling=%s" % payload["handling"])
    return " ".join(parts) if parts else detail


if __name__ == "__main__":
    raise SystemExit(main())

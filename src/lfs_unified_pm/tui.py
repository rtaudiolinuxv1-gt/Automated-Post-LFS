from __future__ import annotations

import curses
import curses.textpad
import json
import time
from datetime import datetime

from .build import BuildExecutor
from .build_scripts import BuildScriptExporter
from .lfs_base import _resolve_lfs_build_root
from .models import BuildPlan, BuildStep


SOURCE_LABELS = [
    ("base", "LFS base"),
    ("blfs", "BLFS"),
    ("t2", "T2"),
    ("arch", "Arch"),
    ("custom", "Custom"),
]

PHASE_ORDER = ["prepare", "configure", "build", "install"]
REVIEW_MODES = ("off", "manual", "timed")
PACKAGE_FORMATS = ("none", "slackware", "tar", "tar.gz", "tar.bz2", "tar.xz")
BUILD_PROCESSES = ("python", "scripts")
BUILD_MODES = ("native", "chroot")
DEPENDENCY_LEVELS = ("required", "recommended", "optional")
DEPENDENCY_SOURCE_LABELS = [
    ("lfs-base", "LFS Base"),
    ("blfs", "BLFS"),
    ("t2", "T2"),
    ("arch", "Arch"),
    ("custom", "Custom"),
]
COLOR_BASE = 1
COLOR_SELECTED = 2
COLOR_ACCENT = 3
COLOR_PANEL = 4


def run_tui(app):
    return curses.wrapper(_main, app)


def _main(screen, app):
    _init_colors(screen)
    curses.curs_set(0)
    screen.keypad(True)
    state = {
        "message": "",
        "queue": [],
        "catalog": None,
        "categories": [],
        "all_packages": [],
        "package_by_key": {},
        "category_packages_cache": {},
        "dashboard_index": 0,
        "lfs_base_plan": None,
    }
    _refresh_catalog(app, state)
    _maybe_prompt_for_sync(screen, app, state)
    while True:
        action = _dashboard(screen, app, state)
        if action == "quit":
            return 0


def _refresh_catalog(app, state):
    catalog = app.catalog()
    state["catalog"] = catalog
    state["all_packages"] = catalog.all()
    state["package_by_key"] = {
        (package.name, package.source_origin): package
        for package in state["all_packages"]
    }
    state["categories"] = catalog.categories()
    state["category_packages_cache"] = {}


def _dashboard(screen, app, state):
    options = [
        "Global Build Settings",
        "System State",
        "Profile.d Settings",
        "Sync Databases",
        "Build LFS Base",
        "Choose Packages",
        "Installed / History",
        "Build Queue",
        "Export Build Scripts",
        "Browse Catalog",
        "Quit",
    ]
    while True:
        settings = app.get_settings()
        _, _, start_y, start_x, content_height, content_width = _draw_layout(screen, state, "Queue Plan")
        last_sync = app.get_last_sync_time()
        sync_label = _format_sync_time(last_sync) if last_sync else "never"
        build = settings["build"]
        system_state = settings["system_state"]
        lines = [
            "Configuration",
            "last sync: %s" % sync_label,
            "prefix: %s   jobs: %s   review: %s" % (
                build["prefix"],
                build["jobs"],
                build["command_review_mode"],
            ),
            "process: %s   mode: %s   chroot: %s" % (
                build.get("build_process", "python"),
                build.get("build_mode", "native"),
                build.get("chroot_root") or app.config.root,
            ),
            "assume LFS base: %s   jhalfs tracking: %s" % (
                "yes" if system_state.get("assume_lfs_base_installed", False) else "no",
                "yes" if system_state.get("use_jhalfs_tracking", False) else "no",
            ),
            "queue: %d package(s)   catalog: %d package(s)   categories: %d" % (
                len(state["queue"]),
                len(state["all_packages"]),
                len(state["categories"]),
            ),
            state["message"] or "Enter select  q quit",
        ]
        for row, line in enumerate(lines):
            screen.addnstr(start_y + row, start_x, line, content_width - 1, _attr("title") if row == 0 else _attr("normal"))
        option_start = start_y + 7
        for row, option in enumerate(options, start=option_start):
            current = row - option_start
            screen.addnstr(
                row,
                start_x,
                option,
                content_width - 1,
                _attr("selected") if current == state["dashboard_index"] else _attr("normal"),
            )
        key, repeat = _read_key(screen)
        if key in (ord("q"), 27):
            return "quit"
        if key in (curses.KEY_DOWN, ord("j")) and state["dashboard_index"] < len(options) - 1:
            state["dashboard_index"] = min(len(options) - 1, state["dashboard_index"] + repeat)
        elif key in (curses.KEY_UP, ord("k")) and state["dashboard_index"] > 0:
            state["dashboard_index"] = max(0, state["dashboard_index"] - repeat)
        elif key in (10, 13):
            index = state["dashboard_index"]
            if index == 0:
                state["message"] = _edit_build_settings(screen, app)
            elif index == 1:
                state["message"] = _edit_system_state(screen, app)
            elif index == 2:
                state["message"] = _edit_profile_settings(screen, app)
            elif index == 3:
                state["message"] = _sync_menu(screen, app)
                _refresh_catalog(app, state)
            elif index == 4:
                state["message"] = _lfs_base_menu(screen, app, state)
            elif index == 5:
                state["message"] = _browse_categories(screen, app, state)
            elif index == 6:
                state["message"] = _installed_history(screen, app, state)
            elif index == 7:
                state["message"] = _build_queue(screen, app, state)
            elif index == 8:
                state["message"] = _save_queue_scripts(screen, app, state)
            elif index == 9:
                state["message"] = _browse_catalog(screen, app, state)
            elif index == 10:
                return "quit"


def _maybe_prompt_for_sync(screen, app, state):
    needs_sync, reason = app.needs_sync_prompt()
    if not needs_sync:
        return
    if _yes_no(screen, "%s Sync now?" % reason):
        state["message"] = _sync_menu(screen, app)
        _refresh_catalog(app, state)
    else:
        state["message"] = reason


def _sync_menu(screen, app):
    settings = app.get_settings()
    sync = dict(settings["sync"])
    selected = set(sync.get("default_sources", ["base", "blfs", "t2"]))
    rows = [
        ("prompt_if_stale", "Prompt If Stale", "bool"),
        ("stale_days", "Stale Days", "int"),
        ("auto_fetch_missing", "Auto Fetch Missing Trees", "bool"),
        ("blfs_git_url", "BLFS Git URL", "str"),
        ("jhalfs_git_url", "jhalfs Git URL", "str"),
        ("t2_git_url", "T2 Git URL", "str"),
    ] + [("source:%s" % key, "Sync %s" % label, "bool") for key, label in SOURCE_LABELS] + [
        ("run_sync", "Run Sync Now", "action"),
    ]
    index = 0
    message = ""
    while True:
        _, _, start_y, start_x, content_height, content_width = _draw_layout(screen, {"queue": []}, "Queue Plan")
        syncs = {item["source"]: item for item in app.list_source_syncs()}
        screen.addnstr(start_y + 0, start_x, "Sync Databases", content_width - 1, _attr("title"))
        screen.addnstr(start_y + 1, start_x, "Enter edit/toggle  s save defaults  q back", content_width - 1, _attr("normal"))
        screen.addnstr(start_y + 2, start_x, message or "Select databases and sync policy", content_width - 1, _attr("accent" if message else "normal"))
        first_row = start_y + 4
        visible_count = max(1, (start_y + content_height) - first_row)
        start = max(0, index - visible_count + 1)
        visible = rows[start : start + visible_count]
        for row_no, (field, label, kind) in enumerate(visible, start=first_row):
            current = start + row_no - first_row
            if field.startswith("source:"):
                source_key = field.split(":", 1)[1]
                synced = syncs.get(_source_name(source_key), {}).get("synced_at", "")
                value = "yes" if source_key in selected else "no"
                line = "%-24s %-4s last=%s" % (label[:24], value, _format_sync_time(synced) if synced else "never")
            elif kind == "action":
                line = label
            else:
                line = "%-24s %s" % (label[:24], _format_setting_value(sync.get(field), kind))
            screen.addnstr(row_no, start_x, line, content_width - 1, _attr("selected") if current == index else _attr("normal"))
        key, repeat = _read_key(screen)
        if key in (ord("q"), 27):
            return "Sync settings closed"
        if key in (curses.KEY_DOWN, ord("j")) and index < len(rows) - 1:
            index = min(len(rows) - 1, index + repeat)
        elif key in (curses.KEY_UP, ord("k")) and index > 0:
            index = max(0, index - repeat)
        elif key == ord("s"):
            sync["default_sources"] = list(selected)
            app.update_settings({"sync": sync})
            return "Saved sync settings"
        elif key in (10, 13, ord(" ")):
            field, label, kind = rows[index]
            if field.startswith("source:"):
                source_key = field.split(":", 1)[1]
                if source_key in selected:
                    selected.remove(source_key)
                else:
                    selected.add(source_key)
                message = "Updated %s" % label
            elif kind == "bool":
                sync[field] = not bool(sync.get(field))
                message = "%s set to %s" % (label, "yes" if sync[field] else "no")
            elif kind == "int":
                sync[field] = int(_prompt(screen, label, str(sync.get(field, 0))) or sync.get(field, 0))
                message = "Updated %s" % label
            elif kind == "str":
                current = str(sync.get(field, "") or "")
                value = _prompt(screen, label, current)
                if value is not None:
                    sync[field] = value.strip()
                    message = "Updated %s" % label
            elif kind == "action":
                if not selected:
                    message = "No sources selected"
                else:
                    try:
                        sync["default_sources"] = list(selected)
                        app.update_settings({"sync": sync})
                        _, report = _run_sync_with_progress(screen, app, selected)
                        return _sync_summary(report, selected)
                    except Exception as error:
                        message = "Sync failed: %s" % error


def _lfs_base_menu(screen, app, state):
    options = [
        "LFS Base Settings",
        "Generate Chapter Plan",
        "Fetch LFS Sources",
        "Dry Run LFS Base",
        "Run LFS Base",
        "LFS Build Status",
        "View Current Log",
        "Export LFS Base Scripts",
        "View Current Plan",
        "Reset LFS Build State",
        "Back",
    ]
    index = 0
    message = ""
    while True:
        _, _, start_y, start_x, content_height, content_width = _draw_layout(screen, state, "Queue Plan")
        settings = app.get_settings()["lfs_base"]
        plan = state.get("lfs_base_plan")
        build_root = _resolve_lfs_build_root(app.config, settings)
        lines = [
            "LFS Base Builder",
            "init: %s  book: %s  commit: %s" % (
                settings.get("init_system", "systemd"),
                settings.get("book_source", "git"),
                settings.get("book_commit", "13.0"),
            ),
            "build root: %s" % build_root,
            "target triplet: %s" % _format_lfs_target_triplet(settings),
            "current plan: %s" % ("%d step(s)" % len(plan.steps) if plan else "not generated"),
            message or "Enter select  q back",
        ]
        for row, line in enumerate(lines):
            screen.addnstr(start_y + row, start_x, line, content_width - 1, _attr("title") if row == 0 else _attr("normal"))
        option_start = start_y + 7
        for row, option in enumerate(options, start=option_start):
            current = row - option_start
            screen.addnstr(
                row,
                start_x,
                option,
                content_width - 1,
                _attr("selected") if current == index else _attr("normal"),
            )
        key, repeat = _read_key(screen)
        if key in (ord("q"), 27):
            return "Closed LFS base builder"
        if key in (curses.KEY_DOWN, ord("j")) and index < len(options) - 1:
            index = min(len(options) - 1, index + repeat)
        elif key in (curses.KEY_UP, ord("k")) and index > 0:
            index = max(0, index - repeat)
        elif key in (10, 13):
            if index == 0:
                message = _edit_lfs_base_settings(screen, app)
            elif index == 1:
                try:
                    plan = _run_lfs_base_plan(screen, app)
                    state["lfs_base_plan"] = plan
                    message = "Generated LFS base plan with %d step(s)" % len(plan.steps)
                except Exception as error:
                    message = "LFS plan failed: %s" % error
            elif index == 2:
                try:
                    plan = state.get("lfs_base_plan") or _run_lfs_base_plan(screen, app)
                    state["lfs_base_plan"] = plan
                    target_dir = _run_lfs_base_fetch(screen, app, plan)
                    message = "Fetched %d LFS source file(s) into %s" % (len(plan.source_entries), target_dir)
                except Exception as error:
                    message = "LFS source fetch failed: %s" % error
            elif index == 3:
                try:
                    plan = state.get("lfs_base_plan") or _run_lfs_base_plan(screen, app)
                    state["lfs_base_plan"] = plan
                    previewed = _run_lfs_base_execute(screen, app, plan, dry_run=True)
                    message = "Previewed %d LFS step(s)" % len(previewed)
                except Exception as error:
                    message = "LFS dry run failed: %s" % error
            elif index == 4:
                try:
                    plan = state.get("lfs_base_plan") or _run_lfs_base_plan(screen, app)
                    state["lfs_base_plan"] = plan
                    executed = _run_lfs_base_execute(screen, app, plan)
                    message = "Executed %d LFS step(s)" % len(executed)
                except Exception as error:
                    message = "LFS execution failed: %s" % error
            elif index == 5:
                message = _view_lfs_base_status(screen, app)
            elif index == 6:
                message = _view_lfs_base_log(screen, app)
            elif index == 7:
                try:
                    plan = state.get("lfs_base_plan") or _run_lfs_base_plan(screen, app)
                    state["lfs_base_plan"] = plan
                    output_dir = app.export_lfs_base_scripts(plan=plan)
                    message = "Exported LFS base scripts to %s" % output_dir
                except Exception as error:
                    message = "LFS export failed: %s" % error
            elif index == 8:
                plan = state.get("lfs_base_plan")
                if not plan:
                    message = "No LFS base plan generated yet"
                else:
                    message = _view_lfs_base_plan(screen, plan)
            elif index == 9:
                app.clear_lfs_base_state()
                message = "Reset LFS base build state"
            else:
                return message or "Closed LFS base builder"


def _run_lfs_base_plan(screen, app):
    progress = {
        "source": "lfs-base",
        "phase": "start",
        "message": "Preparing LFS base plan",
        "tick": 0,
    }

    def callback(event):
        progress.update(event)
        progress["tick"] = progress.get("tick", 0) + 1
        _draw_sync_progress(screen, {"lfs-base"}, progress, title="Generating LFS Base Plan")

    _draw_sync_progress(screen, {"lfs-base"}, progress, title="Generating LFS Base Plan")
    return app.plan_lfs_base(progress_callback=callback)


def _run_lfs_base_fetch(screen, app, plan):
    progress = {
        "source": "lfs-base",
        "phase": "fetch",
        "message": "Fetching LFS sources",
        "tick": 0,
    }

    def callback(event):
        progress.update(event)
        progress["tick"] = progress.get("tick", 0) + 1
        _draw_sync_progress(screen, {"lfs-base"}, progress, title="Fetching LFS Sources")

    _draw_sync_progress(screen, {"lfs-base"}, progress, title="Fetching LFS Sources")
    return app.fetch_lfs_base_sources(plan=plan, progress_callback=callback)


def _run_lfs_base_execute(screen, app, plan, dry_run=False):
    progress = {
        "source": "lfs-base",
        "phase": "dry-run" if dry_run else "execute",
        "message": "Previewing LFS base build" if dry_run else "Executing LFS base build",
        "tick": 0,
    }

    def callback(event):
        progress.update(event)
        progress["tick"] = progress.get("tick", 0) + 1
        _draw_sync_progress(screen, {"lfs-base"}, progress, title="Dry Run LFS Base" if dry_run else "Running LFS Base Build")

    _draw_sync_progress(screen, {"lfs-base"}, progress, title="Dry Run LFS Base" if dry_run else "Running LFS Base Build")
    return app.run_lfs_base(
        plan=plan,
        progress_callback=callback,
        resume=True,
        fetch_sources=False,
        dry_run=dry_run,
        root_approval_callback=lambda payload: _approve_root_action(screen, payload),
        execution_notice_callback=lambda payload: _preview_lfs_execution(screen, payload, app.get_settings().get("lfs_base", {}).get("execution_preview_seconds", 5)),
    )


def _edit_lfs_base_settings(screen, app):
    settings = app.get_settings()
    lfs_base = dict(settings["lfs_base"])
    rows = [
        ("init_system", "Init System", "choice:systemd,sysv"),
        ("book_source", "Book Source", "choice:git,local"),
        ("book_git_url", "Book Git URL", "text"),
        ("book_commit", "Book Commit", "text"),
        ("local_book_path", "Local Book Path", "text"),
        ("build_root", "Build Root", "text"),
        ("source_archive_dir", "Source Archive Dir", "text"),
        ("luser", "Temp User", "text"),
        ("lgroup", "Temp Group", "text"),
        ("multilib", "Multilib", "choice:default,ml_32,ml_x32,ml_all"),
        ("build_method", "Build Method", "choice:chroot,boot"),
        ("package_management", "Package Management", "choice:none,build-pack,wrap-install"),
        ("testsuite", "Testsuite", "choice:none,critical,all"),
        ("jobs", "Jobs", "int"),
        ("jobs_binutils_pass1", "Binutils Pass1 Jobs", "int"),
        ("keep_build_dirs", "Keep Build Dirs", "bool"),
        ("strip_binaries", "Strip Binaries", "bool"),
        ("remove_la_files", "Remove .la Files", "bool"),
        ("target_vendor", "Target Vendor", "text"),
        ("triplet_override", "Triplet Override", "text"),
        ("log_dir", "Log Dir", "text"),
        ("execution_preview_seconds", "Preview Seconds", "int"),
        ("timezone", "Timezone", "text"),
        ("lang", "Language", "text"),
        ("hostname", "Hostname", "text"),
        ("interface", "Interface", "text"),
        ("ip_address", "IP Address", "text"),
        ("gateway", "Gateway", "text"),
        ("subnet_prefix", "Subnet Prefix", "int"),
        ("broadcast", "Broadcast", "text"),
        ("domain", "Domain", "text"),
        ("nameserver1", "Nameserver 1", "text"),
        ("nameserver2", "Nameserver 2", "text"),
        ("console_font", "Console Font", "text"),
        ("console_keymap", "Console Keymap", "text"),
        ("clock_localtime", "Clock Localtime", "bool"),
        ("log_level", "Log Level", "int"),
        ("use_custom_fstab", "Use Custom Fstab", "bool"),
        ("fstab_path", "Fstab Path", "text"),
        ("build_kernel", "Build Kernel", "bool"),
        ("kernel_config", "Kernel Config", "text"),
        ("install_ncurses5", "Install Ncurses5", "bool"),
        ("page_size", "Page Size", "choice:A4,letter"),
        ("optimization_level", "Optimization", "choice:off,final,all"),
        ("create_sbu_report", "Create SBU Report", "bool"),
        ("save_ch5", "Save Chapter 5", "bool"),
        ("script_output_dir", "Script Output Dir", "text"),
    ]
    index = 0
    while True:
        _, _, start_y, start_x, content_height, content_width = _draw_layout(screen, {"queue": []}, "Queue Plan")
        screen.addnstr(start_y + 0, start_x, "LFS Base Settings", content_width - 1, _attr("title"))
        screen.addnstr(start_y + 1, start_x, "Enter edit/toggle  s save  q back", content_width - 1, _attr("normal"))
        first_row = start_y + 3
        visible_count = max(1, (start_y + content_height) - first_row)
        start = max(0, index - visible_count + 1)
        for row_no, (field, label, kind) in enumerate(rows[start : start + visible_count], start=first_row):
            current = start + row_no - first_row
            value = _format_lfs_setting_value(lfs_base.get(field), kind)
            screen.addnstr(
                row_no,
                start_x,
                "%-24s %s" % (label[:24], value),
                content_width - 1,
                _attr("selected") if current == index else _attr("normal"),
            )
        key, repeat = _read_key(screen)
        if key in (ord("q"), 27):
            return "LFS base settings unchanged"
        if key in (curses.KEY_DOWN, ord("j")) and index < len(rows) - 1:
            index = min(len(rows) - 1, index + repeat)
        elif key in (curses.KEY_UP, ord("k")) and index > 0:
            index = max(0, index - repeat)
        elif key == ord("s"):
            app.update_settings({"lfs_base": lfs_base})
            return "Saved LFS base settings"
        elif key in (10, 13, ord(" ")):
            field, label, kind = rows[index]
            if kind == "bool":
                lfs_base[field] = not bool(lfs_base.get(field))
            elif kind == "int":
                current = str(lfs_base.get(field, 0))
                value = _prompt(screen, label, current)
                if value:
                    lfs_base[field] = int(value)
            elif kind.startswith("choice:"):
                choices = tuple(kind.split(":", 1)[1].split(","))
                lfs_base[field] = _next_choice(choices, lfs_base.get(field))
            else:
                current = str(lfs_base.get(field, "") or "")
                value = _prompt(screen, label, current)
                if value is not None:
                    lfs_base[field] = value.strip()


def _view_lfs_base_plan(screen, plan):
    index = 0
    while True:
        _, _, start_y, start_x, content_height, content_width = _draw_layout(screen, {"queue": []}, "Queue Plan")
        screen.addnstr(start_y + 0, start_x, "LFS Base Plan", content_width - 1, _attr("title"))
        screen.addnstr(start_y + 1, start_x, "q back", content_width - 1, _attr("normal"))
        screen.addnstr(start_y + 2, start_x, "Triplet: %s" % plan.target_triplet, content_width - 1, _attr("accent"))
        first_row = start_y + 4
        visible_count = max(1, (start_y + content_height) - first_row)
        start = max(0, index - visible_count + 1)
        visible = plan.steps[start : start + visible_count]
        for row_no, step in enumerate(visible, start=first_row):
            current = start + row_no - first_row
            line = "%03d %-10s %-16s %s" % (step.order, step.chapter[:10], step.stage[:16], step.name[: max(1, content_width - 36)])
            screen.addnstr(row_no, start_x, line, content_width - 1, _attr("selected") if current == index else _attr("normal"))
        key, repeat = _read_key(screen)
        if key in (ord("q"), 27):
            return "Closed LFS base plan"
        if key in (curses.KEY_DOWN, ord("j")) and index < len(plan.steps) - 1:
            index = min(len(plan.steps) - 1, index + repeat)
        elif key in (curses.KEY_UP, ord("k")) and index > 0:
            index = max(0, index - repeat)


def _view_lfs_base_status(screen, app):
    while True:
        state = app.get_lfs_base_state()
        _, _, start_y, start_x, content_height, content_width = _draw_layout(screen, {"queue": []}, "Queue Plan")
        screen.addnstr(start_y + 0, start_x, "LFS Base Build Status", content_width - 1, _attr("title"))
        screen.addnstr(start_y + 1, start_x, "q back", content_width - 1, _attr("normal"))
        lines = [
            "Build root: %s" % (state.get("build_root") or "-"),
            "Target triplet: %s" % (state.get("target_triplet") or "-"),
            "Completed steps: %s/%s" % (len(state.get("completed_steps", [])), state.get("plan_steps", 0)),
            "Last order: %s" % (state.get("last_order", 0)),
            "Last step: %s" % (state.get("last_step") or "-"),
            "Current log: %s" % (state.get("current_log") or "-"),
            "Master log: %s" % (state.get("master_log") or "-"),
            "Sources fetched: %s" % ("yes" if state.get("sources_fetched", False) else "no"),
            "Complete: %s" % ("yes" if state.get("complete", False) else "no"),
        ]
        for note in state.get("preflight", {}).get("notes", [])[: min(4, max(0, content_height - len(lines) - 6))]:
            lines.append("Preflight: %s" % note)
        for row_no, line in enumerate(lines, start=start_y + 3):
            screen.addnstr(row_no, start_x, line, content_width - 1, _attr("normal"))
        key, _ = _read_key(screen)
        if key in (ord("q"), 27):
            return "Closed LFS base build status"


def _view_lfs_base_log(screen, app):
    while True:
        payload = app.get_lfs_base_log_tail(lines=200)
        path = payload.get("path", "")
        lines = payload.get("lines", [])
        _, _, start_y, start_x, content_height, content_width = _draw_layout(screen, {"queue": []}, "Queue Plan")
        screen.addnstr(start_y + 0, start_x, "LFS Build Log", content_width - 1, _attr("title"))
        screen.addnstr(start_y + 1, start_x, "r refresh  q back", content_width - 1, _attr("normal"))
        screen.addnstr(start_y + 2, start_x, path or "No log available yet", content_width - 1, _attr("accent"))
        first_row = start_y + 4
        visible_count = max(1, (start_y + content_height) - first_row)
        visible = lines[-visible_count:]
        for row_no, line in enumerate(visible, start=first_row):
            screen.addnstr(row_no, start_x, line, content_width - 1, _attr("normal"))
        key, _ = _read_key(screen)
        if key in (ord("q"), 27):
            return "Closed LFS build log"
        if key in (ord("r"),):
            continue


def _approve_root_action(screen, payload):
    while True:
        _, _, start_y, start_x, content_height, content_width = _draw_layout(screen, {"queue": []}, "Queue Plan")
        screen.addnstr(start_y + 0, start_x, "Approve Root Action", content_width - 1, _attr("title"))
        screen.addnstr(start_y + 1, start_x, "y approve  n decline", content_width - 1, _attr("normal"))
        rows = [
            "Action: %s" % (payload.get("description") or "root action"),
            "Command:",
            payload.get("command_text", ""),
        ]
        env = payload.get("env", {})
        if env:
            rows.append("Env: " + " ".join("%s=%s" % (key, env[key]) for key in sorted(env)))
        visible = rows[: max(1, content_height - 1)]
        for idx, line in enumerate(visible, start=start_y + 3):
            screen.addnstr(idx, start_x, line, content_width - 1, _attr("accent") if idx == start_y + 3 else _attr("normal"))
        key, _ = _read_key(screen)
        if key in (ord("y"), ord("Y")):
            return True
        if key in (ord("n"), ord("N"), 27):
            return False


def _preview_lfs_execution(screen, payload, seconds=5):
    countdown = max(0, int(seconds or 0))
    deadline = time.time() + countdown
    previous_timeout = screen.timeout(200)
    try:
        while True:
            remaining = max(0, int(round(deadline - time.time()))) if countdown else 0
            _, _, start_y, start_x, content_height, content_width = _draw_layout(screen, {"queue": []}, "Queue Plan")
            screen.addnstr(start_y + 0, start_x, "About To Execute", content_width - 1, _attr("title"))
            screen.addnstr(
                start_y + 1,
                start_x,
                "Enter start now  q cancel%s" % ("  auto in %ds" % remaining if countdown else ""),
                content_width - 1,
                _attr("normal"),
            )
            rows = [
                "Step: %s" % (payload.get("description") or "LFS command"),
                "Context: %s" % (payload.get("context") or "host"),
                "Target Root: %s" % (payload.get("target_root") or ""),
                "Location: %s" % (payload.get("location") or ""),
                "Command:",
                payload.get("command_text", ""),
            ]
            env = payload.get("env", {})
            if env:
                rows.append("Env: " + " ".join("%s=%s" % (key, env[key]) for key in sorted(env)))
            visible = rows[: max(1, content_height - 2)]
            for idx, line in enumerate(visible, start=start_y + 3):
                screen.addnstr(idx, start_x, line, content_width - 1, _attr("accent") if idx == start_y + 3 else _attr("normal"))
            key = screen.getch()
            if key in (10, 13, ord("y"), ord("Y")):
                return True
            if key in (ord("q"), ord("n"), ord("N"), 27):
                return False
            if countdown and time.time() >= deadline:
                return True
    finally:
        screen.timeout(previous_timeout if previous_timeout is not None else -1)


def _browse_categories(screen, app, state):
    query = ""
    index = 0
    message = ""
    while True:
        categories = [
            item for item in state["categories"]
            if not query or query.lower() in item[0].lower()
        ]
        index = min(index, max(0, len(categories) - 1))
        _, _, start_y, start_x, content_height, content_width = _draw_layout(screen, state, "Queue Plan")
        screen.addnstr(start_y + 0, start_x, "Choose Packages", content_width - 1, _attr("title"))
        screen.addnstr(start_y + 1, start_x, "Enter open  / filter  q back", content_width - 1, _attr("normal"))
        screen.addnstr(start_y + 2, start_x, "Filter: %s" % query, content_width - 1, _attr("normal"))
        if message:
            screen.addnstr(start_y + 3, start_x, message, content_width - 1, _attr("accent"))
        first_row = start_y + (5 if message else 4)
        visible_count = max(1, (start_y + content_height) - first_row)
        start = max(0, index - visible_count + 1)
        for row, (category, count) in enumerate(categories[start : start + visible_count], start=first_row):
            current = start + row - first_row
            line = "%-40s %5d" % (category[:40], count)
            screen.addnstr(row, start_x, line, content_width - 1, _attr("selected") if current == index else _attr("normal"))
        key, repeat = _read_key(screen)
        if key in (ord("q"), 27):
            return "Category selection closed"
        if key in (curses.KEY_DOWN, ord("j")) and index < max(0, len(categories) - 1):
            index = min(max(0, len(categories) - 1), index + repeat)
        elif key in (curses.KEY_UP, ord("k")) and index > 0:
            index = max(0, index - repeat)
        elif key == ord("/"):
            query = _prompt(screen, "category filter", query)
            index = 0
        elif key in (10, 13) and categories:
            message = _browse_category_packages(screen, app, state, categories[index][0])


def _browse_category_packages(screen, app, state, category):
    query = ""
    index = 0
    message = ""
    base_packages = list(state["category_packages_cache"].setdefault(category, state["catalog"].packages_in_category(category)))
    while True:
        packages = base_packages
        if query:
            lowered = query.lower()
            packages = [
                package for package in packages
                if lowered in package.name.lower() or lowered in package.summary.lower()
            ]
        index = min(index, max(0, len(packages) - 1))
        _, _, start_y, start_x, content_height, content_width = _draw_layout(screen, state, "Queue Plan")
        screen.addnstr(start_y + 0, start_x, "Category: %s" % category, content_width - 1, _attr("title"))
        screen.addnstr(start_y + 1, start_x, "space add  enter details  / filter  q back", content_width - 1, _attr("normal"))
        screen.addnstr(start_y + 2, start_x, "Filter: %s" % query, content_width - 1, _attr("normal"))
        if message:
            screen.addnstr(start_y + 3, start_x, message, content_width - 1, _attr("accent"))
        first_row = start_y + (5 if message else 4)
        visible_count = max(1, (start_y + content_height) - first_row)
        start = max(0, index - visible_count + 1)
        visible = packages[start : start + visible_count]
        for row, package in enumerate(visible, start=first_row):
            current = start + row - first_row
            line = "%-24s %-10s %-8s %s" % (
                package.name[:24],
                package.version[:10],
                package.source_origin[:8],
                package.summary[: max(0, content_width - 48)],
            )
            screen.addnstr(row, start_x, line, content_width - 1, _attr("selected") if current == index else _attr("normal"))
        key, repeat = _read_key(screen)
        if key in (ord("q"), 27):
            return "Left %s" % category
        if key in (curses.KEY_DOWN, ord("j")) and index < max(0, len(packages) - 1):
            index = min(max(0, len(packages) - 1), index + repeat)
        elif key in (curses.KEY_UP, ord("k")) and index > 0:
            index = max(0, index - repeat)
        elif key == ord("/"):
            query = _prompt(screen, "package filter", query)
            index = 0
        elif key == ord(" ") and packages:
            message = _queue_package(screen, app, state, packages[index])
        elif key in (10, 13) and packages:
            message = _show_package(screen, app, package=packages[index], state=state)


def _browse_catalog(screen, app, state):
    query = ""
    index = 0
    message = ""
    while True:
        packages = state["all_packages"]
        if query:
            lowered = query.lower()
            packages = [
                package for package in packages
                if lowered in package.name.lower() or lowered in package.summary.lower()
            ]
        index = min(index, max(0, len(packages) - 1))
        _, _, start_y, start_x, content_height, content_width = _draw_layout(screen, state, "Queue Plan")
        screen.addnstr(start_y + 0, start_x, "Browse Catalog", content_width - 1, _attr("title"))
        screen.addnstr(start_y + 1, start_x, "enter details  / filter  q back", content_width - 1, _attr("normal"))
        screen.addnstr(start_y + 2, start_x, "Filter: %s" % query, content_width - 1, _attr("normal"))
        if message:
            screen.addnstr(start_y + 3, start_x, message, content_width - 1, _attr("accent"))
        first_row = start_y + (5 if message else 4)
        visible_count = max(1, (start_y + content_height) - first_row)
        start = max(0, index - visible_count + 1)
        visible = packages[start : start + visible_count]
        for row, package in enumerate(visible, start=first_row):
            current = start + row - first_row
            line = "%-24s %-10s %-8s %-20s %s" % (
                package.name[:24],
                package.version[:10],
                package.source_origin[:8],
                package.category[:20],
                package.summary[: max(0, content_width - 66)],
            )
            screen.addnstr(row, start_x, line, content_width - 1, _attr("selected") if current == index else _attr("normal"))
        key, repeat = _read_key(screen)
        if key in (ord("q"), 27):
            return "Catalog browser closed"
        if key in (curses.KEY_DOWN, ord("j")) and index < max(0, len(packages) - 1):
            index = min(max(0, len(packages) - 1), index + repeat)
        elif key in (curses.KEY_UP, ord("k")) and index > 0:
            index = max(0, index - repeat)
        elif key == ord("/"):
            query = _prompt(screen, "catalog filter", query)
            index = 0
        elif key in (10, 13) and packages:
            message = _show_package(screen, app, package=packages[index], state=state)


def _queue_package(screen, app, state, package):
    settings = app.get_settings()
    existing_keys = {(item["name"], item["source_origin"]) for item in state["queue"]}
    deps_only = [dep for dep in package.depends]
    if not deps_only:
        plan = BuildPlan(requested=[package.name], ordered_steps=[BuildStep(package=package)])
        _merge_queue_plan(state, plan, package, "blfs", "required", _default_dependency_sources(package))
        _reposition_new_items_for_existing_roots(
            app,
            state,
            package,
            _new_plan_keys(plan, existing_keys),
        )
        return "Queued %s without required dependencies" % package.name
    include_deps = _yes_no(
        screen,
        "Required deps for %s: %s. Add package with dependencies?" % (
            package.name,
            ", ".join(deps_only[:5]) + (" ..." if len(deps_only) > 5 else ""),
        ),
        True,
    )
    if not include_deps:
        plan = BuildPlan(requested=[package.name], ordered_steps=[BuildStep(package=package)])
        _merge_queue_plan(state, plan, package, "blfs", "required", _default_dependency_sources(package))
        _reposition_new_items_for_existing_roots(
            app,
            state,
            package,
            _new_plan_keys(plan, existing_keys),
        )
        return "Queued %s alone" % package.name

    dependency_level = _default_dependency_level(settings, package)
    if package.recommends or package.optional:
        dependency_level = _prompt_choice(screen, "Dependency level", dependency_level, DEPENDENCY_LEVELS)
    allowed_dependency_sources = _select_dependency_sources(
        screen,
        _default_dependency_sources(package),
    )
    include_recommends, auto_optional = _dependency_flags(dependency_level)
    dependency_mode = "blfs"
    preview_plan = None
    if package.source_origin == "t2":
        blfs_plan = app.plan_selection(
            package.name,
            package.source_origin,
            include_recommends=include_recommends,
            auto_optional=auto_optional,
            resolve_required=True,
            t2_dependency_mode="blfs",
            allowed_dependency_sources=allowed_dependency_sources,
        )
        t2_plan = app.plan_selection(
            package.name,
            package.source_origin,
            include_recommends=include_recommends,
            auto_optional=auto_optional,
            resolve_required=True,
            t2_dependency_mode="t2",
            allowed_dependency_sources=allowed_dependency_sources,
        )
        blfs_ok = not blfs_plan.unresolved
        t2_ok = not t2_plan.unresolved
        if blfs_ok and t2_ok:
            dependency_mode = _prompt_choice(screen, "T2 dependency source", "blfs", ("blfs", "t2"))
            preview_plan = blfs_plan if dependency_mode == "blfs" else t2_plan
        elif t2_ok:
            dependency_mode = "t2"
            preview_plan = t2_plan
        elif blfs_ok:
            dependency_mode = "blfs"
            preview_plan = blfs_plan
        else:
            plan = BuildPlan(requested=[package.name], ordered_steps=[BuildStep(package=package)])
            _merge_queue_plan(state, plan, package, "blfs", dependency_level, allowed_dependency_sources)
            _reposition_new_items_for_existing_roots(
                app,
                state,
                package,
                _new_plan_keys(plan, existing_keys),
            )
            return "Queued %s alone; dependency chains unresolved in both BLFS and T2 modes" % package.name
    if preview_plan is None:
        preview_plan = app.plan_selection(
            package.name,
            package.source_origin,
            include_recommends=include_recommends,
            auto_optional=auto_optional,
            resolve_required=True,
            t2_dependency_mode=dependency_mode,
            allowed_dependency_sources=allowed_dependency_sources,
        )
    dependency_preview = [
        "%s[%s]" % (step.package.name, step.package.source_origin)
        for step in preview_plan.ordered_steps
        if not (step.package.name == package.name and step.package.source_origin == package.source_origin)
    ]
    _merge_queue_plan(state, preview_plan, package, dependency_mode, dependency_level, allowed_dependency_sources)
    _reposition_new_items_for_existing_roots(
        app,
        state,
        package,
        _new_plan_keys(preview_plan, existing_keys),
    )
    return "Queued %s with %d dependency package(s) via %s (%s deps)" % (
        package.name,
        len(dependency_preview),
        dependency_mode,
        dependency_level,
    )


def _build_queue(screen, app, state):
    index = 0
    while True:
        queue = state["queue"]
        index = min(index, max(0, len(queue) - 1))
        _, _, start_y, start_x, content_height, content_width = _draw_layout(screen, state, "Queue Plan", selected_queue_index=index)
        list_width, detail_x, detail_width = _split_workspace(start_x, content_width)
        screen.addnstr(start_y + 0, start_x, "Build Queue", list_width - 1, _attr("title"))
        screen.addnstr(start_y + 1, start_x, "b build/export  w scripts  d remove  Enter commands  i info  q back", list_width - 1, _attr("normal"))
        if not queue:
            screen.addnstr(start_y + 3, start_x, "Queue is empty", list_width - 1, _attr("normal"))
        first_row = start_y + 3
        visible_count = max(1, (start_y + content_height) - first_row)
        start = max(0, index - visible_count + 1)
        for row, item in enumerate(queue[start : start + visible_count], start=first_row):
            current = start + row - first_row
            line = "%-24s %-8s %-8s %s" % (
                item["name"][:24],
                item["source_origin"][:8],
                item["version"][:8],
                "selected" if item.get("selected") else "dependency",
            )
            screen.addnstr(row, start_x, line, list_width - 1, _attr("selected") if current == index else _attr("normal"))
        if content_width >= 70:
            _draw_vertical_rule(screen, start_y, detail_x - 2, content_height)
            if queue:
                _draw_queue_detail(screen, state, queue[index], start_y, detail_x, content_height, detail_width)
        key, repeat = _read_key(screen)
        if key in (ord("q"), 27):
            return "Build queue closed"
        if key in (curses.KEY_DOWN, ord("j")) and index < max(0, len(queue) - 1):
            index = min(max(0, len(queue) - 1), index + repeat)
        elif key in (curses.KEY_UP, ord("k")) and index > 0:
            index = max(0, index - repeat)
        elif key == ord("d") and queue:
            return _remove_queue_item(screen, app, state, index)
        elif key == ord("b") and queue:
            return _run_queue(screen, app, state)
        elif key == ord("w") and queue:
            return _save_queue_scripts(screen, app, state)
        elif key in (10, 13) and queue:
            package = _lookup_package(state, app, queue[index]["name"], queue[index]["source_origin"])
            if package:
                return _command_editor(screen, app, package)
        elif key == ord("i") and queue:
            package = _lookup_package(state, app, queue[index]["name"], queue[index]["source_origin"])
            if package:
                return _show_package(screen, app, package, state)


def _installed_history(screen, app, state):
    installed = app.store.list_installed()
    history = list(app.store.history(limit=20))
    index = 0
    mode = "installed"
    while True:
        _, _, start_y, start_x, content_height, content_width = _draw_layout(screen, state, "Queue Plan")
        screen.addnstr(start_y + 0, start_x, "Installed / History", content_width - 1, _attr("title"))
        screen.addnstr(start_y + 1, start_x, "tab switch view  q back", content_width - 1, _attr("normal"))
        screen.addnstr(start_y + 2, start_x, "View: %s" % mode, content_width - 1, _attr("accent"))
        first_row = start_y + 4
        visible_count = max(1, content_height - 4)
        if mode == "installed":
            rows = [
                "%-24s %-10s %-10s %s" % (
                    item.name[:24],
                    item.version[:10],
                    item.source_origin[:10],
                    ("via %s" % item.metadata.get("build_provider")) if item.metadata.get("build_provider") else item.install_reason,
                )
                for item in installed
            ] or ["No installed packages recorded"]
        else:
            rows = [
                "%-20s %-16s %-20s %s" % (
                    row["created_at"][:20],
                    row["status"][:16],
                    row["package_name"][:20],
                    _history_detail_summary(row["detail"])[: max(0, content_width - 60)],
                )
                for row in history
            ] or ["No transactions recorded"]
        index = min(index, max(0, len(rows) - 1))
        start = max(0, index - visible_count + 1)
        for row_no, line in enumerate(rows[start : start + visible_count], start=first_row):
            current = start + row_no - first_row
            screen.addnstr(row_no, start_x, line, content_width - 1, _attr("selected") if current == index else _attr("normal"))
        key, repeat = _read_key(screen)
        if key in (ord("q"), 27):
            return "Closed installed/history view"
        if key == 9:
            mode = "history" if mode == "installed" else "installed"
            index = 0
        elif key in (curses.KEY_DOWN, ord("j")) and index < len(rows) - 1:
            index = min(len(rows) - 1, index + repeat)
        elif key in (curses.KEY_UP, ord("k")) and index > 0:
            index = max(0, index - repeat)


def _run_queue(screen, app, state):
    plan = _queue_to_plan(app, state)
    if plan.unresolved:
        return "Queue contains unresolved packages: %s" % ", ".join(plan.unresolved)
    provider_preview = _provider_preview_lines(plan)
    if provider_preview and not _yes_no(
        screen,
        "Queue uses grouped build payloads: %s. Continue?" % " | ".join(provider_preview[:3]),
        True,
    ):
        return "Build cancelled"
    settings = app.get_settings()
    package_format = settings["build"].get("package_format", "none")
    build_process = settings["build"].get("build_process", "python")
    build_mode = settings["build"].get("build_mode", "native")
    chroot_root = settings["build"].get("chroot_root") or app.config.root
    if build_process == "scripts":
        exporter = BuildScriptExporter(app.config, app.store)
        base_dir, master_path, _ = exporter.export_queue(
            [step.package for step in plan.ordered_steps],
            output_dir=settings["build"].get("script_output_dir", ""),
            package_format=package_format,
            install_after_build=settings["build"].get("install_after_build", True),
            update_tracking=settings["build"].get("script_update_tracking", True),
            build_mode=build_mode,
            chroot_root=chroot_root,
        )
        return "Exported queue scripts to %s (master: %s)" % (base_dir, master_path)
    executor = BuildExecutor(
        app.config,
        app.store,
        prompt_callback=lambda prompt: _yes_no(screen, prompt),
        command_review_callback=lambda pkg, phases, mode, seconds: _review_package_commands(
            screen, app, pkg, phases, mode, seconds
        ),
    )
    executor.execute_plan(
        plan,
        build_mode=build_mode,
        package_format="" if package_format == "none" else package_format,
        install=settings["build"].get("install_after_build", True),
        allow_la_removal=settings["build"].get("allow_la_removal", False),
        chroot_root=chroot_root,
    )
    return "Built queue: %s" % ", ".join(item["name"] for item in state["queue"])


def _show_package(screen, app, package, state):
    while True:
        _, _, start_y, start_x, content_height, content_width = _draw_layout(screen, state, "Queue Plan")
        override = app.get_package_override(package.name, package.source_origin)
        custom_path = override.get("custom_build_file") or "-"
        provider = _package_provider(package)
        provider_members = _provider_member_names(package)
        list_width, detail_x, detail_width = _split_workspace(start_x, content_width)
        lines = [
            "%s %s [%s]" % (package.name, package.version, package.source_origin),
            "",
            package.summary,
            "",
            "Category: %s" % (package.category or "-"),
            "Depends: %s" % (", ".join(package.depends) or "-"),
            "Recommends: %s" % (", ".join(package.recommends) or "-"),
            "Optional: %s" % (", ".join(package.optional) or "-"),
            "Build provider: %s" % (_provider_label(provider) if provider else "standalone"),
            "Provider members: %s" % (", ".join(provider_members[:5]) + (" ..." if len(provider_members) > 5 else "") if provider_members else "-"),
            "Override prefix: %s" % (override.get("prefix") or "(global default)"),
            "Override rpath: %s" % (", ".join(override.get("rpath_paths", [])) or "-"),
            "Alternate build: %s" % custom_path,
            "",
            "b build  a add to queue  c commands  e edit override  q back",
        ]
        for row, line in enumerate(lines[:content_height]):
            screen.addnstr(start_y + row, start_x, line, list_width - 1, _attr("normal" if row else "title"))
        if content_width >= 70:
            _draw_vertical_rule(screen, start_y, detail_x - 2, content_height)
            detail_lines = [
                "Package Details",
                "",
                "Name: %s" % package.name,
                "Version: %s" % package.version,
                "Source: %s" % package.source_origin,
                "Category: %s" % (package.category or "-"),
                "Depends: %s" % (", ".join(package.depends) or "-"),
                "Recommends: %s" % (", ".join(package.recommends) or "-"),
                "Optional: %s" % (", ".join(package.optional) or "-"),
                "Provider: %s" % (_provider_label(provider) if provider else "standalone"),
                "Provider members: %d" % len(provider_members),
                "Prefix override: %s" % (override.get("prefix") or "(global default)"),
                "RPATH override: %s" % (", ".join(override.get("rpath_paths", [])) or "-"),
            ]
            for offset, line in enumerate(detail_lines[:content_height]):
                screen.addnstr(start_y + offset, detail_x, line, detail_width - 1, _attr("title" if offset == 0 else "panel"))
        key, repeat = _read_key(screen)
        if key in (ord("q"), 27):
            return "Viewed %s" % package.name
        if key == ord("e"):
            return _edit_package_override(screen, app, package)
        if key == ord("c"):
            return _command_editor(screen, app, package)
        if key == ord("a"):
            return _queue_package(screen, app, state, package)
        if key == ord("b"):
            return _build_package(screen, app, package)


def _edit_build_settings(screen, app):
    settings = app.get_settings()
    build = dict(settings["build"])
    rows = [
        ("build_process", "Build Process", "build-process"),
        ("build_mode", "Build Mode", "build-mode"),
        ("chroot_root", "Chroot Root", "text"),
        ("prefix", "Default Prefix", "text"),
        ("bindir", "Bin Dir Override", "text"),
        ("sbindir", "Sbin Dir Override", "text"),
        ("libdir", "Lib Dir Override", "text"),
        ("includedir", "Include Dir Override", "text"),
        ("datadir", "Data Dir Override", "text"),
        ("docdir_root", "Doc Root", "text"),
        ("sysconfdir", "Sysconf Dir", "text"),
        ("localstatedir", "Local State Dir", "text"),
        ("jobs", "Parallel Jobs", "int"),
        ("cflags", "CFLAGS", "text"),
        ("cxxflags", "CXXFLAGS", "text"),
        ("ldflags", "LDFLAGS", "text"),
        ("configure_extra", "Configure Extra", "text"),
        ("meson_extra", "Meson Extra", "text"),
        ("cmake_extra", "CMake Extra", "text"),
        ("make_extra", "Make Extra", "text"),
        ("make_install_extra", "Make Install Extra", "text"),
        ("always_rpath_paths", "Always RPATH Dirs", "csv"),
        ("command_review_mode", "Command Review Mode", "choice"),
        ("command_review_seconds", "Timed Review Seconds", "int"),
        ("package_format", "Package Format", "package-format"),
        ("install_after_build", "Install After Build", "bool"),
        ("script_output_dir", "Script Output Dir", "text"),
        ("script_update_tracking", "Scripts Update Tracking", "bool"),
        ("allow_la_removal", "Allow .la Removal", "bool"),
        ("default_dependency_level", "Default Dependency Level", "dependency-level"),
        ("non_interactive", "Unattended Builds", "bool"),
    ]
    index = 0
    message = ""
    while True:
        _, _, start_y, start_x, content_height, content_width = _draw_layout(screen, {"queue": []}, "Queue Plan")
        screen.addnstr(start_y + 0, start_x, "Global Build Settings", content_width - 1, _attr("title"))
        screen.addnstr(start_y + 1, start_x, "Enter edit/toggle  s save  q back", content_width - 1, _attr("normal"))
        screen.addnstr(start_y + 2, start_x, message or "One line per setting", content_width - 1, _attr("accent" if message else "normal"))
        first_row = start_y + 4
        visible_count = max(1, (start_y + content_height) - first_row)
        start = max(0, index - visible_count + 1)
        visible = rows[start : start + visible_count]
        for row_no, (key, label, kind) in enumerate(visible, start=first_row):
            current = start + row_no - first_row
            value = _format_setting_value(build.get(key), kind)
            line = "%-24s %s" % (label[:24], value)
            screen.addnstr(row_no, start_x, line, content_width - 1, _attr("selected") if current == index else _attr("normal"))
        key, repeat = _read_key(screen)
        if key in (ord("q"), 27):
            return "Build settings unchanged"
        if key in (curses.KEY_DOWN, ord("j")) and index < len(rows) - 1:
            index = min(len(rows) - 1, index + repeat)
        elif key in (curses.KEY_UP, ord("k")) and index > 0:
            index = max(0, index - repeat)
        elif key == ord("s"):
            app.update_settings({"build": build})
            return "Saved build settings"
        elif key in (10, 13):
            field, label, kind = rows[index]
            current = build.get(field)
            if kind == "bool":
                build[field] = not bool(current)
                message = "%s set to %s" % (label, "yes" if build[field] else "no")
            elif kind == "choice":
                build[field] = _prompt_choice(screen, label, str(current), REVIEW_MODES)
                message = "Updated %s" % label
            elif kind == "build-process":
                build[field] = _prompt_choice(screen, label, str(current), BUILD_PROCESSES)
                message = "Updated %s" % label
            elif kind == "build-mode":
                build[field] = _prompt_choice(screen, label, str(current), BUILD_MODES)
                message = "Updated %s" % label
            elif kind == "package-format":
                build[field] = _prompt_choice(screen, label, str(current), PACKAGE_FORMATS)
                message = "Updated %s" % label
            elif kind == "dependency-level":
                build[field] = _prompt_choice(screen, label, str(current), DEPENDENCY_LEVELS)
                message = "Updated %s" % label
            elif kind == "csv":
                build[field] = _split_csv(_prompt(screen, label, ",".join(current or [])))
                message = "Updated %s" % label
            elif kind == "int":
                build[field] = int(_prompt(screen, label, str(current or 0)) or current or 0)
                message = "Updated %s" % label
            else:
                build[field] = _prompt(screen, label, str(current or ""))
                message = "Updated %s" % label


def _edit_profile_settings(screen, app):
    settings = app.get_settings()
    profile = dict(settings["profile"])
    rows = [
        ("prompt_on_new_prefix", "Prompt On New Prefix", "bool"),
        ("auto_create_for_new_prefix", "Auto Create Prefix Profile", "bool"),
        ("nonstandard_only", "Nonstandard Prefixes Only", "bool"),
        ("scan_installed_files", "Scan Installed Files", "bool"),
        ("add_bin_to_path", "Add Bin To PATH", "bool"),
        ("add_lib_to_ld_library_path", "Add Lib To LD_LIBRARY_PATH", "bool"),
        ("add_pkgconfig_to_pkg_config_path", "Add Pkgconfig To PKG_CONFIG_PATH", "bool"),
        ("add_share_to_xdg_data_dirs", "Add Share To XDG_DATA_DIRS", "bool"),
        ("add_python_to_pythonpath", "Add Site-Packages To PYTHONPATH", "bool"),
        ("add_cmake_to_cmake_prefix_path", "Add Prefix To CMAKE_PREFIX_PATH", "bool"),
    ]
    index = 0
    message = ""
    while True:
        _, _, start_y, start_x, content_height, content_width = _draw_layout(screen, {"queue": []}, "Queue Plan")
        screen.addnstr(start_y + 0, start_x, "Profile.d Settings", content_width - 1, _attr("title"))
        screen.addnstr(start_y + 1, start_x, "Enter toggle  s save  q back", content_width - 1, _attr("normal"))
        screen.addnstr(start_y + 2, start_x, message or "Per-prefix environment export policy", content_width - 1, _attr("accent" if message else "normal"))
        first_row = start_y + 4
        visible_count = max(1, (start_y + content_height) - first_row)
        start = max(0, index - visible_count + 1)
        visible = rows[start : start + visible_count]
        for row_no, (field, label, kind) in enumerate(visible, start=first_row):
            current = start + row_no - first_row
            line = "%-34s %s" % (label[:34], _format_setting_value(profile.get(field), kind))
            screen.addnstr(row_no, start_x, line, content_width - 1, _attr("selected") if current == index else _attr("normal"))
        key, repeat = _read_key(screen)
        if key in (ord("q"), 27):
            return "Profile settings unchanged"
        if key in (curses.KEY_DOWN, ord("j")) and index < len(rows) - 1:
            index = min(len(rows) - 1, index + repeat)
        elif key in (curses.KEY_UP, ord("k")) and index > 0:
            index = max(0, index - repeat)
        elif key == ord("s"):
            app.update_settings({"profile": profile})
            return "Saved profile settings"
        elif key in (10, 13, ord(" ")):
            field, label, _ = rows[index]
            profile[field] = not bool(profile.get(field))
            message = "%s set to %s" % (label, "yes" if profile[field] else "no")


def _edit_system_state(screen, app):
    settings = app.get_settings()
    system_state = dict(settings["system_state"])
    rows = [
        ("assume_lfs_base_installed", "Assume Full LFS Base Installed", "bool"),
        ("use_jhalfs_tracking", "Use jhalfs instpkg.xml", "bool"),
        ("jhalfs_tracking_path", "jhalfs Tracking Path", "text"),
    ]
    index = 0
    message = ""
    while True:
        _, _, start_y, start_x, content_height, content_width = _draw_layout(screen, {"queue": []}, "Queue Plan")
        screen.addnstr(start_y + 0, start_x, "System State", content_width - 1, _attr("title"))
        screen.addnstr(start_y + 1, start_x, "Enter edit/toggle  s save  q back", content_width - 1, _attr("normal"))
        screen.addnstr(
            start_y + 2,
            start_x,
            message or "Planner assumptions about what is already installed",
            content_width - 1,
            _attr("accent" if message else "normal"),
        )
        first_row = start_y + 4
        visible_count = max(1, (start_y + content_height) - first_row)
        start = max(0, index - visible_count + 1)
        visible = rows[start : start + visible_count]
        for row_no, (field, label, kind) in enumerate(visible, start=first_row):
            current = start + row_no - first_row
            line = "%-34s %s" % (label[:34], _format_setting_value(system_state.get(field), kind))
            screen.addnstr(
                row_no,
                start_x,
                line,
                content_width - 1,
                _attr("selected") if current == index else _attr("normal"),
            )
        key, repeat = _read_key(screen)
        if key in (ord("q"), 27):
            return "System state unchanged"
        if key in (curses.KEY_DOWN, ord("j")) and index < len(rows) - 1:
            index = min(len(rows) - 1, index + repeat)
        elif key in (curses.KEY_UP, ord("k")) and index > 0:
            index = max(0, index - repeat)
        elif key == ord("s"):
            app.update_settings({"system_state": system_state})
            return "Saved system state settings"
        elif key in (10, 13, ord(" ")):
            field, label, kind = rows[index]
            if kind == "bool":
                system_state[field] = not bool(system_state.get(field))
                message = "%s set to %s" % (label, "yes" if system_state[field] else "no")
            else:
                system_state[field] = _prompt(screen, label, str(system_state.get(field, "")))
                message = "Updated %s" % label


def _edit_package_override(screen, app, package):
    override = app.get_package_override(package.name, package.source_origin)
    override["prefix"] = _prompt(screen, "package prefix", override.get("prefix", ""))
    override["cflags"] = _prompt(screen, "package CFLAGS", override.get("cflags", ""))
    override["cxxflags"] = _prompt(screen, "package CXXFLAGS", override.get("cxxflags", ""))
    override["ldflags"] = _prompt(screen, "package LDFLAGS", override.get("ldflags", ""))
    override["rpath_paths"] = _split_csv(_prompt(screen, "package rpath dirs", ",".join(override.get("rpath_paths", []))))
    override["configure_extra"] = _prompt(screen, "package configure extra", override.get("configure_extra", ""))
    override["meson_extra"] = _prompt(screen, "package meson extra", override.get("meson_extra", ""))
    override["cmake_extra"] = _prompt(screen, "package cmake extra", override.get("cmake_extra", ""))
    override["make_extra"] = _prompt(screen, "package make extra", override.get("make_extra", ""))
    override["make_install_extra"] = _prompt(screen, "package make install extra", override.get("make_install_extra", ""))
    app.save_package_override(package.name, override, package.source_origin)
    return "Saved override for %s[%s]" % (package.name, package.source_origin)


def _command_editor(screen, app, package):
    _, phases, custom_path = app.get_effective_phases(package.name, package.source_origin)
    phases = _clone_phases(phases)
    phase_names = _phase_names(phases)
    index = 0
    message = ""
    while True:
        _, _, start_y, start_x, content_height, content_width = _draw_layout(screen, {"queue": []}, "Queue Plan")
        list_width, detail_x, detail_width = _split_workspace(start_x, content_width)
        phase_names = _phase_names(phases)
        index = min(index, max(0, len(phase_names) - 1))
        phase_name = phase_names[index] if phase_names else ""
        screen.addnstr(start_y + 0, start_x, "Commands for %s[%s]" % (package.name, package.source_origin), list_width - 1, _attr("title"))
        screen.addnstr(start_y + 1, start_x, "e edit  w save alternate  d clear alternate  q back", list_width - 1, _attr("normal"))
        screen.addnstr(start_y + 2, start_x, message or ("alternate: %s" % (custom_path or "imported recipe")), list_width - 1, _attr("accent" if message else "normal"))
        first_row = start_y + 4
        visible_count = max(1, (start_y + content_height) - first_row)
        start = max(0, index - visible_count + 1)
        for row, name in enumerate(phase_names[start : start + visible_count], start=first_row):
            current = start + row - first_row
            count = len(phases.get(name, []))
            screen.addnstr(row, start_x, "%s %-12s (%d)" % (">" if current == index else " ", name, count), list_width - 1, _attr("selected") if current == index else _attr("normal"))
        if content_width >= 70:
            _draw_vertical_rule(screen, start_y, detail_x - 2, content_height)
            if phase_name:
                screen.addnstr(start_y + 0, detail_x, "Phase: %s" % phase_name, detail_width - 1, _attr("title"))
                for offset, command in enumerate(phases.get(phase_name, [])[: max(1, content_height - 2)], start=2):
                    screen.addnstr(start_y + offset, detail_x, command, detail_width - 1, _attr("panel"))
        key, repeat = _read_key(screen)
        if key in (ord("q"), 27):
            return message or "Viewed commands for %s" % package.name
        if key in (curses.KEY_DOWN, ord("j")) and index < max(0, len(phase_names) - 1):
            index = min(max(0, len(phase_names) - 1), index + repeat)
        elif key in (curses.KEY_UP, ord("k")) and index > 0:
            index = max(0, index - repeat)
        elif key == ord("e") and phase_name:
            edited = _edit_multiline(screen, "%s commands" % phase_name, "\n".join(phases.get(phase_name, [])))
            if edited is not None:
                _set_phase_commands(phases, phase_name, edited)
                message = "Updated %s commands (not saved yet)" % phase_name
        elif key == ord("w"):
            path = app.save_custom_build(package.name, phases, package.source_origin)
            custom_path = path
            message = "Saved alternate build to %s" % path
        elif key == ord("d"):
            app.clear_custom_build(package.name, package.source_origin)
            _, phases, custom_path = app.get_effective_phases(package.name, package.source_origin)
            phases = _clone_phases(phases)
            message = "Cleared alternate build for %s" % package.name


def _build_package(screen, app, package):
    settings = app.get_settings()
    dependency_level = _default_dependency_level(settings, package)
    include_recommends, auto_optional = _dependency_flags(dependency_level)
    package_format = settings["build"].get("package_format", "none")
    build_process = settings["build"].get("build_process", "python")
    build_mode = settings["build"].get("build_mode", "native")
    chroot_root = settings["build"].get("chroot_root") or app.config.root
    try:
        dependency_mode = "blfs"
        allowed_dependency_sources = _select_dependency_sources(
            screen,
            _default_dependency_sources(package),
        )
        if package.source_origin == "t2" and package.depends:
            blfs_plan = app.plan_selection(
                package.name,
                package.source_origin,
                include_recommends=include_recommends,
                auto_optional=auto_optional,
                resolve_required=True,
                t2_dependency_mode="blfs",
                allowed_dependency_sources=allowed_dependency_sources,
            )
            t2_plan = app.plan_selection(
                package.name,
                package.source_origin,
                include_recommends=include_recommends,
                auto_optional=auto_optional,
                resolve_required=True,
                t2_dependency_mode="t2",
                allowed_dependency_sources=allowed_dependency_sources,
            )
            blfs_ok = not blfs_plan.unresolved
            t2_ok = not t2_plan.unresolved
            if blfs_ok and t2_ok:
                dependency_mode = _prompt_choice(screen, "T2 dependency source", "blfs", ("blfs", "t2"))
            elif t2_ok:
                dependency_mode = "t2"
        plan = app.plan_selection(
            package.name,
            package.source_origin,
            include_recommends=include_recommends,
            auto_optional=auto_optional,
            resolve_required=True,
            t2_dependency_mode=dependency_mode,
            allowed_dependency_sources=allowed_dependency_sources,
        )
        if plan.unresolved or plan.conflicts:
            return "Build blocked: unresolved deps or conflicts"
        provider_preview = _provider_preview_lines(plan)
        if provider_preview and not _yes_no(
            screen,
            "This build uses grouped payloads: %s. Continue?" % " | ".join(provider_preview[:3]),
            True,
        ):
            return "Build cancelled"
        if build_process == "scripts":
            exporter = BuildScriptExporter(app.config, app.store)
            base_dir, master_path, _ = exporter.export_queue(
                [step.package for step in plan.ordered_steps],
                output_dir=settings["build"].get("script_output_dir", ""),
                package_format=package_format,
                install_after_build=settings["build"].get("install_after_build", True),
                update_tracking=settings["build"].get("script_update_tracking", True),
                build_mode=build_mode,
                chroot_root=chroot_root,
            )
            return "Exported build scripts to %s (master: %s)" % (base_dir, master_path)
        executor = BuildExecutor(
            app.config,
            app.store,
            prompt_callback=lambda prompt: _yes_no(screen, prompt),
            command_review_callback=lambda pkg, phases, mode, seconds: _review_package_commands(
                screen, app, pkg, phases, mode, seconds
            ),
        )
        executor.execute_plan(
            plan,
            build_mode=build_mode,
            package_format="" if package_format == "none" else package_format,
            install=settings["build"].get("install_after_build", True),
            allow_la_removal=settings["build"].get("allow_la_removal", False),
            chroot_root=chroot_root,
        )
        return "Built %s" % package.name
    except Exception as error:
        return "Build failed: %s" % error


def _review_package_commands(screen, app, package, phases, mode, seconds):
    phases = _clone_phases(phases)
    phase_names = _phase_names(phases)
    index = 0
    paused = mode != "timed"
    countdown = max(1, int(seconds or 10))
    message = ""
    screen.timeout(1000 if mode == "timed" else -1)
    try:
        while True:
            screen.erase()
            height, width = screen.getmaxyx()
            phase_name = phase_names[index] if phase_names else ""
            screen.addnstr(0, 0, "Review Commands: %s[%s]" % (package.name, package.source_origin), width - 1, curses.A_BOLD)
            if mode == "timed":
                status = "paused" if paused else "auto-continue in %ds" % countdown
                prompt = "Enter proceed  p pause/resume  e edit  w save alternate  c cancel"
                screen.addnstr(1, 0, "%s  %s" % (status, prompt), width - 1)
            else:
                screen.addnstr(1, 0, "Enter proceed  e edit  w save alternate  c cancel", width - 1)
            screen.addnstr(2, 0, message or "Review mode: %s" % mode, width - 1)
            for row, name in enumerate(phase_names, start=4):
                marker = ">" if row - 4 == index else " "
                count = len(phases.get(name, []))
                screen.addnstr(row, 0, "%s %-12s (%d)" % (marker, name, count), min(24, width - 1), curses.A_REVERSE if row - 4 == index else 0)
            if phase_name:
                screen.addnstr(3, 26, "Phase: %s" % phase_name, width - 27, curses.A_BOLD)
                for offset, command in enumerate(phases.get(phase_name, []), start=4):
                    if offset >= height:
                        break
                    screen.addnstr(offset, 26, command, width - 27)
            key = screen.getch()
            if key == -1 and mode == "timed":
                if not paused:
                    countdown -= 1
                    if countdown <= 0:
                        return phases
                continue
            if key in (curses.KEY_DOWN, ord("j")) and index < max(0, len(phase_names) - 1):
                index += 1
            elif key in (curses.KEY_UP, ord("k")) and index > 0:
                index -= 1
            elif key in (10, 13):
                return phases
            elif key in (ord("c"), ord("q"), 27):
                return None
            elif key == ord("p") and mode == "timed":
                paused = not paused
                message = "Timed review %s" % ("paused" if paused else "resumed")
            elif key == ord("e") and phase_name:
                paused = True
                edited = _edit_multiline(screen, "%s commands" % phase_name, "\n".join(phases.get(phase_name, [])))
                if edited is not None:
                    _set_phase_commands(phases, phase_name, edited)
                    phase_names = _phase_names(phases)
                    message = "Updated %s commands" % phase_name
            elif key == ord("w"):
                path = app.save_custom_build(package.name, phases, package.source_origin)
                message = "Saved alternate build to %s" % path
    finally:
        screen.timeout(-1)


def _save_queue_scripts(screen, app, state):
    plan = _queue_to_plan(app, state)
    if plan.unresolved:
        return "Queue contains unresolved packages: %s" % ", ".join(plan.unresolved)
    settings = app.get_settings()
    exporter = BuildScriptExporter(app.config, app.store)
    base_dir, master_path, _ = exporter.export_queue(
        [step.package for step in plan.ordered_steps],
        output_dir=settings["build"].get("script_output_dir", ""),
        package_format=settings["build"].get("package_format", "none"),
        install_after_build=settings["build"].get("install_after_build", True),
        update_tracking=settings["build"].get("script_update_tracking", True),
        build_mode=settings["build"].get("build_mode", "native"),
        chroot_root=settings["build"].get("chroot_root") or app.config.root,
    )
    return "Saved build scripts to %s (master: %s)" % (base_dir, master_path)


def _select_dependency_sources(screen, active_sources):
    active = {source for source in active_sources if source}
    rows = [("all", "", "action")] + [(key, label, "bool") for key, label in DEPENDENCY_SOURCE_LABELS]
    index = 0
    message = ""
    while True:
        _, _, start_y, start_x, content_height, content_width = _draw_layout(screen, {"queue": []}, "Queue Plan")
        screen.addnstr(start_y + 0, start_x, "Dependency Source Groups", content_width - 1, _attr("title"))
        screen.addnstr(start_y + 1, start_x, "Enter toggle  a all on/off  s save  q back", content_width - 1, _attr("normal"))
        screen.addnstr(
            start_y + 2,
            start_x,
            message or "Only active branches will be considered for dependency packages",
            content_width - 1,
            _attr("accent" if message else "normal"),
        )
        first_row = start_y + 4
        visible_count = max(1, (start_y + content_height) - first_row)
        start = max(0, index - visible_count + 1)
        visible = rows[start : start + visible_count]
        all_active = len(active) == len(DEPENDENCY_SOURCE_LABELS)
        for row_no, (field, label, kind) in enumerate(visible, start=first_row):
            current = start + row_no - first_row
            if kind == "action":
                line = "Deactivate All Sources" if all_active else "Activate All Sources"
            else:
                state_label = "active" if field in active else "inactive"
                action = "Deactivate" if field in active else "Activate"
                line = "%-28s %s (%s)" % ("%s deps" % label, action, state_label)
            screen.addnstr(
                row_no,
                start_x,
                line,
                content_width - 1,
                _attr("selected") if current == index else _attr("normal"),
            )
        key, repeat = _read_key(screen)
        if key in (ord("q"), 27):
            return sorted(active)
        if key in (curses.KEY_DOWN, ord("j")) and index < len(rows) - 1:
            index = min(len(rows) - 1, index + repeat)
        elif key in (curses.KEY_UP, ord("k")) and index > 0:
            index = max(0, index - repeat)
        elif key == ord("a"):
            active = {key for key, _ in DEPENDENCY_SOURCE_LABELS} if len(active) != len(DEPENDENCY_SOURCE_LABELS) else set()
            message = "Activated all sources" if active else "Deactivated all sources"
        elif key in (10, 13, ord(" ")):
            field, label, kind = rows[index]
            if kind == "action":
                active = {key for key, _ in DEPENDENCY_SOURCE_LABELS} if len(active) != len(DEPENDENCY_SOURCE_LABELS) else set()
                message = "Activated all sources" if active else "Deactivated all sources"
            else:
                if field in active:
                    active.remove(field)
                    message = "Deactivated %s dependencies" % label
                else:
                    active.add(field)
                    message = "Activated %s dependencies" % label
        elif key == ord("s"):
            return sorted(active)


def _read_key(screen):
    key = screen.getch()
    if key not in (curses.KEY_DOWN, curses.KEY_UP, ord("j"), ord("k")):
        return key, 1
    repeat = 1
    screen.nodelay(True)
    try:
        while True:
            next_key = screen.getch()
            if next_key == -1:
                break
            if next_key == key:
                repeat += 1
                continue
            curses.ungetch(next_key)
            break
    finally:
        screen.nodelay(False)
    return key, repeat


def _prompt(screen, label, default=""):
    curses.echo()
    screen.erase()
    _, width = screen.getmaxyx()
    screen.addnstr(0, 0, "%s:" % label, width - 1, _attr("title"))
    if default:
        screen.addnstr(1, 0, "default: %s" % default, width - 1, _attr("normal"))
    screen.addnstr(2, 0, "> ", width - 1, _attr("accent"))
    screen.refresh()
    value = screen.getstr(2, 2).decode("utf-8")
    curses.noecho()
    return value if value else default


def _prompt_choice(screen, label, default, choices):
    labels = [(choice, choice) for choice in choices]
    return _select_from_boxes(screen, label, labels, default)


def _yes_no(screen, prompt, default=True):
    result = _select_from_boxes(
        screen,
        prompt,
        [("yes", "Yes"), ("no", "No")],
        "yes" if default else "no",
    )
    return result == "yes"


def _select_from_boxes(screen, label, options, default):
    index = 0
    values = [value for value, _ in options]
    if default in values:
        index = values.index(default)
    while True:
        _, _, start_y, start_x, content_height, content_width = _draw_layout(screen, {"queue": []}, "Queue Plan")
        screen.addnstr(start_y + 0, start_x, label, content_width - 1, _attr("title"))
        screen.addnstr(start_y + 1, start_x, "Arrow keys move  Enter select  q cancel", content_width - 1, _attr("normal"))
        first_row = start_y + 3
        visible_count = max(1, min(len(options), content_height - 3))
        start = max(0, index - visible_count + 1)
        for row_no, (value, display) in enumerate(options[start : start + visible_count], start=first_row):
            current = start + row_no - first_row
            prefix = "[x]" if current == index else "[ ]"
            screen.addnstr(
                row_no,
                start_x,
                "%s %s" % (prefix, display),
                content_width - 1,
                _attr("selected") if current == index else _attr("normal"),
            )
        key, repeat = _read_key(screen)
        if key in (curses.KEY_DOWN, ord("j")) and index < len(options) - 1:
            index = min(len(options) - 1, index + repeat)
        elif key in (curses.KEY_UP, ord("k")) and index > 0:
            index = max(0, index - repeat)
        elif key in (10, 13, ord(" ")):
            return options[index][0]
        elif key in (ord("q"), 27):
            return default


def _edit_multiline(screen, title, initial_text):
    height, width = screen.getmaxyx()
    edit_height = max(6, min(height - 5, 20))
    edit_width = max(20, width - 4)
    top = 2
    left = 1
    window = curses.newwin(edit_height, edit_width, top, left)
    window.erase()
    for row, line in enumerate(initial_text.splitlines()):
        if row >= edit_height - 2:
            break
        window.addnstr(row, 0, line, edit_width - 2)
    state = {"cancelled": False}

    def _validator(ch):
        if ch == 27:
            state["cancelled"] = True
            return 7
        return ch

    while True:
        screen.erase()
        screen.addnstr(0, 0, "%s" % title, width - 1, _attr("title"))
        screen.addnstr(1, 0, "Ctrl-G save  Esc cancel", width - 1, _attr("normal"))
        curses.textpad.rectangle(screen, top - 1, left - 1, top + edit_height, left + edit_width - 1)
        screen.refresh()
        curses.curs_set(1)
        editor = curses.textpad.Textbox(window)
        result = editor.edit(_validator)
        curses.curs_set(0)
        if state["cancelled"]:
            return None
        return [line.rstrip() for line in result.splitlines() if line.strip()]


def _split_csv(value):
    return [item.strip() for item in value.split(",") if item.strip()]


def _source_name(key):
    return {
        "base": "lfs-base",
        "blfs": "blfs",
        "t2": "t2",
        "arch": "arch",
        "custom": "custom",
    }.get(key, key)


def _friendly_source_name(key):
    return {
        "base": "lfs-base",
        "lfs-base": "lfs-base",
        "blfs": "blfs",
        "t2": "t2",
        "arch": "arch",
        "custom": "custom",
    }.get(key, key)


def _format_sync_time(value):
    if not value:
        return ""
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return value
    return parsed.strftime("%Y-%m-%d %H:%M UTC")


def _sync_summary(report, selected):
    parts = []
    for key in ("lfs-base", "blfs", "t2", "arch", "custom"):
        if key == "lfs-base" and "base" not in selected:
            continue
        if key == "blfs" and "blfs" not in selected:
            continue
        if key == "t2" and "t2" not in selected:
            continue
        if key == "arch" and "arch" not in selected:
            continue
        if key == "custom" and "custom" not in selected:
            continue
        details = report.get(key, {})
        parts.append(
            "%s +%d ~v%d ~r%d"
            % (
                key,
                len(details.get("added", [])),
                len(details.get("version_changed", [])),
                len(details.get("recipe_changed", [])),
            )
        )
    return "Synced: " + ", ".join(parts)


def _run_sync_with_progress(screen, app, selected):
    progress = {
        "phase": "start",
        "source": "",
        "message": "Starting sync",
        "tick": 0,
    }

    def callback(event):
        progress.update(event)
        progress["tick"] = progress.get("tick", 0) + 1
        _draw_sync_progress(screen, selected, progress)

    _draw_sync_progress(screen, selected, progress)
    return app.sync_selected_sources(selected, progress_callback=callback)


def _draw_sync_progress(screen, selected, progress, title="Syncing Package Databases"):
    _, _, start_y, start_x, content_height, content_width = _draw_layout(screen, {"queue": []}, "Queue Plan")
    selected_line = "Sources: %s" % (", ".join(_friendly_source_name(key) for key in sorted(selected)) or "none")
    source = progress.get("source", "") or "sync"
    message = progress.get("message", "") or progress.get("phase", "working")
    current = progress.get("current")
    total = progress.get("total")
    percent = progress.get("percent")
    screen.addnstr(start_y + 0, start_x, title, content_width - 1, _attr("title"))
    screen.addnstr(start_y + 1, start_x, selected_line, content_width - 1, _attr("normal"))
    screen.addnstr(start_y + 3, start_x, "Current source: %s" % source, content_width - 1, _attr("accent"))
    screen.addnstr(start_y + 4, start_x, message, content_width - 1, _attr("normal"))
    bar_width = max(10, min(50, content_width - 4))
    if total:
        filled = int((float(current or 0) / float(total)) * bar_width)
        bar = "[" + ("#" * filled).ljust(bar_width) + "]"
        detail = "%d/%d (%d%%)" % (int(current or 0), int(total), int(percent or 0))
    else:
        tick = progress.get("tick", 0)
        marker = tick % bar_width
        chars = [" "] * bar_width
        chars[marker] = ">"
        bar = "[" + "".join(chars) + "]"
        detail = "working"
    screen.addnstr(start_y + 6, start_x, bar, content_width - 1, _attr("selected"))
    screen.addnstr(start_y + 7, start_x, detail, content_width - 1, _attr("normal"))
    screen.refresh()


def _clone_phases(phases):
    return {key: list(value) for key, value in (phases or {}).items()}


def _phase_names(phases):
    ordered = [name for name in PHASE_ORDER if phases.get(name)]
    extras = sorted(name for name in phases if name not in PHASE_ORDER and phases.get(name))
    names = ordered + extras
    if not names:
        return list(PHASE_ORDER)
    return names


def _set_phase_commands(phases, phase_name, commands):
    cleaned = [item.strip() for item in commands if item.strip()]
    if cleaned:
        phases[phase_name] = cleaned
    elif phase_name in phases:
        del phases[phase_name]


def _merge_queue_plan(state, plan, root_package, dependency_mode, dependency_level, dependency_sources):
    root_key = (root_package.name, root_package.source_origin)
    plan_items = []
    for step in plan.ordered_steps:
        package = step.package
        key = (package.name, package.source_origin)
        requested = key == root_key
        plan_items.append(
            {
                "key": key,
                "item": {
                    "name": package.name,
                    "source_origin": package.source_origin,
                    "version": package.version,
                    "summary": package.summary,
                    "category": package.category,
                    "selected": requested,
                    "dependency_mode": dependency_mode,
                    "dependency_level": dependency_level,
                    "dependency_sources": list(dependency_sources or []),
                },
            }
        )

    queue = state["queue"]
    for index, payload in enumerate(plan_items):
        key = payload["key"]
        item = payload["item"]
        existing_index = _find_queue_index(queue, key)
        if existing_index >= 0:
            previous = queue[existing_index]
            merged = dict(previous)
            merged.update(item)
            merged["selected"] = previous.get("selected", False) or item.get("selected", False)
            if not item.get("selected"):
                merged["dependency_mode"] = previous.get("dependency_mode", dependency_mode)
                merged["dependency_level"] = previous.get("dependency_level", dependency_level)
                merged["dependency_sources"] = list(previous.get("dependency_sources", dependency_sources or []))
            queue[existing_index] = merged
            continue

        insert_at = 0
        for prior in plan_items[:index]:
            prior_index = _find_queue_index(queue, prior["key"])
            if prior_index >= 0:
                insert_at = max(insert_at, prior_index + 1)
        queue.insert(insert_at, item)


def _queue_to_plan(app, state):
    steps = []
    requested = []
    unresolved = []
    for item in state["queue"]:
        package = app.get_package(item["name"], item["source_origin"])
        if not package:
            unresolved.append("%s[%s]" % (item["name"], item["source_origin"]))
            continue
        steps.append(BuildStep(package=package))
        if item.get("selected"):
            requested.append(package.name)
    return BuildPlan(requested=requested, ordered_steps=steps, unresolved=unresolved, conflicts=[])


def _find_queue_index(queue, key):
    for index, item in enumerate(queue):
        if (item["name"], item["source_origin"]) == key:
            return index
    return -1


def _lookup_package(state, app, name, source_origin):
    cache = state.get("package_by_key", {})
    package = cache.get((name, source_origin))
    if package:
        return package
    return app.get_package(name, source_origin)


def _new_plan_keys(plan, existing_keys):
    return {
        (step.package.name, step.package.source_origin)
        for step in plan.ordered_steps
        if (step.package.name, step.package.source_origin) not in existing_keys
    }


def _reposition_new_items_for_existing_roots(app, state, new_root_package, new_keys):
    root_key = (new_root_package.name, new_root_package.source_origin)
    if not new_keys or root_key not in new_keys:
        return
    root_items = [
        item
        for item in state["queue"]
        if item.get("selected") and (item["name"], item["source_origin"]) != root_key
    ]
    for root_item in root_items:
        for candidate_level in _broader_dependency_levels(root_item.get("dependency_level", "required")):
            include_recommends, auto_optional = _dependency_flags(candidate_level)
            plan = app.plan_selection(
                root_item["name"],
                root_item["source_origin"],
                include_recommends=include_recommends,
                auto_optional=auto_optional,
                resolve_required=True,
                t2_dependency_mode=root_item.get("dependency_mode", "blfs"),
                allowed_dependency_sources=root_item.get("dependency_sources"),
            )
            ordered_keys = [(step.package.name, step.package.source_origin) for step in plan.ordered_steps]
            if root_key not in ordered_keys:
                continue
            moving_keys = [key for key in ordered_keys if key in new_keys]
            if not moving_keys:
                continue
            _reorder_queue_subset_by_plan(state["queue"], moving_keys, ordered_keys)
            return


def _reorder_queue_subset_by_plan(queue, moving_keys, ordered_keys):
    moving_set = set(moving_keys)
    item_by_key = {}
    remaining = []
    for item in queue:
        key = (item["name"], item["source_origin"])
        if key in moving_set:
            item_by_key[key] = item
        else:
            remaining.append(item)
    for key in [entry for entry in ordered_keys if entry in moving_set]:
        insert_at = _queue_insert_position_from_plan(remaining, ordered_keys, key)
        remaining.insert(insert_at, item_by_key[key])
    queue[:] = remaining


def _queue_insert_position_from_plan(queue, ordered_keys, target_key):
    target_index = ordered_keys.index(target_key)
    insert_after = None
    insert_before = None
    for key in ordered_keys[:target_index]:
        queue_index = _find_queue_index(queue, key)
        if queue_index >= 0:
            insert_after = queue_index + 1
    for key in ordered_keys[target_index + 1:]:
        queue_index = _find_queue_index(queue, key)
        if queue_index >= 0:
            insert_before = queue_index
            break
    if insert_after is not None:
        if insert_before is not None:
            return min(insert_after, insert_before)
        return insert_after
    if insert_before is not None:
        return insert_before
    return len(queue)


def _remove_queue_item(screen, app, state, index):
    item = state["queue"][index]
    if not item.get("selected"):
        removed = state["queue"].pop(index)
        return "Removed dependency entry %s[%s]" % (removed["name"], removed["source_origin"])
    prune_orphans = _yes_no(
        screen,
        "Prune orphaned dependencies after removing %s?" % item["name"],
        True,
    )
    remaining_roots = [entry for pos, entry in enumerate(state["queue"]) if entry.get("selected") and pos != index]
    if not remaining_roots:
        state["queue"] = []
        return "Removed %s and cleared queue" % item["name"]
    if prune_orphans:
        _rebuild_queue_from_roots(app, state, remaining_roots)
        return "Removed %s and pruned orphaned dependencies" % item["name"]
    state["queue"].pop(index)
    return "Removed %s from queue" % item["name"]


def _rebuild_queue_from_roots(app, state, roots):
    new_queue = []
    temp_state = {"queue": new_queue}
    for root in roots:
        package = _lookup_package(state, app, root["name"], root["source_origin"])
        if not package:
            continue
        include_recommends, auto_optional = _dependency_flags(root.get("dependency_level", "required"))
        plan = app.plan_selection(
            root["name"],
            root["source_origin"],
            include_recommends=include_recommends,
            auto_optional=auto_optional,
            resolve_required=True,
            t2_dependency_mode=root.get("dependency_mode", "blfs"),
            allowed_dependency_sources=root.get("dependency_sources"),
        )
        _merge_queue_plan(
            temp_state,
            plan,
            package,
            root.get("dependency_mode", "blfs"),
            root.get("dependency_level", "required"),
            root.get("dependency_sources", _default_dependency_sources(package)),
        )
    state["queue"] = temp_state["queue"]


def _format_setting_value(value, kind):
    if kind == "bool":
        return "yes" if value else "no"
    if kind == "csv":
        return ", ".join(value or []) or "-"
    return str(value) if value not in ("", None) else "-"


def _format_lfs_setting_value(value, kind):
    if kind == "bool":
        return "yes" if value else "no"
    return str(value) if value not in ("", None) else "-"


def _next_choice(options, current):
    if current not in options:
        return options[0]
    index = options.index(current)
    return options[(index + 1) % len(options)]


def _format_lfs_target_triplet(settings):
    override = (settings.get("triplet_override", "") or "").strip()
    if override:
        return override
    vendor = (settings.get("target_vendor", "lfs") or "lfs").strip()
    return "$(uname -m)-%s-linux-gnu" % vendor


def _default_dependency_level(settings, package):
    build = settings["build"]
    configured = build.get("default_dependency_level", "")
    if configured in DEPENDENCY_LEVELS:
        return configured
    if build.get("auto_optional_deps", False):
        return "optional"
    if build.get("include_recommends", False):
        return "recommended"
    if package.source_origin == "blfs":
        return "recommended"
    return "required"


def _dependency_flags(level):
    if level == "optional":
        return True, True
    if level == "recommended":
        return True, False
    return False, False


def _default_dependency_sources(package):
    return [source for source, _ in DEPENDENCY_SOURCE_LABELS]


def _broader_dependency_levels(level):
    if level == "required":
        return ["recommended", "optional"]
    if level == "recommended":
        return ["optional"]
    return []


def _package_provider(package):
    if not package:
        return {}
    return package.metadata.get("build_provider", {})


def _provider_member_names(package):
    provider = _package_provider(package)
    return list(provider.get("members", [])) if provider else []


def _provider_label(provider):
    if not provider:
        return ""
    return "%s[%s]" % (provider.get("name", ""), provider.get("source_origin", ""))


def _provider_preview_lines(plan):
    grouped = {}
    for step in plan.ordered_steps:
        package = step.package
        provider = package.metadata.get("build_provider", {})
        if not provider:
            continue
        key = (provider.get("name", ""), provider.get("source_origin", package.source_origin))
        grouped.setdefault(key, []).append(package.name)
    lines = []
    for (name, source_origin), members in sorted(grouped.items()):
        lines.append("%s[%s] for %s" % (name, source_origin, ", ".join(members[:5]) + (" ..." if len(members) > 5 else "")))
    return lines


def _history_detail_summary(detail):
    if not detail:
        return ""
    try:
        payload = json.loads(detail)
    except ValueError:
        return detail
    parts = []
    if payload.get("build_provider"):
        parts.append("provider=%s" % payload["build_provider"])
    if payload.get("artifact"):
        parts.append("artifact")
    if payload.get("handling"):
        parts.append(payload["handling"])
    return " ".join(parts) if parts else ""


def _init_colors(screen):
    if not curses.has_colors():
        return
    curses.start_color()
    curses.use_default_colors()
    curses.init_pair(COLOR_BASE, curses.COLOR_WHITE, curses.COLOR_BLUE)
    curses.init_pair(COLOR_SELECTED, curses.COLOR_BLACK, curses.COLOR_WHITE)
    curses.init_pair(COLOR_ACCENT, curses.COLOR_CYAN, curses.COLOR_BLUE)
    curses.init_pair(COLOR_PANEL, curses.COLOR_WHITE, curses.COLOR_BLACK)
    screen.bkgd(" ", curses.color_pair(COLOR_BASE))


def _attr(name):
    if not curses.has_colors():
        if name == "title":
            return curses.A_BOLD
        if name == "selected":
            return curses.A_REVERSE
        return 0
    mapping = {
        "title": curses.color_pair(COLOR_BASE) | curses.A_BOLD,
        "normal": curses.color_pair(COLOR_BASE),
        "selected": curses.color_pair(COLOR_SELECTED),
        "accent": curses.color_pair(COLOR_ACCENT) | curses.A_BOLD,
        "panel": curses.color_pair(COLOR_PANEL),
    }
    return mapping.get(name, curses.color_pair(COLOR_BASE))


def _draw_layout(screen, state, queue_title, selected_queue_index=-1):
    screen.erase()
    height, width = screen.getmaxyx()
    pane_width = 0
    content_left = 0
    content_width = width
    if width >= 100:
        pane_width = min(40, max(30, width // 3))
        content_left = pane_width + 1
        content_width = width - content_left
        _draw_box(screen, 0, 0, height, pane_width, queue_title)
        _draw_queue_sidebar(screen, state, 1, 2, height - 2, max(1, pane_width - 4), queue_title, selected_queue_index)
    _draw_box(screen, 0, content_left, height, content_width, "Workspace")
    start_y = 1
    start_x = content_left + 2
    inner_height = max(1, height - 2)
    inner_width = max(1, content_width - 4)
    return height, width, start_y, start_x, inner_height, inner_width


def _draw_queue_sidebar(screen, state, top, left, height, width, title, selected_queue_index=-1):
    if height <= 0 or width <= 0:
        return
    screen.addnstr(top, left, "selected install order", width - 1, _attr("accent"))
    if not state["queue"]:
        if height > 2:
            screen.addnstr(top + 2, left, "Queue is empty", width - 1, _attr("normal"))
        return
    start = max(0, selected_queue_index - (height - 4)) if selected_queue_index >= 0 else 0
    visible = state["queue"][start : start + max(1, height - 2)]
    for row, item in enumerate(visible, start=top + 2):
        current = start + row - (top + 2)
        marker = "*" if item.get("selected") else "-"
        line = "%2d %s %-16s [%s]" % (
            current + 1,
            marker,
            item["name"][:16],
            item["source_origin"][:6],
        )
        attr = _attr("selected") if current == selected_queue_index else _attr("panel")
        screen.addnstr(row, left, line, width - 1, attr)


def _split_workspace(start_x, content_width):
    if content_width < 70:
        return content_width, start_x + content_width, 0
    left_width = max(24, min(36, content_width // 3))
    detail_x = start_x + left_width + 2
    detail_width = max(1, content_width - left_width - 2)
    return left_width, detail_x, detail_width


def _draw_vertical_rule(screen, top, x, height):
    if height <= 0:
        return
    vert = curses.ACS_VLINE if hasattr(curses, "ACS_VLINE") else ord("|")
    for row in range(top, top + height):
        _safe_addch(screen, row, x, vert, _attr("accent"))


def _draw_queue_detail(screen, state, item, start_y, start_x, content_height, content_width):
    package = state.get("package_by_key", {}).get((item["name"], item["source_origin"]))
    provider = _package_provider(package) if package else {}
    provider_members = _provider_member_names(package) if package else []
    lines = [
        "Queue Item",
        "",
        "Name: %s" % item["name"],
        "Version: %s" % item["version"],
        "Source: %s" % item["source_origin"],
        "Role: %s" % ("selected root" if item.get("selected") else "dependency"),
        "Category: %s" % item.get("category", "-"),
        "Dependency mode: %s" % item.get("dependency_mode", "-"),
        "Dependency level: %s" % item.get("dependency_level", "-"),
        "Dependency sources: %s" % (", ".join(item.get("dependency_sources", [])) or "-"),
        "Build provider: %s" % (_provider_label(provider) if provider else "standalone"),
        "Provider members: %s" % (", ".join(provider_members[:4]) + (" ..." if len(provider_members) > 4 else "") if provider_members else "-"),
        "",
        "Summary:",
        item.get("summary", ""),
    ]
    if package:
        lines.extend(
            [
                "",
                "Depends: %s" % (", ".join(package.depends) or "-"),
                "Recommends: %s" % (", ".join(package.recommends) or "-"),
                "Optional: %s" % (", ".join(package.optional) or "-"),
            ]
        )
    for offset, line in enumerate(lines[:content_height]):
        screen.addnstr(start_y + offset, start_x, line, content_width - 1, _attr("title" if offset == 0 else "panel"))


def _draw_box(screen, top, left, height, width, title=""):
    max_y, max_x = screen.getmaxyx()
    if height < 2 or width < 2 or top >= max_y or left >= max_x:
        return
    bottom = min(max_y - 1, top + height - 1)
    right = min(max_x - 1, left + width - 1)
    if bottom <= top or right <= left:
        return
    horiz = curses.ACS_HLINE if hasattr(curses, "ACS_HLINE") else ord("-")
    vert = curses.ACS_VLINE if hasattr(curses, "ACS_VLINE") else ord("|")
    tl = curses.ACS_ULCORNER if hasattr(curses, "ACS_ULCORNER") else ord("+")
    tr = curses.ACS_URCORNER if hasattr(curses, "ACS_URCORNER") else ord("+")
    bl = curses.ACS_LLCORNER if hasattr(curses, "ACS_LLCORNER") else ord("+")
    br = curses.ACS_LRCORNER if hasattr(curses, "ACS_LRCORNER") else ord("+")
    for x in range(left + 1, right):
        _safe_addch(screen, top, x, horiz, _attr("accent"))
        _safe_addch(screen, bottom, x, horiz, _attr("accent"))
    for y in range(top + 1, bottom):
        _safe_addch(screen, y, left, vert, _attr("accent"))
        _safe_addch(screen, y, right, vert, _attr("accent"))
    _safe_addch(screen, top, left, tl, _attr("accent"))
    _safe_addch(screen, top, right, tr, _attr("accent"))
    _safe_addch(screen, bottom, left, bl, _attr("accent"))
    _safe_addch(screen, bottom, right, br, _attr("accent"))
    if title:
        label = " %s " % title
        _safe_addnstr(screen, top, min(right - 1, left + 2), label, max(0, right - left - 3), _attr("title"))


def _safe_addch(screen, y, x, ch, attr=0):
    try:
        screen.addch(y, x, ch, attr)
    except curses.error:
        return


def _safe_addnstr(screen, y, x, text, width, attr=0):
    try:
        screen.addnstr(y, x, text, width, attr)
    except curses.error:
        return

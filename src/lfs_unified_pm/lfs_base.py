from __future__ import annotations

import os
import re
import shutil
import subprocess
import shlex
from glob import glob
from datetime import datetime
from dataclasses import asdict

from .source_fetch import is_remote_source, source_stage_commands
from .git_source import DEFAULT_JHALFS_GIT_URL, DEFAULT_LFS_GIT_URL, GitSourceManager
from .guarded_ops import GuardedOpDeclined, GuardedOperationRunner
from .models import LfsBuildPlan, LfsBuildStep


STAGE_ORDER = {
    "host-root-prep": 0,
    "lfs-user": 1,
    "host-root-kernfs": 2,
    "host-root-chroot": 3,
    "chroot-root": 4,
    "host-root-teardown": 5,
}

SENSITIVE_HOST_PATHS = [
    "/usr",
    "/etc",
    "/var",
    "/lib64",
    "/tools",
    "/bin/bin",
    "/lib/lib",
    "/sbin/sbin",
    "/etc/passwd",
    "/etc/group",
    "/etc/shadow",
    "/etc/gshadow",
    "/home/lfs",
    "/home/lfs/.bash_profile",
    "/home/lfs/.bashrc",
]
LfsExecutionDeclined = GuardedOpDeclined


class LfsBaseBuilder:
    def __init__(self, config, settings):
        self.config = config
        self.settings = settings
        self.jhalfs_manager = GitSourceManager()

    def plan(self, progress_callback=None):
        book_root = self._ensure_book_source(progress_callback=progress_callback)
        jhalfs_root = self._ensure_jhalfs_source(progress_callback=progress_callback)
        work_root = os.path.join(self.config.work_dir, "lfs-base")
        commands_root = os.path.join(work_root, "commands")
        profiled_book = os.path.join(work_root, "prbook.xml")
        self._render_scripts(book_root, jhalfs_root, work_root, profiled_book, commands_root, progress_callback=progress_callback)
        target_triplet = self._target_triplet()
        self._rewrite_target_triplet(commands_root, target_triplet)
        self._make_host_setup_idempotent(commands_root)
        self._inject_lfs_guards(commands_root)
        source_entries = self._extract_source_entries(profiled_book, jhalfs_root, progress_callback=progress_callback)
        steps = _collect_steps(commands_root)
        stage_scripts = _stage_scripts(steps)
        return LfsBuildPlan(
            book_root=book_root,
            profiled_book=profiled_book,
            commands_root=commands_root,
            target_triplet=target_triplet,
            source_entries=source_entries,
            steps=steps,
            stage_scripts=stage_scripts,
        )

    def export_scripts(self, plan, output_dir=""):
        output_dir = _next_output_dir(os.path.abspath(output_dir or self.settings.get("script_output_dir", "./generated-lfs-base")))
        shutil.copytree(plan.commands_root, output_dir)
        manifest_path = os.path.join(output_dir, "plan.json")
        with open(manifest_path, "w", encoding="utf-8") as handle:
            import json

            json.dump(
                {
                    "book_root": plan.book_root,
                    "profiled_book": plan.profiled_book,
                    "commands_root": output_dir,
                    "target_triplet": plan.target_triplet,
                    "source_entries": plan.source_entries,
                    "steps": [asdict(step) for step in plan.steps],
                    "stage_scripts": plan.stage_scripts,
                },
                handle,
                indent=2,
                sort_keys=True,
            )
        self._write_stage_drivers(output_dir, plan)
        self._write_fetch_sources_script(output_dir, plan)
        return output_dir

    def _ensure_book_source(self, progress_callback=None):
        source_mode = self.settings.get("book_source", "git")
        if source_mode == "local":
            book_root = os.path.abspath(self.settings.get("local_book_path", ""))
            if not os.path.isdir(book_root):
                raise FileNotFoundError("LFS local book path not found: %s" % book_root)
            _emit_progress(progress_callback, "lfs-base", "source-tree", "Using local LFS book source")
            return book_root
        local = _find_existing_dir("lfs-git", cwd=os.getcwd(), app_file=__file__)
        if local:
            _emit_progress(progress_callback, "lfs-base", "source-tree", "Using local LFS book source tree")
            return local
        repo_dir = os.path.join(self.config.source_trees_dir, "lfs-git")
        _emit_progress(progress_callback, "lfs-base", "source-tree", "Fetching LFS book source tree")
        self.jhalfs_manager.sync_repo(
            repo_dir,
            repo_url=self.settings.get("book_git_url", DEFAULT_LFS_GIT_URL) or DEFAULT_LFS_GIT_URL,
            branch=self.settings.get("book_commit", "") or "",
        )
        return repo_dir

    def _ensure_jhalfs_source(self, progress_callback=None):
        local = _find_existing_dir("jhalfs", cwd=os.getcwd(), app_file=__file__)
        if local:
            _emit_progress(progress_callback, "lfs-base", "source-tree", "Using local jhalfs source tree")
            return local
        repo_dir = os.path.join(self.config.source_trees_dir, "jhalfs")
        _emit_progress(progress_callback, "lfs-base", "source-tree", "Fetching jhalfs source tree")
        sync_settings_url = self.settings.get("jhalfs_git_url", "") or DEFAULT_JHALFS_GIT_URL
        self.jhalfs_manager.sync_repo(repo_dir, repo_url=sync_settings_url)
        return repo_dir

    def _render_scripts(self, book_root, jhalfs_root, work_root, profiled_book, commands_root, progress_callback=None):
        os.makedirs(work_root, exist_ok=True)
        if os.path.isdir(commands_root):
            shutil.rmtree(commands_root)
        os.makedirs(commands_root)
        init_system = self.settings.get("init_system", "systemd")
        multilib = self.settings.get("multilib", "default")
        jobs = str(int(self.settings.get("jobs", 1) or 1))
        jobs_bp1 = str(int(self.settings.get("jobs_binutils_pass1", 1) or 1))
        testsuite = _testsuite_value(self.settings.get("testsuite", "none"))
        pkgmngt_wrap = _pkgmngt_wrap_value(self.settings.get("package_management", "none"))
        book_process_scripts = os.path.join(book_root, "process-scripts.sh")
        book_git_version = os.path.join(book_root, "git-version.sh")
        if os.path.isfile(book_process_scripts):
            _emit_progress(progress_callback, "lfs-base", "render", "Preparing LFS scripted pages")
            _run(["bash", book_process_scripts], cwd=book_root)
        if os.path.isfile(book_git_version):
            _emit_progress(progress_callback, "lfs-base", "render", "Generating LFS version entities")
            _run(["bash", book_git_version, init_system], cwd=book_root)
        _emit_progress(progress_callback, "lfs-base", "render", "Profiling LFS book")
        _run(
            [
                "xsltproc",
                "--nonet",
                "--xinclude",
                "--stringparam",
                "profile.revision",
                init_system,
                "--stringparam",
                "profile.arch",
                multilib,
                "--output",
                profiled_book,
                os.path.join(book_root, "stylesheets", "lfs-xsl", "profile.xsl"),
                os.path.join(book_root, "index.xml"),
            ]
        )
        _emit_progress(progress_callback, "lfs-base", "render", "Extracting chapter scripts")
        _run(
            [
                "xsltproc",
                "--nonet",
                "--stringparam",
                "luser-lgroup",
                "%s:%s" % (self.settings.get("luser", "lfs"), self.settings.get("lgroup", "lfs")),
                "--stringparam",
                "testsuite",
                testsuite,
                "--stringparam",
                "keepdir",
                _yn(self.settings.get("keep_build_dirs", False)),
                "--stringparam",
                "ncurses5",
                _yn(self.settings.get("install_ncurses5", False)),
                "--stringparam",
                "strip",
                _yn(self.settings.get("strip_binaries", False)),
                "--stringparam",
                "del-la-files",
                _yn(self.settings.get("remove_la_files", False)),
                "--stringparam",
                "full-locale",
                _yn(self.settings.get("full_locale", False)),
                "--stringparam",
                "timezone",
                self.settings.get("timezone", "GMT"),
                "--stringparam",
                "page",
                self.settings.get("page_size", "A4").lower(),
                "--stringparam",
                "lang",
                self.settings.get("lang", "C"),
                "--stringparam",
                "pkgmngt-wrap",
                pkgmngt_wrap,
                "--stringparam",
                "hostname",
                self.settings.get("hostname", "lfs"),
                "--stringparam",
                "interface",
                self.settings.get("interface", "eth0"),
                "--stringparam",
                "ip",
                self.settings.get("ip_address", "10.0.2.9"),
                "--stringparam",
                "gateway",
                self.settings.get("gateway", "10.0.2.2"),
                "--stringparam",
                "prefix",
                str(self.settings.get("subnet_prefix", 24)),
                "--stringparam",
                "broadcast",
                self.settings.get("broadcast", "10.0.2.255"),
                "--stringparam",
                "domain",
                self.settings.get("domain", "local"),
                "--stringparam",
                "nameserver1",
                self.settings.get("nameserver1", "10.0.2.3"),
                "--stringparam",
                "nameserver2",
                self.settings.get("nameserver2", "8.8.8.8"),
                "--stringparam",
                "console",
                "%s@%s:%s/%s"
                % (
                    self.settings.get("console_font", "lat0-16"),
                    self.settings.get("console_keymap", "us"),
                    _yn(self.settings.get("clock_localtime", False)),
                    str(self.settings.get("log_level", 4)),
                ),
                "--stringparam",
                "script-root",
                "lfs-base",
                "--stringparam",
                "jobs",
                jobs,
                "--stringparam",
                "jobs-bp1",
                jobs_bp1,
                "--stringparam",
                "test-mismatch",
                "n",
                "--output",
                commands_root + "/",
                os.path.join(jhalfs_root, "LFS", "lfs.xsl"),
                profiled_book,
            ]
        )
        _emit_progress(progress_callback, "lfs-base", "render", "Extracting chroot scripts")
        _run(
            [
                "xsltproc",
                "--nonet",
                "--xinclude",
                "--stringparam",
                "jobs_2",
                jobs,
                "-o",
                os.path.join(commands_root, "chroot-scripts") + "/",
                os.path.join(jhalfs_root, "common", "chroot.xsl"),
                profiled_book,
            ]
        )
        kernfs_dir = os.path.join(commands_root, "kernfs-scripts")
        os.makedirs(kernfs_dir, exist_ok=True)
        _emit_progress(progress_callback, "lfs-base", "render", "Extracting kernel filesystem scripts")
        _run(
            [
                "xsltproc",
                "--nonet",
                "-o",
                os.path.join(kernfs_dir, "devices.sh"),
                os.path.join(jhalfs_root, "common", "kernfs-devices.xsl"),
                profiled_book,
            ]
        )
        _run(
            [
                "xsltproc",
                "--nonet",
                "-o",
                os.path.join(kernfs_dir, "teardown.sh"),
                os.path.join(jhalfs_root, "common", "kernfs-teardown.xsl"),
                profiled_book,
            ]
        )
        for root, _, files in os.walk(commands_root):
            for name in files:
                os.chmod(os.path.join(root, name), 0o755)

    def _target_triplet(self):
        override = (self.settings.get("triplet_override", "") or "").strip()
        if override:
            return override
        vendor = (self.settings.get("target_vendor", "lfs") or "lfs").strip()
        return "$(uname -m)-%s-linux-gnu" % vendor

    def _rewrite_target_triplet(self, commands_root, target_triplet):
        for root, _, files in os.walk(commands_root):
            for name in files:
                path = os.path.join(root, name)
                try:
                    with open(path, "r", encoding="utf-8") as handle:
                        text = handle.read()
                except OSError:
                    continue
                updated = re.sub(
                    r"(\bLFS_TGT=)\$\(uname -m\)-lfs-linux-gnu",
                    r"\1%s" % target_triplet,
                    text,
                )
                if updated != text:
                    with open(path, "w", encoding="utf-8") as handle:
                        handle.write(updated)

    def _make_host_setup_idempotent(self, commands_root):
        creating_layout = os.path.join(commands_root, "chapter04", "401-creatingminlayout")
        if os.path.isfile(creating_layout):
            with open(creating_layout, "r", encoding="utf-8") as handle:
                text = handle.read()
            original = """for i in bin lib sbin; do
  ln -sv usr/$i $LFS/$i
done"""
            replacement = """for i in bin lib sbin; do
  if [ -L "$LFS/$i" ]; then
    ln -svf usr/$i "$LFS/$i"
  elif [ ! -e "$LFS/$i" ]; then
    ln -sv usr/$i "$LFS/$i"
  else
    echo "Keeping existing $LFS/$i"
  fi
done"""
            updated = text.replace(original, replacement)
            if updated != text:
                with open(creating_layout, "w", encoding="utf-8") as handle:
                    handle.write(updated)
        settingenv = os.path.join(commands_root, "chapter04", "403-settingenvironment")
        if os.path.isfile(settingenv):
            with open(settingenv, "r", encoding="utf-8") as handle:
                text = handle.read()
            updated = text.replace("source ~/.bash_profile\n", 'echo "Prepared ~/.bash_profile and ~/.bashrc for lfs"\n')
            build_root = _resolve_lfs_build_root(self.config, self.settings)
            updated = updated.replace("LFS=/mnt/lfs", "LFS=%s" % build_root)
            if updated != text:
                with open(settingenv, "w", encoding="utf-8") as handle:
                    handle.write(updated)
        addinguser = os.path.join(commands_root, "chapter04", "402-addinguser")
        if not os.path.isfile(addinguser):
            return
        with open(addinguser, "r", encoding="utf-8") as handle:
            text = handle.read()
        original = """groupadd lfs
useradd -s /bin/bash -g lfs -m -k /dev/null lfs
chown -v lfs $LFS/{usr{,/*},var,etc,tools}"""
        replacement = """getent group lfs >/dev/null || groupadd lfs
id -u lfs >/dev/null 2>&1 || useradd -s /bin/bash -g lfs -m -k /dev/null lfs
chown -v lfs:lfs $LFS/{usr{,/*},var,etc,tools}"""
        updated = text.replace(original, replacement)
        updated = updated.replace(
            "x86_64) chown -v lfs $LFS/lib64 ;;",
            "x86_64) [ -e $LFS/lib64 ] && chown -v lfs:lfs $LFS/lib64 ;;",
        )
        if updated != text:
            with open(addinguser, "w", encoding="utf-8") as handle:
                handle.write(updated)

    def _inject_lfs_guards(self, commands_root):
        guard = (
            'if [ -z "${LFS:-}" ]; then echo "Refusing to run without LFS set" >&2; exit 96; fi\n'
            'case "$LFS" in /|"") echo "Refusing unsafe LFS=$LFS" >&2; exit 97 ;; esac\n'
            'case "$LFS" in /*) ;; *) echo "Refusing non-absolute LFS=$LFS" >&2; exit 98 ;; esac\n'
            'export LFS="$LFS"\n'
            'mkdir -p "$LFS"\n'
        )
        for root, _, files in os.walk(commands_root):
            for name in files:
                path = os.path.join(root, name)
                relative = os.path.relpath(path, commands_root)
                if path.endswith("teardown.sh"):
                    continue
                try:
                    with open(path, "r", encoding="utf-8") as handle:
                        text = handle.read()
                except OSError:
                    continue
                if guard in text or not text.startswith("#!/bin/bash\n"):
                    continue
                context = _script_guard_context(relative)
                updated = text.replace("#!/bin/bash\n", "#!/bin/bash\n" + guard + _script_guard_shell(context), 1)
                if relative == os.path.join("chroot-scripts", "001-chroot"):
                    updated = re.sub(
                        r'chroot "\$LFS" /usr/bin/env -i\s+\\\n(?:.*\\\n)*\s*/bin/bash --login',
                        'chroot "$LFS" /usr/bin/env -i HOME=/root TERM="$TERM" PATH=/usr/bin:/usr/sbin /bin/bash -lc "echo chroot-ready"',
                        updated,
                        flags=re.MULTILINE,
                    )
                marker = (
                    'mkdir -p "$LFS/var/lib/lfs-pm/step-markers"\n'
                    'printf "ok\\n" > "$LFS/var/lib/lfs-pm/step-markers/%s.ok"\n'
                ) % _step_marker_name(relative)
                if "\nexit\n" in updated:
                    updated = updated.replace("\nexit\n", "\n" + marker + "exit\n")
                elif updated.endswith("\nexit"):
                    updated = updated[:-5] + "\n" + marker + "exit"
                else:
                    updated = updated + "\n" + marker
                with open(path, "w", encoding="utf-8") as handle:
                    handle.write(updated)

    def _write_stage_drivers(self, output_dir, plan):
        stages = [
            ("00-host-root-prep.sh", "host-root-prep"),
            ("01-lfs-user.sh", "lfs-user"),
            ("02-host-root-kernfs.sh", "host-root-kernfs"),
            ("03-host-root-chroot.sh", "host-root-chroot"),
            ("04-chroot-root.sh", "chroot-root"),
            ("05-host-root-teardown.sh", "host-root-teardown"),
        ]
        for filename, stage in stages:
            lines = [
                "#!/bin/bash",
                "set -e",
                "",
                '# Generated by RTAudioLinux LFS base planner',
                'ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"',
                'echo "Running stage: %s"' % stage,
            ]
            for relative in plan.stage_scripts.get(stage, []):
                lines.append('echo " -> %s"' % relative)
                lines.append('bash "$ROOT_DIR/%s"' % relative)
            lines.append("")
            with open(os.path.join(output_dir, filename), "w", encoding="utf-8") as handle:
                handle.write("\n".join(lines))
            os.chmod(os.path.join(output_dir, filename), 0o755)

    def _write_fetch_sources_script(self, output_dir, plan):
        archive_dir = self.settings.get("source_archive_dir", "/sources")
        lines = [
            "#!/bin/bash",
            "set -e",
            "",
            '# Generated by RTAudioLinux LFS base planner',
            "ARCHIVE_DIR=%s" % _shell_quote(archive_dir),
            'mkdir -p "$ARCHIVE_DIR"',
            "cd \"$ARCHIVE_DIR\"",
            "",
        ]
        lines.extend(source_stage_commands([entry["url"] for entry in plan.source_entries], build_dir_var="$ARCHIVE_DIR"))
        for entry in plan.source_entries:
            if not entry.get("md5"):
                continue
            filename = entry.get("filename", "")
            lines.append(
                'printf "%s  %s\\n" %s %s | md5sum -c -'
                % (
                    entry["md5"],
                    filename,
                    _shell_quote(entry["md5"]),
                    _shell_quote(filename),
                )
            )
        lines.append("")
        path = os.path.join(output_dir, "00-fetch-sources.sh")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("\n".join(lines))
        os.chmod(path, 0o755)

    def _extract_source_entries(self, profiled_book, jhalfs_root, progress_callback=None):
        urls_path = os.path.join(os.path.dirname(profiled_book), "urls.lst")
        revision = self.settings.get("init_system", "systemd")
        _emit_progress(progress_callback, "lfs-base", "render", "Extracting LFS source list")
        _run(
            [
                "xsltproc",
                "--nonet",
                "--stringparam",
                "revision",
                revision,
                "--output",
                urls_path,
                os.path.join(jhalfs_root, "common", "urls.xsl"),
                profiled_book,
            ]
        )
        entries = []
        seen = set()
        if not os.path.isfile(urls_path):
            return entries
        with open(urls_path, "r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                parts = line.split()
                if not parts:
                    continue
                url = parts[0]
                md5 = parts[1] if len(parts) > 1 else ""
                filename = os.path.basename(url.split("?", 1)[0])
                if not filename or filename in seen:
                    continue
                seen.add(filename)
                entries.append({"url": url, "md5": md5, "filename": filename})
        return entries


def _collect_steps(commands_root):
    steps = []
    order = 0
    for relative, stage in _ordered_script_paths(commands_root):
        order += 1
        script_path = os.path.join(commands_root, relative)
        parts = relative.split(os.sep)
        chapter = parts[0] if parts else ""
        name = os.path.basename(relative)
        description = name
        match = re.match(r"^\d+(?:-\d+)?-(.+)$", name)
        if match:
            description = match.group(1)
        steps.append(
            LfsBuildStep(
                name=description,
                chapter=chapter,
                stage=stage,
                order=order,
                script_path=script_path,
                relative_path=relative,
                description=description.replace("-", " "),
            )
        )
    return steps


def _ordered_script_paths(commands_root):
    paths = []
    for chapter in sorted(_chapter_dirs(commands_root)):
        for name in sorted(os.listdir(os.path.join(commands_root, chapter))):
            relative = os.path.join(chapter, name)
            stage = _stage_for_relative_path(relative)
            paths.append((relative, stage))
    kernfs_dir = os.path.join(commands_root, "kernfs-scripts")
    if os.path.isdir(kernfs_dir):
        for name in ("devices.sh",):
            if os.path.isfile(os.path.join(kernfs_dir, name)):
                paths.append((os.path.join("kernfs-scripts", name), "host-root-kernfs"))
    chroot_dir = os.path.join(commands_root, "chroot-scripts")
    if os.path.isdir(chroot_dir):
        for name in sorted(os.listdir(chroot_dir)):
            paths.append((os.path.join("chroot-scripts", name), "host-root-chroot"))
    for chapter in sorted(_chroot_chapters(commands_root)):
        for name in sorted(os.listdir(os.path.join(commands_root, chapter))):
            relative = os.path.join(chapter, name)
            if _chapter_key(chapter) < 7:
                continue
            paths.append((relative, "chroot-root"))
    if os.path.isdir(kernfs_dir) and os.path.isfile(os.path.join(kernfs_dir, "teardown.sh")):
        paths.append((os.path.join("kernfs-scripts", "teardown.sh"), "host-root-teardown"))
    unique = []
    seen = set()
    for relative, stage in paths:
        key = (relative, stage)
        if key in seen:
            continue
        seen.add(key)
        unique.append((relative, stage))
    return unique


def _chapter_dirs(commands_root):
    dirs = []
    for name in os.listdir(commands_root):
        full = os.path.join(commands_root, name)
        if os.path.isdir(full) and name.startswith("chapter"):
            if _chapter_key(name) < 7:
                dirs.append(name)
    return sorted(dirs, key=_chapter_key)


def _chroot_chapters(commands_root):
    dirs = []
    for name in os.listdir(commands_root):
        full = os.path.join(commands_root, name)
        if os.path.isdir(full) and name.startswith("chapter") and _chapter_key(name) >= 7:
            dirs.append(name)
    return sorted(dirs, key=_chapter_key)


def _chapter_key(name):
    match = re.search(r"chapter(\d+)", name)
    return int(match.group(1)) if match else 999


def _stage_for_relative_path(relative):
    lowered = relative.lower()
    chapter = lowered.split(os.sep, 1)[0]
    chapter_no = _chapter_key(chapter)
    if chapter_no >= 7:
        return "chroot-root"
    if "settingenvironment" in lowered:
        return "lfs-user"
    if chapter_no in (5, 6):
        return "lfs-user"
    return "host-root-prep"


def _stage_scripts(steps):
    mapping = {}
    for step in sorted(steps, key=lambda item: (STAGE_ORDER[item.stage], item.order)):
        mapping.setdefault(step.stage, []).append(step.relative_path)
    return mapping


def _testsuite_value(value):
    mapping = {
        "none": "0",
        "critical": "1",
        "all": "2",
    }
    return mapping.get(str(value or "none"), "0")


def _pkgmngt_wrap_value(value):
    mapping = {
        "none": "nn",
        "build-pack": "yn",
        "wrap-install": "yy",
    }
    return mapping.get(str(value or "none"), "nn")


def _yn(value):
    return "y" if value else "n"


def _run(command, cwd=""):
    subprocess.run(command, cwd=cwd or None, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _emit_progress(progress_callback, source, phase, message):
    if not progress_callback:
        return
    progress_callback(
        {
            "source": source,
            "phase": phase,
            "message": message,
        }
    )


def _find_existing_dir(*parts, **kwargs):
    for base in _candidate_source_roots(**kwargs):
        candidate = os.path.join(base, *parts)
        if os.path.isdir(candidate):
            return candidate
    for base in _candidate_source_roots(**kwargs):
        pattern = os.path.join(base, ".lfs-pm", "*", "state", "sources", *parts)
        for candidate in sorted(glob(pattern)):
            if os.path.isdir(candidate):
                return candidate
    return ""


def _candidate_source_roots(cwd="", app_file=""):
    cwd = os.path.abspath(cwd or os.getcwd())
    app_file = app_file or __file__
    project_root = os.path.abspath(os.path.join(os.path.dirname(app_file), "..", ".."))
    roots = []
    seen = set()
    for start in (cwd, project_root):
        current = start
        while current not in seen:
            roots.append(current)
            seen.add(current)
            parent = os.path.dirname(current)
            if parent == current:
                break
            current = parent
    return roots


def _next_output_dir(path):
    if not os.path.exists(path):
        return path
    for index in range(1, 1000):
        candidate = "%s-%d" % (path, index)
        if not os.path.exists(candidate):
            return candidate
    raise RuntimeError("No free LFS base export directory under %s" % path)


def _resolve_lfs_build_root(config, settings):
    raw = (settings.get("build_root", "") or "").strip()
    if raw:
        return os.path.abspath(raw)
    return os.path.abspath(os.path.join(os.path.dirname(config.root), "lfs-build-root"))


def _resolve_lfs_archive_dir(config, settings, build_root=""):
    build_root = build_root or _resolve_lfs_build_root(config, settings)
    raw = (settings.get("source_archive_dir", "") or "").strip()
    if not raw or raw == "/sources":
        return os.path.join(build_root, "sources")
    if os.path.isabs(raw):
        return raw
    return os.path.abspath(raw)


def _resolve_lfs_log_dir(config, settings, build_root=""):
    build_root = build_root or _resolve_lfs_build_root(config, settings)
    raw = (settings.get("log_dir", "") or "").strip()
    preferred = os.path.abspath(raw) if raw else os.path.join(build_root, "lfs-logs")
    if _is_writable_path(preferred):
        return preferred
    fallback = os.path.join(config.work_dir, "lfs-base-logs")
    os.makedirs(fallback, exist_ok=True)
    return fallback


class LfsBaseExecutor:
    def __init__(self, config, settings, store, root_approval_callback=None, execution_notice_callback=None):
        self.config = config
        self.settings = settings
        self.store = store
        self.root_approval_callback = root_approval_callback
        self.execution_notice_callback = execution_notice_callback
        self.guarded_ops = GuardedOperationRunner(
            root_approval_callback=root_approval_callback,
            execution_notice_callback=execution_notice_callback,
        )

    def fetch_sources(self, plan, progress_callback=None):
        build_root = _resolve_lfs_build_root(self.config, self.settings)
        archive_dir = _resolve_lfs_archive_dir(self.config, self.settings, build_root)
        log_dir = _resolve_lfs_log_dir(self.config, self.settings, build_root)
        fetch_log_path = os.path.join(log_dir, "fetch.log")
        target_sources = os.path.join(build_root, "sources")
        os.makedirs(archive_dir, exist_ok=True)
        os.makedirs(target_sources, exist_ok=True)
        os.makedirs(log_dir, exist_ok=True)
        _append_log(fetch_log_path, "[%s] Starting LFS source fetch into %s\n" % (_timestamp(), target_sources))
        total = len(plan.source_entries)
        for index, entry in enumerate(plan.source_entries, start=1):
            filename = entry["filename"]
            archive_target = os.path.join(archive_dir, filename)
            target = os.path.join(target_sources, filename)
            _emit_progress(
                progress_callback,
                "lfs-base",
                "fetch",
                "Fetching sources %d/%d: %s" % (index, total, filename),
            )
            _append_log(
                fetch_log_path,
                "[%s] %03d/%03d %s\n" % (_timestamp(), index, total, filename),
            )
            if not os.path.exists(archive_target):
                _download_entry(entry, archive_target)
            _verify_md5(archive_target, entry.get("md5", ""))
            if os.path.abspath(archive_target) != os.path.abspath(target):
                shutil.copy2(archive_target, target)
        state = self.store.get_lfs_base_state()
        state["sources_fetched"] = True
        state["source_count"] = total
        state["log_dir"] = log_dir
        state["fetch_log"] = fetch_log_path
        state["last_log"] = fetch_log_path
        self.store.save_lfs_base_state(state)
        _append_log(fetch_log_path, "[%s] Completed LFS source fetch\n" % _timestamp())
        return target_sources

    def execute(self, plan, progress_callback=None, resume=True, stop_after_stage="", fetch_sources=False, dry_run=False):
        build_root = _resolve_lfs_build_root(self.config, self.settings)
        log_dir = _resolve_lfs_log_dir(self.config, self.settings, build_root)
        step_log_dir = os.path.join(log_dir, "steps")
        master_log_path = os.path.join(log_dir, "build.log")
        preflight = self._preflight_environment(build_root, log_dir)
        self._require_safe_build_root(preflight)
        self._lint_plan(plan, build_root)
        if dry_run:
            if fetch_sources:
                raise RuntimeError("Dry run does not fetch sources")
            return self._dry_run(plan, build_root, progress_callback=progress_callback, preflight=preflight)
        os.makedirs(step_log_dir, exist_ok=True)
        if fetch_sources:
            self.fetch_sources(plan, progress_callback=progress_callback)
        self._prepare_target_root(build_root, master_log_path)
        commands_dest = os.path.join(build_root, "lfs-base")
        _sync_commands_into_root(
            plan.commands_root,
            commands_dest,
            env={"LFS": build_root},
            guarded_ops=self.guarded_ops,
        )
        state = self.store.get_lfs_base_state()
        completed = set(state.get("completed_steps", [])) if resume else set()
        last_order = int(state.get("last_order", 0)) if resume else 0
        if not resume:
            state = {}
            self.store.clear_lfs_base_state()
        state.update(
            {
                "build_root": build_root,
                "target_triplet": plan.target_triplet,
                "plan_steps": len(plan.steps),
                "completed_steps": sorted(completed),
                "last_order": last_order,
                "log_dir": log_dir,
                "master_log": master_log_path,
                "preflight": preflight,
            }
        )
        self.store.save_lfs_base_state(state)
        _append_log(master_log_path, "[%s] Starting LFS base execution in %s\n" % (_timestamp(), build_root))
        for note in preflight.get("notes", []):
            _append_log(master_log_path, "[%s] PREFLIGHT %s\n" % (_timestamp(), note))
        verified_steps = set(state.get("verified_steps", []))
        executed = []
        current_stage = ""
        for step in plan.steps:
            if step.stage != current_stage:
                self._precheck_stage_transition(plan, build_root, step.stage, verified_steps)
                current_stage = step.stage
            marker_path = _step_marker_path(build_root, step.relative_path)
            if os.path.isfile(marker_path):
                verified_steps.add(step.relative_path)
            if step.relative_path in verified_steps:
                completed.add(step.relative_path)
                last_order = max(last_order, step.order)
                continue
            if step.order <= last_order or step.relative_path in completed:
                continue
            step_log_path = _step_log_path(step_log_dir, step)
            before_snapshot = _capture_sensitive_snapshot()
            self._precheck_step(build_root, step)
            _emit_progress(progress_callback, "lfs-base", "execute", "Running %s" % step.relative_path)
            state["current_step"] = step.relative_path
            state["current_log"] = step_log_path
            self.store.save_lfs_base_state(state)
            _append_log(master_log_path, "[%s] START %03d %s\n" % (_timestamp(), step.order, step.relative_path))
            try:
                self._run_step(build_root, step, log_path=step_log_path, master_log_path=master_log_path)
            except Exception as error:
                state["failed"] = True
                state["failed_step"] = step.relative_path
                state["failure_log"] = step_log_path
                state["last_log"] = step_log_path
                state["error"] = str(error)
                self.store.save_lfs_base_state(state)
                _append_log(master_log_path, "[%s] FAIL %03d %s: %s\n" % (_timestamp(), step.order, step.relative_path, error))
                raise
            _verify_step_state(build_root, step, marker_path, before_snapshot, target_triplet=plan.target_triplet)
            completed.add(step.relative_path)
            verified_steps.add(step.relative_path)
            last_order = step.order
            state["completed_steps"] = sorted(completed)
            state["verified_steps"] = sorted(verified_steps)
            state["last_order"] = last_order
            state["last_step"] = step.relative_path
            state["last_log"] = step_log_path
            state["current_log"] = ""
            state["current_step"] = ""
            state["failed"] = False
            state.pop("failed_step", None)
            state.pop("failure_log", None)
            state.pop("error", None)
            self.store.save_lfs_base_state(state)
            _append_log(master_log_path, "[%s] DONE %03d %s\n" % (_timestamp(), step.order, step.relative_path))
            executed.append(step.relative_path)
            if stop_after_stage and step.stage == stop_after_stage:
                break
        state["complete"] = len(completed) == len(plan.steps)
        state["current_log"] = ""
        state["current_step"] = ""
        self.store.save_lfs_base_state(state)
        _append_log(master_log_path, "[%s] LFS base execution finished\n" % _timestamp())
        return executed

    def _dry_run(self, plan, build_root, progress_callback=None, preflight=None):
        preflight = preflight or self._preflight_environment(build_root, _resolve_lfs_log_dir(self.config, self.settings, build_root))
        self._lint_plan(plan, build_root)
        previewed = []
        current_stage = ""
        for step in plan.steps:
            if step.stage != current_stage:
                self._precheck_stage_transition(plan, build_root, step.stage, set(), allow_missing_markers=True)
                current_stage = step.stage
            _emit_progress(progress_callback, "lfs-base", "dry-run", "Previewing %s" % step.relative_path)
            for payload in _step_notice_payloads(build_root, step, self.settings):
                _emit_execution_notice(self.execution_notice_callback, payload)
            previewed.append(step.relative_path)
        return previewed

    def _preflight_environment(self, build_root, log_dir):
        notes = []
        lfs_uid = _lookup_user("lfs")
        lfs_gid = _lookup_group("lfs")
        if lfs_gid is not None:
            notes.append("Reusing existing host group 'lfs' (gid=%s)" % lfs_gid)
        if lfs_uid is not None:
            notes.append("Reusing existing host user 'lfs' (uid=%s)" % lfs_uid)
        commands_dest = os.path.join(build_root, "lfs-base")
        if os.path.isdir(commands_dest) and not _is_tree_writable(commands_dest):
            notes.append("Existing %s is not writable by the current user; root-assisted cleanup will be used" % commands_dest)
        if log_dir.startswith(build_root) and not _is_writable_path(log_dir):
            notes.append("Build-root log directory is not writable; falling back to sidecar logs")
        if os.path.isdir(build_root):
            notes.append("Using existing build root %s" % build_root)
        else:
            notes.append("Build root %s will be created during execution" % build_root)
        real_build_root = os.path.realpath(build_root)
        unsafe = real_build_root == "/" or real_build_root == os.path.realpath(os.sep)
        if unsafe:
            notes.append("Unsafe build root resolves to /")
        return {
            "notes": notes,
            "build_root": build_root,
            "real_build_root": real_build_root,
            "unsafe_build_root": unsafe,
        }

    def _require_safe_build_root(self, preflight):
        if preflight.get("unsafe_build_root", False):
            raise RuntimeError("Refusing to run LFS build with build root resolving to /")

    def _lint_plan(self, plan, build_root):
        if not os.path.isdir(plan.commands_root):
            raise RuntimeError("Missing generated command tree: %s" % plan.commands_root)
        issues = []
        for step in plan.steps:
            if _is_python_guarded_step(step.relative_path):
                continue
            if not os.path.isfile(step.script_path):
                issues.append("%s: missing script %s" % (step.relative_path, step.script_path))
                continue
            issues.extend(_lint_step_script(step, build_root))
        if issues:
            raise RuntimeError("LFS script audit failed:\n- %s" % "\n- ".join(issues))

    def _precheck_stage_transition(self, plan, build_root, target_stage, verified_steps, allow_missing_markers=False):
        expected_prior = []
        for step in plan.steps:
            if STAGE_ORDER.get(step.stage, 999) >= STAGE_ORDER.get(target_stage, 999):
                continue
            expected_prior.append(step.relative_path)
        missing = []
        for relative in expected_prior:
            marker_path = _step_marker_path(build_root, relative)
            if relative in verified_steps or os.path.isfile(marker_path):
                continue
            missing.append(relative)
        if missing and not allow_missing_markers:
            raise RuntimeError(
                "Refusing to enter stage %s before prior stages are verified: %s"
                % (target_stage, ", ".join(missing[:8]))
            )
        if target_stage == "lfs-user":
            luser = self.settings.get("luser", "lfs")
            home_dir = _lookup_user_home(luser) or os.path.join("/home", luser)
            for path in (os.path.join(home_dir, ".bash_profile"), os.path.join(home_dir, ".bashrc")):
                if not os.path.isfile(path) and not allow_missing_markers:
                    raise RuntimeError("Refusing lfs-user stage without %s" % path)
            sources_dir = os.path.join(build_root, "sources")
            if not os.path.isdir(sources_dir) and not allow_missing_markers:
                raise RuntimeError("Refusing lfs-user stage without sources directory %s" % sources_dir)
            for path in (os.path.join(build_root, "sources"), os.path.join(build_root, "tools"), os.path.join(build_root, "usr"), os.path.join(build_root, "var"), os.path.join(build_root, "etc")):
                if os.path.exists(path) and not _user_can_write_path(luser, path) and not allow_missing_markers:
                    raise RuntimeError("Refusing lfs-user stage because %s is not writable by %s" % (path, luser))
        if target_stage in ("host-root-kernfs", "host-root-chroot", "chroot-root", "host-root-teardown"):
            commands_dest = os.path.join(build_root, "lfs-base")
            if not os.path.isdir(commands_dest) and not allow_missing_markers:
                raise RuntimeError("Refusing stage %s without synced command tree %s" % (target_stage, commands_dest))
        if target_stage in ("host-root-chroot", "chroot-root", "host-root-teardown"):
            missing_mounts = [path for path in _expected_kernfs_mounts(build_root) if not _is_mount_target(path)]
            if missing_mounts and not allow_missing_markers:
                raise RuntimeError(
                    "Refusing stage %s without active kernel filesystem mounts: %s"
                    % (target_stage, ", ".join(missing_mounts))
                )
        if target_stage == "chroot-root":
            for required in (os.path.join(build_root, "bin", "bash"), os.path.join(build_root, "usr", "bin", "env")):
                if not os.path.exists(required) and not allow_missing_markers:
                    raise RuntimeError("Refusing chroot stage without %s" % required)

    def _prepare_target_root(self, build_root, master_log_path=""):
        sources_dir = os.path.join(build_root, "sources")
        resolved_sources_dir = os.path.realpath(sources_dir)
        if os.path.islink(sources_dir) or (
            os.path.exists(sources_dir) and os.path.abspath(sources_dir) != resolved_sources_dir
        ):
            resolved = resolved_sources_dir
            if not os.path.isdir(sources_dir):
                raise RuntimeError(
                    "Refusing to use sources path that resolves outside the build tree but is not a directory: %s -> %s"
                    % (sources_dir, resolved)
                )
            source_target_allowed = [resolved]
            source_target_description = (
                "sources/ isn't a directory but a symlink to another folder %s. "
                "Ownership needs to be changed to root:root before continuing."
            ) % resolved
            mode_description = (
                "sources/ isn't a directory but a symlink to another folder %s. "
                "Mode needs to be changed to 1777 before continuing."
            ) % resolved
            owner_result = self.guarded_ops.ensure_owner(
                resolved,
                "root:root",
                target_root=build_root,
                env={"LFS": build_root},
                log_path=master_log_path,
                master_log_path=master_log_path,
                description=source_target_description,
                allowed_roots=source_target_allowed,
            )
            mode_result = self.guarded_ops.ensure_mode(
                resolved,
                0o1777,
                target_root=build_root,
                env={"LFS": build_root},
                log_path=master_log_path,
                master_log_path=master_log_path,
                description=mode_description,
                allowed_roots=source_target_allowed,
            )
            if master_log_path:
                if owner_result.changed or mode_result.changed:
                    _append_log(
                        master_log_path,
                        "[%s] PREP adjusted external sources symlink target %s -> %s\n"
                        % (_timestamp(), sources_dir, resolved),
                    )
                else:
                    _append_log(
                        master_log_path,
                        "[%s] PREP external sources symlink target already satisfies owner/mode policy %s -> %s\n"
                        % (_timestamp(), sources_dir, resolved),
                    )
            return
        result = self.guarded_ops.ensure_dir(
            sources_dir,
            target_root=build_root,
            env={"LFS": build_root},
            owner="root:root",
            mode=0o1777,
            master_log_path=master_log_path,
            description="prepare target sources directory",
        )
        if master_log_path and result.status == "skipped":
            _append_log(master_log_path, "[%s] PREP no changes required for %s\n" % (_timestamp(), sources_dir))

    def _run_step(self, build_root, step, log_path="", master_log_path=""):
        if self._run_guarded_step(build_root, step, log_path=log_path, master_log_path=master_log_path):
            return
        env = os.environ.copy()
        env["LFS"] = build_root
        if step.stage == "lfs-user":
            command, home_dir = _lfs_user_command(os.path.join(build_root, "lfs-base", step.relative_path))
            self.guarded_ops.run_command(
                command,
                env=env,
                require_root=False,
                context="lfs-user",
                target_root=build_root,
                location=home_dir,
                log_path=log_path,
                master_log_path=master_log_path,
                description=step.relative_path,
            )
            return
        if step.stage == "host-root-prep":
            self.guarded_ops.run_command(
                [os.path.join(build_root, "lfs-base", step.relative_path)],
                env=env,
                require_root=True,
                context=step.stage,
                target_root=build_root,
                location=build_root,
                log_path=log_path,
                master_log_path=master_log_path,
                description=step.relative_path,
            )
            return
        if step.stage == "host-root-kernfs":
            self.guarded_ops.run_command(
                [os.path.join(build_root, "lfs-base", step.relative_path)],
                env=env,
                require_root=True,
                context=step.stage,
                target_root=build_root,
                location=build_root,
                log_path=log_path,
                master_log_path=master_log_path,
                description=step.relative_path,
            )
            return
        if step.stage == "host-root-chroot":
            self.guarded_ops.run_command(
                [os.path.join(build_root, "lfs-base", step.relative_path)],
                env=env,
                require_root=True,
                context=step.stage,
                target_root=build_root,
                location=build_root,
                log_path=log_path,
                master_log_path=master_log_path,
                description=step.relative_path,
            )
            return
        if step.stage == "chroot-root":
            term = os.environ.get("TERM", "xterm")
            command = [
                "chroot",
                build_root,
                "/usr/bin/env",
                "-i",
                "HOME=/root",
                "TERM=%s" % term,
                "PATH=/usr/bin:/usr/sbin",
                "/bin/bash",
                "--login",
                "-c",
                "/" + os.path.join("lfs-base", step.relative_path),
            ]
            self.guarded_ops.run_command(
                command,
                env={"LFS": build_root, "TERM": term},
                require_root=True,
                context="chroot-root",
                target_root=build_root,
                location="chroot:%s" % build_root,
                log_path=log_path,
                master_log_path=master_log_path,
                description=step.relative_path,
            )
            return
        if step.stage == "host-root-teardown":
            self.guarded_ops.run_command(
                [os.path.join(build_root, "lfs-base", step.relative_path)],
                env=env,
                require_root=True,
                context=step.stage,
                target_root=build_root,
                location=build_root,
                log_path=log_path,
                master_log_path=master_log_path,
                description=step.relative_path,
            )
            return
        raise RuntimeError("Unknown LFS build stage: %s" % step.stage)

    def _run_guarded_step(self, build_root, step, log_path="", master_log_path=""):
        relative = step.relative_path
        if relative == os.path.join("chapter04", "401-creatingminlayout"):
            self._guarded_chapter04_401(build_root, step, log_path=log_path, master_log_path=master_log_path)
            return True
        if relative == os.path.join("chapter04", "402-addinguser"):
            self._guarded_chapter04_402(build_root, step, log_path=log_path, master_log_path=master_log_path)
            return True
        if relative == os.path.join("chapter04", "403-settingenvironment"):
            self._guarded_chapter04_403(build_root, step, log_path=log_path, master_log_path=master_log_path)
            return True
        return False

    def _precheck_step(self, build_root, step):
        relative = step.relative_path
        if step.stage == "lfs-user":
            luser = self.settings.get("luser", "lfs")
            home_dir = _lookup_user_home(luser) or os.path.join("/home", luser)
            if _lookup_user(luser) is None:
                raise RuntimeError("Missing lfs build user before %s" % relative)
            for path in (os.path.join(home_dir, ".bash_profile"), os.path.join(home_dir, ".bashrc")):
                if not os.path.isfile(path):
                    raise RuntimeError("Missing %s before %s" % (path, relative))
            sources_dir = os.path.join(build_root, "sources")
            if not os.path.isdir(sources_dir):
                raise RuntimeError("Missing sources directory before %s: %s" % (relative, sources_dir))
            package = _extract_script_variable(step.script_path, "PACKAGE")
            if package and not os.path.isfile(os.path.join(sources_dir, package)):
                raise RuntimeError("Missing source archive before %s: %s" % (relative, os.path.join(sources_dir, package)))
        if step.stage == "chroot-root":
            for subdir in ("dev", "proc", "sys", "run"):
                path = os.path.join(build_root, subdir)
                if not os.path.exists(path):
                    raise RuntimeError("Missing chroot prerequisite before %s: %s" % (relative, path))

    def _guarded_chapter04_401(self, build_root, step, log_path="", master_log_path=""):
        env = {"LFS": build_root}
        for path in (
            os.path.join(build_root, "etc"),
            os.path.join(build_root, "var"),
            os.path.join(build_root, "usr", "bin"),
            os.path.join(build_root, "usr", "lib"),
            os.path.join(build_root, "usr", "sbin"),
        ):
            self.guarded_ops.ensure_dir(path, target_root=build_root, env=env, log_path=log_path, master_log_path=master_log_path, description="ensure %s" % path)
        for name in ("bin", "lib", "sbin"):
            self.guarded_ops.ensure_symlink(
                os.path.join(build_root, name),
                "usr/%s" % name,
                target_root=build_root,
                env=env,
                log_path=log_path,
                master_log_path=master_log_path,
                description="ensure %s symlink" % name,
                keep_existing_nonlink=True,
            )
        if os.uname().machine == "x86_64":
            self.guarded_ops.ensure_dir(
                os.path.join(build_root, "lib64"),
                target_root=build_root,
                env=env,
                log_path=log_path,
                master_log_path=master_log_path,
                description="ensure %s" % os.path.join(build_root, "lib64"),
            )
        self.guarded_ops.ensure_dir(
            os.path.join(build_root, "tools"),
            target_root=build_root,
            env=env,
            log_path=log_path,
            master_log_path=master_log_path,
            description="ensure %s" % os.path.join(build_root, "tools"),
        )
        self._write_step_marker(build_root, step.relative_path)

    def _guarded_chapter04_402(self, build_root, step, log_path="", master_log_path=""):
        env = {"LFS": build_root}
        luser = self.settings.get("luser", "lfs")
        lgroup = self.settings.get("lgroup", "lfs")
        home_dir = _lookup_user_home(luser) or os.path.join("/home", luser)
        self.guarded_ops.ensure_group(lgroup, target_root=build_root, env=env, log_path=log_path, master_log_path=master_log_path, description="ensure host group %s" % lgroup)
        self.guarded_ops.ensure_user(
            luser,
            target_root=build_root,
            group=lgroup,
            shell="/bin/bash",
            home=home_dir,
            env=env,
            log_path=log_path,
            master_log_path=master_log_path,
            description="ensure host user %s" % luser,
        )
        owner = "%s:%s" % (luser, lgroup)
        owned_paths = [
            os.path.join(build_root, "usr"),
            os.path.join(build_root, "var"),
            os.path.join(build_root, "etc"),
            os.path.join(build_root, "tools"),
        ]
        usr_root = os.path.join(build_root, "usr")
        if os.path.isdir(usr_root):
            for entry in sorted(os.listdir(usr_root)):
                owned_paths.append(os.path.join(usr_root, entry))
        if os.uname().machine == "x86_64":
            owned_paths.append(os.path.join(build_root, "lib64"))
        existing_owned_paths = [path for path in owned_paths if os.path.exists(path)]
        if existing_owned_paths:
            self.guarded_ops.ensure_owner_many(
                existing_owned_paths,
                owner,
                target_root=build_root,
                env=env,
                log_path=log_path,
                master_log_path=master_log_path,
                description="ensure ownership for chapter04 target directories",
            )
        self._write_step_marker(build_root, step.relative_path)

    def _guarded_chapter04_403(self, build_root, step, log_path="", master_log_path=""):
        env = {"LFS": build_root}
        luser = self.settings.get("luser", "lfs")
        lgroup = self.settings.get("lgroup", "lfs")
        home_dir = _lookup_user_home(luser) or os.path.join("/home", luser)
        allowed = [home_dir]
        self.guarded_ops.write_text_file(
            os.path.join(home_dir, ".bash_profile"),
            _lfs_bash_profile_content(),
            target_root=build_root,
            env=env,
            log_path=log_path,
            master_log_path=master_log_path,
            description="write %s" % os.path.join(home_dir, ".bash_profile"),
            owner="%s:%s" % (luser, lgroup),
            mode=0o644,
            allowed_roots=allowed,
        )
        self.guarded_ops.write_text_file(
            os.path.join(home_dir, ".bashrc"),
            _lfs_bashrc_content(build_root, self._target_triplet(), int(self.settings.get("jobs", 1) or 1)),
            target_root=build_root,
            env=env,
            log_path=log_path,
            master_log_path=master_log_path,
            description="write %s" % os.path.join(home_dir, ".bashrc"),
            owner="%s:%s" % (luser, lgroup),
            mode=0o644,
            allowed_roots=allowed,
        )
        self._write_step_marker(build_root, step.relative_path)

    def _target_triplet(self):
        override = (self.settings.get("triplet_override", "") or "").strip()
        if override:
            return override
        vendor = (self.settings.get("target_vendor", "lfs") or "lfs").strip()
        return "$(uname -m)-%s-linux-gnu" % vendor

    def _write_step_marker(self, build_root, relative_path):
        marker_path = _step_marker_path(build_root, relative_path)
        self.guarded_ops.write_text_file(
            marker_path,
            "ok\n",
            target_root=build_root,
            env={"LFS": build_root},
            description="mark %s complete" % relative_path,
            mode=0o644,
        )


def _sync_commands_into_root(commands_root, commands_dest, env=None, guarded_ops=None):
    if os.path.isdir(commands_dest):
        if guarded_ops is not None:
            guarded_ops.remove_tree(commands_dest, target_root=(env or {}).get("LFS", ""), env=env, description="remove stale target script tree")
        else:
            _remove_tree(commands_dest, env=env)
    shutil.copytree(commands_root, commands_dest)
    for root, _, files in os.walk(commands_dest):
        for name in files:
            os.chmod(os.path.join(root, name), 0o755)


def _remove_tree(path, env=None):
    if not os.path.exists(path):
        return
    if _is_tree_writable(path):
        shutil.rmtree(path)
        return
    if os.geteuid() == 0:
        shutil.rmtree(path)
        return
    command = ["sudo", "rm", "-rf", path]
    subprocess.run(command, check=True)


def _is_tree_writable(path):
    for root, dirs, files in os.walk(path):
        if not os.access(root, os.W_OK | os.X_OK):
            return False
        for name in files:
            full = os.path.join(root, name)
            if not os.access(full, os.W_OK):
                return False
    return True


def _run_as_root(
    command,
    env=None,
    log_path="",
    master_log_path="",
    approval_callback=None,
    execution_notice_callback=None,
    description="",
    context="host-root",
    target_root="",
    location="",
):
    if isinstance(command, str):
        command = [command]
    target_root = target_root or _validated_lfs_env(env)
    if os.geteuid() == 0:
        _confirm_root_action(approval_callback, command, env=env, description=description)
        _run_logged(
            command,
            env=env,
            log_path=log_path,
            master_log_path=master_log_path,
            execution_notice_callback=execution_notice_callback,
            notice_payload=_execution_notice_payload(command, env=env, context=context, target_root=target_root, description=description, location=location),
        )
        return
    sudo_command = _sudo_command(command, env)
    _confirm_root_action(approval_callback, sudo_command, env=env, description=description)
    _run_logged(
        sudo_command,
        log_path=log_path,
        master_log_path=master_log_path,
        execution_notice_callback=execution_notice_callback,
        notice_payload=_execution_notice_payload(sudo_command, env=env, context=context, target_root=target_root, description=description, location=location),
    )


def _run_as_lfs_user(script_path, env, log_path="", master_log_path="", execution_notice_callback=None, description="", target_root=""):
    command, home_dir = _lfs_user_command(script_path)
    if os.geteuid() == 0:
        _run_logged(
            command,
            env=env,
            log_path=log_path,
            master_log_path=master_log_path,
            execution_notice_callback=execution_notice_callback,
            notice_payload=_execution_notice_payload(command, env=env, context="lfs-user", target_root=target_root or _validated_lfs_env(env), description=description or script_path, location=home_dir),
        )
        return
    _run_logged(
        command,
        env=env,
        log_path=log_path,
        master_log_path=master_log_path,
        execution_notice_callback=execution_notice_callback,
        notice_payload=_execution_notice_payload(command, env=env, context="lfs-user", target_root=target_root or _validated_lfs_env(env), description=description or script_path, location=home_dir),
    )


def _run_chroot_step(
    build_root,
    script_path,
    log_path="",
    master_log_path="",
    approval_callback=None,
    execution_notice_callback=None,
    description="",
):
    term = os.environ.get("TERM", "xterm")
    env = {"LFS": build_root, "TERM": term}
    command = [
        "chroot",
        build_root,
        "/usr/bin/env",
        "-i",
        "HOME=/root",
        "TERM=%s" % term,
        "PATH=/usr/bin:/usr/sbin",
        "/bin/bash",
        "--login",
        "-c",
        script_path,
    ]
    _run_as_root(
        command,
        env=env,
        log_path=log_path,
        master_log_path=master_log_path,
        approval_callback=approval_callback,
        execution_notice_callback=execution_notice_callback,
        description=description or script_path,
        context="chroot-root",
        target_root=build_root,
        location="chroot:%s" % build_root,
    )


def _lfs_user_command(script_path):
    home_dir = _lookup_user_home("lfs") or "/home/lfs"
    bashrc = os.path.join(home_dir, ".bashrc")
    if os.path.basename(script_path) == "403-settingenvironment":
        command = ["sudo", "-H", "-u", "lfs", "bash", script_path]
    else:
        shell_cmd = ". %s && bash %s" % (_shell_quote(bashrc), _shell_quote(script_path))
        command = [
            "sudo",
            "-H",
            "-u",
            "lfs",
            "env",
            "-i",
            "HOME=%s" % home_dir,
            "TERM=%s" % os.environ.get("TERM", "xterm"),
            "PS1=\\u:\\w\\$ ",
            "/bin/bash",
            "--noprofile",
            "--norc",
            "-c",
            shell_cmd,
        ]
    if os.geteuid() == 0 and command and command[0] == "sudo":
        if os.path.basename(script_path) == "403-settingenvironment":
            command = ["su", "-", "lfs", "-c", script_path]
        else:
            command = ["su", "-", "lfs", "-c", command[-1]]
    return command, home_dir


def _run_logged(command, env=None, log_path="", master_log_path="", execution_notice_callback=None, notice_payload=None):
    if not log_path and not master_log_path:
        _emit_execution_notice(execution_notice_callback, notice_payload)
        subprocess.run(command, check=True, env=env)
        return
    handles = []
    try:
        for path in (log_path, master_log_path):
            if path:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                handles.append(open(path, "a", encoding="utf-8"))
        _write_log_line(handles, "[%s] $ %s\n" % (_timestamp(), " ".join(shlex.quote(part) for part in command)))
        _emit_execution_notice(execution_notice_callback, notice_payload)
        process = subprocess.Popen(
            command,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            errors="replace",
            bufsize=1,
        )
        if process.stdout:
            try:
                for line in process.stdout:
                    _write_log_line(handles, line)
            finally:
                process.stdout.close()
        return_code = process.wait()
        if return_code:
            raise subprocess.CalledProcessError(return_code, command)
    finally:
        for handle in handles:
            handle.close()


def _write_log_line(handles, text):
    for handle in handles:
        handle.write(text)
        handle.flush()


def _append_log(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(text)


def _timestamp():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")


def _step_log_path(step_log_dir, step):
    safe = step.relative_path.replace(os.sep, "__").replace("/", "__")
    return os.path.join(step_log_dir, "%03d-%s.log" % (step.order, safe))


def _is_writable_path(path):
    probe = path
    while not os.path.exists(probe):
        parent = os.path.dirname(probe)
        if parent == probe:
            break
        probe = parent
    return os.access(probe, os.W_OK)


def _lookup_user(name):
    import pwd

    try:
        return pwd.getpwnam(name).pw_uid
    except KeyError:
        return None


def _lookup_group(name):
    import grp

    try:
        return grp.getgrnam(name).gr_gid
    except KeyError:
        return None


def _lookup_user_home(name):
    import pwd

    try:
        return pwd.getpwnam(name).pw_dir
    except KeyError:
        return ""


def _sudo_command(command, env=None):
    command = list(command)
    lfs = _validated_lfs_env(env)
    forwarded = []
    for key in ("LFS", "TERM", "LC_ALL", "PATH", "CONFIG_SITE", "LFS_TGT", "MAKEFLAGS"):
        value = (env or {}).get(key)
        if value:
            forwarded.append("%s=%s" % (key, value))
    if not forwarded:
        return ["sudo"] + command
    return ["sudo", "env"] + forwarded + command


def _confirm_root_action(callback, command, env=None, description=""):
    _validated_lfs_env(env)
    if not callback:
        return
    payload = {
        "description": description or "root action",
        "command": list(command),
        "command_text": " ".join(shlex.quote(part) for part in command),
        "env": {key: value for key, value in (env or {}).items() if key in ("LFS", "TERM", "LC_ALL", "PATH", "CONFIG_SITE", "LFS_TGT", "MAKEFLAGS")},
    }
    allowed = callback(payload)
    if not allowed:
        raise LfsExecutionDeclined("Root action declined: %s" % payload["description"])


def _execution_notice_payload(command, env=None, context="", target_root="", description="", location=""):
    return {
        "description": description or "",
        "command": list(command),
        "command_text": " ".join(shlex.quote(part) for part in command),
        "context": context or "host",
        "target_root": target_root or _validated_lfs_env(env),
        "location": location or "",
        "env": {key: value for key, value in (env or {}).items() if key in ("LFS", "TERM", "LC_ALL", "PATH", "CONFIG_SITE", "LFS_TGT", "MAKEFLAGS")},
    }


def _emit_execution_notice(callback, payload):
    if not callback:
        return
    allowed = callback(payload or {})
    if allowed is False:
        raise LfsExecutionDeclined("Execution declined: %s" % ((payload or {}).get("description") or "command"))


def _step_notice_payload(build_root, step):
    env = os.environ.copy()
    env["LFS"] = build_root
    script_path = os.path.join(build_root, "lfs-base", step.relative_path)
    if step.stage == "lfs-user":
        command, home_dir = _lfs_user_command(script_path)
        return _execution_notice_payload(
            command,
            env=env,
            context="lfs-user",
            target_root=build_root,
            description=step.relative_path,
            location=home_dir,
        )
    if step.stage == "chroot-root":
        term = os.environ.get("TERM", "xterm")
        env = {"LFS": build_root, "TERM": term}
        command = [
            "chroot",
            build_root,
            "/usr/bin/env",
            "-i",
            "HOME=/root",
            "TERM=%s" % term,
            "PATH=/usr/bin:/usr/sbin",
            "/bin/bash",
            "--login",
            "-c",
            "/" + os.path.join("lfs-base", step.relative_path),
        ]
        if os.geteuid() != 0:
            command = _sudo_command(command, env)
        return _execution_notice_payload(
            command,
            env=env,
            context="chroot-root",
            target_root=build_root,
            description=step.relative_path,
            location="chroot:%s" % build_root,
        )
    command = [script_path]
    if os.geteuid() != 0:
        command = _sudo_command(command, env)
    return _execution_notice_payload(
        command,
        env=env,
        context=step.stage,
        target_root=build_root,
        description=step.relative_path,
        location=build_root,
    )


def _step_notice_payloads(build_root, step, settings):
    special = _guarded_step_notice_payloads(build_root, step, settings)
    if special is not None:
        return special
    payloads = [_step_notice_payload(build_root, step)]
    payloads.extend(_script_command_preview_payloads(build_root, step))
    return payloads


def _guarded_step_notice_payloads(build_root, step, settings):
    relative = step.relative_path
    env = {"LFS": build_root, "TERM": os.environ.get("TERM", "xterm")}
    if relative == os.path.join("chapter04", "401-creatingminlayout"):
        payloads = []
        for path in (
            os.path.join(build_root, "etc"),
            os.path.join(build_root, "var"),
            os.path.join(build_root, "usr", "bin"),
            os.path.join(build_root, "usr", "lib"),
            os.path.join(build_root, "usr", "sbin"),
        ):
            if not os.path.isdir(path):
                payloads.append(_guarded_notice_payload(["mkdir", "-p", path], env=env, context="host-root-prep", target_root=build_root, description="ensure %s" % path, location=path, require_root=True))
        for name in ("bin", "lib", "sbin"):
            path = os.path.join(build_root, name)
            target = "usr/%s" % name
            if os.path.islink(path):
                if os.readlink(path) != target:
                    payloads.append(_guarded_notice_payload(["ln", "-svf", target, path], env=env, context="host-root-prep", target_root=build_root, description="ensure %s symlink" % name, location=path, require_root=True))
            elif not os.path.exists(path):
                payloads.append(_guarded_notice_payload(["ln", "-sv", target, path], env=env, context="host-root-prep", target_root=build_root, description="ensure %s symlink" % name, location=path, require_root=True))
        if os.uname().machine == "x86_64" and not os.path.isdir(os.path.join(build_root, "lib64")):
            path = os.path.join(build_root, "lib64")
            payloads.append(_guarded_notice_payload(["mkdir", "-p", path], env=env, context="host-root-prep", target_root=build_root, description="ensure %s" % path, location=path, require_root=True))
        tools = os.path.join(build_root, "tools")
        if not os.path.isdir(tools):
            payloads.append(_guarded_notice_payload(["mkdir", "-p", tools], env=env, context="host-root-prep", target_root=build_root, description="ensure %s" % tools, location=tools, require_root=True))
        return payloads or [_execution_notice_payload(["true"], env=env, context="host-root-prep", target_root=build_root, description="%s already satisfied" % relative, location=build_root)]
    if relative == os.path.join("chapter04", "402-addinguser"):
        payloads = []
        luser = settings.get("luser", "lfs")
        lgroup = settings.get("lgroup", "lfs")
        home_dir = _lookup_user_home(luser) or os.path.join("/home", luser)
        if _lookup_group(lgroup) is None:
            payloads.append(_guarded_notice_payload(["groupadd", lgroup], env=env, context="host-account", target_root=build_root, description="ensure host group %s" % lgroup, location="/etc/group", require_root=True))
        if _lookup_user(luser) is None:
            payloads.append(_guarded_notice_payload(["useradd", "-s", "/bin/bash", "-g", lgroup, "-m", "-k", "/dev/null", "-d", home_dir, luser], env=env, context="host-account", target_root=build_root, description="ensure host user %s" % luser, location="/etc/passwd", require_root=True))
        uid = _lookup_user(luser)
        gid = _lookup_group(lgroup)
        if uid is not None and gid is not None:
            owned_paths = [os.path.join(build_root, "usr"), os.path.join(build_root, "var"), os.path.join(build_root, "etc"), os.path.join(build_root, "tools")]
            usr_root = os.path.join(build_root, "usr")
            if os.path.isdir(usr_root):
                owned_paths.extend(os.path.join(usr_root, entry) for entry in sorted(os.listdir(usr_root)))
            if os.uname().machine == "x86_64":
                owned_paths.append(os.path.join(build_root, "lib64"))
            changed_paths = []
            for path in owned_paths:
                if not os.path.exists(path):
                    continue
                st = os.stat(path)
                if st.st_uid != uid or st.st_gid != gid:
                    changed_paths.append(path)
            if changed_paths:
                payloads.append(
                    _guarded_notice_payload(
                        ["chown", "%s:%s" % (luser, lgroup)] + changed_paths,
                        env=env,
                        context="host-root-prep",
                        target_root=build_root,
                        description="ensure ownership for chapter04 target directories",
                        location=", ".join(changed_paths[:3]) + (" ..." if len(changed_paths) > 3 else ""),
                        require_root=True,
                    )
                )
        return payloads or [_execution_notice_payload(["true"], env=env, context="host-root-prep", target_root=build_root, description="%s already satisfied" % relative, location=build_root)]
    if relative == os.path.join("chapter04", "403-settingenvironment"):
        luser = settings.get("luser", "lfs")
        home_dir = _lookup_user_home(luser) or os.path.join("/home", luser)
        return [
            {
                "description": "write %s" % os.path.join(home_dir, ".bash_profile"),
                "command": ["write-file", os.path.join(home_dir, ".bash_profile")],
                "command_text": "write-file %s" % os.path.join(home_dir, ".bash_profile"),
                "context": "lfs-user",
                "target_root": build_root,
                "location": home_dir,
                "env": {"LFS": build_root},
            },
            {
                "description": "write %s" % os.path.join(home_dir, ".bashrc"),
                "command": ["write-file", os.path.join(home_dir, ".bashrc")],
                "command_text": "write-file %s" % os.path.join(home_dir, ".bashrc"),
                "context": "lfs-user",
                "target_root": build_root,
                "location": home_dir,
                "env": {"LFS": build_root},
            },
        ]
    return None


def _guarded_notice_payload(command, env=None, context="", target_root="", description="", location="", require_root=False):
    command = list(command)
    if require_root and os.geteuid() != 0:
        command = _sudo_command(command, env)
    return _execution_notice_payload(
        command,
        env=env,
        context=context,
        target_root=target_root,
        description=description,
        location=location,
    )


def _script_command_preview_payloads(build_root, step):
    if _is_python_guarded_step(step.relative_path):
        return []
    commands = _extract_preview_commands(step.script_path)
    if not commands:
        return []
    env = os.environ.copy()
    env["LFS"] = build_root
    context = "script-body:%s" % step.stage
    location = os.path.join(build_root, "lfs-base", step.relative_path)
    payloads = []
    for command_text in commands:
        payloads.append(
            {
                "description": "%s planned command" % step.relative_path,
                "command": [command_text],
                "command_text": command_text,
                "context": context,
                "target_root": build_root,
                "location": location,
                "env": {"LFS": build_root},
                "preview_seconds": 0,
            }
        )
    return payloads


def _extract_preview_commands(script_path):
    try:
        with open(script_path, "r", encoding="utf-8", errors="ignore") as handle:
            lines = handle.readlines()
    except OSError:
        return []
    body_start, body_end = _script_body_window(lines)
    if body_start is not None:
        lines = lines[body_start - 1 : body_end - 1 if body_end is not None else None]
    commands = []
    current = []
    for raw_line in lines:
        line = raw_line.rstrip("\n")
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        continuation = stripped.endswith("\\")
        if continuation:
            stripped = stripped[:-1].rstrip()
        current.append(stripped)
        if continuation:
            continue
        command_text = " ".join(part for part in current if part).strip()
        command_text = " ".join(command_text.split())
        current = []
        if _skip_preview_command(command_text):
            continue
        commands.append(command_text)
    if current:
        command_text = " ".join(part for part in current if part).strip()
        command_text = " ".join(command_text.split())
        if command_text and not _skip_preview_command(command_text):
            commands.append(command_text)
    return commands


def _skip_preview_command(command_text):
    if not command_text:
        return True
    stripped = command_text.strip()
    if stripped in ("then", "do", "done", "fi", "esac", "else", "{", "}"):
        return True
    if stripped.startswith(("if ", "for ", "while ", "case ", "elif ")):
        return True
    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*=.*$", stripped):
        return True
    if stripped.startswith("export "):
        return True
    return False


def _validated_lfs_env(env):
    value = ((env or {}).get("LFS", "") or "").strip()
    if not value:
        raise RuntimeError("Refusing root action without non-empty LFS")
    if value == "/":
        raise RuntimeError("Refusing root action with LFS=/")
    if not os.path.isabs(value):
        raise RuntimeError("Refusing root action with non-absolute LFS=%s" % value)
    return value


def _user_can_write_path(user_name, path):
    import pwd

    try:
        pw = pwd.getpwnam(user_name)
    except KeyError:
        return False
    probe = os.path.abspath(path)
    while not os.path.exists(probe):
        parent = os.path.dirname(probe)
        if parent == probe:
            break
        probe = parent
    try:
        st = os.stat(probe)
    except OSError:
        return False
    mode = st.st_mode
    if st.st_uid == pw.pw_uid:
        return bool(mode & 0o200)
    groups = {pw.pw_gid}
    try:
        import grp

        for group in grp.getgrall():
            if user_name in group.gr_mem:
                groups.add(group.gr_gid)
    except Exception:
        pass
    if st.st_gid in groups:
        return bool(mode & 0o020)
    return bool(mode & 0o002)


def _step_marker_name(relative_path):
    return relative_path.replace(os.sep, "__").replace("/", "__")


def _step_marker_path(build_root, relative_path):
    return os.path.join(build_root, "var", "lib", "lfs-pm", "step-markers", "%s.ok" % _step_marker_name(relative_path))


def _lfs_bash_profile_content():
    return "exec env -i HOME=$HOME TERM=$TERM PS1='\\u:\\w\\\\$ ' /bin/bash\n"


def _lfs_bashrc_content(build_root, target_triplet, jobs):
    lines = [
        "set +h",
        "umask 022",
        "LFS=%s" % build_root,
        "LC_ALL=POSIX",
        "LFS_TGT=%s" % target_triplet,
        "PATH=/usr/bin",
        'if [ ! -L /bin ]; then PATH=/bin:$PATH; fi',
        "PATH=$LFS/tools/bin:$PATH",
        "CONFIG_SITE=$LFS/usr/share/config.site",
        "export LFS LC_ALL LFS_TGT PATH CONFIG_SITE",
    ]
    if int(jobs or 1) > 1:
        lines.append("export MAKEFLAGS=-j%d" % int(jobs))
    return "\n".join(lines) + "\n"


def _script_guard_context(relative):
    if relative.startswith("chroot-scripts" + os.sep):
        return "host"
    chapter = relative.split(os.sep, 1)[0]
    if chapter.startswith("chapter"):
        try:
            if int(chapter.replace("chapter", "")) >= 7:
                return "chroot"
        except ValueError:
            pass
    return "host"


def _script_guard_shell(context):
    return (
        "RTAL_CONTEXT=%s\n"
        "RTAL_LFS_REAL=$(readlink -m -- \"$LFS\")\n"
        "rtal_die() { echo \"$1\" >&2; exit 119; }\n"
        "rtal_require_chroot() {\n"
        "  [ \"$RTAL_CONTEXT\" = chroot ] || rtal_die \"Refusing command outside chroot context: $1\"\n"
        "}\n"
        "rtal_resolve() {\n"
        "  case \"$1\" in\n"
        "    /*) readlink -m -- \"$1\" ;;\n"
        "    *) readlink -m -- \"$PWD/$1\" ;;\n"
        "  esac\n"
        "}\n"
        "rtal_allow_path() {\n"
        "  [ -n \"$1\" ] || return 0\n"
        "  case \"$1\" in\n"
        "    -*) return 0 ;;\n"
        "    /*)\n"
        "      if [ \"$RTAL_CONTEXT\" = chroot ]; then\n"
        "        return 0\n"
        "      fi\n"
        "      case \"$(rtal_resolve \"$1\")\" in\n"
        "        \"$LFS\"|\"$LFS\"/*|\"$RTAL_LFS_REAL\"|\"$RTAL_LFS_REAL\"/*) return 0 ;;\n"
        "      esac\n"
        "      return 1\n"
        "      ;;\n"
        "    *) return 0 ;;\n"
        "  esac\n"
        "}\n"
        "rtal_assert_path() {\n"
        "  rtal_allow_path \"$1\" || rtal_die \"Refusing path outside allowed context: $1\"\n"
        "}\n"
        "rtal_assert_absolute_args() {\n"
        "  for arg in \"$@\"; do\n"
        "    case \"$arg\" in /*) rtal_assert_path \"$arg\" ;; esac\n"
        "  done\n"
        "}\n"
        "mkdir() {\n"
        "  for arg in \"$@\"; do case \"$arg\" in -*) ;; *) rtal_assert_path \"$arg\" ;; esac; done\n"
        "  command mkdir \"$@\"\n"
        "}\n"
        "rm() {\n"
        "  for arg in \"$@\"; do case \"$arg\" in -*) ;; *) rtal_assert_path \"$arg\" ;; esac; done\n"
        "  command rm \"$@\"\n"
        "}\n"
        "ln() {\n"
        "  for arg in \"$@\"; do case \"$arg\" in -*) ;; *) rtal_assert_path \"$arg\" ;; esac; done\n"
        "  command ln \"$@\"\n"
        "}\n"
        "chmod() {\n"
        "  skip_mode=1\n"
        "  for arg in \"$@\"; do\n"
        "    case \"$arg\" in -*) continue ;; esac\n"
        "    if [ $skip_mode -eq 1 ]; then skip_mode=0; continue; fi\n"
        "    rtal_assert_path \"$arg\"\n"
        "  done\n"
        "  command chmod \"$@\"\n"
        "}\n"
        "chown() {\n"
        "  skip_owner=1\n"
        "  for arg in \"$@\"; do\n"
        "    case \"$arg\" in -*) continue ;; esac\n"
        "    if [ $skip_owner -eq 1 ]; then skip_owner=0; continue; fi\n"
        "    rtal_assert_path \"$arg\"\n"
        "  done\n"
        "  command chown \"$@\"\n"
        "}\n"
        "cd() {\n"
        "  if [ $# -gt 0 ]; then rtal_assert_path \"$1\"; fi\n"
        "  builtin cd \"$@\"\n"
        "}\n"
        "mount() {\n"
        "  last=''\n"
        "  for arg in \"$@\"; do case \"$arg\" in -*) ;; *) last=\"$arg\" ;; esac; done\n"
        "  [ -n \"$last\" ] && rtal_assert_path \"$last\"\n"
        "  command mount \"$@\"\n"
        "}\n"
        "umount() {\n"
        "  last=''\n"
        "  for arg in \"$@\"; do case \"$arg\" in -*) ;; *) last=\"$arg\" ;; esac; done\n"
        "  [ -n \"$last\" ] && rtal_assert_path \"$last\"\n"
        "  command umount \"$@\"\n"
        "}\n"
        "cp() {\n"
        "  rtal_assert_absolute_args \"$@\"\n"
        "  command cp \"$@\"\n"
        "}\n"
        "mv() {\n"
        "  rtal_assert_absolute_args \"$@\"\n"
        "  command mv \"$@\"\n"
        "}\n"
        "install() {\n"
        "  rtal_assert_absolute_args \"$@\"\n"
        "  command install \"$@\"\n"
        "}\n"
        "touch() {\n"
        "  rtal_assert_absolute_args \"$@\"\n"
        "  command touch \"$@\"\n"
        "}\n"
        "chgrp() {\n"
        "  skip_group=1\n"
        "  for arg in \"$@\"; do\n"
        "    case \"$arg\" in -*) continue ;; esac\n"
        "    if [ $skip_group -eq 1 ]; then skip_group=0; continue; fi\n"
        "    rtal_assert_path \"$arg\"\n"
        "  done\n"
        "  command chgrp \"$@\"\n"
        "}\n"
        "sed() {\n"
        "  in_place=0\n"
        "  for arg in \"$@\"; do\n"
        "    case \"$arg\" in -i|--in-place|-i*) in_place=1 ;; esac\n"
        "  done\n"
        "  if [ $in_place -eq 1 ]; then rtal_assert_absolute_args \"$@\"; fi\n"
        "  command sed \"$@\"\n"
        "}\n"
        "useradd() { rtal_require_chroot useradd; command useradd \"$@\"; }\n"
        "userdel() { rtal_require_chroot userdel; command userdel \"$@\"; }\n"
        "usermod() { rtal_require_chroot usermod; command usermod \"$@\"; }\n"
        "groupadd() { rtal_require_chroot groupadd; command groupadd \"$@\"; }\n"
        "groupdel() { rtal_require_chroot groupdel; command groupdel \"$@\"; }\n"
        "groupmod() { rtal_require_chroot groupmod; command groupmod \"$@\"; }\n"
        "pwconv() { rtal_require_chroot pwconv; command pwconv \"$@\"; }\n"
        "grpconv() { rtal_require_chroot grpconv; command grpconv \"$@\"; }\n"
        "pwunconv() { rtal_require_chroot pwunconv; command pwunconv \"$@\"; }\n"
        "grpunconv() { rtal_require_chroot grpunconv; command grpunconv \"$@\"; }\n"
        "tar() {\n"
        "  next_is_c=0\n"
        "  for arg in \"$@\"; do\n"
        "    if [ $next_is_c -eq 1 ]; then rtal_assert_path \"$arg\"; next_is_c=0; continue; fi\n"
        "    case \"$arg\" in -C) next_is_c=1 ;; /*) rtal_assert_path \"$arg\" ;; esac\n"
        "  done\n"
        "  command tar \"$@\"\n"
        "}\n"
    ) % context


def _lint_step_script(step, build_root):
    try:
        with open(step.script_path, "r", encoding="utf-8", errors="ignore") as handle:
            lines = handle.readlines()
    except OSError as error:
        return ["%s: cannot read script: %s" % (step.relative_path, error)]
    context = _script_guard_context(step.relative_path)
    issues = []
    body_start, body_end = _script_body_window(lines)
    for index, line in enumerate(lines, start=1):
        if body_start is not None and index < body_start:
            continue
        if body_end is not None and index >= body_end:
            continue
        issue = _lint_script_line(line, step.relative_path, context, build_root)
        if issue:
            issues.append("%s:%d %s" % (step.relative_path, index, issue))
    return issues


def _script_body_window(lines):
    body_start = None
    body_end = None
    for index, line in enumerate(lines, start=1):
        if "# Start of LFS book script" in line and body_start is None:
            body_start = index + 1
        elif "# End of LFS book script" in line and body_end is None:
            body_end = index
    if body_start is not None:
        return body_start, body_end
    tar_start = None
    for index, line in enumerate(lines, start=1):
        if line.strip() == "tar() {":
            tar_start = index
            continue
        if tar_start is not None and line.strip() == "}":
            return index + 1, None
    return None, None


def _lint_script_line(line, relative_path, context, build_root):
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return ""
    if re.search(r"\b(sudo|doas)\b", stripped):
        return "refuses embedded privilege escalation"
    if re.search(r"\bsu\s+-?\b", stripped):
        return "refuses embedded user switching"
    if re.search(r"\bcommand\s+(mkdir|rm|ln|chown|chmod|cd|mount|umount|cp|mv|install|sed|tar)\b", stripped):
        return "bypasses guarded shell wrapper with command builtin"
    if re.search(r"(^|[=\s])/(usr/)?bin/(mkdir|rm|ln|chown|chmod|cp|mv|install|sed|tar|mount|umount)\b", stripped):
        return "bypasses guarded shell wrapper with absolute tool path"
    if re.search(r"(^|\s)(unset\s+LFS|LFS\s*=\s*\"?\"?$|LFS\s*=\s*/\s*$)", stripped):
        return "mutates LFS to an unsafe value"
    if re.search(r"(^|\s)(export\s+)?LFS\s*=\s*/($|[\"'])", stripped):
        return "mutates LFS to host root"
    if context != "chroot" and re.search(r"\b(useradd|userdel|usermod|groupadd|groupdel|groupmod|pwconv|grpconv|pwunconv|grpunconv)\b", stripped):
        return "uses account-management command outside chroot context"
    if context == "host":
        redirection_match = re.search(r"(?:^|[;&|]\s*|\s)(?:echo|printf|cat|tee)\b.*(?:>|>>)\s*([\"']?)(/[^ \t;|&]+)\1", stripped)
        if redirection_match and not _host_path_allowed(redirection_match.group(2), build_root):
            return "writes to host path outside LFS with shell redirection"
        tee_match = re.search(r"\btee\b(?:\s+-a)?\s+([\"']?)(/[^ \t;|&]+)\1", stripped)
        if tee_match and not _host_path_allowed(tee_match.group(2), build_root):
            return "writes to host path outside LFS with tee"
    return ""


def _host_path_allowed(path, build_root):
    if "$LFS" in path or "${LFS}" in path:
        return True
    normalized = path.rstrip("/")
    safe_prefixes = (
        build_root.rstrip("/"),
        "/dev",
        "/proc",
        "/sys",
        "/run",
        "/tmp",
    )
    for prefix in safe_prefixes:
        if normalized == prefix or normalized.startswith(prefix + "/"):
            return True
    return False


def _is_python_guarded_step(relative_path):
    return relative_path in {
        os.path.join("chapter04", "401-creatingminlayout"),
        os.path.join("chapter04", "402-addinguser"),
        os.path.join("chapter04", "403-settingenvironment"),
    }


def _capture_sensitive_snapshot():
    snapshot = {}
    for path in SENSITIVE_HOST_PATHS:
        snapshot[path] = _path_signature(path)
    return snapshot


def _path_signature(path):
    try:
        st = os.lstat(path)
    except OSError:
        return None
    signature = {
        "mode": st.st_mode,
        "uid": st.st_uid,
        "gid": st.st_gid,
    }
    if os.path.islink(path):
        try:
            signature["link"] = os.readlink(path)
        except OSError:
            signature["link"] = None
    return signature


def _allowed_host_changes(step):
    relative = step.relative_path
    if relative == os.path.join("chapter04", "402-addinguser"):
        return {
            "/etc/passwd",
            "/etc/group",
            "/etc/shadow",
            "/etc/gshadow",
            "/home/lfs",
            "/home/lfs/.bash_profile",
            "/home/lfs/.bashrc",
        }
    if relative == os.path.join("chapter04", "403-settingenvironment"):
        return {
            "/home/lfs/.bash_profile",
            "/home/lfs/.bashrc",
        }
    return set()


def _verify_step_state(build_root, step, marker_path, before_snapshot, target_triplet=""):
    if not os.path.isfile(marker_path):
        raise RuntimeError("Step did not create completion marker: %s" % marker_path)
    allowed = _allowed_host_changes(step)
    after_snapshot = _capture_sensitive_snapshot()
    unexpected = []
    for path in SENSITIVE_HOST_PATHS:
        if before_snapshot.get(path) != after_snapshot.get(path) and path not in allowed:
            unexpected.append(path)
    if unexpected:
        raise RuntimeError("Step modified host-sensitive paths outside the target root: %s" % ", ".join(unexpected))
    _verify_stage_state(build_root, step)
    _verify_step_artifacts(build_root, step, target_triplet=target_triplet)
    _verify_step_semantics(build_root, step)


def _download_entry(entry, target):
    from urllib.request import urlretrieve

    if not is_remote_source(entry["url"]):
        shutil.copy2(entry["url"], target)
        return
    urlretrieve(entry["url"], target)


def _verify_md5(path, md5):
    if not md5:
        return
    import hashlib

    digest = hashlib.md5()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest().lower() != md5.lower():
        raise RuntimeError("MD5 mismatch for %s" % path)


def _shell_quote(value):
    return "'" + str(value).replace("'", "'\"'\"'") + "'"


def _extract_script_variable(script_path, name):
    try:
        with open(script_path, "r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                match = re.match(r"%s=(.+)$" % re.escape(name), line.strip())
                if match:
                    return match.group(1).strip().strip('"').strip("'")
    except OSError:
        return ""
    return ""


def _verify_step_artifacts(build_root, step, target_triplet=""):
    relative = step.relative_path
    checks = []
    if relative == os.path.join("chapter05", "501-binutils-pass1"):
        checks.append(os.path.join(build_root, "tools", "bin", "%s-ld" % target_triplet))
    elif relative == os.path.join("chapter05", "502-gcc-pass1"):
        checks.append(os.path.join(build_root, "tools", "bin", "%s-gcc" % target_triplet))
    elif relative == os.path.join("chapter05", "503-linux-headers"):
        checks.append(os.path.join(build_root, "usr", "include", "linux", "version.h"))
    elif relative == os.path.join("chapter05", "504-glibc"):
        checks.extend(
            [
                os.path.join(build_root, "tools", "lib", "libc.so"),
                os.path.join(build_root, "tools", "lib64", "libc.so"),
            ]
        )
    elif relative == os.path.join("chapter05", "505-gcc-libstdc++"):
        checks.extend(
            [
                os.path.join(build_root, "tools", target_triplet, "include", "c++"),
                os.path.join(build_root, "tools", "include", "c++"),
            ]
        )
    elif relative == os.path.join("chapter06", "616-binutils-pass2"):
        checks.append(os.path.join(build_root, "tools", "bin", "%s-ld" % target_triplet))
    elif relative == os.path.join("chapter06", "617-gcc-pass2"):
        checks.append(os.path.join(build_root, "tools", "bin", "%s-gcc" % target_triplet))
    elif relative == os.path.join("chapter07", "704-createfiles"):
        checks.extend(
            [
                os.path.join(build_root, "etc", "passwd"),
                os.path.join(build_root, "etc", "group"),
                os.path.join(build_root, "home", "tester"),
                os.path.join(build_root, "var", "log", "wtmp"),
            ]
        )
    elif relative == os.path.join("chapter08", "803-glibc"):
        checks.append(os.path.join(build_root, "etc", "ld.so.conf"))
    elif relative == os.path.join("chapter08", "827-shadow"):
        checks.extend(
            [
                os.path.join(build_root, "etc", "default", "useradd"),
                os.path.join(build_root, "usr", "bin", "passwd"),
            ]
        )
    elif relative == os.path.join("chapter09", "901-network"):
        checks.extend(
            [
                os.path.join(build_root, "etc", "systemd", "network", "10-eth-static.network"),
                os.path.join(build_root, "etc", "hostname"),
                os.path.join(build_root, "etc", "hosts"),
            ]
        )
    elif relative == os.path.join("chapter09", "902-clock"):
        checks.append(os.path.join(build_root, "etc", "adjtime"))
    elif relative == os.path.join("chapter09", "903-console"):
        checks.append(os.path.join(build_root, "etc", "vconsole.conf"))
    elif relative == os.path.join("chapter09", "904-locale"):
        checks.extend(
            [
                os.path.join(build_root, "etc", "locale.conf"),
                os.path.join(build_root, "etc", "profile"),
            ]
        )
    elif relative == os.path.join("chapter09", "905-inputrc"):
        checks.append(os.path.join(build_root, "etc", "inputrc"))
    elif relative == os.path.join("chapter09", "906-etcshells"):
        checks.append(os.path.join(build_root, "etc", "shells"))
    elif relative == os.path.join("chapter10", "1001-fstab"):
        checks.append(os.path.join(build_root, "etc", "fstab"))
    elif relative == os.path.join("chapter10", "1003-grub"):
        checks.append(os.path.join(build_root, "boot", "grub", "grub.cfg"))
    checks.extend(_extract_expected_outputs(step, build_root))
    if not checks:
        return
    if any(os.path.exists(path) for path in checks):
        missing_required = [path for path in checks if _is_required_expected_output(path) and not os.path.exists(path)]
        if missing_required:
            raise RuntimeError(
                "Step completed but expected outputs are missing for %s: %s"
                % (relative, ", ".join(missing_required))
            )
        return
    raise RuntimeError(
        "Step completed without expected artifacts for %s. Checked: %s"
        % (relative, ", ".join(checks))
    )


def _verify_stage_state(build_root, step):
    if step.stage == "host-root-kernfs":
        missing = [path for path in _expected_kernfs_mounts(build_root) if not _is_mount_target(path)]
        if missing:
            raise RuntimeError("Kernel filesystem setup incomplete after %s: %s" % (step.relative_path, ", ".join(missing)))
    if step.stage == "host-root-teardown":
        lingering = [path for path in _expected_kernfs_mounts(build_root) if _is_mount_target(path)]
        if lingering:
            raise RuntimeError("Kernel filesystem teardown incomplete after %s: %s" % (step.relative_path, ", ".join(lingering)))


def _verify_step_semantics(build_root, step):
    relative = step.relative_path
    if relative == os.path.join("chapter07", "704-createfiles"):
        passwd = os.path.join(build_root, "etc", "passwd")
        group = os.path.join(build_root, "etc", "group")
        if not _file_contains(passwd, "tester:x:101:101::/home/tester:/bin/bash"):
            raise RuntimeError("chapter07/704-createfiles did not populate tester user in /etc/passwd")
        if not _file_contains(group, "tester:x:101:"):
            raise RuntimeError("chapter07/704-createfiles did not populate tester group in /etc/group")
    elif relative == os.path.join("chapter08", "827-shadow"):
        useradd_defaults = os.path.join(build_root, "etc", "default", "useradd")
        if not _file_contains(useradd_defaults, "MAIL=no"):
            raise RuntimeError("chapter08/827-shadow did not set MAIL=no in /etc/default/useradd")
    elif relative == os.path.join("chapter08", "883-cleanup"):
        passwd = os.path.join(build_root, "etc", "passwd")
        group = os.path.join(build_root, "etc", "group")
        if _file_contains(passwd, "tester:x:101:101::/home/tester:/bin/bash"):
            raise RuntimeError("chapter08/883-cleanup did not remove tester from /etc/passwd")
        if _file_contains(group, "tester:x:101:"):
            raise RuntimeError("chapter08/883-cleanup did not remove tester from /etc/group")
        if os.path.exists(os.path.join(build_root, "home", "tester")):
            raise RuntimeError("chapter08/883-cleanup did not remove /home/tester")
    elif relative == os.path.join("chapter10", "1001-fstab"):
        fstab = os.path.join(build_root, "etc", "fstab")
        if not _file_contains(fstab, "proc") and not _file_contains(fstab, "/dev/"):
            raise RuntimeError("chapter10/1001-fstab did not populate expected filesystem entries")
    elif relative == os.path.join("chapter10", "1003-grub"):
        grub_cfg = os.path.join(build_root, "boot", "grub", "grub.cfg")
        if not _file_contains(grub_cfg, "menuentry"):
            raise RuntimeError("chapter10/1003-grub did not write a GRUB menuentry")


def _expected_kernfs_mounts(build_root):
    return [
        os.path.join(build_root, "dev"),
        os.path.join(build_root, "dev", "pts"),
        os.path.join(build_root, "proc"),
        os.path.join(build_root, "sys"),
        os.path.join(build_root, "run"),
    ]


def _is_mount_target(path):
    try:
        with open("/proc/self/mountinfo", "r", encoding="utf-8", errors="ignore") as handle:
            target = os.path.realpath(path)
            for line in handle:
                parts = line.split()
                if len(parts) > 4 and os.path.realpath(parts[4]) == target:
                    return True
    except OSError:
        return os.path.ismount(path)
    return False


def _file_contains(path, needle):
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as handle:
            return needle in handle.read()
    except OSError:
        return False


def _extract_expected_outputs(step, build_root):
    if not step.script_path or not os.path.isfile(step.script_path):
        return []
    context = _script_guard_context(step.relative_path)
    outputs = []
    try:
        with open(step.script_path, "r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                outputs.extend(_extract_expected_outputs_from_line(line, build_root, context))
    except OSError:
        return []
    unique = []
    seen = set()
    for path in outputs:
        if not path or path in seen:
            continue
        seen.add(path)
        unique.append(path)
    return unique


def _extract_expected_outputs_from_line(line, build_root, context):
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return []
    patterns = [
        r"\bcat\s+>>?\s+([\"']?)(/[^ \t;|&]+)\1",
        r"\b(?:echo|printf)\b.*>>?\s+([\"']?)(/[^ \t;|&]+)\1",
        r"\btee\b(?:\s+-a)?\s+([\"']?)(/[^ \t;|&]+)\1",
        r"\binstall\b(?:\s+[-A-Za-z0-9=._/]+)*\s+([\"']?)(/[^ \t;|&]+)\1",
        r"\bln\b(?:\s+[-A-Za-z0-9=._/]+)*\s+([\"']?)(/[^ \t;|&]+)\1",
        r"\bmkdir\b(?:\s+[-A-Za-z0-9=._/]+)*\s+([\"']?)(/[^ \t;|&]+)\1",
        r"\btouch\b(?:\s+[-A-Za-z0-9=._/]+)*\s+([\"']?)(/[^ \t;|&]+)\1",
    ]
    outputs = []
    for pattern in patterns:
        for match in re.finditer(pattern, stripped):
            path = match.group(2)
            resolved = _resolve_expected_output_path(path, build_root, context)
            if resolved:
                outputs.append(resolved)
    return outputs


def _resolve_expected_output_path(path, build_root, context):
    if not path.startswith("/"):
        return ""
    if context == "chroot":
        return os.path.join(build_root, path.lstrip("/"))
    if path.startswith(build_root.rstrip("/") + "/") or path == build_root.rstrip("/"):
        return path
    return ""


def _is_required_expected_output(path):
    return True

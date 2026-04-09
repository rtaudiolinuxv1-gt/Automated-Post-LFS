from __future__ import annotations

import os
import shlex
from datetime import datetime

from .build import BuildExecutor
from .source_fetch import source_stage_commands


class BuildScriptExporter:
    def __init__(self, config, store):
        self.config = config
        self.store = store
        self.executor = BuildExecutor(config, store)
        self.app_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

    def export_queue(
        self,
        packages,
        output_dir="",
        package_format="none",
        install_after_build=True,
        update_tracking=True,
        build_mode="native",
        chroot_root="",
    ):
        stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        base_root = self._resolve_output_dir(output_dir)
        base_dir = os.path.join(base_root, stamp)
        artifact_dir = os.path.join(base_dir, "packages")
        os.makedirs(base_dir, exist_ok=True)
        os.makedirs(artifact_dir, exist_ok=True)
        script_paths = []
        export_items = self._export_items(packages)
        for index, item in enumerate(export_items, start=1):
            package = item["package"]
            payload = item["payload"]
            path = os.path.join(
                base_dir,
                "%03d-%s-%s.sh" % (index, _safe_name(payload.name), _safe_name(payload.source_origin)),
            )
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(
                    self._package_script(
                        package,
                        payload,
                        item["members"],
                        artifact_dir=artifact_dir,
                        package_format=package_format,
                        install_after_build=install_after_build,
                        update_tracking=update_tracking,
                        build_mode=build_mode,
                        chroot_root=chroot_root,
                    )
                )
            os.chmod(path, 0o755)
            script_paths.append(path)
        master_path = os.path.join(base_dir, "build-all.sh")
        with open(master_path, "w", encoding="utf-8") as handle:
            handle.write(self._master_script(script_paths))
        os.chmod(master_path, 0o755)
        return base_dir, master_path, script_paths

    def _export_items(self, packages):
        items = []
        handled = set()
        plan_provider_members = {}
        for package in packages:
            provider = package.metadata.get("build_provider", {})
            if not provider:
                continue
            key = (provider.get("name", ""), provider.get("source_origin", package.source_origin))
            plan_provider_members.setdefault(key, []).append(package)
        for package in packages:
            if package.name in handled:
                continue
            payload = self.executor._build_payload(package)
            members = self.executor._payload_member_packages(package, plan_provider_members)
            for member in members:
                handled.add(member.name)
            items.append({"package": package, "payload": payload, "members": members})
        return items

    def _resolve_output_dir(self, output_dir):
        output_dir = (output_dir or "./generated-build-scripts").strip()
        if os.path.isabs(output_dir):
            return output_dir
        return os.path.abspath(os.path.join(os.getcwd(), output_dir))

    def _package_script(
        self,
        package,
        payload,
        member_packages,
        artifact_dir,
        package_format,
        install_after_build,
        update_tracking,
        build_mode,
        chroot_root,
    ):
        policy = self.executor._effective_policy(package.name, package.source_origin)
        phases = self.executor._effective_phases(package.name, package.source_origin, package.phases)
        env = self.executor._package_environment(package, policy)
        install_root = os.path.abspath(chroot_root or self.config.root) if build_mode == "chroot" else self.config.root
        if build_mode == "chroot":
            work_root = os.path.join(install_root, "var", "cache", "lfs-pm", "work")
        else:
            work_root = self.config.work_dir
        build_dir = os.path.join(work_root, "%s-%s-build" % (payload.name, payload.version))
        staging_dir = os.path.join(work_root, "%s-%s-image" % (payload.name, payload.version))
        commands = _script_commands(payload, phases)
        adapted = [self.executor._apply_prefix_adaptations(command, env) for command in commands]
        artifact_target = _artifact_target(artifact_dir, payload.name, payload.version, package_format)
        lines = [
            "#!/bin/bash",
            "set -e",
            "",
            "# package: %s [%s] %s" % (package.name, package.source_origin, package.version),
            "# payload: %s [%s] %s" % (payload.name, payload.source_origin, payload.version),
            "APP_DIR=%s" % shlex.quote(self.app_dir),
            'export PYTHONPATH="$APP_DIR/src${PYTHONPATH:+:$PYTHONPATH}"',
            "ROOT=%s" % shlex.quote(install_root),
            "BUILD_MODE=%s" % shlex.quote(build_mode),
            "CHROOT_ROOT=%s" % shlex.quote(install_root),
            "STATE_DB=%s" % shlex.quote(self.config.db_path),
            "WORK_DIR=%s" % shlex.quote(work_root),
            "BUILD_DIR=%s" % shlex.quote(build_dir),
            "STAGING_DIR=%s" % shlex.quote(staging_dir),
            "ARTIFACT_DIR=%s" % shlex.quote(artifact_dir),
            "PACKAGE_FORMAT=%s" % shlex.quote(package_format),
            "INSTALL_AFTER_BUILD=%s" % ("1" if install_after_build else "0"),
            "UPDATE_TRACKING=%s" % ("1" if update_tracking else "0"),
            "ARTIFACT_PATH=",
            'rm -rf "$BUILD_DIR" "$STAGING_DIR"',
            'mkdir -p "$BUILD_DIR" "$STAGING_DIR" "$ARTIFACT_DIR"',
            'export LFS_PM_ROOT="$ROOT"',
            'export DESTDIR="$STAGING_DIR"',
            'export PKG_BUILD_DIR="$BUILD_DIR"',
            'export PKG_NAME=%s' % shlex.quote(package.name),
            'export PKG_VERSION=%s' % shlex.quote(package.version),
            'export LFS_PM_ALLOW_LA_REMOVAL=0',
        ]
        for key in sorted(env):
            lines.append("export %s=%s" % (key, shlex.quote(str(env[key]))))
        lines.append("")
        lines.extend(_runner_functions())
        lines.extend(source_stage_commands(payload.sources))
        if package.build_system == "arch-pkgbuild":
            repo = package.metadata.get("git_repo", build_dir)
            lines.append("cd %s" % shlex.quote(repo))
        else:
            lines.append('mkdir -p "$BUILD_DIR"')
        for command in adapted:
            lines.append("run_build_command %s %s" % (shlex.quote(build_dir), shlex.quote(command)))
        lines.append("")
        lines.extend(_artifact_commands(package_format, artifact_target))
        lines.append("")
        lines.extend(_install_and_track_commands(member_packages, payload, policy, artifact_target, install_after_build, update_tracking, package.name))
        lines.append("")
        return "\n".join(lines) + "\n"

    def _master_script(self, script_paths):
        lines = [
            "#!/bin/bash",
            "set -e",
            'DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"',
            "",
        ]
        for path in script_paths:
            lines.append('"$DIR/%s"' % os.path.basename(path))
        lines.append("")
        return "\n".join(lines)


def _script_commands(package, phases):
    commands = []
    if package.build_system == "arch-pkgbuild":
        return ["makepkg -s --noconfirm --nodeps"]
    for phase_name in ("prepare", "configure", "build", "install"):
        commands.extend(phases.get(phase_name, []))
    if not commands and phases.get("build"):
        commands.extend(phases["build"])
    return commands


def _artifact_target(artifact_dir, package_name, version, package_format):
    if package_format in ("", "none"):
        return ""
    suffix_map = {
        "slackware": ".tgz",
        "tar": ".tar",
        "tar.gz": ".tar.gz",
        "tar.bz2": ".tar.bz2",
        "tar.xz": ".tar.xz",
    }
    suffix = suffix_map.get(package_format, ".tar.gz")
    return os.path.join(artifact_dir, "%s-%s%s" % (package_name, version, suffix))


def _artifact_commands(package_format, artifact_target):
    if package_format in ("", "none"):
        return []
    lines = ["if [ -n \"$PACKAGE_FORMAT\" ] && [ \"$PACKAGE_FORMAT\" != \"none\" ]; then"]
    if package_format == "slackware":
        lines.append("  (cd \"$STAGING_DIR\" && makepkg -l y -c n %s)" % shlex.quote(artifact_target))
    else:
        flag_map = {
            "tar": "-cf",
            "tar.gz": "-czf",
            "tar.bz2": "-cjf",
            "tar.xz": "-cJf",
        }
        tar_flags = flag_map.get(package_format, "-czf")
        lines.append("  tar -C \"$STAGING_DIR\" %s %s ." % (tar_flags, shlex.quote(artifact_target)))
    lines.append("  ARTIFACT_PATH=%s" % shlex.quote(artifact_target))
    lines.append("fi")
    return lines


def _install_and_track_commands(member_packages, payload, policy, artifact_target, install_after_build, update_tracking, requested_name):
    if not install_after_build:
        return []
    package_payload = [
        {
            "name": package.name,
            "version": package.version,
            "source_origin": package.source_origin,
            "depends": list(package.depends),
        }
        for package in member_packages
    ]
    lines = [
        'if [ -d "$STAGING_DIR" ] && [ "$(find "$STAGING_DIR" -mindepth 1 -print -quit)" ]; then',
        '  mkdir -p "$ROOT"',
        '  cp -a "$STAGING_DIR"/. "$ROOT"/',
        "fi",
        'FILES_TMP="$(mktemp)"',
        '(cd "$STAGING_DIR" && find . -mindepth 1 \\( -type f -o -type l \\) | sed "s#^\\.#/#" | sort) > "$FILES_TMP"',
    ]
    if update_tracking:
        lines.extend(
            [
                'export LFS_PM_TRACK_FILES="$FILES_TMP"',
                'export LFS_PM_TRACK_ARTIFACT="$ARTIFACT_PATH"',
                'export LFS_PM_TRACK_ROOT="$ROOT"',
                'export LFS_PM_TRACK_DB="$STATE_DB"',
                'python3 - <<\'PY\'',
                "import os",
                "from lfs_unified_pm.jhalfs import write_instpkg_xml",
                "from lfs_unified_pm.models import InstalledRecord",
                "from lfs_unified_pm.state import StateStore",
                "db_path = os.environ['LFS_PM_TRACK_DB']",
                "root = os.environ['LFS_PM_TRACK_ROOT']",
                "files_path = os.environ['LFS_PM_TRACK_FILES']",
                "packages = %s" % repr(package_payload),
                "with open(files_path, 'r', encoding='utf-8') as handle:",
                "    files = [line.strip() for line in handle if line.strip()]",
                "store = StateStore(db_path)",
                "try:",
                "    for package in packages:",
                "        record = InstalledRecord(",
                "            name=package['name'],",
                "            version=package['version'],",
                "            source_origin=package['source_origin'],",
                "            install_reason='explicit' if package['name'] == %s else 'dependency'," % repr(requested_name),
                "            files=files,",
                "            depends=package['depends'],",
                "            metadata={",
                "                'artifact': os.environ.get('LFS_PM_TRACK_ARTIFACT', ''),",
                "                'build_mode': 'script',",
                "                'prefix': %s," % repr(policy["prefix"]),
                "                'build_provider': %s," % repr(payload.name if len(member_packages) > 1 else ""),
                "                'provider_members': %s," % repr([package.name for package in member_packages] if len(member_packages) > 1 else []),
                "            },",
                "        )",
                "        store.mark_installed(record)",
                "        detail = {'artifact': os.environ.get('LFS_PM_TRACK_ARTIFACT', '')}",
                "        if %s:" % repr(payload.name if len(member_packages) > 1 else ""),
                "            detail['build_provider'] = %s" % repr(payload.name),
                "            detail['provider_source'] = %s" % repr(payload.source_origin),
                "            detail['provider_members'] = %s" % repr([package.name for package in member_packages]),
                "        import json",
                "        store.add_transaction('build', record.name, record.version, record.source_origin, 'completed', json.dumps(detail, sort_keys=True))",
                "    write_instpkg_xml(root, store.list_installed())",
                "finally:",
                "    store.close()",
                "PY",
            ]
        )
    lines.append('rm -f "$FILES_TMP"')
    return lines


def _runner_functions():
    return [
        'run_build_command() {',
        '  build_cwd="$1"',
        '  shift',
        '  build_cmd="$1"',
        '  if [ "$BUILD_MODE" = "chroot" ]; then',
        '    case "$build_cwd" in',
        '      "$CHROOT_ROOT"/*) rel_cwd="${build_cwd#${CHROOT_ROOT%/}/}"; chroot_cwd="/$rel_cwd" ;;',
        '      *) echo "build directory is outside chroot: $build_cwd" >&2; exit 1 ;;',
        '    esac',
        '    chroot "$CHROOT_ROOT" /bin/bash -lc "cd ${chroot_cwd} && ${build_cmd}"',
        '  else',
        '    (cd "$build_cwd" && /bin/bash -lc "$build_cmd")',
        '  fi',
        '}',
        "",
    ]


def _safe_name(value):
    return "".join(char if char.isalnum() or char in "._-" else "-" for char in value).strip("-") or "package"

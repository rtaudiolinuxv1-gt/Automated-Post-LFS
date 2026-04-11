from __future__ import annotations

import os
import re
import shutil

from .custom_builds import load_custom_build
from .guarded_ops import GuardedOperationRunner
from .jhalfs import read_instpkg_xml, write_instpkg_xml
from .models import InstalledRecord, PackageRecord
from .packaging import PackageExporter
from .settings import merged_override
from .source_fetch import fetch_sources_into


class BuildExecutor:
    def __init__(
        self,
        config,
        store,
        prompt_callback=None,
        command_review_callback=None,
        root_approval_callback=None,
        execution_notice_callback=None,
    ):
        self.config = config
        self.store = store
        self.exporter = PackageExporter(config.root, config.dist_dir)
        self.prompt_callback = prompt_callback
        self.command_review_callback = command_review_callback
        self.guarded_ops = GuardedOperationRunner(
            root_approval_callback=root_approval_callback,
            execution_notice_callback=execution_notice_callback,
        )

    def execute_plan(
        self,
        plan,
        build_mode="native",
        package_format="",
        install=True,
        allow_la_removal=False,
        chroot_root="",
    ):
        artifacts = []
        install_root = self._install_root(build_mode, chroot_root)
        handled_names = set(self._effective_installed_names())
        executed_payloads = {}
        provider_plan_members = _plan_provider_members(plan)
        for step in plan.ordered_steps:
            package = step.package
            payload = self._build_payload(package)
            payload_key = (payload.name, payload.source_origin)
            member_packages = self._payload_member_packages(package, provider_plan_members)
            if package.name in handled_names:
                self.store.add_transaction("build", package.name, package.version, package.source_origin, "skipped", "already handled")
                continue
            if payload_key in executed_payloads:
                for member in member_packages:
                    handled_names.add(member.name)
                    self.store.add_transaction(
                        "build",
                        member.name,
                        member.version,
                        member.source_origin,
                        "completed",
                        _json_detail(
                            build_provider=payload.name,
                            provider_source=payload.source_origin,
                            handling="shared-provider",
                            provider_members=[entry.name for entry in member_packages],
                        ),
                    )
                continue
            policy = self._effective_policy(package.name, package.source_origin)
            self.store.add_transaction(
                "build",
                package.name,
                package.version,
                package.source_origin,
                "started",
                _json_detail(
                    build_provider=payload.name if payload.name != package.name else "",
                    provider_source=payload.source_origin if payload.name != package.name else "",
                    provider_members=[entry.name for entry in member_packages] if payload.name != package.name else [],
                ),
            )
            build_dir, staging_dir = self._work_paths(payload, build_mode, install_root)
            work_root = os.path.dirname(build_dir)
            self.guarded_ops.remove_tree(staging_dir, target_root=work_root)
            self.guarded_ops.remove_tree(build_dir, target_root=work_root)
            self.guarded_ops.ensure_dir(staging_dir, target_root=work_root)
            self.guarded_ops.ensure_dir(build_dir, target_root=work_root)
            effective_allow_la = allow_la_removal or policy["build"].get("allow_la_removal", False)
            phases = self._effective_phases(package.name, package.source_origin, package.phases)
            phases = self._review_commands(package, phases, policy)
            self._run_package(payload, build_dir, staging_dir, build_mode, install_root, effective_allow_la, policy, phases)
            artifact = self.exporter.export(payload, staging_dir, package_format)
            if install:
                files = self._install_image(staging_dir, install_root)
                for installed in self._installed_records_for_payload(member_packages, files, artifact, build_mode, policy, plan.requested, payload):
                    self.store.mark_installed(installed)
                    handled_names.add(installed.name)
                write_instpkg_xml(install_root, self.store.list_installed(), self.config.jhalfs_instpkg)
                self._handle_prefix_profile(package, policy, files, install_root)
            else:
                for member in member_packages:
                    handled_names.add(member.name)
            for member in member_packages:
                self.store.add_transaction(
                    "build",
                    member.name,
                    member.version,
                    member.source_origin,
                    "completed",
                    _json_detail(
                        artifact=artifact,
                        build_provider=payload.name if payload.name != member.name else "",
                        provider_source=payload.source_origin if payload.name != member.name else "",
                        provider_members=[entry.name for entry in member_packages] if payload.name != member.name else [],
                    ),
                )
            executed_payloads[payload_key] = {
                "artifact": artifact,
                "members": [member.name for member in member_packages],
            }
            artifacts.append((payload.name, artifact))
        return artifacts

    def _run_package(self, package, build_dir, staging_dir, build_mode, install_root, allow_la_removal, policy, phases):
        env = os.environ.copy()
        env["LFS_PM_ROOT"] = install_root
        env["DESTDIR"] = staging_dir
        env["PKG_BUILD_DIR"] = build_dir
        env["PKG_NAME"] = package.name
        env["PKG_VERSION"] = package.version
        env["LFS_PM_ALLOW_LA_REMOVAL"] = "1" if allow_la_removal else "0"
        env.update(self._package_environment(package, policy))
        fetch_sources_into(build_dir, package.sources)
        commands = []
        if package.build_system == "arch-pkgbuild":
            repo = package.metadata.get("git_repo", "")
            if not repo:
                raise RuntimeError("Arch package %s has no PKGBUILD repository path" % package.name)
            command = "makepkg -s --noconfirm --nodeps"
            commands = [command]
            build_dir = repo
            env["DESTDIR"] = staging_dir
        else:
            t2_recipe = package.metadata.get("t2_recipe", {})
            if t2_recipe and not t2_recipe.get("supported", True):
                raise RuntimeError(
                    "T2 recipe for %s was imported but not translated to a buildable universal recipe (%s)"
                    % (package.name, ", ".join(t2_recipe.get("unsupported_reasons", [])) or "unknown reason")
                )
            for phase_name in ("prepare", "configure", "build", "install"):
                commands.extend(phases.get(phase_name, []))
            if not commands and phases.get("build"):
                commands.extend(phases["build"])
        if not commands:
            raise RuntimeError("No build commands available for %s" % package.name)
        for command in commands:
            self._assert_la_policy(package.name, command, allow_la_removal)
            adapted = self._apply_prefix_adaptations(command, env)
            self._run_command(adapted, build_dir, env, build_mode)

    def _run_command(self, command, cwd, env, build_mode):
        if build_mode == "chroot":
            chroot_cwd = cwd
            install_root = os.path.abspath(env.get("LFS_PM_ROOT", self.config.root))
            if cwd.startswith(install_root):
                chroot_cwd = "/" + os.path.relpath(cwd, install_root)
            quoted = "cd %s && %s" % (chroot_cwd, command)
            self.guarded_ops.run_command(
                ["chroot", install_root, "/bin/bash", "-lc", quoted],
                env=env,
                require_root=True,
                context="chroot-build",
                target_root=install_root,
                location="chroot:%s%s" % (install_root, chroot_cwd if chroot_cwd.startswith("/") else "/" + chroot_cwd),
            )
            return
        self.guarded_ops.run_command(
            ["/bin/bash", "-lc", command],
            env=env,
            require_root=False,
            cwd=cwd,
            context="native-build",
            target_root=os.path.abspath(cwd),
            location=os.path.abspath(cwd),
        )

    def _install_image(self, staging_dir, install_root):
        files = []
        for root, _, filenames in os.walk(staging_dir):
            for name in filenames:
                source = os.path.join(root, name)
                relative = os.path.relpath(source, staging_dir)
                destination = os.path.join(install_root, relative)
                parent = os.path.dirname(destination)
                self.guarded_ops.ensure_dir(parent, target_root=install_root)
                self._copy_into_root(source, destination, install_root)
                files.append("/" + relative)
        return sorted(files)

    def _package_environment(self, package, policy):
        env = {
            "LFS_PM_PREFIX": policy["prefix"],
            "LFS_PM_PREFIX_NAME": policy["prefix"].lstrip("/") or "/",
            "LFS_PM_BINDIR": policy["bindir"],
            "LFS_PM_SBINDIR": policy["sbindir"],
            "LFS_PM_LIBDIR": policy["libdir"],
            "LFS_PM_INCLUDEDIR": policy["includedir"],
            "LFS_PM_DATADIR": policy["datadir"],
            "LFS_PM_DOCDIR": policy["docdir"],
            "LFS_PM_SYSCONFDIR": policy["sysconfdir"],
            "LFS_PM_LOCALSTATEDIR": policy["localstatedir"],
            "LFS_PM_JOBS": str(policy["build"].get("jobs", 1)),
            "LFS_PM_INSTALLED_PACKAGES": ":".join(self._effective_installed_names()),
            "CFLAGS": policy["cflags"],
            "CXXFLAGS": policy["cxxflags"],
            "LDFLAGS": policy["ldflags"],
        }
        if package.metadata.get("tree_root"):
            env["T2_TREE_ROOT"] = package.metadata["tree_root"]
        if package.metadata.get("path"):
            env["T2_PKG_DIR"] = os.path.dirname(package.metadata["path"])
        env["T2_PKG_NAME"] = package.name
        env["T2_PKG_VERSION"] = package.version
        package_override = self._load_override(package.name, package.source_origin)
        env["LFS_PM_T2_CONFOPT_EXTRA"] = _join_flags(policy["build"].get("configure_extra", ""), package_override.get("configure_extra", ""))
        env["LFS_PM_T2_MESONOPT_EXTRA"] = _join_flags(policy["build"].get("meson_extra", ""), package_override.get("meson_extra", ""))
        env["LFS_PM_T2_CMAKEOPT_EXTRA"] = _join_flags(policy["build"].get("cmake_extra", ""), package_override.get("cmake_extra", ""))
        env["LFS_PM_T2_MAKEOPT_EXTRA"] = _join_flags(policy["build"].get("make_extra", ""), package_override.get("make_extra", ""))
        env["LFS_PM_T2_MAKEINSTOPT_EXTRA"] = _join_flags(policy["build"].get("make_install_extra", ""), package_override.get("make_install_extra", ""))
        return env

    def _effective_installed_names(self):
        settings = self.store.get_settings()
        system_state = settings.get("system_state", {})
        installed = {item.name for item in self.store.list_installed()}
        if system_state.get("assume_lfs_base_installed", False):
            installed.update(
                package.name
                for package in self.store.list_packages_by_source("lfs-base")
            )
        if system_state.get("use_jhalfs_tracking", False):
            tracking_path = system_state.get("jhalfs_tracking_path", self.config.jhalfs_instpkg)
            installed.update(read_instpkg_xml(self.config.root, tracking_path).keys())
        return sorted(installed)

    def _effective_phases(self, package_name, source_origin, default_phases):
        override = self._load_override(package_name, source_origin)
        custom_build_file = override.get("custom_build_file", "")
        if custom_build_file and os.path.isfile(custom_build_file):
            payload = load_custom_build(custom_build_file)
            return payload.get("phases", {}) or {}
        package = self._stored_package(package_name, source_origin)
        provider_phases = package.metadata.get("build_provider", {}).get("phases", {}) if package else {}
        return dict(default_phases or provider_phases)

    def _build_payload(self, package):
        provider = package.metadata.get("build_provider", {})
        if not provider:
            return package
        return PackageRecord(
            name=provider.get("name", package.name),
            version=provider.get("version", package.version),
            source_origin=provider.get("source_origin", package.source_origin),
            summary=provider.get("summary", package.summary),
            category=provider.get("category", package.category),
            description=provider.get("summary", package.description),
            build_system=package.build_system or "blfs-commands",
            recipe_format="docbook-provider",
            depends=list(package.depends),
            recommends=[],
            optional=[],
            provides=[],
            conflicts=[],
            sources=list(provider.get("sources", package.sources)),
            phases=dict(provider.get("phases", package.phases)),
            metadata={
                "provider_for": package.name,
                "git_repo": package.metadata.get("git_repo", ""),
                "pkgbase": provider.get("name", package.metadata.get("pkgbase", "")),
                "provider_members": list(provider.get("members", [])),
            },
        )

    def _payload_member_packages(self, package, provider_plan_members=None):
        provider = package.metadata.get("build_provider", {})
        if not provider:
            return [package]
        if provider_plan_members and (provider.get("name"), provider.get("source_origin", package.source_origin)) in provider_plan_members:
            return provider_plan_members[(provider.get("name"), provider.get("source_origin", package.source_origin))]
        members = []
        for name in provider.get("members", []):
            member = self._stored_package(name, package.source_origin)
            if member:
                members.append(member)
        if not members:
            return [package]
        return members

    def _stored_package(self, name, source_origin):
        cache = getattr(self, "_package_cache", None)
        if cache is None:
            cache = {}
            for package in self.store.list_packages():
                cache[(package.name, package.source_origin)] = package
            self._package_cache = cache
        return cache.get((name, source_origin))

    def _installed_records_for_payload(self, member_packages, files, artifact, build_mode, policy, requested_names, payload):
        records = []
        for member in member_packages:
            records.append(
                InstalledRecord(
                    name=member.name,
                    version=member.version,
                    source_origin=member.source_origin,
                    install_reason="explicit" if member.name in requested_names else "dependency",
                    files=files,
                    depends=list(member.depends),
                    metadata={
                        "artifact": artifact,
                        "build_mode": build_mode,
                        "prefix": policy["prefix"],
                        "build_provider": payload.name if payload.name != member.name else "",
                        "provider_members": [entry.name for entry in member_packages] if payload.name != member.name else [],
                    },
                )
            )
        return records

    def _review_commands(self, package, phases, policy):
        mode = policy["build"].get("command_review_mode", "off")
        if mode == "off" or not self.command_review_callback:
            return phases
        result = self.command_review_callback(
            package,
            dict(phases),
            mode,
            int(policy["build"].get("command_review_seconds", 10)),
        )
        if result is None:
            raise RuntimeError("Build cancelled before executing commands for %s" % package.name)
        return result

    def _effective_policy(self, package_name, source_origin):
        settings = self.store.get_settings()
        override = self._load_override(package_name, source_origin)
        prefix = override.get("prefix") or settings["build"].get("prefix", "/usr")
        prefix = prefix.rstrip("/") or "/"
        policy = {
            "build": settings["build"],
            "profile": settings["profile"],
            "prefix": prefix,
            "bindir": override.get("bindir") or settings["build"].get("bindir") or _join_path(prefix, "bin"),
            "sbindir": override.get("sbindir") or settings["build"].get("sbindir") or _join_path(prefix, "sbin"),
            "libdir": override.get("libdir") or settings["build"].get("libdir") or _join_path(prefix, "lib"),
            "includedir": override.get("includedir") or settings["build"].get("includedir") or _join_path(prefix, "include"),
            "datadir": override.get("datadir") or settings["build"].get("datadir") or _join_path(prefix, "share"),
            "docdir": _join_path(settings["build"].get("docdir_root", "/usr/share/doc"), "%s-%s" % (package_name, "")),
            "sysconfdir": override.get("sysconfdir") or settings["build"].get("sysconfdir") or "/etc",
            "localstatedir": override.get("localstatedir") or settings["build"].get("localstatedir") or "/var",
        }
        policy["docdir"] = policy["docdir"].rstrip("-")
        rpath_paths = list(settings["build"].get("always_rpath_paths", [])) + list(override.get("rpath_paths", []))
        policy["cflags"] = _join_flags(settings["build"].get("cflags", ""), override.get("cflags", ""))
        policy["cxxflags"] = _join_flags(settings["build"].get("cxxflags", ""), override.get("cxxflags", ""))
        policy["ldflags"] = _join_flags(
            settings["build"].get("ldflags", ""),
            override.get("ldflags", ""),
            " ".join("-Wl,-rpath,%s" % path for path in rpath_paths if path),
        )
        return policy

    def _load_override(self, package_name, source_origin):
        exact = self.store.get_raw_package_override(_override_key(package_name, source_origin))
        if exact:
            return merged_override(exact)
        return merged_override(self.store.get_package_override(package_name))

    def _apply_prefix_adaptations(self, command, env):
        replacements = {
            "--prefix=/usr": "--prefix=%s" % env["LFS_PM_PREFIX"],
            "--bindir=/usr/bin": "--bindir=%s" % env["LFS_PM_BINDIR"],
            "--sbindir=/usr/sbin": "--sbindir=%s" % env["LFS_PM_SBINDIR"],
            "--libdir=/usr/lib": "--libdir=%s" % env["LFS_PM_LIBDIR"],
            "--includedir=/usr/include": "--includedir=%s" % env["LFS_PM_INCLUDEDIR"],
            "--datadir=/usr/share": "--datadir=%s" % env["LFS_PM_DATADIR"],
            "--sysconfdir=/etc": "--sysconfdir=%s" % env["LFS_PM_SYSCONFDIR"],
            "--localstatedir=/var": "--localstatedir=%s" % env["LFS_PM_LOCALSTATEDIR"],
        }
        for old, new in replacements.items():
            command = command.replace(old, new)
        return command

    def _assert_la_policy(self, package_name, command, allow_la_removal):
        normalized = " ".join(command.strip().split())
        risky_patterns = (
            ".la",
            "libtool archive",
            "libtool archives",
        )
        removal_markers = (
            " rm ",
            "rm -",
            "find ",
            " delete",
            "unlink ",
            "xargs rm",
        )
        haystack = " %s " % normalized.lower()
        if not any(pattern in haystack for pattern in risky_patterns):
            return
        if not any(marker in haystack for marker in removal_markers):
            return
        if allow_la_removal:
            return
        raise RuntimeError(
            "Blocked possible .la file removal in %s. Re-run with explicit .la removal permission if intended."
            % package_name
        )

    def _handle_prefix_profile(self, package, policy, files, install_root):
        profile_settings = policy["profile"]
        prefix = policy["prefix"]
        if not prefix or prefix in ("/usr", "/usr/local") and profile_settings.get("nonstandard_only", True):
            return
        if self.store.get_prefix_profile(prefix):
            return
        exports = self._infer_profile_exports(prefix, files, profile_settings)
        if not exports:
            return
        should_create = profile_settings.get("auto_create_for_new_prefix", False)
        if not should_create and profile_settings.get("prompt_on_new_prefix", True) and not policy["build"].get("non_interactive", False):
            if self.prompt_callback:
                should_create = self.prompt_callback(
                    "Create profile.d script for prefix %s used by %s?" % (prefix, package.name)
                )
        if should_create:
            script_path = self._write_profile_script(package.name, prefix, exports, install_root)
            self.store.save_prefix_profile(prefix, script_path, exports)

    def _infer_profile_exports(self, prefix, files, profile_settings):
        files = [path for path in files if path.startswith(prefix.rstrip("/") + "/") or path == prefix]
        if not files and not profile_settings.get("scan_installed_files", True):
            return {}
        exports = {}
        prefix_norm = prefix.rstrip("/")
        candidates = {
            "PATH": [],
            "LD_LIBRARY_PATH": [],
            "PKG_CONFIG_PATH": [],
            "XDG_DATA_DIRS": [],
            "PYTHONPATH": [],
            "CMAKE_PREFIX_PATH": [],
        }
        if profile_settings.get("add_bin_to_path", True):
            for subdir in ("bin", "sbin"):
                path = _join_path(prefix_norm, subdir)
                if any(item.startswith(path + "/") for item in files):
                    candidates["PATH"].append(path)
        if profile_settings.get("add_lib_to_ld_library_path", True):
            for subdir in ("lib", "lib64", "lib32", "libx32"):
                path = _join_path(prefix_norm, subdir)
                if any(item.startswith(path + "/") and re.search(r"\.(so(\.|$)|a$|la$)", item) for item in files):
                    candidates["LD_LIBRARY_PATH"].append(path)
        if profile_settings.get("add_pkgconfig_to_pkg_config_path", True):
            for subdir in ("lib/pkgconfig", "lib64/pkgconfig", "share/pkgconfig"):
                path = _join_path(prefix_norm, subdir)
                if any(item.startswith(path + "/") for item in files):
                    candidates["PKG_CONFIG_PATH"].append(path)
        if profile_settings.get("add_share_to_xdg_data_dirs", True):
            path = _join_path(prefix_norm, "share")
            if any(item.startswith(path + "/") for item in files):
                candidates["XDG_DATA_DIRS"].append(path)
        if profile_settings.get("add_python_to_pythonpath", True):
            python_paths = set()
            for item in files:
                match = re.match(r"%s/(lib(?:64)?/python[^/]+/site-packages)(/|$)" % re.escape(prefix_norm), item)
                if match:
                    python_paths.add(_join_path(prefix_norm, match.group(1)))
            candidates["PYTHONPATH"].extend(sorted(python_paths))
        if profile_settings.get("add_cmake_to_cmake_prefix_path", True):
            for subdir in ("lib/cmake", "lib64/cmake", "share/cmake"):
                path = _join_path(prefix_norm, subdir)
                if any(item.startswith(path + "/") for item in files):
                    candidates["CMAKE_PREFIX_PATH"].append(prefix_norm)
                    break
        for key, values in candidates.items():
            unique = []
            for value in values:
                if value and value not in unique:
                    unique.append(value)
            if unique:
                exports[key] = unique
        return exports

    def _write_profile_script(self, package_name, prefix, exports, install_root):
        profile_dir = os.path.join(install_root, "etc", "profile.d")
        self.guarded_ops.ensure_dir(profile_dir, target_root=install_root)
        safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "-", package_name).strip("-") or "prefix"
        script_path = os.path.join(profile_dir, "%s.sh" % safe_name)
        lines = ["# generated for prefix %s" % prefix]
        for variable, values in exports.items():
            for value in values:
                lines.append('case ":${%s:-}:" in *:"%s":*) ;; *) export %s="%s${%s:+:${%s}}" ;; esac' % (
                    variable, value, variable, value, variable, variable
                ))
        content = "\n".join(lines) + "\n"
        if _is_writable_parent(script_path):
            with open(script_path, "w", encoding="utf-8") as handle:
                handle.write(content)
        else:
            command = "cat > %s <<'EOF'\n%sEOF" % (_shell_quote(script_path), content)
            self.guarded_ops.run_command(
                ["/bin/bash", "-lc", command],
                env={"LFS": install_root},
                require_root=True,
                context="native-install",
                target_root=install_root,
                location=script_path,
                description="write profile script %s" % script_path,
            )
        return script_path

    def _install_root(self, build_mode, chroot_root):
        if build_mode != "chroot":
            return self.config.root
        return os.path.abspath(chroot_root or self.config.root)

    def _work_paths(self, package, build_mode, install_root):
        if build_mode == "chroot":
            work_root = os.path.join(install_root, "var", "cache", "lfs-pm", "work")
        else:
            work_root = self.config.work_dir
        self.guarded_ops.ensure_dir(work_root, target_root=work_root)
        build_dir = os.path.join(work_root, "%s-%s-build" % (package.name, package.version))
        staging_dir = os.path.join(work_root, "%s-%s-image" % (package.name, package.version))
        return build_dir, staging_dir

    def _copy_into_root(self, source, destination, install_root):
        if _is_writable_parent(destination):
            shutil.copy2(source, destination)
            return
        self.guarded_ops.run_command(
            ["cp", "-a", source, destination],
            env={"LFS": install_root},
            require_root=True,
            context="native-install",
            target_root=install_root,
            location=destination,
            description="install %s" % os.path.basename(destination),
        )


def _join_path(prefix, child):
    if prefix == "/":
        return "/" + child.lstrip("/")
    return prefix.rstrip("/") + "/" + child.lstrip("/")


def _join_flags(*parts):
    return " ".join(part for part in parts if part).strip()


def _override_key(package_name, source_origin=""):
    if not source_origin:
        return package_name
    return "%s:%s" % (source_origin, package_name)


def _plan_provider_members(plan):
    providers = {}
    for step in plan.ordered_steps:
        package = step.package
        provider = package.metadata.get("build_provider", {})
        if not provider:
            continue
        key = (provider.get("name", ""), provider.get("source_origin", package.source_origin))
        providers.setdefault(key, []).append(package)
    return providers


def _json_detail(**payload):
    import json

    filtered = {key: value for key, value in payload.items() if value not in ("", None, [], {})}
    return json.dumps(filtered, sort_keys=True) if filtered else ""


def _is_writable_parent(path):
    probe = path
    while not os.path.exists(probe):
        parent = os.path.dirname(probe)
        if parent == probe:
            break
        probe = parent
    return os.access(probe, os.W_OK)


def _shell_quote(value):
    return "'" + str(value).replace("'", "'\"'\"'") + "'"

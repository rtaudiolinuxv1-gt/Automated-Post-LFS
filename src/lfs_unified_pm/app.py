from __future__ import annotations

import os
from datetime import datetime, timedelta

from .adapters import (
    ArchJsonAdapter,
    BaseCatalogAdapter,
    BlfsXmlAdapter,
    CustomRecipeAdapter,
    T2PackageAdapter,
)
from .catalog import PackageCatalog
from .config import default_config, ensure_directories
from .custom_builds import load_custom_build, save_custom_build
from .git_source import DEFAULT_T2_GIT_URL, GitSourceManager
from .jhalfs import read_instpkg_xml
from .scanner import RootScanner
from .settings import deep_merge, merged_override, merged_settings
from .simple_yaml import load_file
from .solver import DependencySolver
from .state import StateStore


class PackageManagerApp:
    def __init__(self, root):
        self.config = default_config(root)
        ensure_directories(self.config)
        self.store = StateStore(self.config.db_path)

    def close(self):
        self.store.close()

    def sync(
        self,
        base_catalog="",
        base_override=(),
        blfs_xml=(),
        t2_tree=(),
        arch_json=(),
        arch_repos="",
        custom=(),
    ):
        imported, _ = self.sync_with_report(
            base_catalog=base_catalog,
            base_override=base_override,
            blfs_xml=blfs_xml,
            t2_tree=t2_tree,
            arch_json=arch_json,
            arch_repos=arch_repos,
            custom=custom,
            autodetect_sources=True,
        )
        return imported

    def sync_with_report(
        self,
        base_catalog="",
        base_override=(),
        blfs_xml=(),
        t2_tree=(),
        arch_json=(),
        arch_repos="",
        custom=(),
        selected_sources=None,
        autodetect_sources=True,
    ):
        previous_by_source = {
            source: {item.name: item for item in self.store.list_packages_by_source(source)}
            for source in ("lfs-base", "blfs", "t2", "arch", "custom")
        }
        imported = []
        if not base_catalog:
            base_catalog = os.path.join(
                os.path.dirname(__file__), "data", "lfs13_base.yaml"
            )
        t2_blacklist_path = os.path.join(os.path.dirname(__file__), "data", "t2_blacklist.txt")
        t2_blacklist = _load_name_list(t2_blacklist_path)
        lfs_base_names = {package.name for package in BaseCatalogAdapter().load(base_catalog)}
        if not base_override:
            local_override = os.path.join(os.getcwd(), "root-overrides.yaml")
            if os.path.isfile(local_override):
                base_override = [local_override]
        if autodetect_sources and not blfs_xml and os.path.isdir(os.path.join(os.getcwd(), "blfs-git")):
            blfs_xml = [os.path.join(os.getcwd(), "blfs-git")]
        jhalfs_root = ""
        if os.path.isdir(os.path.join(os.getcwd(), "jhalfs", "BLFS")):
            jhalfs_root = os.path.join(os.getcwd(), "jhalfs", "BLFS")
        if autodetect_sources and not t2_tree and os.path.isdir(os.path.join(os.getcwd(), "t2sde")):
            t2_tree = [os.path.join(os.getcwd(), "t2sde", "package")]
        selected_sources = set(selected_sources or ("base", "blfs", "t2", "arch", "custom"))
        if "base" in selected_sources:
            base_packages = BaseCatalogAdapter().load(base_catalog)
            for path in base_override:
                base_packages = _merge_package_records(base_packages, BaseCatalogAdapter().load(path))
            imported.extend(base_packages)
        if "blfs" in selected_sources:
            for path in blfs_xml:
                imported.extend(
                    BlfsXmlAdapter(
                        jhalfs_root=jhalfs_root,
                        work_dir=os.path.join(self.config.work_dir, "blfs"),
                    ).load(path)
                )
        if "t2" in selected_sources:
            for path in t2_tree:
                imported.extend(T2PackageAdapter(blacklist_names=t2_blacklist, lfs_base_names=lfs_base_names).load(path))
        if "arch" in selected_sources:
            for path in arch_json:
                imported.extend(ArchJsonAdapter().load(path, arch_repos))
        if "custom" in selected_sources:
            for path in custom:
                imported.extend(CustomRecipeAdapter().load(path))
        for package in imported:
            self.store.upsert_package(package)
        t2_names = {package.name for package in imported if package.source_origin == "t2"}
        removed = self.store.delete_packages_by_source_except("t2", t2_names) if "t2" in selected_sources and t2_names else []
        report = _build_sync_report(previous_by_source, imported)
        report["removed"] = {"t2": removed}
        self._record_syncs(report, selected_sources)
        return imported, report

    def sync_t2_git(self, repo_dir="", repo_url=DEFAULT_T2_GIT_URL, branch=""):
        repo_dir = os.path.abspath(repo_dir or os.path.join(os.getcwd(), "t2sde"))
        manager = GitSourceManager()
        git_report = manager.sync_repo(repo_dir, repo_url=repo_url, branch=branch)
        imported, sync_report = self.sync_with_report(
            blfs_xml=[],
            t2_tree=[os.path.join(repo_dir, "package")],
            arch_json=[],
            custom=[],
            selected_sources={"t2"},
            autodetect_sources=False,
        )
        sync_report["git"] = git_report
        return imported, sync_report

    def catalog(self):
        return PackageCatalog(self.store.list_packages(), self.config.source_priority)

    def get_package(self, package_name, source_origin=""):
        if source_origin:
            return self.catalog().resolve_exact(package_name, source_origin)
        return self.catalog().resolve(package_name)

    def get_effective_phases(self, package_name, source_origin=""):
        package = self.get_package(package_name, source_origin)
        if not package:
            return None, {}, ""
        override = self.get_package_override(package_name, source_origin)
        custom_build_file = override.get("custom_build_file", "")
        phases = dict(package.phases or package.metadata.get("build_provider", {}).get("phases", {}))
        if custom_build_file and os.path.isfile(custom_build_file):
            payload = load_custom_build(custom_build_file)
            phases = payload.get("phases", {}) or {}
        return package, phases, custom_build_file

    def save_custom_build(self, package_name, phases, source_origin=""):
        package = self.get_package(package_name, source_origin)
        if not package:
            raise ValueError("Package not found: %s" % package_name)
        path = save_custom_build(self.config.custom_builds_dir, package, phases)
        override = self.get_package_override(package_name, source_origin)
        override["custom_build_file"] = path
        self.save_package_override(package_name, override, source_origin)
        return path

    def clear_custom_build(self, package_name, source_origin=""):
        override = self.get_package_override(package_name, source_origin)
        override["custom_build_file"] = ""
        self.save_package_override(package_name, override, source_origin)

    def scan_root(self):
        catalog = self.catalog()
        base_packages = [pkg for pkg in catalog.all() if pkg.source_origin == "lfs-base"]
        scanner = RootScanner(self.config.root)
        report = scanner.scan(base_packages)
        for record in scanner.derive_installed_records(base_packages, report):
            self.store.mark_installed(record)
        self.store.save_scan(self.config.root, report.__dict__)
        return report

    def plan(self, package_names, include_recommends=False, auto_optional=False):
        catalog = self.catalog()
        installed_names = self.effective_installed_names(catalog)
        solver = DependencySolver(catalog, installed_names)
        return solver.make_plan(
            package_names,
            include_recommends=include_recommends,
            auto_optional=auto_optional,
        )

    def plan_selection(
        self,
        package_name,
        source_origin="",
        include_recommends=False,
        auto_optional=False,
        resolve_required=True,
        t2_dependency_mode="blfs",
        allowed_dependency_sources=None,
    ):
        catalog = self.catalog()
        installed_names = self.effective_installed_names(catalog)
        solver = DependencySolver(catalog, installed_names)
        return solver.make_plan_for_requests(
            [
                {
                    "name": package_name,
                    "source_origin": source_origin,
                    "resolve_required": resolve_required,
                    "t2_dependency_mode": t2_dependency_mode,
                    "allowed_dependency_sources": allowed_dependency_sources,
                }
            ],
            include_recommends=include_recommends,
            auto_optional=auto_optional,
        )

    def effective_installed_names(self, catalog=None):
        catalog = catalog or self.catalog()
        settings = self.get_settings()
        system_state = settings.get("system_state", {})
        installed = {item.name for item in self.store.list_installed()}
        if system_state.get("assume_lfs_base_installed", False):
            installed.update(
                package.name
                for package in catalog.all()
                if package.source_origin == "lfs-base"
            )
        if system_state.get("use_jhalfs_tracking", False):
            tracking_path = system_state.get("jhalfs_tracking_path", self.config.jhalfs_instpkg)
            installed.update(read_instpkg_xml(self.config.root, tracking_path).keys())
        return sorted(installed)

    def load_recipe_file(self, path):
        return load_file(path)

    def get_settings(self):
        return self.store.get_settings()

    def update_settings(self, patch):
        settings = self.get_settings()
        merged = deep_merge(settings, patch)
        self.store.save_settings(merged)
        return merged

    def get_package_override(self, package_name, source_origin=""):
        if source_origin:
            exact = self.store.get_raw_package_override(_override_key(package_name, source_origin))
            if exact:
                return merged_override(exact)
        return self.store.get_package_override(package_name)

    def save_package_override(self, package_name, override, source_origin=""):
        self.store.save_package_override(_override_key(package_name, source_origin), override)
        return self.get_package_override(package_name, source_origin)

    def list_source_syncs(self):
        return self.store.list_source_syncs()

    def get_last_sync_time(self):
        return self.store.get_last_sync_time()

    def needs_sync_prompt(self):
        settings = self.get_settings()
        sync_settings = settings["sync"]
        if not sync_settings.get("prompt_if_stale", True):
            return False, ""
        last_synced = self.get_last_sync_time()
        if not last_synced:
            return True, "No package metadata has been synced yet."
        last_time = datetime.strptime(last_synced, "%Y-%m-%dT%H:%M:%SZ")
        threshold = timedelta(days=int(sync_settings.get("stale_days", 30)))
        if datetime.utcnow() - last_time > threshold:
            return True, "Package metadata is older than %d days." % int(sync_settings.get("stale_days", 30))
        return False, ""

    def sync_selected_sources(self, selected_sources):
        return self.sync_with_report(selected_sources=set(selected_sources))

    def _record_syncs(self, report, selected_sources):
        mapping = {
            "base": "lfs-base",
            "blfs": "blfs",
            "t2": "t2",
            "arch": "arch",
            "custom": "custom",
        }
        for key, source_name in mapping.items():
            if key not in selected_sources:
                continue
            self.store.record_source_sync(source_name, report.get(source_name, {}))


def _merge_package_records(base_packages, override_packages):
    merged = {package.name: package for package in base_packages}
    ordered_names = [package.name for package in base_packages]
    for package in override_packages:
        current = merged.get(package.name)
        if current is None:
            merged[package.name] = package
            ordered_names.append(package.name)
            continue
        current.version = package.version or current.version
        current.summary = package.summary or current.summary
        current.category = package.category or current.category
        current.description = package.description or current.description
        current.homepage = package.homepage or current.homepage
        current.build_system = package.build_system or current.build_system
        current.recipe_format = package.recipe_format or current.recipe_format
        if package.depends:
            current.depends = list(package.depends)
        if package.recommends:
            current.recommends = list(package.recommends)
        if package.optional:
            current.optional = list(package.optional)
        if package.provides:
            current.provides = list(package.provides)
        if package.conflicts:
            current.conflicts = list(package.conflicts)
        if package.sources:
            current.sources = list(package.sources)
        if package.phases:
            current.phases = dict(package.phases)
        current.metadata.update(package.metadata)
    return [merged[name] for name in ordered_names]


def _load_name_list(path):
    names = []
    if not os.path.exists(path):
        return names
    with open(path, "r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            names.append(line)
    return names


def _override_key(package_name, source_origin=""):
    if not source_origin:
        return package_name
    return "%s:%s" % (source_origin, package_name)


def _build_sync_report(previous_by_source, imported):
    report = {}
    imported_by_source = {}
    for package in imported:
        imported_by_source.setdefault(package.source_origin, {})[package.name] = package
    for source, current in imported_by_source.items():
        previous = previous_by_source.get(source, {})
        added = []
        version_changed = []
        recipe_changed = []
        unchanged = []
        for name, package in current.items():
            old = previous.get(name)
            if old is None:
                added.append(name)
                continue
            old_digest = old.metadata.get("recipe_digest", "")
            new_digest = package.metadata.get("recipe_digest", "")
            if old.version != package.version:
                version_changed.append(name)
            elif old_digest and new_digest and old_digest != new_digest:
                recipe_changed.append(name)
            else:
                unchanged.append(name)
        report[source] = {
            "added": added,
            "version_changed": version_changed,
            "recipe_changed": recipe_changed,
            "unchanged": unchanged,
        }
    return report

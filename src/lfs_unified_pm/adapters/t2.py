from __future__ import annotations

import os

from ..models import PackageRecord
from ..t2_recipe import T2RecipeTranslator


class T2PackageAdapter:
    source_origin = "t2"

    def __init__(self, blacklist_names=None, lfs_base_names=None):
        self.translator = T2RecipeTranslator()
        self.blacklist_names = set(blacklist_names or ())
        self.lfs_base_names = set(lfs_base_names or ())

    def load(self, path, progress_callback=None):
        packages = []
        desc_paths = []
        for root, _, files in os.walk(path):
            for name in files:
                if name.endswith(".desc"):
                    desc_paths.append(os.path.join(root, name))
        desc_paths.sort()
        total = len(desc_paths)
        _emit_progress(progress_callback, "Scanning T2 package tree", current=0, total=total)
        for index, full in enumerate(desc_paths, start=1):
            if self._is_blacklisted(full, path):
                if index == 1 or index % 50 == 0 or index == total:
                    _emit_progress(progress_callback, "Scanning T2 package tree", current=index, total=total)
                continue
            record = self._parse_package(full, path)
            if record:
                packages.append(record)
            if index == 1 or index % 50 == 0 or index == total:
                _emit_progress(progress_callback, "Scanning T2 package tree", current=index, total=total)
        return packages

    def _is_blacklisted(self, desc_path, tree_root):
        package_name = os.path.basename(desc_path).rsplit(".", 1)[0]
        if package_name in self.blacklist_names:
            return True
        relative = os.path.relpath(desc_path, tree_root)
        parts = relative.split(os.sep)
        if parts and parts[0] == "base" and package_name in self.lfs_base_names:
            return True
        return False

    def _parse_package(self, desc_path, tree_root):
        values = self._parse_desc(desc_path)
        if not values.get("name"):
            return None
        cache_path = desc_path[:-5] + ".cache"
        cache_depends = self._parse_cache(cache_path)
        package_dir = os.path.dirname(desc_path)
        sources = list(values.get("sources", []))
        shell_lines = list(values.get("shell_lines", []))
        shell_lines.extend(self._read_optional_file(desc_path[:-5] + ".conf"))
        shell_lines.extend(self._read_optional_file(os.path.join(package_dir, "parse-config")))
        for name in sorted(os.listdir(package_dir)):
            if name.endswith(".patch") or name.endswith(".conf") or name.endswith(".init"):
                sources.append(os.path.join(package_dir, name))
        phases, recipe = self.translator.translate(values["name"], package_dir, shell_lines)
        metadata = {
            "path": desc_path,
            "cache_path": cache_path if os.path.exists(cache_path) else "",
            "tree_root": os.path.abspath(tree_root),
            "t2_group": self._group_name(desc_path, tree_root),
            "t2_tags": {
                "license": values.get("license", ""),
                "flags": values.get("flags", []),
                "authors": values.get("authors", []),
                "maintainers": values.get("maintainers", []),
                "downloads": values.get("downloads", []),
            },
            "t2_recipe": recipe,
            "recipe_digest": recipe["recipe_digest"],
        }
        return PackageRecord(
            name=values["name"],
            version=values.get("version", "unknown"),
            source_origin=self.source_origin,
            summary=values.get("summary", ""),
            category=self._package_category(desc_path, tree_root, values),
            description=values.get("description", values.get("summary", "")),
            homepage=values.get("homepage", ""),
            build_system=recipe["build_system"],
            recipe_format="t2-universal",
            depends=sorted(set(cache_depends)),
            recommends=[],
            optional=[],
            provides=[],
            conflicts=[],
            sources=sources,
            phases=phases,
            metadata=metadata,
        )

    def _group_name(self, desc_path, tree_root):
        relative = os.path.relpath(desc_path, tree_root)
        parts = relative.split(os.sep)
        return parts[0] if parts else "t2"

    def _package_category(self, desc_path, tree_root, values):
        relative = os.path.relpath(desc_path, tree_root)
        parts = relative.split(os.sep)
        if len(parts) >= 2:
            return "t2/%s" % parts[0]
        category = values.get("category", "").strip()
        return category or "t2"

    def _parse_desc(self, path):
        current_text = []
        values = {
            "sources": [],
            "downloads": [],
            "flags": [],
            "authors": [],
            "maintainers": [],
            "shell_lines": [],
        }
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as handle:
                for line in handle:
                    line = line.rstrip("\n")
                    if line.startswith("[I]"):
                        values["summary"] = line[3:].strip()
                    elif line.startswith("[T]"):
                        current_text.append(line[3:].strip())
                    elif line.startswith("[U]"):
                        values["homepage"] = line[3:].strip()
                    elif line.startswith("[A]"):
                        values["authors"].append(line[3:].strip())
                    elif line.startswith("[M]"):
                        values["maintainers"].append(line[3:].strip())
                    elif line.startswith("[C]"):
                        values["category"] = line[3:].strip()
                    elif line.startswith("[F]"):
                        values["flags"].extend(line[3:].strip().split())
                    elif line.startswith("[L]"):
                        values["license"] = line[3:].strip()
                    elif line.startswith("[V]"):
                        values["version"] = line[3:].strip()
                    elif line.startswith("[D]"):
                        parts = line[3:].strip().split()
                        if len(parts) >= 2:
                            source_name = parts[1]
                            source_base = parts[2] if len(parts) >= 3 else ""
                            values["sources"].append("%s%s" % (source_base, source_name))
                            values["downloads"].append({"checksum": parts[0], "filename": source_name, "base": source_base})
                    elif line.startswith("["):
                        continue
                    else:
                        values["shell_lines"].append(line)
        except OSError:
            return None
        values["description"] = " ".join(current_text).strip()
        values["name"] = os.path.basename(path).rsplit(".", 1)[0]
        return values

    def _parse_cache(self, path):
        depends = []
        if not os.path.exists(path):
            return depends
        with open(path, "r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                if line.startswith("[DEP]"):
                    dep = line[5:].strip()
                    if dep:
                        depends.append(dep)
        return depends

    def _read_optional_file(self, path):
        if not os.path.exists(path):
            return []
        with open(path, "r", encoding="utf-8", errors="ignore") as handle:
            return [line.rstrip("\n") for line in handle]


def _emit_progress(progress_callback, message, current=None, total=None):
    if not progress_callback:
        return
    event = {
        "source": "t2",
        "phase": "load",
        "message": message,
    }
    if current is not None:
        event["current"] = int(current)
    if total is not None:
        event["total"] = int(total)
        if total:
            event["percent"] = int((float(current or 0) / float(total)) * 100)
    progress_callback(event)

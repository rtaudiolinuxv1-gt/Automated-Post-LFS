from __future__ import annotations

import json
import os

from ..models import PackageRecord


class ArchJsonAdapter:
    source_origin = "arch"

    def load(self, json_path, repos_path=""):
        packages = []
        payloads = []
        if os.path.isdir(json_path):
            for name in sorted(os.listdir(json_path)):
                if name.endswith(".json"):
                    payloads.append(os.path.join(json_path, name))
        else:
            payloads.append(json_path)

        for path in payloads:
            with open(path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            if isinstance(data, dict):
                if "results" in data:
                    items = data["results"]
                else:
                    items = [data]
            else:
                items = data
            for item in items:
                packages.append(self._convert_item(item, repos_path))
        packages = [package for package in packages if package is not None]
        return self._apply_pkgbase_providers(packages)

    def _convert_item(self, item, repos_path):
        name = item.get("pkgname") or item.get("name")
        if not name:
            return None
        repo_path = ""
        if repos_path:
            candidate = os.path.join(repos_path, name)
            if os.path.isdir(candidate):
                repo_path = candidate
        metadata = {
            "repo": item.get("repo", ""),
            "arch": item.get("arch", ""),
            "required_by": item.get("required_by", []),
            "git_repo": repo_path or item.get("git_repo", ""),
            "pkgbase": item.get("pkgbase", ""),
        }
        return PackageRecord(
            name=name,
            version=str(item.get("pkgver") or item.get("version") or "unknown"),
            source_origin=self.source_origin,
            summary=item.get("pkgdesc", ""),
            category=item.get("repo", "arch"),
            description=item.get("pkgdesc", ""),
            homepage=item.get("url", ""),
            build_system="arch-pkgbuild",
            recipe_format="json",
            depends=_flat_list(item.get("depends", [])),
            recommends=_flat_list(item.get("optdepends", [])),
            optional=[],
            provides=_flat_list(item.get("provides", [])),
            conflicts=_flat_list(item.get("conflicts", [])),
            sources=[],
            phases={},
            metadata=metadata,
        )

    def _apply_pkgbase_providers(self, packages):
        groups = {}
        for package in packages:
            pkgbase = package.metadata.get("pkgbase", "")
            if not pkgbase or pkgbase == package.name:
                continue
            groups.setdefault(pkgbase, []).append(package)
        for pkgbase, members in groups.items():
            member_names = sorted(package.name for package in members)
            representative = members[0]
            provider = {
                "name": pkgbase,
                "version": representative.version,
                "source_origin": representative.source_origin,
                "summary": representative.summary,
                "category": representative.category,
                "sources": [],
                "phases": {},
                "members": member_names,
            }
            for package in members:
                package.metadata["build_provider"] = dict(provider)
        return packages


def _flat_list(values):
    result = []
    for value in values:
        name = str(value).split(":", 1)[0].split(">=", 1)[0].split("=", 1)[0].strip()
        if name:
            result.append(name)
    return sorted(set(result))

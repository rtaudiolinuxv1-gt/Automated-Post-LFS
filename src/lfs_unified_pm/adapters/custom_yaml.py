from __future__ import annotations

from ..models import PackageRecord
from ..simple_yaml import load_file


class CustomRecipeAdapter:
    source_origin = "custom"

    def load(self, path):
        payload = load_file(path)
        packages = []
        for entry in payload.get("packages", []):
            metadata = dict(entry.get("metadata", {}))
            build_provider = entry.get("build_provider") or metadata.get("build_provider", {})
            if build_provider:
                metadata["build_provider"] = _normalize_build_provider(build_provider, entry)
            packages.append(
                PackageRecord(
                    name=entry["name"],
                    version=str(entry["version"]),
                    source_origin=entry.get("source_origin", self.source_origin),
                    summary=entry.get("summary", ""),
                    category=entry.get("category", "custom"),
                    description=entry.get("description", ""),
                    homepage=entry.get("homepage", ""),
                    build_system=entry.get("build_system", ""),
                    recipe_format="simple-yaml",
                    depends=list(entry.get("depends", [])),
                    recommends=list(entry.get("recommends", [])),
                    optional=list(entry.get("optional", [])),
                    provides=list(entry.get("provides", [])),
                    conflicts=list(entry.get("conflicts", [])),
                    sources=list(entry.get("sources", [])),
                    phases=_normalize_phases(entry.get("phases", {})),
                    metadata=metadata,
                )
            )
        return packages


def _normalize_phases(phases):
    result = {}
    for key, value in phases.items():
        if isinstance(value, list):
            result[key] = [str(item) for item in value]
        else:
            result[key] = [str(value)]
    return result


def _normalize_build_provider(provider, entry):
    source_origin = entry.get("source_origin", "custom")
    return {
        "name": provider.get("name", entry["name"]),
        "version": str(provider.get("version", entry.get("version", "group"))),
        "source_origin": provider.get("source_origin", source_origin),
        "summary": provider.get("summary", entry.get("summary", "")),
        "category": provider.get("category", entry.get("category", "custom")),
        "sources": list(provider.get("sources", entry.get("sources", []))),
        "phases": _normalize_phases(provider.get("phases", entry.get("phases", {}))),
        "members": list(provider.get("members", [entry["name"]])),
    }

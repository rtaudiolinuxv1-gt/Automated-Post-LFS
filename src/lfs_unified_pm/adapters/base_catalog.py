from __future__ import annotations

from ..models import PackageRecord
from ..simple_yaml import load_file


class BaseCatalogAdapter:
    source_origin = "lfs-base"

    def load(self, path):
        payload = load_file(path)
        packages = []
        for entry in payload.get("packages", []):
            packages.append(
                PackageRecord(
                    name=entry["name"],
                    version=str(entry["version"]),
                    source_origin=self.source_origin,
                    summary=entry.get("summary", ""),
                    category=entry.get("category", "base"),
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
                    phases=dict(entry.get("phases", {})),
                    metadata=dict(entry.get("metadata", {})),
                )
            )
        return packages


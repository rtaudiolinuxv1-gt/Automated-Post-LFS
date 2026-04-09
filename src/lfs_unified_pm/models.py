from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class PackageRecord:
    name: str
    version: str
    source_origin: str
    summary: str = ""
    category: str = ""
    description: str = ""
    homepage: str = ""
    build_system: str = ""
    recipe_format: str = ""
    depends: List[str] = field(default_factory=list)
    recommends: List[str] = field(default_factory=list)
    optional: List[str] = field(default_factory=list)
    provides: List[str] = field(default_factory=list)
    conflicts: List[str] = field(default_factory=list)
    sources: List[str] = field(default_factory=list)
    phases: Dict[str, List[str]] = field(default_factory=dict)
    metadata: Dict[str, object] = field(default_factory=dict)


@dataclass
class InstalledRecord:
    name: str
    version: str
    source_origin: str
    install_reason: str
    files: List[str] = field(default_factory=list)
    depends: List[str] = field(default_factory=list)
    metadata: Dict[str, object] = field(default_factory=dict)


@dataclass
class BuildRequest:
    packages: List[str]
    include_recommends: bool = False
    auto_optional: bool = False
    package_format: str = ""
    install: bool = True


@dataclass
class BuildStep:
    package: PackageRecord
    required: List[str] = field(default_factory=list)
    missing_recommends: List[str] = field(default_factory=list)
    missing_optional: List[str] = field(default_factory=list)


@dataclass
class BuildPlan:
    requested: List[str]
    ordered_steps: List[BuildStep]
    unresolved: List[str] = field(default_factory=list)
    conflicts: List[str] = field(default_factory=list)


@dataclass
class ScanReport:
    root: str
    observed_commands: List[str] = field(default_factory=list)
    observed_libraries: List[str] = field(default_factory=list)
    observed_headers: List[str] = field(default_factory=list)
    detected_pkgtools: List[str] = field(default_factory=list)
    base_hits: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)


@dataclass
class AppConfig:
    root: str
    db_path: str
    work_dir: str
    dist_dir: str
    custom_builds_dir: str
    source_trees_dir: str
    source_priority: List[str]
    default_build_mode: str = "native"
    jhalfs_instpkg: str = "/var/lib/jhalfs/BLFS/instpkg.xml"


SOURCE_PRIORITY = ["lfs-base", "blfs", "t2", "arch", "custom"]

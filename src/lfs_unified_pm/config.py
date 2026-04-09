from __future__ import annotations

import os
import hashlib

from .models import AppConfig, SOURCE_PRIORITY


def default_config(root):
    root = os.path.abspath(root)
    state_dir, cache_dir = _resolve_state_paths(root)
    dist_dir = os.path.join(cache_dir, "packages")
    custom_builds_dir = os.path.join(state_dir, "custom-builds")
    return AppConfig(
        root=root,
        db_path=os.path.join(state_dir, "state.db"),
        work_dir=os.path.join(cache_dir, "work"),
        dist_dir=dist_dir,
        custom_builds_dir=custom_builds_dir,
        source_priority=list(SOURCE_PRIORITY),
    )


def ensure_directories(config):
    for path in (
        os.path.dirname(config.db_path),
        config.work_dir,
        config.dist_dir,
        config.custom_builds_dir,
    ):
        os.makedirs(path, exist_ok=True)
    jhalfs_dir = os.path.join(config.root, "var", "lib", "jhalfs", "BLFS")
    if _is_writable_parent(jhalfs_dir):
        os.makedirs(jhalfs_dir, exist_ok=True)


def _resolve_state_paths(root):
    preferred_state = os.path.join(root, "var", "lib", "lfs-pm")
    preferred_cache = os.path.join(root, "var", "cache", "lfs-pm")
    if _is_writable_parent(preferred_state) and _is_writable_parent(preferred_cache):
        return preferred_state, preferred_cache
    digest = hashlib.sha256(root.encode("utf-8")).hexdigest()[:12]
    sidecar_base = os.environ.get("LFS_PM_STATE_DIR", os.path.join(os.getcwd(), ".lfs-pm"))
    state_dir = os.path.join(sidecar_base, digest, "state")
    cache_dir = os.path.join(sidecar_base, digest, "cache")
    return state_dir, cache_dir


def _is_writable_parent(path):
    probe = path
    while not os.path.exists(probe):
        parent = os.path.dirname(probe)
        if parent == probe:
            break
        probe = parent
    return os.access(probe, os.W_OK)

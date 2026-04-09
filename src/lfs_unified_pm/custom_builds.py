from __future__ import annotations

import json
import os
import re
from datetime import datetime


def build_path(custom_builds_dir, package_name, source_origin):
    safe_name = _sanitize(package_name)
    safe_source = _sanitize(source_origin)
    return os.path.join(custom_builds_dir, "%s--%s.json" % (safe_name, safe_source))


def load_custom_build(path):
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload


def save_custom_build(custom_builds_dir, package, phases):
    os.makedirs(custom_builds_dir, exist_ok=True)
    path = build_path(custom_builds_dir, package.name, package.source_origin)
    payload = {
        "name": package.name,
        "version": package.version,
        "source_origin": package.source_origin,
        "saved_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "phases": phases,
    }
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return path


def _sanitize(value):
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-") or "package"

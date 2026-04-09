from __future__ import annotations

import os

from .jhalfs import read_instpkg_xml
from .models import InstalledRecord, ScanReport


BASE_COMMAND_HINTS = {
    "autoconf": "/usr/bin/autoconf",
    "bash": "/usr/bin/bash",
    "coreutils": "/usr/bin/ls",
    "e2fsprogs": "/usr/sbin/mke2fs",
    "expat": "/usr/lib/libexpat.so",
    "findutils": "/usr/bin/find",
    "gawk": "/usr/bin/awk",
    "grep": "/usr/bin/grep",
    "groff": "/usr/bin/groff",
    "gzip": "/usr/bin/gzip",
    "iproute2": "/usr/sbin/ip",
    "xz": "/usr/bin/xz",
    "bzip2": "/usr/bin/bzip2",
    "meson": "/usr/bin/meson",
    "nano": "/usr/bin/nano",
    "sqlite": "/usr/bin/sqlite3",
    "tar": "/usr/bin/tar",
    "sed": "/usr/bin/sed",
    "setuptools": "/usr/bin/pip3",
    "make": "/usr/bin/make",
    "perl": "/usr/bin/perl",
    "python": "/usr/bin/python3",
    "systemd": "/usr/bin/systemctl",
    "which": "/usr/bin/which",
    "XML-Parser": "/usr/lib/perl5",
}


class RootScanner:
    def __init__(self, root):
        self.root = os.path.abspath(root)

    def scan(self, base_packages):
        report = ScanReport(root=self.root)
        report.observed_commands = self._collect("usr/bin", limit=250)
        report.observed_libraries = self._collect("usr/lib", suffixes=(".so",), limit=250)
        report.observed_headers = self._collect("usr/include", suffixes=(".h",), limit=250)
        pkgtools_dir = os.path.join(self.root, "var", "lib", "pkgtools", "packages")
        if os.path.isdir(pkgtools_dir):
            try:
                report.detected_pkgtools = sorted(os.listdir(pkgtools_dir))
            except OSError:
                report.notes.append("pkgtools database exists but could not be read")
        for package in base_packages:
            if self._package_present(package):
                report.base_hits.append(package.name)
        if os.path.exists(os.path.join(self.root, "usr", "x86_64-rtaudio-linux")):
            report.notes.append("custom target triplet detected: /usr/x86_64-rtaudio-linux")
        if os.path.exists(os.path.join(self.root, "usr", "bin", "systemctl")):
            report.notes.append("systemd userland detected in target root")
        tracked = read_instpkg_xml(self.root)
        if tracked:
            report.notes.append("existing jhalfs tracking file detected")
        return report

    def derive_installed_records(self, base_packages, scan_report):
        observed = []
        base_hit_names = set(scan_report.base_hits)
        tracked = read_instpkg_xml(self.root)
        for package in base_packages:
            if package.name in base_hit_names or package.name in tracked:
                observed.append(
                    InstalledRecord(
                        name=package.name,
                        version=tracked.get(package.name, package.version),
                        source_origin=package.source_origin,
                        install_reason="jhalfs-track" if package.name in tracked else "base-scan",
                        files=[],
                        depends=list(package.depends),
                        metadata={"scan_root": self.root},
                    )
                )
        return observed

    def _collect(self, relative, suffixes=(), limit=200):
        base = os.path.join(self.root, relative)
        results = []
        if not os.path.isdir(base):
            return results
        try:
            for entry in sorted(os.listdir(base)):
                full = os.path.join(base, entry)
                if not os.path.isfile(full):
                    continue
                if suffixes and not entry.endswith(suffixes):
                    continue
                results.append("/" + os.path.relpath(full, self.root))
                if len(results) >= limit:
                    break
        except OSError:
            return results
        return results

    def _package_present(self, package):
        detect_paths = list(package.metadata.get("detect_paths", []))
        hint = BASE_COMMAND_HINTS.get(package.name)
        if hint:
            detect_paths.append(hint)
        for path in detect_paths:
            if os.path.exists(os.path.join(self.root, path.lstrip("/"))):
                return True
        return False

from __future__ import annotations

import os
import subprocess
import tarfile


class PackageExporter:
    def __init__(self, root, dist_dir):
        self.root = root
        self.dist_dir = dist_dir
        os.makedirs(dist_dir, exist_ok=True)

    def export(self, package, staging_dir, package_format):
        if not package_format:
            return ""
        if package_format == "slackware":
            return self._make_slackware_package(package, staging_dir)
        return self._make_tarball(package, staging_dir, package_format)

    def _make_slackware_package(self, package, staging_dir):
        output = os.path.join(
            self.dist_dir, "%s-%s-1_local.tgz" % (package.name, package.version)
        )
        command = [
            "makepkg",
            "-l",
            "y",
            "-c",
            "n",
            output,
        ]
        subprocess.run(command, cwd=staging_dir, check=True)
        return output

    def _make_tarball(self, package, staging_dir, package_format):
        mode_map = {
            "tar": ("w", ".tar"),
            "tar.gz": ("w:gz", ".tar.gz"),
            "tar.bz2": ("w:bz2", ".tar.bz2"),
            "tar.xz": ("w:xz", ".tar.xz"),
        }
        tar_mode, suffix = mode_map.get(package_format, ("w:gz", ".tar.gz"))
        target = os.path.join(self.dist_dir, "%s-%s%s" % (package.name, package.version, suffix))
        with tarfile.open(target, tar_mode) as archive:
            archive.add(staging_dir, arcname=".")
        return target


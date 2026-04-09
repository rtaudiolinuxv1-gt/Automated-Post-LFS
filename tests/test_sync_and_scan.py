import os
import shutil
import tempfile
import unittest
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from lfs_unified_pm import app as app_module
from lfs_unified_pm.app import PackageManagerApp


class SyncAndScanTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix="lfs-pm-test-")
        os.makedirs(os.path.join(self.tempdir, "usr", "bin"))
        with open(os.path.join(self.tempdir, "usr", "bin", "bash"), "w", encoding="utf-8") as handle:
            handle.write("#!/bin/sh\n")

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_sync_and_scan_root(self):
        app = PackageManagerApp(self.tempdir)
        try:
            imported, _ = app.sync_with_report(
                selected_sources={"base"},
                autodetect_sources=False,
            )
            self.assertTrue(imported)
            report = app.scan_root()
            self.assertIn("bash", report.base_hits)
        finally:
            app.close()

    def test_base_override_merge(self):
        override_path = os.path.join(self.tempdir, "override.yaml")
        with open(override_path, "w", encoding="utf-8") as handle:
            handle.write(
                """packages:
  - name: bash
    version: 9.9
    metadata:
      detect_paths: [/usr/bin/bash]
  - name: nano
    version: 8.7.1
    summary: Small text editor
    category: base-extra
    depends: [ncurses]
"""
            )
        app = PackageManagerApp(self.tempdir)
        try:
            imported, _ = app.sync_with_report(
                base_override=[override_path],
                selected_sources={"base"},
                autodetect_sources=False,
            )
            by_name = {item.name: item for item in imported if item.source_origin == "lfs-base"}
            self.assertEqual(by_name["bash"].version, "9.9")
            self.assertIn("nano", by_name)
        finally:
            app.close()

    def test_t2_blacklist_skips_special_and_lfs_base_overlap(self):
        t2_root = os.path.join(self.tempdir, "t2", "package")
        os.makedirs(os.path.join(t2_root, "base", "00-dirtree"))
        os.makedirs(os.path.join(t2_root, "base", "coreutils"))
        os.makedirs(os.path.join(t2_root, "custom", "hello"))
        with open(os.path.join(t2_root, "base", "00-dirtree", "00-dirtree.desc"), "w", encoding="utf-8") as handle:
            handle.write("[V] 1.0\n[I] ignored package\n")
        with open(os.path.join(t2_root, "base", "coreutils", "coreutils.desc"), "w", encoding="utf-8") as handle:
            handle.write("[V] 1.0\n[I] ignored overlap\n")
        with open(os.path.join(t2_root, "custom", "hello", "hello.desc"), "w", encoding="utf-8") as handle:
            handle.write("[V] 1.0\n[I] hello package\n")
        app = PackageManagerApp(self.tempdir)
        try:
            imported, _ = app.sync_with_report(
                t2_tree=[t2_root],
                selected_sources={"t2"},
                autodetect_sources=False,
            )
            names = sorted(item.name for item in imported if item.source_origin == "t2")
            self.assertEqual(names, ["hello"])
        finally:
            app.close()

    def test_custom_recipe_can_define_generic_build_provider(self):
        custom_path = os.path.join(self.tempdir, "custom.yaml")
        with open(custom_path, "w", encoding="utf-8") as handle:
            handle.write(
                """packages:
  - name: demo-runtime
    version: 1.0
    source_origin: custom
    build_provider:
      name: demo-suite
      version: 1.0
      members: [demo-runtime, demo-tools]
      phases:
        build:
          - printf "suite\\n" > "$DESTDIR/suite.txt"
"""
            )
        app = PackageManagerApp(self.tempdir)
        try:
            imported, _ = app.sync_with_report(
                custom=[custom_path],
                selected_sources={"custom"},
                autodetect_sources=False,
            )
            package = imported[0]
            provider = package.metadata.get("build_provider", {})
            self.assertEqual(provider.get("name"), "demo-suite")
            self.assertEqual(provider.get("members"), ["demo-runtime", "demo-tools"])
            self.assertIn('printf "suite\\n" > "$DESTDIR/suite.txt"', provider.get("phases", {}).get("build", []))
        finally:
            app.close()

    def test_source_autodetect_searches_parent_of_nested_checkout(self):
        workspace = os.path.join(self.tempdir, "workspace")
        clone_root = os.path.join(workspace, "Automated-Post-LFS")
        module_file = os.path.join(clone_root, "src", "lfs_unified_pm", "app.py")
        os.makedirs(os.path.dirname(module_file))
        with open(module_file, "w", encoding="utf-8") as handle:
            handle.write("# placeholder\n")
        os.makedirs(os.path.join(workspace, "blfs-git"))
        os.makedirs(os.path.join(workspace, "jhalfs", "BLFS"))
        os.makedirs(os.path.join(workspace, "t2sde", "package"))
        override_path = os.path.join(workspace, "root-overrides.yaml")
        with open(override_path, "w", encoding="utf-8") as handle:
            handle.write("packages: []\n")
        self.assertEqual(
            app_module._find_existing_dir("blfs-git", cwd=clone_root, app_file=module_file),
            os.path.join(workspace, "blfs-git"),
        )
        self.assertEqual(
            app_module._find_existing_dir("jhalfs", "BLFS", cwd=clone_root, app_file=module_file),
            os.path.join(workspace, "jhalfs", "BLFS"),
        )
        self.assertEqual(
            app_module._find_existing_dir("t2sde", "package", cwd=clone_root, app_file=module_file),
            os.path.join(workspace, "t2sde", "package"),
        )
        self.assertEqual(
            app_module._find_existing_file("root-overrides.yaml", cwd=clone_root, app_file=module_file),
            override_path,
        )


if __name__ == "__main__":
    unittest.main()

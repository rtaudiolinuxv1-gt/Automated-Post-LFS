import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from lfs_unified_pm.build_scripts import BuildScriptExporter
from lfs_unified_pm.config import default_config, ensure_directories
from lfs_unified_pm.models import PackageRecord
from lfs_unified_pm.state import StateStore


class BuildScriptsTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix="lfs-pm-scripts-")
        self.config = default_config(self.tempdir)
        ensure_directories(self.config)
        self.store = StateStore(self.config.db_path)

    def tearDown(self):
        self.store.close()
        shutil.rmtree(self.tempdir)

    def test_export_queue_creates_master_and_package_scripts(self):
        package = PackageRecord(
            name="demo",
            version="1.0",
            source_origin="custom",
            build_system="shell",
            phases={"build": ['printf "hello\\n" > "$DESTDIR/demo.txt"']},
        )
        exporter = BuildScriptExporter(self.config, self.store)
        output_root = os.path.join(self.tempdir, "generated")
        base_dir, master_path, scripts = exporter.export_queue(
            [package],
            output_dir=output_root,
            package_format="tar.gz",
            install_after_build=True,
            update_tracking=True,
            build_mode="chroot",
            chroot_root=self.tempdir,
        )
        self.assertTrue(base_dir.startswith(output_root))
        self.assertTrue(os.path.isdir(base_dir))
        self.assertTrue(os.path.isfile(master_path))
        self.assertEqual(len(scripts), 1)
        with open(scripts[0], "r", encoding="utf-8") as handle:
            body = handle.read()
        self.assertIn('printf "hello\\n" > "$DESTDIR/demo.txt"', body)
        self.assertIn("StateStore", body)
        self.assertIn("write_instpkg_xml", body)
        self.assertIn("PACKAGE_FORMAT=tar.gz", body)
        self.assertIn("BUILD_MODE=chroot", body)
        self.assertIn("run_build_command", body)

    def test_export_queue_includes_source_fetch_commands(self):
        package = PackageRecord(
            name="demo",
            version="1.0",
            source_origin="custom",
            build_system="shell",
            sources=["https://example.org/demo-1.0.tar.xz"],
            phases={"build": ['printf "hello\\n" > "$DESTDIR/demo.txt"']},
        )
        exporter = BuildScriptExporter(self.config, self.store)
        _, master_path, scripts = exporter.export_queue([package], output_dir=self.tempdir)
        with open(scripts[0], "r", encoding="utf-8") as handle:
            body = handle.read()
        self.assertIn("fetch_source", body)
        self.assertIn("https://example.org/demo-1.0.tar.xz", body)
        self.assertIn("demo-1.0.tar.xz", body)
        with open(master_path, "r", encoding="utf-8") as handle:
            master = handle.read()
        self.assertIn(os.path.basename(scripts[0]), master)

    def test_export_queue_collapses_shared_build_provider(self):
        provider = {
            "name": "xorg7-lib",
            "version": "group",
            "source_origin": "blfs",
            "summary": "Xorg Libraries",
            "category": "blfs/x/installing",
            "sources": ["https://example.org/xorg7-lib.tar.xz"],
            "phases": {"build": ['printf "group\\n" > "$DESTDIR/provider-ran"']},
            "members": ["libX11", "libXext"],
        }
        libx11 = PackageRecord(
            name="libX11",
            version="1.8.12",
            source_origin="blfs",
            build_system="blfs-commands",
            metadata={"build_provider": provider},
        )
        libxext = PackageRecord(
            name="libXext",
            version="1.3.7",
            source_origin="blfs",
            build_system="blfs-commands",
            metadata={"build_provider": provider},
        )
        self.store.upsert_package(libx11)
        self.store.upsert_package(libxext)
        exporter = BuildScriptExporter(self.config, self.store)
        _, _, scripts = exporter.export_queue([libx11, libxext], output_dir=self.tempdir)
        self.assertEqual(len(scripts), 1)
        with open(scripts[0], "r", encoding="utf-8") as handle:
            body = handle.read()
        self.assertIn("payload: xorg7-lib [blfs] group", body)
        self.assertIn("libX11", body)
        self.assertIn("libXext", body)
        self.assertIn("xorg7-lib.tar.xz", body)


if __name__ == "__main__":
    unittest.main()

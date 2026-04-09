import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from lfs_unified_pm.app import PackageManagerApp
from lfs_unified_pm.build import BuildExecutor
from lfs_unified_pm.jhalfs import write_instpkg_xml
from lfs_unified_pm.models import BuildPlan, BuildStep, PackageRecord


class SettingsAndProfilesTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix="lfs-pm-settings-")
        self.app = PackageManagerApp(self.tempdir)

    def tearDown(self):
        self.app.close()
        shutil.rmtree(self.tempdir)

    def test_stale_sync_prompt_detection(self):
        needs_sync, reason = self.app.needs_sync_prompt()
        self.assertTrue(needs_sync)
        self.assertIn("No package metadata", reason)
        self.app.sync_selected_sources({"base"})
        needs_sync, _ = self.app.needs_sync_prompt()
        self.assertFalse(needs_sync)

    def test_nonstandard_prefix_profile_created_once(self):
        self.app.update_settings({"profile": {"auto_create_for_new_prefix": True}})
        self.app.save_package_override("demoapp", {"prefix": "/opt/demo"})
        package = PackageRecord(
            name="demoapp",
            version="1.0",
            source_origin="custom",
            build_system="shell",
            phases={
                "build": [
                    'mkdir -p "$DESTDIR$LFS_PM_BINDIR" "$DESTDIR$LFS_PM_LIBDIR/pkgconfig" "$DESTDIR$LFS_PM_DATADIR/applications"',
                    'printf "#!/bin/sh\\necho demo\\n" > "$DESTDIR$LFS_PM_BINDIR/demoapp"',
                    'chmod 0755 "$DESTDIR$LFS_PM_BINDIR/demoapp"',
                    'printf "prefix=%s\\n" "$LFS_PM_PREFIX" > "$DESTDIR$LFS_PM_LIBDIR/pkgconfig/demoapp.pc"',
                ]
            },
        )
        plan = BuildPlan(requested=["demoapp"], ordered_steps=[BuildStep(package=package)])
        BuildExecutor(self.app.config, self.app.store).execute_plan(plan)
        profile_dir = os.path.join(self.tempdir, "etc", "profile.d")
        scripts = sorted(os.listdir(profile_dir))
        self.assertEqual(scripts, ["demoapp.sh"])
        profile = self.app.store.get_prefix_profile("/opt/demo")
        self.assertTrue(profile)
        self.assertIn("PATH", profile["exports"])

    def test_custom_build_saved_separately_and_loaded(self):
        package = PackageRecord(
            name="demoapp",
            version="1.0",
            source_origin="custom",
            build_system="shell",
            phases={"build": ['printf "original\\n" > "$DESTDIR/original.txt"']},
        )
        self.app.store.upsert_package(package)
        path = self.app.save_custom_build(
            "demoapp",
            {"build": ['printf "alternate\\n" > "$DESTDIR/alternate.txt"']},
        )
        self.assertTrue(os.path.isfile(path))
        package, phases, custom_path = self.app.get_effective_phases("demoapp")
        self.assertEqual(package.name, "demoapp")
        self.assertEqual(custom_path, path)
        self.assertEqual(phases["build"], ['printf "alternate\\n" > "$DESTDIR/alternate.txt"'])

    def test_build_executor_fetches_local_sources(self):
        source_path = os.path.join(self.tempdir, "demo-source.txt")
        with open(source_path, "w", encoding="utf-8") as handle:
            handle.write("demo source\n")
        package = PackageRecord(
            name="fetchdemo",
            version="1.0",
            source_origin="custom",
            build_system="shell",
            sources=[source_path],
            phases={
                "build": [
                    'test -f "$PKG_BUILD_DIR/demo-source.txt"',
                    'printf "fetched\\n" > "$DESTDIR/fetched.txt"',
                ]
            },
        )
        plan = BuildPlan(requested=["fetchdemo"], ordered_steps=[BuildStep(package=package)])
        BuildExecutor(self.app.config, self.app.store).execute_plan(plan)
        self.assertTrue(os.path.isfile(os.path.join(self.tempdir, "fetched.txt")))

    def test_build_provider_executes_once_and_marks_group_members_installed(self):
        provider = {
            "name": "xorg7-lib",
            "version": "group",
            "source_origin": "blfs",
            "summary": "Xorg Libraries",
            "category": "blfs/x/installing",
            "sources": [],
            "phases": {
                "build": [
                    'test ! -e "$LFS_PM_ROOT/provider-ran"',
                    'printf "group\\n" > "$DESTDIR/provider-ran"',
                ]
            },
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
            depends=["libX11"],
            metadata={"build_provider": provider},
        )
        self.app.store.upsert_package(libx11)
        self.app.store.upsert_package(libxext)
        package, phases, _ = self.app.get_effective_phases("libX11", "blfs")
        self.assertEqual(package.name, "libX11")
        self.assertIn('printf "group\\n" > "$DESTDIR/provider-ran"', phases["build"])
        plan = BuildPlan(
            requested=["libXext"],
            ordered_steps=[BuildStep(package=libx11), BuildStep(package=libxext)],
        )
        BuildExecutor(self.app.config, self.app.store).execute_plan(plan)
        self.assertTrue(os.path.isfile(os.path.join(self.tempdir, "provider-ran")))
        installed = {item.name for item in self.app.store.list_installed()}
        self.assertIn("libX11", installed)
        self.assertIn("libXext", installed)

    def test_build_provider_does_not_mark_unplanned_group_members_installed(self):
        provider = {
            "name": "xorg7-lib",
            "version": "group",
            "source_origin": "blfs",
            "summary": "Xorg Libraries",
            "category": "blfs/x/installing",
            "sources": [],
            "phases": {"build": ['printf "group\\n" > "$DESTDIR/provider-ran"']},
            "members": ["libX11", "libXext", "libXrender"],
        }
        for name in provider["members"]:
            self.app.store.upsert_package(
                PackageRecord(
                    name=name,
                    version="1.0",
                    source_origin="blfs",
                    build_system="blfs-commands",
                    metadata={"build_provider": provider},
                )
            )
        plan = BuildPlan(requested=["libX11"], ordered_steps=[BuildStep(package=self.app.get_package("libX11", "blfs"))])
        BuildExecutor(self.app.config, self.app.store).execute_plan(plan)
        installed = {item.name for item in self.app.store.list_installed()}
        self.assertIn("libX11", installed)
        self.assertNotIn("libXext", installed)
        self.assertNotIn("libXrender", installed)

    def test_assume_lfs_base_installed_skips_base_dependencies(self):
        self.app.sync_selected_sources({"base"})
        self.app.store.upsert_package(
            PackageRecord(
                name="demoapp",
                version="1.0",
                source_origin="custom",
                depends=["bash"],
            )
        )
        plan = self.app.plan(["demoapp"])
        self.assertEqual([step.package.name for step in plan.ordered_steps][-2:], ["bash", "demoapp"])
        self.app.update_settings({"system_state": {"assume_lfs_base_installed": True}})
        plan = self.app.plan(["demoapp"])
        self.assertEqual([step.package.name for step in plan.ordered_steps], ["demoapp"])

    def test_jhalfs_tracking_skips_tracked_dependencies(self):
        self.app.store.upsert_package(PackageRecord(name="trackeddep", version="1.0", source_origin="blfs"))
        self.app.store.upsert_package(
            PackageRecord(
                name="demoapp",
                version="1.0",
                source_origin="custom",
                depends=["trackeddep"],
            )
        )
        plan = self.app.plan(["demoapp"])
        self.assertEqual([step.package.name for step in plan.ordered_steps], ["trackeddep", "demoapp"])
        write_instpkg_xml(
            self.tempdir,
            [PackageRecord("trackeddep", "1.0", "blfs")],
        )
        self.app.update_settings({"system_state": {"use_jhalfs_tracking": True}})
        plan = self.app.plan(["demoapp"])
        self.assertEqual([step.package.name for step in plan.ordered_steps], ["demoapp"])


if __name__ == "__main__":
    unittest.main()

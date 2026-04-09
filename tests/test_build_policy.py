import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from lfs_unified_pm.build import BuildExecutor
from lfs_unified_pm.config import default_config, ensure_directories
from lfs_unified_pm.models import BuildPlan, BuildStep, PackageRecord
from lfs_unified_pm.state import StateStore


class BuildPolicyTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix="lfs-pm-policy-")
        self.config = default_config(self.tempdir)
        ensure_directories(self.config)
        self.store = StateStore(self.config.db_path)
        self.executor = BuildExecutor(self.config, self.store)

    def tearDown(self):
        self.store.close()
        shutil.rmtree(self.tempdir)

    def test_blocks_la_removal_without_flag(self):
        with self.assertRaises(RuntimeError):
            self.executor._assert_la_policy(
                "demo",
                'find /usr/lib -name "*.la" -delete',
                allow_la_removal=False,
            )

    def test_allows_la_removal_with_flag(self):
        self.executor._assert_la_policy(
            "demo",
            'find /usr/lib -name "*.la" -delete',
            allow_la_removal=True,
        )

    def test_non_removal_command_is_allowed(self):
        package = PackageRecord(name="demo", version="1", source_origin="custom")
        self.executor._assert_la_policy(
            package.name,
            'echo "leave libtool archives alone"',
            allow_la_removal=False,
        )

    def test_command_review_can_cancel_before_execution(self):
        package = PackageRecord(
            name="demo",
            version="1",
            source_origin="custom",
            build_system="shell",
            phases={"build": ['printf "hello\\n" > "$DESTDIR/demo.txt"']},
        )
        self.store.upsert_package(package)
        self.store.save_settings({"build": {"command_review_mode": "manual"}})
        plan = BuildPlan(requested=["demo"], ordered_steps=[BuildStep(package=package)])
        executor = BuildExecutor(
            self.config,
            self.store,
            command_review_callback=lambda package, phases, mode, seconds: None,
        )
        with self.assertRaises(RuntimeError):
            executor.execute_plan(plan)


if __name__ == "__main__":
    unittest.main()

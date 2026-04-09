import unittest
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from lfs_unified_pm.catalog import PackageCatalog
from lfs_unified_pm.models import PackageRecord
from lfs_unified_pm.solver import DependencySolver


class SolverTests(unittest.TestCase):
    def test_topological_order(self):
        packages = [
            PackageRecord("zlib", "1", "lfs-base"),
            PackageRecord("openssl", "1", "lfs-base", depends=["zlib"]),
            PackageRecord("python", "1", "lfs-base", depends=["openssl"]),
        ]
        catalog = PackageCatalog(packages, ["lfs-base"])
        solver = DependencySolver(catalog, [])
        plan = solver.make_plan(["python"])
        self.assertEqual([step.package.name for step in plan.ordered_steps], ["zlib", "openssl", "python"])

    def test_t2_dependency_mode_can_prefer_t2(self):
        packages = [
            PackageRecord("demo", "1", "t2", depends=["libfoo"]),
            PackageRecord("libfoo", "1", "blfs"),
            PackageRecord("libfoo", "1", "t2"),
        ]
        catalog = PackageCatalog(packages, ["lfs-base", "blfs", "t2", "arch", "custom"])
        solver = DependencySolver(catalog, [])
        plan = solver.make_plan_for_requests(
            [{"name": "demo", "source_origin": "t2", "resolve_required": True, "t2_dependency_mode": "t2"}]
        )
        self.assertEqual(
            [(step.package.name, step.package.source_origin) for step in plan.ordered_steps],
            [("libfoo", "t2"), ("demo", "t2")],
        )

    def test_recommended_cycle_does_not_abort_plan(self):
        packages = [
            PackageRecord("rootpkg", "1", "blfs", recommends=["cycle-a"]),
            PackageRecord("cycle-a", "1", "blfs", depends=["cycle-b"]),
            PackageRecord("cycle-b", "1", "blfs", depends=["cycle-a"]),
        ]
        catalog = PackageCatalog(packages, ["lfs-base", "blfs", "t2", "arch", "custom"])
        solver = DependencySolver(catalog, [])
        plan = solver.make_plan(["rootpkg"], include_recommends=True)
        self.assertEqual(plan.unresolved, [])
        self.assertEqual(plan.conflicts, [])
        self.assertIn("rootpkg", [step.package.name for step in plan.ordered_steps])

    def test_allowed_dependency_sources_filter_candidates(self):
        packages = [
            PackageRecord("demo", "1", "custom", depends=["libfoo"]),
            PackageRecord("libfoo", "1", "blfs"),
            PackageRecord("libfoo", "1", "t2"),
        ]
        catalog = PackageCatalog(packages, ["lfs-base", "blfs", "t2", "arch", "custom"])
        solver = DependencySolver(catalog, [])
        plan = solver.make_plan_for_requests(
            [
                {
                    "name": "demo",
                    "source_origin": "custom",
                    "resolve_required": True,
                    "allowed_dependency_sources": ["t2"],
                }
            ]
        )
        self.assertEqual(
            [(step.package.name, step.package.source_origin) for step in plan.ordered_steps],
            [("libfoo", "t2"), ("demo", "custom")],
        )

    def test_empty_allowed_dependency_sources_make_dependency_unresolved(self):
        packages = [
            PackageRecord("demo", "1", "custom", depends=["libfoo"]),
            PackageRecord("libfoo", "1", "blfs"),
        ]
        catalog = PackageCatalog(packages, ["lfs-base", "blfs", "t2", "arch", "custom"])
        solver = DependencySolver(catalog, [])
        plan = solver.make_plan_for_requests(
            [
                {
                    "name": "demo",
                    "source_origin": "custom",
                    "resolve_required": True,
                    "allowed_dependency_sources": [],
                }
            ]
        )
        self.assertEqual(plan.unresolved, ["libfoo"])
        self.assertEqual([step.package.name for step in plan.ordered_steps], ["demo"])

    def test_dependency_aliases_resolve_common_blfs_names(self):
        packages = [
            PackageRecord("sendmail", "1", "blfs"),
            PackageRecord("xinit", "1", "blfs"),
            PackageRecord("demo", "1", "blfs", depends=["server-mail", "x-window-system"]),
        ]
        catalog = PackageCatalog(packages, ["lfs-base", "blfs", "t2", "arch", "custom"])
        solver = DependencySolver(catalog, [])
        plan = solver.make_plan(["demo"])
        self.assertEqual(
            [step.package.name for step in plan.ordered_steps],
            ["sendmail", "xinit", "demo"],
        )


if __name__ == "__main__":
    unittest.main()

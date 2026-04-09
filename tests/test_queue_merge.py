import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from lfs_unified_pm.models import BuildPlan, BuildStep, PackageRecord
from lfs_unified_pm.tui import _default_dependency_level, _merge_queue_plan, _reposition_new_items_for_existing_roots


class QueueMergeTests(unittest.TestCase):
    def test_default_dependency_level_uses_global_setting(self):
        package = PackageRecord("demo", "1", "blfs")
        settings = {"build": {"default_dependency_level": "optional"}}
        self.assertEqual(_default_dependency_level(settings, package), "optional")

    def test_shared_dependency_keeps_original_position_when_new_root_added(self):
        shared = PackageRecord("pixman", "1", "blfs")
        xorg_font = PackageRecord("xorg7-font", "1", "blfs", depends=["pixman"])
        xorg = PackageRecord("xorg-server", "1", "blfs", depends=["pixman", "xorg7-font"])
        extra = PackageRecord("demoapp", "1", "blfs", depends=["pixman"])

        state = {"queue": []}
        _merge_queue_plan(
            state,
            BuildPlan(
                requested=["xorg-server"],
                ordered_steps=[
                    BuildStep(package=shared),
                    BuildStep(package=xorg_font),
                    BuildStep(package=xorg),
                ],
            ),
            xorg,
            "blfs",
            "required",
            ["lfs-base", "blfs", "t2", "arch", "custom"],
        )
        _merge_queue_plan(
            state,
            BuildPlan(
                requested=["demoapp"],
                ordered_steps=[
                    BuildStep(package=shared),
                    BuildStep(package=extra),
                ],
            ),
            extra,
            "blfs",
            "required",
            ["lfs-base", "blfs", "t2", "arch", "custom"],
        )

        self.assertEqual(
            [item["name"] for item in state["queue"]],
            ["pixman", "demoapp", "xorg7-font", "xorg-server"],
        )

    def test_new_root_is_moved_to_recommended_position_for_existing_root(self):
        shared = PackageRecord("pixman", "1", "blfs")
        middle = PackageRecord("xorg7-font", "1", "blfs", depends=["pixman"])
        root = PackageRecord("xorg-server", "1", "blfs", depends=["pixman", "xorg7-font"])
        added = PackageRecord("xkeyboard-config", "1", "blfs", depends=["pixman"])

        state = {"queue": []}
        _merge_queue_plan(
            state,
            BuildPlan(
                requested=["xorg-server"],
                ordered_steps=[
                    BuildStep(package=shared),
                    BuildStep(package=middle),
                    BuildStep(package=root),
                ],
            ),
            root,
            "blfs",
            "required",
            ["lfs-base", "blfs", "t2", "arch", "custom"],
        )
        _merge_queue_plan(
            state,
            BuildPlan(
                requested=["xkeyboard-config"],
                ordered_steps=[
                    BuildStep(package=shared),
                    BuildStep(package=added),
                ],
            ),
            added,
            "blfs",
            "required",
            ["lfs-base", "blfs", "t2", "arch", "custom"],
        )

        class FakeApp:
            def plan_selection(self, name, source_origin, include_recommends=False, auto_optional=False, **kwargs):
                if name == "xorg-server" and include_recommends:
                    return BuildPlan(
                        requested=["xorg-server"],
                        ordered_steps=[
                            BuildStep(package=shared),
                            BuildStep(package=middle),
                            BuildStep(package=added),
                            BuildStep(package=root),
                        ],
                    )
                raise AssertionError("unexpected plan request")

        _reposition_new_items_for_existing_roots(
            FakeApp(),
            state,
            added,
            {("xkeyboard-config", "blfs")},
        )

        self.assertEqual(
            [item["name"] for item in state["queue"]],
            ["pixman", "xorg7-font", "xkeyboard-config", "xorg-server"],
        )


if __name__ == "__main__":
    unittest.main()

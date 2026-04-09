import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from lfs_unified_pm.adapters.t2 import T2PackageAdapter


class T2RecipeTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix="lfs-pm-t2-")
        self.pkgdir = os.path.join(self.tempdir, "package", "archiver", "demo")
        os.makedirs(self.pkgdir)

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_translates_t2_package_to_universal_recipe(self):
        desc = """[I] Demo package
[T] Demo package body
[U] https://example.invalid/demo
[C] base/tool
[L] GPL
[V] 1.2.3
[D] deadbeef demo-1.2.3.tar.gz https://example.invalid/

var_append confopt ' ' --disable-static
hook_add postmake 5 "install -Dm0644 README $root$docdir/README"
"""
        cache = """[DEP] bash
[DEP] zlib
"""
        with open(os.path.join(self.pkgdir, "demo.desc"), "w", encoding="utf-8") as handle:
            handle.write(desc)
        with open(os.path.join(self.pkgdir, "demo.cache"), "w", encoding="utf-8") as handle:
            handle.write(cache)

        package = T2PackageAdapter().load(os.path.join(self.tempdir, "package"))[0]
        self.assertEqual(package.name, "demo")
        self.assertEqual(package.version, "1.2.3")
        self.assertEqual(package.depends, ["bash", "zlib"])
        self.assertEqual(package.recipe_format, "t2-universal")
        self.assertIn("configure", package.phases)
        self.assertIn("install", package.phases)
        self.assertEqual(package.metadata["t2_recipe"]["supported"], True)
        self.assertTrue(package.metadata["recipe_digest"])


if __name__ == "__main__":
    unittest.main()


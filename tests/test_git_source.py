import os
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from lfs_unified_pm.app import PackageManagerApp


class GitSourceTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix="lfs-pm-git-")
        self.source_repo = os.path.join(self.tempdir, "source")
        self.clone_repo = os.path.join(self.tempdir, "clone")
        self.root = os.path.join(self.tempdir, "root")
        os.makedirs(self.root)
        subprocess.run(["git", "init", self.source_repo], check=True, capture_output=True)
        subprocess.run(["git", "-C", self.source_repo, "config", "user.name", "tester"], check=True)
        subprocess.run(["git", "-C", self.source_repo, "config", "user.email", "tester@example.invalid"], check=True)
        self._write_package("1.0")
        subprocess.run(["git", "-C", self.source_repo, "add", "."], check=True)
        subprocess.run(["git", "-C", self.source_repo, "commit", "-m", "initial"], check=True, capture_output=True)

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def _write_package(self, version, extra_line=""):
        pkgdir = os.path.join(self.source_repo, "package", "archiver", "demo")
        os.makedirs(pkgdir, exist_ok=True)
        with open(os.path.join(pkgdir, "demo.desc"), "w", encoding="utf-8") as handle:
            handle.write(
                "[I] Demo\n[T] Demo\n[C] base/tool\n[L] GPL\n[V] %s\n[D] deadbeef demo-%s.tar.gz https://example.invalid/\n%s\n"
                % (version, version, extra_line)
            )
        with open(os.path.join(pkgdir, "demo.cache"), "w", encoding="utf-8") as handle:
            handle.write("[DEP] bash\n")

    def test_sync_t2_git_reports_changes(self):
        app = PackageManagerApp(self.root)
        try:
            imported, report = app.sync_t2_git(repo_dir=self.clone_repo, repo_url=self.source_repo)
            self.assertEqual(len([item for item in imported if item.source_origin == "t2"]), 1)
            self.assertEqual(report["t2"]["added"], ["demo"])

            self._write_package("1.1")
            subprocess.run(["git", "-C", self.source_repo, "add", "."], check=True)
            subprocess.run(["git", "-C", self.source_repo, "commit", "-m", "update"], check=True, capture_output=True)

            _, report = app.sync_t2_git(repo_dir=self.clone_repo, repo_url=self.source_repo)
            self.assertEqual(report["t2"]["version_changed"], ["demo"])
            self.assertTrue(report["git"]["previous_head"])
            self.assertTrue(report["git"]["current_head"])
        finally:
            app.close()


if __name__ == "__main__":
    unittest.main()

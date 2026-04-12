import io
import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from lfs_unified_pm import cli


class CliTests(unittest.TestCase):
    def test_main_reports_runtime_error_cleanly(self):
        stderr = io.StringIO()
        with patch("lfs_unified_pm.cli._dispatch", side_effect=RuntimeError("demo failure")), patch(
            "sys.stderr", stderr
        ):
            status = cli.main(["--root", "./lfs-build-root", "lfs-base-status"])
        self.assertEqual(status, 1)
        self.assertIn("Error: demo failure", stderr.getvalue())

    def test_execution_notice_callback_defaults_to_stdout_and_prints_command(self):
        stdout = io.StringIO()
        callback = cli._make_execution_notice_callback(stream=stdout, seconds=0)
        allowed = callback(
            {
                "description": "chapter05/501-binutils-pass1",
                "context": "lfs-user",
                "target_root": "/tmp/lfs-build-root",
                "location": "/home/lfs",
                "env": {"LFS": "/tmp/lfs-build-root"},
                "command_text": "sudo -H -u lfs bash /tmp/lfs-build-root/lfs-base/chapter05/501-binutils-pass1",
            }
        )
        self.assertTrue(allowed)
        text = stdout.getvalue()
        self.assertIn("About to execute LFS step", text)
        self.assertIn("Command:\n", text)
        self.assertIn("sudo -H -u lfs bash /tmp/lfs-build-root/lfs-base/chapter05/501-binutils-pass1", text)

    def test_execution_notice_callback_tags_chroot_commands(self):
        stdout = io.StringIO()
        callback = cli._make_execution_notice_callback(stream=stdout, seconds=0)
        allowed = callback(
            {
                "description": "chapter08/801-man-pages",
                "context": "chroot-root",
                "target_root": "/tmp/lfs-build-root",
                "location": "chroot:/tmp/lfs-build-root",
                "env": {"LFS": "/tmp/lfs-build-root"},
                "command_text": "sudo env LFS=/tmp/lfs-build-root chroot /tmp/lfs-build-root /usr/bin/env -i HOME=/root /bin/bash --login -c /lfs-base/chapter08/801-man-pages",
            }
        )
        self.assertTrue(allowed)
        text = stdout.getvalue()
        self.assertIn(
            "[chroot /tmp/lfs-build-root] sudo env LFS=/tmp/lfs-build-root chroot /tmp/lfs-build-root /usr/bin/env -i HOME=/root /bin/bash --login -c /lfs-base/chapter08/801-man-pages",
            text,
        )


if __name__ == "__main__":
    unittest.main()

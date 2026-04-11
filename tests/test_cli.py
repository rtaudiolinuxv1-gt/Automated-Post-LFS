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


if __name__ == "__main__":
    unittest.main()

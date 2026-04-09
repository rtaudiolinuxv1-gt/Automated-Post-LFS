import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from lfs_unified_pm.jhalfs import read_instpkg_xml, write_instpkg_xml
from lfs_unified_pm.models import InstalledRecord


class JhalfsXmlTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix="lfs-pm-jhalfs-")

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_write_and_read_tracking_file(self):
        path = write_instpkg_xml(
            self.tempdir,
            [
                InstalledRecord("bash", "5.3.3", "lfs-base", "base-scan"),
                InstalledRecord("hello-local", "1.0", "custom", "explicit"),
            ],
        )
        with open(path, "r", encoding="utf-8") as handle:
            content = handle.read()
        self.assertIn("<sublist>", content)
        self.assertIn("<name>Installed</name>", content)
        self.assertIn("<package><name>bash</name><version>5.3.3</version></package>", content.replace("\n", ""))
        tracked = read_instpkg_xml(self.tempdir)
        self.assertEqual(tracked["bash"], "5.3.3")
        self.assertEqual(tracked["hello-local"], "1.0")


if __name__ == "__main__":
    unittest.main()

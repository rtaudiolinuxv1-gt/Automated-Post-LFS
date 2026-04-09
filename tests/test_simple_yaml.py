import unittest
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from lfs_unified_pm.simple_yaml import load


class SimpleYamlTests(unittest.TestCase):
    def test_mapping_and_list(self):
        payload = load(
            """
packages:
  - name: demo
    version: 1
    depends: [bash, zlib]
"""
        )
        self.assertEqual(payload["packages"][0]["name"], "demo")
        self.assertEqual(payload["packages"][0]["depends"], ["bash", "zlib"])


if __name__ == "__main__":
    unittest.main()

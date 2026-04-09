import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from lfs_unified_pm.adapters.blfs_xml import BlfsXmlAdapter, _parse_packages_xml


class BlfsAdapterTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix="lfs-pm-blfs-")

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_extracts_dependencies_and_sources(self):
        xml_path = os.path.join(self.tempdir, "demo.xml")
        with open(xml_path, "w", encoding="utf-8") as handle:
            handle.write(
                """<?xml version="1.0"?>
<sect1 id="demo" xreflabel="Demo-1.2.3">
  <title>Demo-1.2.3</title>
  <sect2 role="package">
    <title>Introduction to Demo</title>
    <para>Demo package.</para>
    <bridgehead renderas="sect3">Package Information</bridgehead>
    <itemizedlist spacing="compact">
      <listitem><para>Download: <ulink url="https://example.org/demo-1.2.3.tar.xz"/></para></listitem>
    </itemizedlist>
    <bridgehead renderas="sect3">Additional Downloads</bridgehead>
    <itemizedlist spacing="compact">
      <listitem><para>Patch: <ulink url="https://example.org/demo-fix.patch"/></para></listitem>
    </itemizedlist>
    <bridgehead renderas="sect3">Demo Dependencies</bridgehead>
    <para role="required"><xref linkend="bash"/></para>
    <para role="recommended"><xref linkend="python3"/></para>
    <para role="optional"><ulink url="https://example.org/external-lib">external-lib</ulink></para>
  </sect2>
  <sect2 role="installation">
    <title>Installation of Demo</title>
    <screen><userinput>tar -xf demo-1.2.3.tar.xz</userinput></screen>
  </sect2>
</sect1>
"""
            )
        package = BlfsXmlAdapter().load(self.tempdir)[0]
        self.assertEqual(package.name, "demo")
        self.assertEqual(package.depends, ["bash"])
        self.assertEqual(package.recommends, ["python3"])
        self.assertEqual(package.optional, ["external-lib"])
        self.assertEqual(
            package.sources,
            [
                "https://example.org/demo-1.2.3.tar.xz",
                "https://example.org/demo-fix.patch",
            ],
        )

    def test_parses_jhalfs_module_dependencies(self):
        packages_xml = os.path.join(self.tempdir, "packages.xml")
        with open(packages_xml, "w", encoding="utf-8") as handle:
            handle.write(
                """<?xml version="1.0"?>
<princList>
  <list id="x">
    <name>X</name>
    <sublist id="installing">
      <name>Installing X</name>
      <package>
        <name>xorg7-lib</name>
        <module>
          <name>libX11</name>
          <version>1.8.12</version>
          <dependency status="required" build="before" name="libxcb" type="ref"/>
        </module>
        <module>
          <name>libXext</name>
          <version>1.3.7</version>
          <dependency status="required" build="before" name="libX11" type="ref"/>
        </module>
      </package>
      <package>
        <name>xorg-server</name>
        <version>21.1.21</version>
        <dependency status="required" build="before" name="libXext" type="ref"/>
        <dependency status="optional" build="before" name="dmx" type="link"/>
      </package>
    </sublist>
  </list>
</princList>
"""
            )
        graph = _parse_packages_xml(packages_xml)
        self.assertNotIn("xorg7-lib", graph)
        self.assertEqual(graph["libX11"]["depends"], ["libxcb"])
        self.assertEqual(graph["libX11"]["group_parent"], "xorg7-lib")
        self.assertEqual(graph["libX11"]["category"], "blfs/x/installing")
        self.assertEqual(graph["xorg-server"]["depends"], ["libXext"])
        self.assertEqual(graph["xorg-server"]["optional"], ["dmx"])


if __name__ == "__main__":
    unittest.main()

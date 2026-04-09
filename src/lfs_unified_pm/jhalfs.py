from __future__ import annotations

import os
import xml.etree.ElementTree as ET


def write_instpkg_xml(root, records, relative_path="/var/lib/jhalfs/BLFS/instpkg.xml"):
    target = os.path.join(root, relative_path.lstrip("/"))
    os.makedirs(os.path.dirname(target), exist_ok=True)
    document = ET.Element("sublist")
    name = ET.SubElement(document, "name")
    name.text = "Installed"
    for record in sorted(records, key=lambda item: item.name):
        node = ET.SubElement(document, "package")
        pkg_name = ET.SubElement(node, "name")
        pkg_name.text = record.name
        version = ET.SubElement(node, "version")
        version.text = record.version
    tree = ET.ElementTree(document)
    tree.write(target, encoding="utf-8", xml_declaration=True)
    return target


def read_instpkg_xml(root, relative_path="/var/lib/jhalfs/BLFS/instpkg.xml"):
    target = os.path.join(root, relative_path.lstrip("/"))
    if not os.path.exists(target):
        return {}
    try:
        tree = ET.parse(target)
    except ET.ParseError:
        return {}
    result = {}
    for node in tree.findall(".//package"):
        name = node.findtext("name", default="").strip()
        version = node.findtext("version", default="").strip()
        if name:
            result[name] = version
    return result

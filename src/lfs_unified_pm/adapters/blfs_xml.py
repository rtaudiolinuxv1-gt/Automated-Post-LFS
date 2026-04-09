from __future__ import annotations

import html
import json
import os
import re
import hashlib
import shutil
import subprocess
import tempfile
import xml.etree.ElementTree as ET

from ..models import PackageRecord


class BlfsXmlAdapter:
    source_origin = "blfs"

    def __init__(self, jhalfs_root="", revision="systemd", work_dir=""):
        self.jhalfs_root = os.path.abspath(jhalfs_root) if jhalfs_root else ""
        self.revision = revision
        self.work_dir = os.path.abspath(work_dir) if work_dir else ""

    def load(self, path):
        if os.path.isdir(path):
            packages = self._load_generated_bundle(path)
            if packages:
                return packages
        xml_files, entity_map, root_dir = self._discover_files_and_entities(path)
        packages = []
        for xml_path in sorted(xml_files):
            package = self._parse_file(xml_path, entity_map, root_dir)
            if package:
                packages.append(package)
        graph = self._load_jhalfs_graph(path)
        if graph:
            packages = self._merge_with_jhalfs_graph(packages, graph)
        return packages

    def _discover_files_and_entities(self, path):
        if os.path.isdir(path):
            xml_files = []
            root_dir = path
            for root, _, files in os.walk(path):
                for name in files:
                    if name.endswith(".xml"):
                        xml_files.append(os.path.join(root, name))
        else:
            xml_files = [path]
            root_dir = os.path.dirname(path)
        entity_map = _load_entities(root_dir)
        return xml_files, entity_map, root_dir

    def _parse_file(self, path, entity_map, root_dir):
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as handle:
                raw = handle.read()
        except OSError:
            return None
        merged_entities = dict(entity_map)
        merged_entities.update(_parse_inline_entities(raw))
        merged_entities = _resolve_entities(merged_entities)

        if "<sect1" not in raw and "<sect2" not in raw:
            return None
        header = re.search(r"<(sect1|sect2)\b([^>]*)>", raw)
        if not header:
            return None
        attrs = _parse_attrs(header.group(2))
        package_id = attrs.get("id", "")
        if not package_id:
            return None
        xreflabel = _expand_entities(attrs.get("xreflabel", ""), merged_entities)
        title_match = re.search(r"<title>(.*?)</title>", raw, re.S)
        title = _clean_xml_text(title_match.group(1), merged_entities) if title_match else package_id
        version = _extract_version(xreflabel or title)
        package_block = _find_block(raw, r"<sect2\b[^>]*role=\"package\"[^>]*>", "</sect2>")
        install_block = _find_block(raw, r"<sect2\b[^>]*role=\"installation\"[^>]*>", "</sect2>")
        summary = _extract_summary(package_block, merged_entities) or title
        depends, recommends, optional, dep_details = self._parse_dependencies(package_block, merged_entities)
        commands = self._parse_commands(install_block, merged_entities)
        sources = self._parse_sources(package_block, merged_entities)
        return PackageRecord(
            name=package_id,
            version=version,
            source_origin=self.source_origin,
            summary=summary,
            category=self._category_for_path(path, root_dir),
            description=summary,
            build_system="blfs-commands" if commands else "",
            recipe_format="docbook",
            depends=depends,
            recommends=recommends,
            optional=optional,
            provides=[],
            conflicts=[],
            sources=sources,
            phases=commands,
            metadata={"xml_path": path, "title": title, "xreflabel": xreflabel, "dependency_detail": dep_details},
        )

    def _category_for_path(self, path, root_dir):
        relative_dir = os.path.dirname(os.path.relpath(path, root_dir))
        if not relative_dir or relative_dir == ".":
            return "blfs"
        return "blfs/%s" % relative_dir.replace(os.sep, "/")

    def _parse_dependencies(self, package_block, entity_map):
        required = []
        recommended = []
        optional = []
        details = []
        if not package_block:
            return required, recommended, optional, details
        for match in re.finditer(r"<para\b([^>]*)>(.*?)</para>", package_block, re.S):
            attrs = _parse_attrs(match.group(1))
            role = attrs.get("role", "")
            if role not in ("required", "recommended", "optional"):
                continue
            body = match.group(2)
            items = []
            for xref in re.finditer(r"<xref\b([^>]*)/>", body):
                dep_attrs = _parse_attrs(xref.group(1))
                if dep_attrs.get("role") == "nodep":
                    continue
                name = dep_attrs.get("linkend", "").strip()
                if name:
                    items.append(name)
                    details.append({"name": name, "status": role, "build": _dep_build(dep_attrs), "type": "ref"})
            for ulink in re.finditer(r"<ulink\b([^>]*)>(.*?)</ulink>", body, re.S):
                dep_attrs = _parse_attrs(ulink.group(1))
                if dep_attrs.get("role") == "nodep":
                    continue
                name = _slug(_clean_xml_text(ulink.group(2), entity_map))
                if name:
                    items.append(name)
                    details.append({"name": name, "status": role, "build": _dep_build(dep_attrs), "type": "link"})
            if role == "required":
                required.extend(items)
            elif role == "recommended":
                recommended.extend(items)
            else:
                optional.extend(items)
        return sorted(set(required)), sorted(set(recommended)), sorted(set(optional)), details

    def _parse_commands(self, install_block, entity_map):
        commands = []
        if not install_block:
            return {}
        for userinput in re.finditer(r"<userinput[^>]*>(.*?)</userinput>", install_block, re.S):
            text = _clean_xml_text(userinput.group(1), entity_map)
            if text:
                commands.append(text)
        if not commands:
            return {}
        return {"build": commands}

    def _parse_sources(self, package_block, entity_map):
        if not package_block:
            return []
        start = package_block.find("Package Information")
        start = start if start >= 0 else 0
        end = len(package_block)
        dep_match = re.search(r"<bridgehead\b[^>]*>\s*[^<]*Dependencies\s*</bridgehead>", package_block, re.S)
        if dep_match:
            end = dep_match.start()
        source_block = package_block[start:end]
        urls = []
        seen = set()
        for match in re.finditer(r'<ulink\b([^>]*)/?>', source_block, re.S):
            attrs = _parse_attrs(match.group(1))
            url = _expand_entities(attrs.get("url", "").strip(), entity_map)
            if not url or url in seen:
                continue
            seen.add(url)
            urls.append(url)
        return urls

    def _load_jhalfs_graph(self, path):
        if not self.jhalfs_root or not os.path.isdir(path):
            return {}
        if not shutil.which("xsltproc") or not shutil.which("xmllint"):
            return {}
        try:
            return _generate_jhalfs_dependency_graph(path, self.jhalfs_root, self.revision)
        except Exception:
            return {}

    def _load_generated_bundle(self, path):
        if not self.jhalfs_root or not os.path.isdir(path):
            return []
        if not shutil.which("xsltproc") or not shutil.which("xmllint") or not shutil.which("make"):
            return []
        try:
            full_xml, packages_xml = _prepare_jhalfs_bundle(
                path,
                self.jhalfs_root,
                self.revision,
                self.work_dir,
            )
        except Exception:
            return []
        return _load_packages_from_generated_files(full_xml, packages_xml, self.source_origin)

    def _merge_with_jhalfs_graph(self, packages, graph):
        merged = []
        for package in packages:
            details = graph.get(package.name)
            if not details:
                merged.append(package)
                continue
            package.depends = details.get("depends", package.depends)
            package.recommends = details.get("recommends", package.recommends)
            package.optional = details.get("optional", package.optional)
            package.metadata["dependency_detail"] = details.get(
                "dependency_detail",
                package.metadata.get("dependency_detail", []),
            )
            package.metadata["dependency_source"] = "jhalfs-packages-xml"
            if details.get("version") and package.version in ("", "unknown"):
                package.version = details["version"]
            merged.append(package)
        return merged


def _slug(text):
    result = []
    for char in text.lower():
        if char.isalnum():
            result.append(char)
        elif result and result[-1] != "-":
            result.append("-")
    return "".join(result).strip("-")


def _parse_attrs(text):
    return dict(re.findall(r'([A-Za-z_:][A-Za-z0-9_.:-]*)="([^"]*)"', text))


def _load_entities(root_dir):
    entity_map = {}
    for name in ("general.ent", "packages.ent", "gnome.ent"):
        path = os.path.join(root_dir, name)
        if os.path.exists(path):
            entity_map.update(_parse_entity_file(path))
    return _resolve_entities(entity_map)


def _parse_entity_file(path):
    entity_map = {}
    with open(path, "r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            match = re.search(r'<!ENTITY\s+(?:%\s+)?([A-Za-z0-9_.:-]+)\s+"(.*?)">', line)
            if match:
                entity_map[match.group(1)] = match.group(2)
    return entity_map


def _resolve_entities(entity_map):
    resolved = dict(entity_map)
    pattern = re.compile(r"([&%])([A-Za-z0-9_.:-]+);")
    changed = True
    rounds = 0
    while changed and rounds < 20:
        changed = False
        rounds += 1
        for key, value in list(resolved.items()):
            new_value = pattern.sub(lambda m: resolved.get(m.group(2), m.group(0)), value)
            if new_value != value:
                resolved[key] = new_value
                changed = True
    return resolved


def _expand_entities(text, entity_map):
    pattern = re.compile(r"([&%])([A-Za-z0-9_.:-]+);")
    for _ in range(10):
        new_text = pattern.sub(lambda m: entity_map.get(m.group(2), m.group(0)), text)
        if new_text == text:
            break
        text = new_text
    return html.unescape(text)


def _parse_inline_entities(raw):
    return dict(re.findall(r'<!ENTITY\s+(?:%\s+)?([A-Za-z0-9_.:-]+)\s+"(.*?)">', raw))


def _clean_xml_text(text, entity_map):
    expanded = _expand_entities(text, entity_map)
    expanded = re.sub(r"<[^>]+>", " ", expanded)
    expanded = re.sub(r"\s+", " ", expanded)
    return expanded.strip()


def _find_block(text, start_pattern, end_tag):
    start = re.search(start_pattern, text)
    if not start:
        return ""
    end = text.find(end_tag, start.start())
    if end == -1:
        return text[start.start():]
    return text[start.start(): end + len(end_tag)]


def _extract_summary(package_block, entity_map):
    if not package_block:
        return ""
    for match in re.finditer(r"<para\b([^>]*)>(.*?)</para>", package_block, re.S):
        attrs = _parse_attrs(match.group(1))
        if attrs.get("role") in ("required", "recommended", "optional", "usernotes"):
            continue
        text = _clean_xml_text(match.group(2), entity_map)
        if text:
            return text
    return ""


def _extract_version(text):
    if not text:
        return "unknown"
    match = re.search(r"-([0-9][A-Za-z0-9._+-]*)\s*$", text)
    if match:
        return match.group(1)
    match = re.search(r"\b([0-9]+(?:\.[0-9A-Za-z_+-]+)+)\b", text)
    return match.group(1) if match else "unknown"


def _dep_build(attrs):
    role = attrs.get("role", "")
    if role == "runtime":
        return "after"
    if role == "first":
        return "first"
    return "before"


def _prepare_jhalfs_bundle(blfs_root, jhalfs_root, revision, work_dir):
    bundle_dir = _bundle_dir(blfs_root, revision, work_dir)
    os.makedirs(bundle_dir, exist_ok=True)
    full_xml = os.path.join(bundle_dir, _blfs_full_name(revision))
    packages_xml = os.path.join(bundle_dir, "packages.xml")
    metadata_path = os.path.join(bundle_dir, "bundle-meta.json")
    source_stamp = _bundle_source_stamp(blfs_root, jhalfs_root)
    metadata = _load_bundle_metadata(metadata_path)
    if (
        metadata.get("source_stamp") != source_stamp
        or not os.path.isfile(full_xml)
        or not os.path.isfile(packages_xml)
    ):
        _render_bundle(blfs_root, jhalfs_root, revision, bundle_dir)
        with open(metadata_path, "w", encoding="utf-8") as handle:
            json.dump({"source_stamp": source_stamp, "revision": revision}, handle)
    return full_xml, packages_xml


def _load_bundle_metadata(path):
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return {}


def _bundle_dir(blfs_root, revision, work_dir):
    digest = hashlib.sha256(
        ("%s:%s" % (os.path.abspath(blfs_root), revision)).encode("utf-8")
    ).hexdigest()[:16]
    base_dir = work_dir or tempfile.gettempdir()
    return os.path.join(base_dir, "blfs-jhalfs-%s" % digest)


def _bundle_source_stamp(blfs_root, jhalfs_root):
    paths = [
        os.path.join(blfs_root, "index.xml"),
        os.path.join(blfs_root, "general.ent"),
        os.path.join(blfs_root, "packages.ent"),
        os.path.join(blfs_root, "version.ent"),
        os.path.join(jhalfs_root, "gen-special.sh"),
        os.path.join(jhalfs_root, "packdesc.dtd"),
        os.path.join(jhalfs_root, "xsl", "gen_pkg_list.xsl"),
    ]
    latest = 0
    for path in paths:
        try:
            latest = max(latest, int(os.path.getmtime(path)))
        except OSError:
            continue
    return "%s:%s" % (latest, os.path.abspath(blfs_root))


def _render_bundle(blfs_root, jhalfs_root, revision, bundle_dir):
    _run(
        ["make", "-C", blfs_root, "validate", "REV=%s" % revision, "RENDERTMP=%s" % bundle_dir],
    )
    full_xml = os.path.join(bundle_dir, _blfs_full_name(revision))
    lfs_full = os.path.join(bundle_dir, "lfs-full.xml")
    instpkg = os.path.join(bundle_dir, "instpkg.xml")
    special_cases = os.path.join(bundle_dir, "specialCases.xsl")
    packdesc = os.path.join(bundle_dir, "packdesc.dtd")
    packages_xml = os.path.join(bundle_dir, "packages.xml")
    with open(lfs_full, "w", encoding="utf-8") as handle:
        handle.write("<?xml version=\"1.0\"?>\n<book/>\n")
    with open(instpkg, "w", encoding="utf-8") as handle:
        handle.write("<?xml version=\"1.0\"?>\n<sublist><name>Installed</name></sublist>\n")
    _run(["bash", os.path.join(jhalfs_root, "gen-special.sh"), full_xml, special_cases, blfs_root])
    shutil.copyfile(os.path.join(jhalfs_root, "packdesc.dtd"), packdesc)
    _run(
        [
            "xsltproc",
            "--nonet",
            "--path",
            "%s:%s:%s" % (bundle_dir, os.path.join(jhalfs_root, "xsl"), blfs_root),
            "--stringparam",
            "lfs-full",
            lfs_full,
            "--stringparam",
            "installed-packages",
            instpkg,
            "-o",
            packages_xml,
            os.path.join(jhalfs_root, "xsl", "gen_pkg_list.xsl"),
            full_xml,
        ]
    )


def _blfs_full_name(revision):
    return "blfs-full.xml" if revision == "sysv" else "blfs-systemd-full.xml"


def _run(command):
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _load_packages_from_generated_files(full_xml_path, packages_xml_path, source_origin):
    package_graph = _parse_packages_xml(packages_xml_path)
    if not package_graph:
        return []
    section_map = _index_sections(full_xml_path)
    packages = []
    for name in sorted(package_graph):
        details = package_graph[name]
        section = section_map.get(name)
        commands = _extract_section_commands(section)
        sources = _extract_section_sources(section)
        summary = _extract_section_summary(section) or details.get("summary", name)
        metadata = {
            "dependency_detail": details.get("dependency_detail", []),
            "dependency_source": "jhalfs-packages-xml",
            "group_parent": details.get("group_parent", ""),
        }
        provider = _provider_metadata(details, section_map, source_origin)
        if provider:
            metadata["build_provider"] = provider
        packages.append(
            PackageRecord(
                name=name,
                version=details.get("version", "unknown"),
                source_origin=source_origin,
                summary=summary,
                category=details.get("category", "blfs"),
                description=summary,
                build_system="blfs-commands" if commands else "",
                recipe_format="docbook-jhalfs",
                depends=details.get("depends", []),
                recommends=details.get("recommends", []),
                optional=details.get("optional", []),
                provides=[],
                conflicts=[],
                sources=sources,
                phases=commands,
                metadata=metadata,
            )
        )
    return packages


def _parse_packages_xml(path):
    root = ET.parse(path).getroot()
    graph = {}
    groups = {}
    for list_node in root.findall("list"):
        list_id = list_node.get("id", "").strip()
        for sublist in list_node.findall("sublist"):
            sublist_id = sublist.get("id", "").strip()
            category = _category_from_list_ids(list_id, sublist_id)
            for package in sublist.findall("package"):
                modules = package.findall("module")
                if modules:
                    group_name = _child_text(package, "name")
                    member_names = []
                    for module in modules:
                        details = _dependency_payload(module)
                        details["category"] = category
                        details["group_parent"] = group_name
                        graph[details["name"]] = details
                        member_names.append(details["name"])
                    groups[group_name] = {
                        "category": category,
                        "members": member_names,
                    }
                    continue
                details = _dependency_payload(package)
                details["category"] = category
                graph[details["name"]] = details
    for package in graph.values():
        group_name = package.get("group_parent", "")
        if not group_name:
            continue
        group = groups.get(group_name, {})
        package["group_members"] = list(group.get("members", []))
    return graph


def _category_from_list_ids(list_id, sublist_id):
    parts = ["blfs"]
    if list_id and list_id != "lfs":
        parts.append(list_id)
    if sublist_id:
        parts.append(sublist_id)
    return "/".join(parts)


def _dependency_payload(node):
    details = {
        "name": _child_text(node, "name"),
        "version": _child_text(node, "version") or "unknown",
        "depends": [],
        "recommends": [],
        "optional": [],
        "dependency_detail": [],
    }
    for dep in node.findall("dependency"):
        _append_dependency(details, dep)
    details["depends"] = _dedupe(details["depends"])
    details["recommends"] = _dedupe(details["recommends"])
    details["optional"] = _dedupe(details["optional"])
    return details


def _append_dependency(details, dep):
    for child in dep.findall("dependency"):
        _append_dependency(details, child)
    name = dep.get("name", "").strip()
    if not name:
        return
    status = dep.get("status", "recommended").strip()
    build = dep.get("build", "before").strip()
    dep_type = dep.get("type", "ref").strip()
    if status == "required":
        details["depends"].append(name)
    elif status == "optional":
        details["optional"].append(name)
    else:
        details["recommends"].append(name)
    details["dependency_detail"].append(
        {"name": name, "status": status, "build": build, "type": dep_type}
    )


def _dedupe(items):
    seen = set()
    ordered = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def _index_sections(path):
    root = ET.parse(path).getroot()
    index = {}
    for element in root.iter():
        element_id = element.get("id", "").strip()
        if element_id and element_id not in index:
            index[element_id] = element
    return index


def _extract_section_summary(section):
    if section is None:
        return ""
    if section.tag == "varlistentry":
        para = section.find("./listitem/para")
        return _normalize_text("".join(para.itertext())) if para is not None else ""
    package_block = _package_block(section)
    for para in package_block.iter("para"):
        role = para.get("role", "")
        if role in ("required", "recommended", "optional", "usernotes"):
            continue
        text = _normalize_text("".join(para.itertext()))
        if text:
            return text
    title = package_block.find("title")
    return _normalize_text("".join(title.itertext())) if title is not None else ""


def _extract_section_commands(section):
    if section is None:
        return {}
    install_block = section.find("./sect2[@role='installation']")
    if install_block is None:
        return {}
    commands = []
    for userinput in install_block.iter("userinput"):
        text = _normalize_text("".join(userinput.itertext()))
        if text:
            commands.append(text)
    return {"build": commands} if commands else {}


def _extract_section_version(section):
    if section is None:
        return "group"
    for candidate in (section.get("xreflabel", ""), _section_title(section)):
        version = _extract_version(candidate)
        if version and version != "unknown":
            return version
    return "group"


def _extract_section_sources(section):
    if section is None:
        return []
    package_block = _package_block(section)
    urls = []
    seen = set()
    for link in package_block.iter("ulink"):
        url = (link.get("url") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def _section_title(section):
    title = section.find("title")
    if title is None:
        return ""
    return _normalize_text("".join(title.itertext()))


def _package_block(section):
    if section.tag == "varlistentry":
        return section
    package_block = section.find("./sect2[@role='package']")
    return package_block if package_block is not None else section


def _provider_metadata(details, section_map, source_origin):
    group_parent = details.get("group_parent", "")
    if not group_parent:
        return {}
    parent_section = section_map.get(group_parent)
    if parent_section is None:
        return {}
    phases = _extract_section_commands(parent_section)
    sources = _extract_section_sources(parent_section)
    if not phases:
        return {}
    return {
        "name": group_parent,
        "version": _extract_section_version(parent_section),
        "source_origin": source_origin,
        "category": details.get("category", "blfs"),
        "summary": _extract_section_summary(parent_section) or group_parent,
        "phases": phases,
        "sources": sources,
        "members": list(details.get("group_members", [])),
    }


def _normalize_text(text):
    return re.sub(r"\s+", " ", text or "").strip()


def _child_text(node, tag):
    child = node.find(tag)
    if child is None:
        return ""
    return _normalize_text(child.text or "")


def _generate_jhalfs_dependency_graph(path, jhalfs_root, revision):
    full_xml, packages_xml = _prepare_jhalfs_bundle(path, jhalfs_root, revision, "")
    return _parse_packages_xml(packages_xml)

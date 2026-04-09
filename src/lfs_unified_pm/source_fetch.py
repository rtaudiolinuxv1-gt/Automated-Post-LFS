from __future__ import annotations

import os
import shutil
import urllib.parse
import urllib.request


REMOTE_PREFIXES = ("http://", "https://", "ftp://")


def fetch_sources_into(build_dir, sources):
    os.makedirs(build_dir, exist_ok=True)
    for source, filename in unique_source_specs(sources):
        target = os.path.join(build_dir, filename)
        if os.path.exists(target):
            continue
        if is_remote_source(source):
            _download_source(source, target)
            continue
        if not os.path.exists(source):
            raise RuntimeError("Missing local source file: %s" % source)
        if os.path.isdir(source):
            raise RuntimeError("Source path is a directory, expected file: %s" % source)
        shutil.copy2(source, target)


def source_stage_commands(sources, build_dir_var="$BUILD_DIR"):
    specs = unique_source_specs(sources)
    if not specs:
        return []
    lines = [
        "fetch_source() {",
        "  src=\"$1\"",
        "  name=\"$2\"",
        "  target=%s/$name" % build_dir_var,
        "  if [ -f \"$target\" ]; then",
        "    return 0",
        "  fi",
        "  case \"$src\" in",
        "    http://*|https://*|ftp://*)",
        "      if command -v wget >/dev/null 2>&1; then",
        "        wget -O \"$target\" \"$src\"",
        "      elif command -v curl >/dev/null 2>&1; then",
        "        curl -L \"$src\" -o \"$target\"",
        "      else",
        "        python3 - \"$src\" \"$target\" <<'PY'",
        "import sys",
        "import urllib.request",
        "urllib.request.urlretrieve(sys.argv[1], sys.argv[2])",
        "PY",
        "      fi",
        "      ;;",
        "    *)",
        "      cp -f \"$src\" \"$target\"",
        "      ;;",
        "  esac",
        "}",
        "",
    ]
    for source, filename in specs:
        lines.append("fetch_source %s %s" % (_shell_quote(source), _shell_quote(filename)))
    lines.append("")
    return lines


def unique_source_specs(sources):
    specs = []
    seen_names = set()
    for source in sources or ():
        source = (source or "").strip()
        if not source:
            continue
        filename = source_filename(source)
        if not filename or filename in seen_names:
            continue
        seen_names.add(filename)
        specs.append((source, filename))
    return specs


def source_filename(source):
    source = (source or "").strip()
    if not source:
        return ""
    if is_remote_source(source):
        parsed = urllib.parse.urlparse(source)
        path = urllib.parse.unquote(parsed.path or "")
        return os.path.basename(path.rstrip("/"))
    return os.path.basename(source)


def is_remote_source(source):
    source = (source or "").strip().lower()
    return source.startswith(REMOTE_PREFIXES)


def _download_source(source, target):
    try:
        urllib.request.urlretrieve(source, target)
    except Exception as error:
        raise RuntimeError("Failed to download %s: %s" % (source, error))


def _shell_quote(value):
    return "'" + value.replace("'", "'\"'\"'") + "'"

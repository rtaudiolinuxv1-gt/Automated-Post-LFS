from __future__ import annotations

import hashlib
import json
import os
import re


T2_OPTION_VARS = {
    "confopt": "LFS_PM_T2_CONFOPT_EXTRA",
    "mesonopt": "LFS_PM_T2_MESONOPT_EXTRA",
    "cmakeopt": "LFS_PM_T2_CMAKEOPT_EXTRA",
    "makeopt": "LFS_PM_T2_MAKEOPT_EXTRA",
    "makeinstopt": "LFS_PM_T2_MAKEINSTOPT_EXTRA",
    "cargoopt": "LFS_PM_T2_CARGOOPT_EXTRA",
    "cargoinstopt": "LFS_PM_T2_CARGOINSTOPT_EXTRA",
    "zigconfopt": "LFS_PM_T2_ZIGCONFOPT_EXTRA",
    "goconfopt": "LFS_PM_T2_GOCONFOPT_EXTRA",
    "pyconfopt": "LFS_PM_T2_PYCONFOPT_EXTRA",
    "plconfopt": "LFS_PM_T2_PLCONFOPT_EXTRA",
}

HOOK_PHASE_MAP = {
    "prepatch": "prepare",
    "preconf": "prepare",
    "prepare": "prepare",
    "premake": "build",
    "inmake": "build",
    "postmake": "install",
    "postinstall": "install",
    "postdoc": "install",
    "finish": "install",
}


class T2RecipeTranslator:
    def translate(self, package_name, package_dir, shell_lines):
        settings = self._default_settings()
        option_state = {key: [] for key in T2_OPTION_VARS}
        conditional_rules = []
        hooks = []
        unsupported = []
        passthrough = []
        custom_mainfunction = ""

        for raw_line in _join_lines(shell_lines):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("["):
                continue
            if self._parse_assignment(line, settings):
                if line.startswith("mainfunction="):
                    custom_mainfunction = settings.get("mainfunction", "")
                continue
            conditional = self._parse_pkginstalled_condition(line)
            if conditional:
                conditional_rules.append(conditional)
                continue
            option = self._parse_option_mutation(line)
            if option:
                self._apply_option(option_state, option)
                continue
            hook = self._parse_hook(line)
            if hook:
                hooks.append(hook)
                continue
            if _is_plain_shell_statement(line):
                passthrough.append(self._rewrite_shell(line))
                continue
            unsupported.append(line)

        build_system = self._infer_build_system(settings, option_state)
        supported = not unsupported and not custom_mainfunction
        unsupported_reasons = []
        if unsupported:
            unsupported_reasons.append("unparsed-shell")
        if custom_mainfunction and custom_mainfunction != "build_this_package":
            unsupported_reasons.append("custom-mainfunction:%s" % custom_mainfunction)

        phases = self._build_phases(
            build_system,
            settings,
            option_state,
            conditional_rules,
            hooks,
            passthrough,
            supported,
        )
        recipe = {
            "build_system": build_system,
            "settings": settings,
            "option_state": option_state,
            "conditional_rules": conditional_rules,
            "hooks": hooks,
            "passthrough": passthrough,
            "supported": supported,
            "unsupported_reasons": unsupported_reasons,
            "recipe_digest": _digest(
                {
                    "build_system": build_system,
                    "settings": settings,
                    "options": option_state,
                    "conditionals": conditional_rules,
                    "hooks": hooks,
                    "passthrough": passthrough,
                    "unsupported": unsupported,
                }
            ),
            "package_dir": package_dir,
        }
        return phases, recipe

    def _default_settings(self):
        return {
            "runconf": "1",
            "runmake": "1",
            "runmeson": "1",
            "runcmake": "1",
            "runcargo": "1",
            "runzig": "1",
            "rungo": "1",
            "runpysetup": "1",
            "runpipinstall": "0",
            "rungpepinstall": "1",
            "mainfunction": "build_this_package",
            "cleanconfopt": "1",
        }

    def _parse_assignment(self, line, settings):
        match = re.match(r"([A-Za-z_][A-Za-z0-9_]*)=(.*)$", line)
        if not match:
            return False
        key, value = match.groups()
        if key not in settings and key not in T2_OPTION_VARS and not key.startswith("run"):
            return False
        settings[key] = value.strip().strip('"').strip("'")
        return True

    def _parse_pkginstalled_condition(self, line):
        match = re.match(
            r"pkginstalled\s+([A-Za-z0-9_.+-]+)\s*(\|\||&&)\s*(var_(?:append|insert|remove)\s+\w+\s+' '\s+.+)$",
            line,
        )
        if not match:
            return None
        dependency, operator, body = match.groups()
        option = self._parse_option_mutation(body)
        if not option:
            return None
        option["dependency"] = dependency
        option["condition"] = "missing" if operator == "||" else "present"
        return option

    def _parse_option_mutation(self, line):
        match = re.match(r"var_(append|insert|remove)\s+(\w+)\s+' '\s+(.+)$", line)
        if not match:
            return None
        action, target, value = match.groups()
        if target not in T2_OPTION_VARS:
            return None
        return {"action": action, "target": target, "value": self._rewrite_shell(value)}

    def _parse_hook(self, line):
        match = re.match(r'hook_add\s+(\w+)\s+\d+\s+(.+)$', line)
        if not match:
            return None
        hook_name, command = match.groups()
        phase = HOOK_PHASE_MAP.get(hook_name)
        if not phase:
            return None
        return {"hook": hook_name, "phase": phase, "command": self._rewrite_shell(command)}

    def _apply_option(self, option_state, option):
        values = option_state[option["target"]]
        value = option["value"]
        if option["action"] == "append":
            values.append(value)
        elif option["action"] == "insert":
            values.insert(0, value)
        elif option["action"] == "remove":
            values[:] = [item for item in values if item != value]

    def _infer_build_system(self, settings, option_state):
        if option_state["mesonopt"] or settings.get("runmeson") == "1" and settings.get("runconf") == "0":
            return "meson"
        if option_state["cmakeopt"] or settings.get("runcmake") == "1" and settings.get("runconf") == "0":
            return "cmake"
        if option_state["cargoopt"] or option_state["cargoinstopt"] or settings.get("runcargo") == "1" and settings.get("runmake") == "0":
            return "cargo"
        if option_state["zigconfopt"] or settings.get("runzig") == "1" and settings.get("runmake") == "0":
            return "zig"
        if option_state["goconfopt"] or settings.get("rungo") == "1" and settings.get("runmake") == "0":
            return "go"
        if settings.get("runpysetup") == "1" and settings.get("runconf") == "0" and settings.get("runmake") == "0":
            return "pysetup"
        if settings.get("runpipinstall") == "1" or settings.get("rungpepinstall") == "1" and settings.get("runmake") == "0":
            return "python-installer"
        return "t2-universal"

    def _build_phases(self, build_system, settings, option_state, conditional_rules, hooks, passthrough, supported):
        phases = {"prepare": [], "configure": [], "build": [], "install": []}
        phases["prepare"].extend(self._prepare_exports(option_state, conditional_rules))
        phases["prepare"].extend(passthrough)
        for hook in hooks:
            phases[hook["phase"]].append(hook["command"])

        if supported:
            if build_system == "meson":
                phases["configure"].append(
                    'meson setup objs '
                    '--prefix="${LFS_PM_PREFIX}" '
                    '--bindir="${LFS_PM_BINDIR}" '
                    '--sbindir="${LFS_PM_SBINDIR}" '
                    '--libdir="${LFS_PM_LIBDIR}" '
                    '--datadir="${LFS_PM_DATADIR}" '
                    '--includedir="${LFS_PM_INCLUDEDIR}" '
                    '--sysconfdir="${LFS_PM_SYSCONFDIR}" '
                    '--localstatedir="${LFS_PM_LOCALSTATEDIR}" '
                    '${LFS_PM_T2_MESONOPT_EXTRA}'
                )
                phases["build"].append('ninja -C objs -j "${LFS_PM_JOBS}"')
                phases["install"].append('DESTDIR="$DESTDIR" ninja -C objs install')
            elif build_system == "cmake":
                phases["configure"].append(
                    'cmake -S . -B objs '
                    '-DCMAKE_INSTALL_PREFIX="${LFS_PM_PREFIX}" '
                    '-DCMAKE_INSTALL_LIBDIR="${LFS_PM_LIBDIR}" '
                    '-DCMAKE_BUILD_TYPE=Release '
                    '${LFS_PM_T2_CMAKEOPT_EXTRA}'
                )
                phases["build"].append('cmake --build objs --parallel "${LFS_PM_JOBS}"')
                phases["install"].append('DESTDIR="$DESTDIR" cmake --install objs')
            elif build_system == "cargo":
                phases["build"].append('cargo build --jobs "${LFS_PM_JOBS}" ${LFS_PM_T2_CARGOOPT_EXTRA}')
                phases["install"].append(
                    'cargo install --path . --root "$DESTDIR${LFS_PM_PREFIX}" --no-track --force ${LFS_PM_T2_CARGOINSTOPT_EXTRA}'
                )
            elif build_system == "zig":
                phases["build"].append('zig build ${LFS_PM_T2_ZIGCONFOPT_EXTRA}')
                phases["install"].append('DESTDIR="$DESTDIR" zig build install ${LFS_PM_T2_ZIGCONFOPT_EXTRA}')
            elif build_system == "go":
                phases["build"].append('go build ${LFS_PM_T2_GOCONFOPT_EXTRA}')
                phases["install"].append('install -Dm0755 "./${PKG_NAME}" "$DESTDIR${LFS_PM_BINDIR}/${PKG_NAME}"')
            elif build_system == "pysetup":
                phases["build"].append('python3 setup.py build')
                phases["install"].append(
                    'python3 setup.py install --root "$DESTDIR" --prefix "${LFS_PM_PREFIX}" ${LFS_PM_T2_PYCONFOPT_EXTRA}'
                )
            elif build_system == "python-installer":
                phases["install"].append('python3 -m pip install . --root "$DESTDIR" --prefix "${LFS_PM_PREFIX}"')
            else:
                if settings.get("runconf", "1") != "0":
                    phases["configure"].append(
                        'if [ -x ./configure ]; then '
                        './configure '
                        '--prefix="${LFS_PM_PREFIX}" '
                        '--bindir="${LFS_PM_BINDIR}" '
                        '--sbindir="${LFS_PM_SBINDIR}" '
                        '--libdir="${LFS_PM_LIBDIR}" '
                        '--datadir="${LFS_PM_DATADIR}" '
                        '--includedir="${LFS_PM_INCLUDEDIR}" '
                        '--sysconfdir="${LFS_PM_SYSCONFDIR}" '
                        '--localstatedir="${LFS_PM_LOCALSTATEDIR}" '
                        '${LFS_PM_T2_CONFOPT_EXTRA}; '
                        'fi'
                    )
                if settings.get("runmake", "1") != "0":
                    phases["build"].append(
                        'if [ -f Makefile ] || [ -f makefile ] || [ -f GNUmakefile ]; then '
                        'make -j "${LFS_PM_JOBS}" ${LFS_PM_T2_MAKEOPT_EXTRA}; '
                        'fi'
                    )
                    phases["install"].append(
                        'if [ -f Makefile ] || [ -f makefile ] || [ -f GNUmakefile ]; then '
                        'make DESTDIR="$DESTDIR" install ${LFS_PM_T2_MAKEINSTOPT_EXTRA}; '
                        'fi'
                    )

        return {key: value for key, value in phases.items() if value}

    def _prepare_exports(self, option_state, conditional_rules):
        exports = []
        for target, env_name in T2_OPTION_VARS.items():
            values = " ".join(option_state[target]).strip()
            if values:
                exports.append('export %s="${%s:+${%s} }%s"' % (env_name, env_name, env_name, values))
            else:
                exports.append('export %s="${%s:-}"' % (env_name, env_name))
        for rule in conditional_rules:
            env_name = T2_OPTION_VARS[rule["target"]]
            dep = rule["dependency"]
            value = rule["value"]
            if rule["condition"] == "missing":
                exports.append(
                    'case ":${LFS_PM_INSTALLED_PACKAGES}:" in *:%s:*) ;; *) export %s="${%s:+${%s} }%s" ;; esac'
                    % (dep, env_name, env_name, env_name, value)
                )
            else:
                exports.append(
                    'case ":${LFS_PM_INSTALLED_PACKAGES}:" in *:%s:*) export %s="${%s:+${%s} }%s" ;; esac'
                    % (dep, env_name, env_name, env_name, value)
                )
        return exports

    def _rewrite_shell(self, text):
        replacements = [
            ("$MAKE", "make"),
            ("$root$bindir", '$DESTDIR${LFS_PM_BINDIR}'),
            ("$root$sbindir", '$DESTDIR${LFS_PM_SBINDIR}'),
            ("$root$libdir", '$DESTDIR${LFS_PM_LIBDIR}'),
            ("$root$includedir", '$DESTDIR${LFS_PM_INCLUDEDIR}'),
            ("$root$datadir", '$DESTDIR${LFS_PM_DATADIR}'),
            ("$root$docdir", '$DESTDIR${LFS_PM_DOCDIR}'),
            ("$root$sysconfdir", '$DESTDIR${LFS_PM_SYSCONFDIR}'),
            ("$root$localstatedir", '$DESTDIR${LFS_PM_LOCALSTATEDIR}'),
            ("$root/$prefix", '$DESTDIR${LFS_PM_PREFIX}'),
            ("$root", "$DESTDIR"),
            ("$confdir", "$T2_PKG_DIR"),
            ("$base", "$T2_TREE_ROOT"),
            ("$pkg", "$T2_PKG_NAME"),
            ("$ver", "$T2_PKG_VERSION"),
            ("$prefix", "${LFS_PM_PREFIX_NAME}"),
            ("$bindir", "${LFS_PM_BINDIR}"),
            ("$sbindir", "${LFS_PM_SBINDIR}"),
            ("$libdir", "${LFS_PM_LIBDIR}"),
            ("$includedir", "${LFS_PM_INCLUDEDIR}"),
            ("$datadir", "${LFS_PM_DATADIR}"),
            ("$docdir", "${LFS_PM_DOCDIR}"),
            ("$sysconfdir", "${LFS_PM_SYSCONFDIR}"),
            ("$localstatedir", "${LFS_PM_LOCALSTATEDIR}"),
            ("$makeopt", "${LFS_PM_T2_MAKEOPT_EXTRA}"),
            ("$makeinstopt", "${LFS_PM_T2_MAKEINSTOPT_EXTRA}"),
            ("$confopt", "${LFS_PM_T2_CONFOPT_EXTRA}"),
            ("$mesonopt", "${LFS_PM_T2_MESONOPT_EXTRA}"),
            ("$cmakeopt", "${LFS_PM_T2_CMAKEOPT_EXTRA}"),
        ]
        text = text.strip().strip('"').strip("'")
        for source, target in replacements:
            text = text.replace(source, target)
        return text


def _join_lines(lines):
    current = []
    for line in lines:
        stripped = line.rstrip("\n")
        if stripped.endswith("\\"):
            current.append(stripped[:-1].rstrip())
            continue
        current.append(stripped)
        yield " ".join(part for part in current if part)
        current = []
    if current:
        yield " ".join(part for part in current if part)


def _is_plain_shell_statement(line):
    tokens = ("mkdir ", "install ", "cp ", "ln ", "sed ", "autoreconf ", "bootstrap", "./", "cmake ", "meson ", "ninja ", "cargo ", "go ", "python")
    return line.startswith(tokens)


def _shell_quote(value):
    escaped = str(value).replace("'", "'\"'\"'")
    return "'" + escaped + "'"


def _digest(payload):
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()

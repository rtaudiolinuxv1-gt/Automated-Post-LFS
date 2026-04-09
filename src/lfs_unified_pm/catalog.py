from __future__ import annotations

from collections import defaultdict

DEPENDENCY_ALIASES = {
    "x-window-system": ["xinit"],
    "xorg7": ["xinit"],
    "server-mail": ["sendmail", "postfix", "exim"],
    "Berkeley-DB": ["db"],
    "MySQL": ["mariadb", "mysql"],
    "JACK": ["jack2", "pipewire"],
    "GTK+-2": ["gtk2"],
    "GTK": ["gtk3", "gtk4", "gtk2"],
    "GLib": ["glib2"],
    "Python2": ["python2"],
    "OpenAL": ["openal"],
    "SDL1": ["sdl", "sdl12-compat"],
}


class PackageCatalog:
    def __init__(self, packages, priorities):
        self.packages = list(packages)
        self.priorities = {name: index for index, name in enumerate(priorities)}
        self.by_name = defaultdict(list)
        self.by_lower_name = defaultdict(list)
        self.providers = defaultdict(list)
        for package in self.packages:
            self.by_name[package.name].append(package)
            self.by_lower_name[package.name.lower()].append(package)
            for provided in package.provides:
                self.providers[provided].append(package)
        for entries in self.by_name.values():
            entries.sort(key=self._sort_key)
        for entries in self.by_lower_name.values():
            entries.sort(key=self._sort_key)
        for entries in self.providers.values():
            entries.sort(key=self._sort_key)
        self._apply_alias_providers()

    def all(self):
        return sorted(self.packages, key=lambda item: (item.name, self._sort_key(item)))

    def categories(self, source_origin=""):
        counts = defaultdict(int)
        for package in self.packages:
            if source_origin and package.source_origin != source_origin:
                continue
            counts[package.category or package.source_origin] += 1
        return sorted(counts.items())

    def search(self, query):
        query = query.lower()
        return [
            package for package in self.all()
            if query in package.name.lower() or query in package.summary.lower()
        ]

    def packages_in_category(self, category, source_origin=""):
        packages = [
            package for package in self.packages
            if (package.category or package.source_origin) == category and (not source_origin or package.source_origin == source_origin)
        ]
        return sorted(packages, key=lambda item: (item.name, self._sort_key(item)))

    def resolve(self, name):
        entries = self.by_name.get(name)
        if entries:
            return entries[0]
        lower = self.by_lower_name.get(name.lower())
        if lower:
            return lower[0]
        provided = self.providers.get(name)
        if provided:
            return provided[0]
        return None

    def candidates(self, name):
        entries = self.by_name.get(name, [])
        if entries:
            return list(entries)
        return list(self.by_lower_name.get(name.lower(), []))

    def resolve_exact(self, name, source_origin):
        for package in self.candidates(name):
            if package.source_origin == source_origin:
                return package
        return None

    def resolve_with_preferences(self, name, preferred_sources=None, allowed_sources=None):
        entries = self.candidates(name)
        if not entries:
            entries = list(self.providers.get(name, []))
        if not entries:
            return None
        if allowed_sources is not None:
            allowed = set(allowed_sources)
            entries = [package for package in entries if package.source_origin in allowed]
            if not entries:
                return None
        if not preferred_sources:
            return entries[0]
        index = {source: position for position, source in enumerate(preferred_sources)}
        return sorted(
            entries,
            key=lambda package: (
                index.get(package.source_origin, len(index) + self.priorities.get(package.source_origin, len(self.priorities))),
                self.priorities.get(package.source_origin, len(self.priorities)),
                package.name,
                package.version,
            ),
        )[0]

    def reverse_dependencies(self, target):
        result = []
        for package in self.packages:
            deps = package.depends + package.recommends + package.optional
            if target in deps:
                result.append(package)
        return sorted(result, key=lambda item: (item.name, self._sort_key(item)))

    def _sort_key(self, package):
        return (
            self.priorities.get(package.source_origin, len(self.priorities)),
            package.name,
            package.version,
        )

    def _apply_alias_providers(self):
        for alias, target_names in DEPENDENCY_ALIASES.items():
            providers = []
            for target_name in target_names:
                providers.extend(self.by_name.get(target_name, []))
            if providers:
                self.providers[alias].extend(providers)
                self.providers[alias].sort(key=self._sort_key)

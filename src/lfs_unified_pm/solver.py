from __future__ import annotations

from .models import BuildPlan, BuildStep


class SolverError(Exception):
    pass


class DependencySolver:
    def __init__(self, catalog, installed_names):
        self.catalog = catalog
        self.installed_names = set(installed_names)

    def make_plan(self, requested, include_recommends=False, auto_optional=False):
        requests = [
            {
                "name": name,
                "source_origin": "",
                "resolve_required": True,
                "t2_dependency_mode": "blfs",
                "allowed_dependency_sources": None,
            }
            for name in requested
        ]
        return self.make_plan_for_requests(
            requests,
            include_recommends=include_recommends,
            auto_optional=auto_optional,
        )

    def make_plan_for_requests(self, requests, include_recommends=False, auto_optional=False):
        ordered = []
        unresolved = []
        conflicts = []
        visiting = set()
        visited = set()

        def visit(
            name,
            source_origin="",
            resolve_required=True,
            parent_package=None,
            t2_dependency_mode="blfs",
            allowed_dependency_sources=None,
            strict_required_chain=True,
        ):
            if name in self.installed_names:
                return
            visit_key = (name, source_origin or "")
            if visit_key in visited:
                return
            if visit_key in visiting:
                if strict_required_chain:
                    conflicts.append("Dependency cycle detected at %s" % name)
                return
            package = self._resolve_package(
                name,
                source_origin,
                parent_package,
                t2_dependency_mode,
                allowed_dependency_sources,
            )
            if package is None:
                unresolved.append(name)
                return
            if package.name in self.installed_names:
                return
            package_key = (package.name, package.source_origin)
            if package_key in visited:
                return
            if package_key in visiting:
                if strict_required_chain:
                    conflicts.append("Dependency cycle detected at %s" % package.name)
                return
            visiting.add(package_key)
            for conflict in package.conflicts:
                if conflict in self.installed_names:
                    conflicts.append("%s conflicts with installed %s" % (package.name, conflict))
            if resolve_required:
                for dependency in package.depends:
                    visit(
                        dependency,
                        parent_package=package,
                        t2_dependency_mode=t2_dependency_mode,
                        allowed_dependency_sources=allowed_dependency_sources,
                        strict_required_chain=strict_required_chain,
                    )
                if include_recommends:
                    for dependency in package.recommends:
                        visit(
                            dependency,
                            parent_package=package,
                            t2_dependency_mode=t2_dependency_mode,
                            allowed_dependency_sources=allowed_dependency_sources,
                            strict_required_chain=False,
                        )
                if auto_optional:
                    for dependency in package.optional:
                        visit(
                            dependency,
                            parent_package=package,
                            t2_dependency_mode=t2_dependency_mode,
                            allowed_dependency_sources=allowed_dependency_sources,
                            strict_required_chain=False,
                        )
            visiting.remove(package_key)
            visited.add(package_key)
            ordered.append(
                BuildStep(
                    package=package,
                    required=[dep for dep in package.depends if dep not in self.installed_names],
                    missing_recommends=[
                        dep for dep in package.recommends if dep not in self.installed_names
                    ],
                    missing_optional=[
                        dep for dep in package.optional if dep not in self.installed_names
                    ],
                )
            )

        for request in requests:
            visit(
                request["name"],
                source_origin=request.get("source_origin", ""),
                resolve_required=request.get("resolve_required", True),
                t2_dependency_mode=request.get("t2_dependency_mode", "blfs"),
                allowed_dependency_sources=request.get("allowed_dependency_sources"),
                strict_required_chain=True,
            )
        return BuildPlan(
            requested=[request["name"] for request in requests],
            ordered_steps=ordered,
            unresolved=sorted(set(unresolved)),
            conflicts=conflicts,
        )

    def _resolve_package(self, name, source_origin, parent_package, t2_dependency_mode, allowed_dependency_sources):
        if source_origin:
            return self.catalog.resolve_exact(name, source_origin)
        if allowed_dependency_sources is not None:
            preferred_sources = [source for source in ("lfs-base", "blfs", "t2", "arch", "custom") if source in allowed_dependency_sources]
            if parent_package and parent_package.source_origin == "t2" and t2_dependency_mode == "t2":
                preferred_sources = [source for source in ("lfs-base", "t2", "blfs", "arch", "custom") if source in allowed_dependency_sources]
            return self.catalog.resolve_with_preferences(
                name,
                preferred_sources=preferred_sources,
                allowed_sources=allowed_dependency_sources,
            )
        if parent_package and parent_package.source_origin == "t2":
            if t2_dependency_mode == "t2":
                return self.catalog.resolve_with_preferences(
                    name,
                    preferred_sources=["lfs-base", "t2", "blfs", "arch", "custom"],
                )
            return self.catalog.resolve_with_preferences(
                name,
                preferred_sources=["lfs-base", "blfs", "t2", "arch", "custom"],
            )
        return self.catalog.resolve(name)

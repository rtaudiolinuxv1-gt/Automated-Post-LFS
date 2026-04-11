from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime

from .models import InstalledRecord, PackageRecord
from .settings import DEFAULT_SETTINGS, merged_override, merged_settings


class StateStore:
    def __init__(self, db_path):
        self.db_path = db_path
        self.connection = sqlite3.connect(db_path, timeout=30.0)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("pragma busy_timeout = 30000")
        self._init_schema()

    def _init_schema(self):
        for attempt in range(6):
            try:
                self.connection.executescript(
                    """
                    create table if not exists packages (
                        name text not null,
                        version text not null,
                        source_origin text not null,
                        summary text default '',
                        category text default '',
                        description text default '',
                        homepage text default '',
                        build_system text default '',
                        recipe_format text default '',
                        depends_json text default '[]',
                        recommends_json text default '[]',
                        optional_json text default '[]',
                        provides_json text default '[]',
                        conflicts_json text default '[]',
                        sources_json text default '[]',
                        phases_json text default '{}',
                        metadata_json text default '{}',
                        updated_at text not null,
                        primary key (name, source_origin)
                    );

                    create table if not exists installed_packages (
                        name text primary key,
                        version text not null,
                        source_origin text not null,
                        install_reason text default '',
                        files_json text default '[]',
                        depends_json text default '[]',
                        metadata_json text default '{}',
                        installed_at text not null
                    );

                    create table if not exists transactions (
                        id integer primary key autoincrement,
                        action text not null,
                        package_name text not null,
                        version text not null,
                        source_origin text not null,
                        status text not null,
                        detail text default '',
                        created_at text not null
                    );

                    create table if not exists root_scans (
                        root text primary key,
                        scan_json text not null,
                        scanned_at text not null
                    );

                    create table if not exists settings (
                        key text primary key,
                        value_json text not null,
                        updated_at text not null
                    );

                    create table if not exists package_overrides (
                        package_name text primary key,
                        override_json text not null,
                        updated_at text not null
                    );

                    create table if not exists source_syncs (
                        source text primary key,
                        synced_at text not null,
                        detail_json text not null
                    );

                    create table if not exists prefix_profiles (
                        prefix text primary key,
                        script_path text not null,
                        exports_json text not null,
                        created_at text not null
                    );

                    create table if not exists lfs_base_state (
                        key text primary key,
                        value_json text not null,
                        updated_at text not null
                    );
                    """
                )
                self.connection.commit()
                return
            except sqlite3.OperationalError as error:
                if "locked" not in str(error).lower() or attempt == 5:
                    raise
                time.sleep(1.0)

    def close(self):
        self.connection.close()

    def upsert_package(self, package):
        payload = (
            package.name,
            package.version,
            package.source_origin,
            package.summary,
            package.category,
            package.description,
            package.homepage,
            package.build_system,
            package.recipe_format,
            json.dumps(package.depends, sort_keys=True),
            json.dumps(package.recommends, sort_keys=True),
            json.dumps(package.optional, sort_keys=True),
            json.dumps(package.provides, sort_keys=True),
            json.dumps(package.conflicts, sort_keys=True),
            json.dumps(package.sources, sort_keys=True),
            json.dumps(package.phases, sort_keys=True),
            json.dumps(package.metadata, sort_keys=True),
            _now(),
        )
        self.connection.execute(
            """
            insert into packages (
                name, version, source_origin, summary, category, description,
                homepage, build_system, recipe_format, depends_json,
                recommends_json, optional_json, provides_json, conflicts_json,
                sources_json, phases_json, metadata_json, updated_at
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(name, source_origin) do update set
                version=excluded.version,
                summary=excluded.summary,
                category=excluded.category,
                description=excluded.description,
                homepage=excluded.homepage,
                build_system=excluded.build_system,
                recipe_format=excluded.recipe_format,
                depends_json=excluded.depends_json,
                recommends_json=excluded.recommends_json,
                optional_json=excluded.optional_json,
                provides_json=excluded.provides_json,
                conflicts_json=excluded.conflicts_json,
                sources_json=excluded.sources_json,
                phases_json=excluded.phases_json,
                metadata_json=excluded.metadata_json,
                updated_at=excluded.updated_at
            """,
            payload,
        )
        self.connection.commit()

    def list_packages(self):
        rows = self.connection.execute(
            "select * from packages order by name, source_origin"
        ).fetchall()
        return [_row_to_package(row) for row in rows]

    def list_packages_by_source(self, source_origin):
        rows = self.connection.execute(
            "select * from packages where source_origin = ? order by name", (source_origin,)
        ).fetchall()
        return [_row_to_package(row) for row in rows]

    def delete_packages_by_source_except(self, source_origin, names):
        names = set(names)
        existing = {item.name for item in self.list_packages_by_source(source_origin)}
        stale = sorted(existing - names)
        if not stale:
            return []
        placeholders = ",".join("?" for _ in stale)
        self.connection.execute(
            "delete from packages where source_origin = ? and name in (%s)" % placeholders,
            [source_origin] + stale,
        )
        self.connection.commit()
        return stale

    def list_installed(self):
        rows = self.connection.execute(
            "select * from installed_packages order by name"
        ).fetchall()
        return [_row_to_installed(row) for row in rows]

    def get_installed(self, name):
        row = self.connection.execute(
            "select * from installed_packages where name = ?", (name,)
        ).fetchone()
        return _row_to_installed(row) if row else None

    def mark_installed(self, record):
        self.connection.execute(
            """
            insert into installed_packages (
                name, version, source_origin, install_reason, files_json,
                depends_json, metadata_json, installed_at
            ) values (?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(name) do update set
                version=excluded.version,
                source_origin=excluded.source_origin,
                install_reason=excluded.install_reason,
                files_json=excluded.files_json,
                depends_json=excluded.depends_json,
                metadata_json=excluded.metadata_json,
                installed_at=excluded.installed_at
            """,
            (
                record.name,
                record.version,
                record.source_origin,
                record.install_reason,
                json.dumps(record.files, sort_keys=True),
                json.dumps(record.depends, sort_keys=True),
                json.dumps(record.metadata, sort_keys=True),
                _now(),
            ),
        )
        self.connection.commit()

    def add_transaction(self, action, package_name, version, source_origin, status, detail):
        self.connection.execute(
            """
            insert into transactions (
                action, package_name, version, source_origin, status, detail, created_at
            ) values (?, ?, ?, ?, ?, ?, ?)
            """,
            (action, package_name, version, source_origin, status, detail, _now()),
        )
        self.connection.commit()

    def history(self, limit=50):
        return self.connection.execute(
            "select * from transactions order by id desc limit ?", (limit,)
        ).fetchall()

    def save_scan(self, root, report):
        self.connection.execute(
            """
            insert into root_scans (root, scan_json, scanned_at) values (?, ?, ?)
            on conflict(root) do update set
                scan_json=excluded.scan_json,
                scanned_at=excluded.scanned_at
            """,
            (root, json.dumps(report, sort_keys=True), _now()),
        )
        self.connection.commit()

    def load_scan(self, root):
        row = self.connection.execute(
            "select scan_json from root_scans where root = ?", (root,)
        ).fetchone()
        return json.loads(row["scan_json"]) if row else None

    def get_settings(self):
        row = self.connection.execute(
            "select value_json from settings where key = 'global'"
        ).fetchone()
        payload = json.loads(row["value_json"]) if row else DEFAULT_SETTINGS
        return merged_settings(payload)

    def save_settings(self, settings):
        payload = json.dumps(settings, sort_keys=True)
        self.connection.execute(
            """
            insert into settings (key, value_json, updated_at) values ('global', ?, ?)
            on conflict(key) do update set
                value_json=excluded.value_json,
                updated_at=excluded.updated_at
            """,
            (payload, _now()),
        )
        self.connection.commit()

    def get_package_override(self, package_name):
        payload = self.get_raw_package_override(package_name)
        return merged_override(payload)

    def get_raw_package_override(self, package_name):
        row = self.connection.execute(
            "select override_json from package_overrides where package_name = ?",
            (package_name,),
        ).fetchone()
        return json.loads(row["override_json"]) if row else {}

    def save_package_override(self, package_name, override):
        payload = json.dumps(override, sort_keys=True)
        self.connection.execute(
            """
            insert into package_overrides (package_name, override_json, updated_at)
            values (?, ?, ?)
            on conflict(package_name) do update set
                override_json=excluded.override_json,
                updated_at=excluded.updated_at
            """,
            (package_name, payload, _now()),
        )
        self.connection.commit()

    def record_source_sync(self, source, detail):
        payload = json.dumps(detail, sort_keys=True)
        self.connection.execute(
            """
            insert into source_syncs (source, synced_at, detail_json) values (?, ?, ?)
            on conflict(source) do update set
                synced_at=excluded.synced_at,
                detail_json=excluded.detail_json
            """,
            (source, _now(), payload),
        )
        self.connection.commit()

    def list_source_syncs(self):
        rows = self.connection.execute(
            "select source, synced_at, detail_json from source_syncs order by source"
        ).fetchall()
        return [
            {
                "source": row["source"],
                "synced_at": row["synced_at"],
                "detail": json.loads(row["detail_json"]),
            }
            for row in rows
        ]

    def get_source_sync(self, source):
        row = self.connection.execute(
            "select source, synced_at, detail_json from source_syncs where source = ?",
            (source,),
        ).fetchone()
        if not row:
            return None
        return {
            "source": row["source"],
            "synced_at": row["synced_at"],
            "detail": json.loads(row["detail_json"]),
        }

    def get_last_sync_time(self):
        row = self.connection.execute(
            "select max(synced_at) as synced_at from source_syncs"
        ).fetchone()
        return row["synced_at"] if row and row["synced_at"] else ""

    def get_prefix_profile(self, prefix):
        row = self.connection.execute(
            "select prefix, script_path, exports_json, created_at from prefix_profiles where prefix = ?",
            (prefix,),
        ).fetchone()
        if not row:
            return None
        return {
            "prefix": row["prefix"],
            "script_path": row["script_path"],
            "exports": json.loads(row["exports_json"]),
            "created_at": row["created_at"],
        }

    def save_prefix_profile(self, prefix, script_path, exports):
        self.connection.execute(
            """
            insert into prefix_profiles (prefix, script_path, exports_json, created_at)
            values (?, ?, ?, ?)
            on conflict(prefix) do update set
                script_path=excluded.script_path,
                exports_json=excluded.exports_json,
                created_at=excluded.created_at
            """,
            (prefix, script_path, json.dumps(exports, sort_keys=True), _now()),
        )
        self.connection.commit()

    def get_lfs_base_state(self, key="current"):
        row = self.connection.execute(
            "select value_json from lfs_base_state where key = ?",
            (key,),
        ).fetchone()
        return json.loads(row["value_json"]) if row else {}

    def save_lfs_base_state(self, value, key="current"):
        self.connection.execute(
            """
            insert into lfs_base_state (key, value_json, updated_at) values (?, ?, ?)
            on conflict(key) do update set
                value_json=excluded.value_json,
                updated_at=excluded.updated_at
            """,
            (key, json.dumps(value, sort_keys=True), _now()),
        )
        self.connection.commit()

    def clear_lfs_base_state(self, key="current"):
        self.connection.execute("delete from lfs_base_state where key = ?", (key,))
        self.connection.commit()


def _row_to_package(row):
    return PackageRecord(
        name=row["name"],
        version=row["version"],
        source_origin=row["source_origin"],
        summary=row["summary"],
        category=row["category"],
        description=row["description"],
        homepage=row["homepage"],
        build_system=row["build_system"],
        recipe_format=row["recipe_format"],
        depends=json.loads(row["depends_json"]),
        recommends=json.loads(row["recommends_json"]),
        optional=json.loads(row["optional_json"]),
        provides=json.loads(row["provides_json"]),
        conflicts=json.loads(row["conflicts_json"]),
        sources=json.loads(row["sources_json"]),
        phases=json.loads(row["phases_json"]),
        metadata=json.loads(row["metadata_json"]),
    )


def _row_to_installed(row):
    return InstalledRecord(
        name=row["name"],
        version=row["version"],
        source_origin=row["source_origin"],
        install_reason=row["install_reason"],
        files=json.loads(row["files_json"]),
        depends=json.loads(row["depends_json"]),
        metadata=json.loads(row["metadata_json"]),
    )


def _now():
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

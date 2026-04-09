from __future__ import annotations

from copy import deepcopy


DEFAULT_SETTINGS = {
    "build": {
        "build_process": "python",
        "build_mode": "native",
        "chroot_root": "",
        "prefix": "/usr",
        "bindir": "",
        "sbindir": "",
        "libdir": "",
        "includedir": "",
        "datadir": "",
        "docdir_root": "/usr/share/doc",
        "sysconfdir": "/etc",
        "localstatedir": "/var",
        "jobs": 1,
        "cflags": "",
        "cxxflags": "",
        "ldflags": "",
        "configure_extra": "",
        "meson_extra": "",
        "cmake_extra": "",
        "make_extra": "",
        "make_install_extra": "",
        "always_rpath_paths": [],
        "allow_la_removal": False,
        "default_dependency_level": "recommended",
        "include_recommends": False,
        "auto_optional_deps": False,
        "non_interactive": False,
        "command_review_mode": "off",
        "command_review_seconds": 10,
        "package_format": "none",
        "install_after_build": True,
        "script_output_dir": "./generated-build-scripts",
        "script_update_tracking": True,
    },
    "profile": {
        "prompt_on_new_prefix": True,
        "auto_create_for_new_prefix": False,
        "nonstandard_only": True,
        "scan_installed_files": True,
        "add_bin_to_path": True,
        "add_lib_to_ld_library_path": True,
        "add_pkgconfig_to_pkg_config_path": True,
        "add_share_to_xdg_data_dirs": True,
        "add_python_to_pythonpath": True,
        "add_cmake_to_cmake_prefix_path": True,
    },
    "sync": {
        "prompt_if_stale": True,
        "stale_days": 30,
        "default_sources": ["base", "blfs", "t2"],
    },
    "system_state": {
        "assume_lfs_base_installed": False,
        "use_jhalfs_tracking": False,
        "jhalfs_tracking_path": "/var/lib/jhalfs/BLFS/instpkg.xml",
    },
}


DEFAULT_PACKAGE_OVERRIDE = {
    "prefix": "",
    "cflags": "",
    "cxxflags": "",
    "ldflags": "",
    "rpath_paths": [],
    "configure_extra": "",
    "meson_extra": "",
    "cmake_extra": "",
    "make_extra": "",
    "make_install_extra": "",
    "custom_build_file": "",
}


def merged_settings(stored):
    return deep_merge(DEFAULT_SETTINGS, stored or {})


def merged_override(stored):
    return deep_merge(DEFAULT_PACKAGE_OVERRIDE, stored or {})


def deep_merge(base, override):
    result = deepcopy(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result

LFS Unified Package Manager

Created by RTAudioLinux

LFS Unified Package Manager is a source-based package manager and build frontend for Linux From Scratch systems, especially an LFS 13 base system with BLFS software layered on top. It is designed to give an LFS system a practical way to manage source packages, dependency ordering, build settings, logging, packaging, and install history without turning the system into a binary-distribution clone.

At its core, the software combines several package sources into one environment:

- the LFS 13 base package set, including local version overrides
- BLFS package metadata from the XML book
- BLFS dependency handling based on the same special-case transformation logic used by jhalfs
- T2 SDE package metadata imported from the package tree
- optional Arch package metadata
- custom user-defined package recipes

The goal is to give an LFS user one place to plan and build software after the base system is in place. Instead of manually reading package pages one by one, the program imports package metadata, resolves dependencies, chooses a sane build order, tracks what has already been installed, and provides both a command-line interface and an ncurses interface for interactive use.

## What It Does

This software currently provides:

- package metadata import and syncing for LFS base, BLFS, T2, Arch, and custom packages
- dependency resolution with required, recommended, and optional dependency levels
- source-aware dependency selection, so you can prefer BLFS, T2, Arch, or other available branches where that makes sense
- support for assumed system state, including:
  - treating the full LFS base set as already installed
  - reading `/var/lib/jhalfs/BLFS/instpkg.xml`
  - scanning a mounted target root
- native builds or builds targeted at a chroot root
- package command review before execution
- per-package command overrides saved separately from imported recipes
- install tracking in SQLite plus a mirrored `jhalfs` install list
- optional package export as Slackware packages or tar archives
- generation of standalone build scripts for queued packages
- automatic prefix-profile generation for nonstandard install prefixes

## BLFS Support

BLFS support is one of the main focuses of the project.

The program imports BLFS metadata from the XML book, but it does not rely only on simple page parsing. For dependency handling it uses a jhalfs-style generated dependency graph, including the special transformations jhalfs applies for grouped packages and other BLFS edge cases. That means dependency planning is much closer to how experienced BLFS users expect it to behave.

For grouped BLFS package families such as Xorg and other compound sections, the program now supports a shared build-provider model. Logical packages like `libX11` can still appear as dependency nodes, but the actual build can be routed through the compound parent payload where that is how BLFS organizes the build instructions.

## T2 Support

T2 support is based on the package metadata in the T2 package tree, especially the `.desc` and `.cache` files. The application imports that metadata into its own internal recipe model instead of depending on T2 itself as the active build system. This keeps the package information while allowing the build process to stay under one consistent interface and policy.

## Interface

The software has both:

- a CLI for syncing, searching, planning, scanning, building, and exporting
- an ncurses interface for browsing packages, editing settings, building queues, reviewing commands, and viewing installed/history state

The ncurses interface is built around an LFS workflow rather than a general desktop package manager workflow. It opens on configuration and system state, then lets you choose package categories, inspect package details, add items to a build queue, and build or export the resulting plan.

## Package Management And Logging

The program keeps its own package state in SQLite and also mirrors installed-package information into the `jhalfs` XML tracking format. That gives it a practical package-management layer for a source-based LFS system while staying compatible with the tools and conventions many LFS users already know.

Each installed package can record:

- version
- source origin
- dependency information
- installed file list
- artifact path
- build provider, if a grouped payload was used

## Running It

From the project root:

```bash
cd /home/jim/LFS_APP
PYTHONPATH=src python3 -m lfs_unified_pm.cli --root ./filesystem_mountpoint tui
```

To refresh metadata first:

```bash
cd /home/jim/LFS_APP
PYTHONPATH=src python3 -m lfs_unified_pm.cli --root ./filesystem_mountpoint sync --base-override ./root-overrides.yaml
```

There are also helper scripts in the project root:

- `./run-lfs-pm.sh`
- `./install-lfs-pm.sh`

## In Short

LFS Unified Package Manager is an attempt to give an LFS 13 system a serious source-package workflow: BLFS awareness, T2 package coverage, install tracking, packaging, command editing, and a usable interactive frontend, all while remaining compatible with the way an LFS machine is actually built and maintained.

import os
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from lfs_unified_pm.lfs_base import (
    LfsBaseBuilder,
    LfsBaseExecutor,
    _collect_steps,
    _guarded_step_notice_payloads,
    _script_guard_context,
    _lint_step_script,
    _extract_script_variable,
    _verify_step_semantics,
    _resolve_lfs_archive_dir,
    _resolve_lfs_build_root,
    _run_logged,
    _sudo_command,
    _validated_lfs_env,
    _verify_step_state,
    _step_marker_path,
    _capture_sensitive_snapshot,
)
from lfs_unified_pm.config import default_config, ensure_directories
from lfs_unified_pm.models import LfsBuildPlan, LfsBuildStep
from lfs_unified_pm.state import StateStore


class LfsBaseTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix="lfs-pm-lfs-base-")
        self.root = os.path.join(self.tempdir, "root")
        os.makedirs(self.root)
        self.config = default_config(self.root)
        ensure_directories(self.config)
        self.store = StateStore(self.config.db_path)

    def tearDown(self):
        self.store.close()
        shutil.rmtree(self.tempdir)

    def test_collect_steps_preserves_stage_order(self):
        commands_root = os.path.join(self.tempdir, "commands")
        os.makedirs(os.path.join(commands_root, "chapter04"))
        os.makedirs(os.path.join(commands_root, "chapter05"))
        os.makedirs(os.path.join(commands_root, "chapter07"))
        os.makedirs(os.path.join(commands_root, "chroot-scripts"))
        os.makedirs(os.path.join(commands_root, "kernfs-scripts"))
        for relative in (
            "chapter04/001-addinguser",
            "chapter04/002-settingenvironment",
            "chapter05/001-binutils-pass1",
            "chapter07/001-creatingdirs",
            "chroot-scripts/001-chroot",
            "kernfs-scripts/devices.sh",
            "kernfs-scripts/teardown.sh",
        ):
            full = os.path.join(commands_root, relative)
            with open(full, "w", encoding="utf-8") as handle:
                handle.write("#!/bin/bash\n")
        steps = _collect_steps(commands_root)
        self.assertEqual(
            [step.stage for step in steps],
            [
                "host-root-prep",
                "lfs-user",
                "lfs-user",
                "host-root-kernfs",
                "host-root-chroot",
                "chroot-root",
                "host-root-teardown",
            ],
        )

    def test_target_triplet_uses_vendor_or_override(self):
        settings = {"target_vendor": "rtaudio", "triplet_override": ""}
        builder = LfsBaseBuilder(self.config, settings)
        self.assertEqual(builder._target_triplet(), "$(uname -m)-rtaudio-linux-gnu")
        builder = LfsBaseBuilder(self.config, {"target_vendor": "ignored", "triplet_override": "x86_64-demo-linux"})
        self.assertEqual(builder._target_triplet(), "x86_64-demo-linux")

    def test_rewrite_target_triplet_updates_generated_scripts(self):
        commands_root = os.path.join(self.tempdir, "commands")
        os.makedirs(commands_root)
        script_path = os.path.join(commands_root, "env.sh")
        with open(script_path, "w", encoding="utf-8") as handle:
            handle.write('export LFS_TGT=$(uname -m)-lfs-linux-gnu\n')
        builder = LfsBaseBuilder(self.config, {"target_vendor": "custom"})
        builder._rewrite_target_triplet(commands_root, builder._target_triplet())
        with open(script_path, "r", encoding="utf-8") as handle:
            data = handle.read()
        self.assertIn('export LFS_TGT=$(uname -m)-custom-linux-gnu', data)

    def test_host_setup_idempotent_reuses_existing_lfs_account(self):
        commands_root = os.path.join(self.tempdir, "commands")
        os.makedirs(os.path.join(commands_root, "chapter04"))
        script_path = os.path.join(commands_root, "chapter04", "402-addinguser")
        with open(script_path, "w", encoding="utf-8") as handle:
            handle.write(
                "#!/bin/bash\n"
                "set +h\n"
                "set -e\n\n"
                "groupadd lfs\n"
                "useradd -s /bin/bash -g lfs -m -k /dev/null lfs\n"
                "chown -v lfs $LFS/{usr{,/*},var,etc,tools}\n"
            )
        builder = LfsBaseBuilder(self.config, {})
        builder._make_host_setup_idempotent(commands_root)
        with open(script_path, "r", encoding="utf-8") as handle:
            data = handle.read()
        self.assertIn("getent group lfs >/dev/null || groupadd lfs", data)
        self.assertIn("id -u lfs >/dev/null 2>&1 || useradd -s /bin/bash -g lfs -m -k /dev/null lfs", data)
        self.assertIn("chown -v lfs:lfs", data)

    def test_host_setup_idempotent_keeps_existing_layout_entries(self):
        commands_root = os.path.join(self.tempdir, "commands")
        os.makedirs(os.path.join(commands_root, "chapter04"))
        script_path = os.path.join(commands_root, "chapter04", "401-creatingminlayout")
        with open(script_path, "w", encoding="utf-8") as handle:
            handle.write(
                "#!/bin/bash\n"
                "set +h\n"
                "set -e\n\n"
                "mkdir -pv $LFS/{etc,var} $LFS/usr/{bin,lib,sbin}\n\n"
                "for i in bin lib sbin; do\n"
                "  ln -sv usr/$i $LFS/$i\n"
                "done\n"
            )
        builder = LfsBaseBuilder(self.config, {})
        builder._make_host_setup_idempotent(commands_root)
        with open(script_path, "r", encoding="utf-8") as handle:
            data = handle.read()
        self.assertIn('if [ -L "$LFS/$i" ]; then', data)
        self.assertIn('echo "Keeping existing $LFS/$i"', data)

    def test_injected_guards_add_step_marker_write(self):
        commands_root = os.path.join(self.tempdir, "commands")
        os.makedirs(os.path.join(commands_root, "chapter04"))
        script_path = os.path.join(commands_root, "chapter04", "401-creatingminlayout")
        with open(script_path, "w", encoding="utf-8") as handle:
            handle.write("#!/bin/bash\nset -e\nmkdir -pv $LFS/etc\nexit\n")
        builder = LfsBaseBuilder(self.config, {})
        builder._inject_lfs_guards(commands_root)
        with open(script_path, "r", encoding="utf-8") as handle:
            data = handle.read()
        self.assertIn('Refusing to run without LFS set', data)
        self.assertIn('export LFS="$LFS"', data)
        self.assertIn("rtal_assert_path()", data)
        self.assertIn("cp() {", data)
        self.assertIn("mv() {", data)
        self.assertIn("install() {", data)
        self.assertIn("sed() {", data)
        self.assertIn("tar() {", data)
        self.assertIn("mkdir() {", data)
        self.assertIn("touch() {", data)
        self.assertIn("chgrp() {", data)
        self.assertIn('useradd() { rtal_require_chroot useradd;', data)
        self.assertIn('RTAL_LFS_REAL=$(readlink -m -- "$LFS")', data)
        self.assertIn('"$RTAL_LFS_REAL"|"$RTAL_LFS_REAL"/*', data)
        self.assertIn('step-markers', data)

    def test_script_guard_context_uses_chroot_for_chapter7_plus(self):
        self.assertEqual(_script_guard_context("chapter05/501-binutils-pass1"), "host")
        self.assertEqual(_script_guard_context("chroot-scripts/001-chroot"), "host")
        self.assertEqual(_script_guard_context("chapter07/701-creatingdirs"), "chroot")
        self.assertEqual(_script_guard_context("chapter08/801-man-pages"), "chroot")

    def test_fetch_sources_stages_local_files_and_updates_state(self):
        source_dir = os.path.join(self.tempdir, "input")
        os.makedirs(source_dir)
        source_file = os.path.join(source_dir, "demo-1.0.tar.xz")
        with open(source_file, "w", encoding="utf-8") as handle:
            handle.write("demo\n")
        plan = LfsBuildPlan(
            book_root="",
            profiled_book="",
            commands_root=os.path.join(self.tempdir, "commands"),
            target_triplet="$(uname -m)-lfs-linux-gnu",
            source_entries=[{"url": source_file, "md5": "", "filename": "demo-1.0.tar.xz"}],
            steps=[],
            stage_scripts={},
        )
        settings = {"build_root": self.root, "source_archive_dir": os.path.join(self.tempdir, "archive")}
        executor = LfsBaseExecutor(self.config, settings, self.store)
        target_dir = executor.fetch_sources(plan)
        self.assertTrue(os.path.isfile(os.path.join(target_dir, "demo-1.0.tar.xz")))
        state = self.store.get_lfs_base_state()
        self.assertTrue(state.get("sources_fetched"))
        self.assertEqual(state.get("source_count"), 1)

    def test_prepare_target_root_skips_existing_sources_dir(self):
        sources_dir = os.path.join(self.root, "sources")
        os.makedirs(sources_dir)
        executor = LfsBaseExecutor(self.config, {"build_root": self.root}, self.store)

        class _Stat:
            st_uid = 0
            st_gid = 0
            st_mode = 0o41777

        real_stat = os.stat

        def fake_stat(path, *args, **kwargs):
            if os.path.abspath(path) == os.path.abspath(sources_dir):
                return _Stat()
            return real_stat(path, *args, **kwargs)

        with patch("lfs_unified_pm.lfs_base.os.stat", side_effect=fake_stat), patch.object(
            executor.guarded_ops, "run_command"
        ) as run_command:
            executor._prepare_target_root(self.root)

        run_command.assert_not_called()

    def test_prepare_target_root_skips_symlinked_sources_dir_with_correct_owner_and_mode(self):
        external_root = os.path.join(self.tempdir, "external")
        os.makedirs(external_root)
        sources_dir = os.path.join(self.root, "sources")
        os.symlink(external_root, sources_dir)
        log_path = os.path.join(self.tempdir, "prep.log")
        executor = LfsBaseExecutor(self.config, {"build_root": self.root}, self.store)

        class _Stat:
            st_uid = 0
            st_gid = 0
            st_mode = 0o41777

        real_stat = os.stat

        def fake_stat(path, *args, **kwargs):
            if os.path.abspath(path) == os.path.abspath(external_root):
                return _Stat()
            return real_stat(path, *args, **kwargs)

        with patch("lfs_unified_pm.lfs_base.os.stat", side_effect=fake_stat), patch.object(executor.guarded_ops, "run_command") as run_command:
            executor._prepare_target_root(self.root, master_log_path=log_path)

        run_command.assert_not_called()
        with open(log_path, "r", encoding="utf-8") as handle:
            self.assertIn("already satisfies owner/mode policy", handle.read())

    def test_prepare_target_root_fixes_symlinked_sources_dir_target_permissions(self):
        external_root = os.path.join(self.tempdir, "external")
        os.makedirs(external_root)
        sources_dir = os.path.join(self.root, "sources")
        os.symlink(external_root, sources_dir)
        executor = LfsBaseExecutor(self.config, {"build_root": self.root}, self.store)

        class _Stat:
            st_uid = 1000
            st_gid = 1000
            st_mode = 0o40755

        real_stat = os.stat

        def fake_stat(path, *args, **kwargs):
            if os.path.abspath(path) == os.path.abspath(external_root):
                return _Stat()
            return real_stat(path, *args, **kwargs)

        with patch("lfs_unified_pm.lfs_base.os.stat", side_effect=fake_stat), patch.object(executor.guarded_ops, "ensure_owner") as ensure_owner, patch.object(
            executor.guarded_ops, "ensure_mode"
        ) as ensure_mode:
            from lfs_unified_pm.guarded_ops import GuardedOpResult

            ensure_owner.return_value = GuardedOpResult(status="performed", changed=True)
            ensure_mode.return_value = GuardedOpResult(status="performed", changed=True)
            executor._prepare_target_root(self.root)

        ensure_owner.assert_called_once()
        ensure_mode.assert_called_once()
        self.assertEqual(ensure_owner.call_args.args[0], external_root)
        self.assertEqual(ensure_owner.call_args.args[1], "root:root")
        self.assertIn("symlink to another folder", ensure_owner.call_args.kwargs["description"])
        self.assertEqual(ensure_mode.call_args.args[0], external_root)
        self.assertEqual(ensure_mode.call_args.args[1], 0o1777)

    def test_prepare_target_root_fixes_sources_under_symlinked_build_root(self):
        external_build_root = os.path.join(self.tempdir, "external-build-root")
        os.makedirs(os.path.join(external_build_root, "sources"))
        linked_root = os.path.join(self.tempdir, "linked-root")
        os.symlink(external_build_root, linked_root)

        linked_config = default_config(linked_root)
        ensure_directories(linked_config)
        linked_store = StateStore(linked_config.db_path)
        try:
            executor = LfsBaseExecutor(linked_config, {"build_root": linked_root}, linked_store)

            class _Stat:
                st_uid = 1000
                st_gid = 1000
                st_mode = 0o40755

            real_stat = os.stat

            def fake_stat(path, *args, **kwargs):
                if os.path.abspath(path) == os.path.abspath(os.path.join(external_build_root, "sources")):
                    return _Stat()
                return real_stat(path, *args, **kwargs)

            with patch("lfs_unified_pm.lfs_base.os.stat", side_effect=fake_stat), patch.object(
                executor.guarded_ops, "ensure_owner"
            ) as ensure_owner, patch.object(executor.guarded_ops, "ensure_mode") as ensure_mode:
                from lfs_unified_pm.guarded_ops import GuardedOpResult

                ensure_owner.return_value = GuardedOpResult(status="performed", changed=True)
                ensure_mode.return_value = GuardedOpResult(status="performed", changed=True)
                executor._prepare_target_root(linked_root)

            resolved_sources = os.path.join(external_build_root, "sources")
            self.assertEqual(ensure_owner.call_args.args[0], resolved_sources)
            self.assertEqual(ensure_mode.call_args.args[0], resolved_sources)
        finally:
            linked_store.close()

    def test_guarded_notice_payloads_show_sudo_for_root_actions(self):
        step = LfsBuildStep(
            name="creatingminlayout",
            chapter="chapter04",
            stage="host-root-prep",
            order=1,
            script_path="",
            relative_path="chapter04/401-creatingminlayout",
            description="",
        )
        with patch("lfs_unified_pm.lfs_base.os.geteuid", return_value=1000):
            payloads = _guarded_step_notice_payloads(self.root, step, {})
        self.assertTrue(payloads)
        self.assertEqual(payloads[0]["command"][0], "sudo")

    def test_guarded_notice_payloads_batch_chown_paths_for_addinguser(self):
        step = LfsBuildStep(
            name="addinguser",
            chapter="chapter04",
            stage="host-root-prep",
            order=1,
            script_path="",
            relative_path="chapter04/402-addinguser",
            description="",
        )
        os.makedirs(os.path.join(self.root, "usr", "bin"), exist_ok=True)
        os.makedirs(os.path.join(self.root, "var"), exist_ok=True)
        os.makedirs(os.path.join(self.root, "etc"), exist_ok=True)
        os.makedirs(os.path.join(self.root, "tools"), exist_ok=True)

        class _Stat:
            st_uid = 1000
            st_gid = 1000
            st_mode = 0o40755

        real_stat = os.stat

        def fake_stat(path, *args, **kwargs):
            interesting = {
                os.path.abspath(os.path.join(self.root, "usr")),
                os.path.abspath(os.path.join(self.root, "usr", "bin")),
                os.path.abspath(os.path.join(self.root, "var")),
                os.path.abspath(os.path.join(self.root, "etc")),
                os.path.abspath(os.path.join(self.root, "tools")),
            }
            if os.path.abspath(path) in interesting:
                return _Stat()
            return real_stat(path, *args, **kwargs)

        with patch("lfs_unified_pm.lfs_base._lookup_user", return_value=1001), patch(
            "lfs_unified_pm.lfs_base._lookup_group", return_value=1001
        ), patch("lfs_unified_pm.lfs_base.os.stat", side_effect=fake_stat), patch(
            "lfs_unified_pm.lfs_base.os.geteuid", return_value=1000
        ):
            payloads = _guarded_step_notice_payloads(self.root, step, {"luser": "lfs", "lgroup": "lfs"})

        chown_payloads = [payload for payload in payloads if payload["command"][0] == "sudo" and "chown" in payload["command"]]
        self.assertEqual(len(chown_payloads), 1)
        self.assertIn("chapter04 target directories", chown_payloads[0]["description"])

    def test_default_build_root_uses_separate_directory(self):
        resolved = _resolve_lfs_build_root(self.config, {})
        self.assertEqual(resolved, os.path.join(self.tempdir, "lfs-build-root"))
        self.assertNotEqual(resolved, self.config.root)

    def test_default_source_archive_dir_uses_build_root_sources(self):
        build_root = _resolve_lfs_build_root(self.config, {})
        resolved = _resolve_lfs_archive_dir(self.config, {"source_archive_dir": "/sources"}, build_root)
        self.assertEqual(resolved, os.path.join(build_root, "sources"))

    def test_lfs_base_state_round_trip(self):
        payload = {"completed_steps": ["chapter04/001-demo"], "last_order": 1}
        self.store.save_lfs_base_state(payload)
        self.assertEqual(self.store.get_lfs_base_state(), payload)
        self.store.clear_lfs_base_state()
        self.assertEqual(self.store.get_lfs_base_state(), {})

    def test_execute_creates_master_and_step_logs(self):
        commands_root = os.path.join(self.tempdir, "commands")
        os.makedirs(os.path.join(commands_root, "chapter04"))
        script_path = os.path.join(commands_root, "chapter04", "401-demo")
        with open(script_path, "w", encoding="utf-8") as handle:
            handle.write("#!/bin/bash\necho demo-step\n")
        os.chmod(script_path, 0o755)
        plan = LfsBuildPlan(
            book_root="",
            profiled_book="",
            commands_root=commands_root,
            target_triplet="$(uname -m)-lfs-linux-gnu",
            source_entries=[],
            steps=[
                LfsBuildStep(
                    name="demo",
                    chapter="chapter04",
                    stage="host-root-prep",
                    order=1,
                    script_path=script_path,
                    relative_path="chapter04/401-demo",
                    description="demo",
                )
            ],
            stage_scripts={"host-root-prep": ["chapter04/401-demo"]},
        )
        executor = LfsBaseExecutor(self.config, {"build_root": self.root}, self.store)

        def fake_run_command(command, env=None, require_root=False, context="", target_root="", location="", log_path="", master_log_path="", description=""):
            if command and command[0] == "mkdir":
                os.makedirs(command[-1], exist_ok=True)
                return
            if command and command[0] in ("chown", "chmod"):
                return
            _run_logged(command, env=env, log_path=log_path, master_log_path=master_log_path)
            if command == [os.path.join(self.root, "lfs-base", "chapter04/401-demo")]:
                marker = _step_marker_path(self.root, "chapter04/401-demo")
                os.makedirs(os.path.dirname(marker), exist_ok=True)
                with open(marker, "w", encoding="utf-8") as handle:
                    handle.write("ok\n")

        with patch.object(executor.guarded_ops, "run_command", side_effect=fake_run_command):
            executed = executor.execute(plan, resume=False)

        self.assertEqual(executed, ["chapter04/401-demo"])
        state = self.store.get_lfs_base_state()
        self.assertTrue(os.path.isfile(state["master_log"]))
        self.assertTrue(os.path.isfile(state["last_log"]))
        with open(state["master_log"], "r", encoding="utf-8") as handle:
            self.assertIn("START 001 chapter04/401-demo", handle.read())
        with open(state["last_log"], "r", encoding="utf-8") as handle:
            self.assertIn("demo-step", handle.read())
        self.assertIn("preflight", state)
        self.assertTrue(os.path.isfile(_step_marker_path(self.root, "chapter04/401-demo")))

    def test_dry_run_previews_without_writing_state_or_target(self):
        commands_root = os.path.join(self.tempdir, "commands")
        os.makedirs(os.path.join(commands_root, "chapter04"))
        script_path = os.path.join(commands_root, "chapter04", "401-demo")
        with open(script_path, "w", encoding="utf-8") as handle:
            handle.write("#!/bin/bash\necho demo-step\n")
        notices = []
        executor = LfsBaseExecutor(
            self.config,
            {"build_root": self.root},
            self.store,
            execution_notice_callback=lambda payload: notices.append(payload),
        )
        plan = LfsBuildPlan(
            book_root="",
            profiled_book="",
            commands_root=commands_root,
            target_triplet="$(uname -m)-lfs-linux-gnu",
            source_entries=[],
            steps=[
                LfsBuildStep(
                    name="demo",
                    chapter="chapter04",
                    stage="host-root-prep",
                    order=1,
                    script_path=script_path,
                    relative_path="chapter04/401-demo",
                    description="demo",
                )
            ],
            stage_scripts={"host-root-prep": ["chapter04/401-demo"]},
        )

        previewed = executor.execute(plan, dry_run=True, resume=False)

        self.assertEqual(previewed, ["chapter04/401-demo"])
        self.assertEqual(self.store.get_lfs_base_state(), {})
        self.assertFalse(os.path.exists(os.path.join(self.root, "lfs-base")))
        self.assertEqual(len(notices), 1)
        self.assertEqual(notices[0]["description"], "chapter04/401-demo")
        self.assertEqual(notices[0]["target_root"], self.root)

    def test_sudo_command_forwards_lfs_environment(self):
        command = _sudo_command(["/tmp/demo-step"], {"LFS": "/mnt/test-lfs", "TERM": "xterm"})
        self.assertEqual(command[:4], ["sudo", "env", "LFS=/mnt/test-lfs", "TERM=xterm"])
        self.assertEqual(command[-1], "/tmp/demo-step")

    def test_validated_lfs_env_rejects_empty(self):
        with self.assertRaises(RuntimeError):
            _validated_lfs_env({})
        with self.assertRaises(RuntimeError):
            _validated_lfs_env({"LFS": ""})

    def test_extract_script_variable_reads_package_name(self):
        script_path = os.path.join(self.tempdir, "script.sh")
        with open(script_path, "w", encoding="utf-8") as handle:
            handle.write("PACKAGE=binutils-2.46.0.tar.xz\n")
        self.assertEqual(_extract_script_variable(script_path, "PACKAGE"), "binutils-2.46.0.tar.xz")

    def test_lint_step_script_refuses_embedded_sudo(self):
        script_path = os.path.join(self.tempdir, "bad.sh")
        with open(script_path, "w", encoding="utf-8") as handle:
            handle.write("#!/bin/bash\nsudo make install\n")
        step = LfsBuildStep(
            name="bad",
            chapter="chapter05",
            stage="lfs-user",
            order=1,
            script_path=script_path,
            relative_path="chapter05/599-bad",
            description="",
        )
        issues = _lint_step_script(step, self.root)
        self.assertTrue(any("embedded privilege escalation" in issue for issue in issues))

    def test_lint_step_script_refuses_host_redirection_outside_lfs(self):
        script_path = os.path.join(self.tempdir, "bad-redirect.sh")
        with open(script_path, "w", encoding="utf-8") as handle:
            handle.write("#!/bin/bash\necho test > /etc/issue\n")
        step = LfsBuildStep(
            name="bad",
            chapter="chapter05",
            stage="lfs-user",
            order=1,
            script_path=script_path,
            relative_path="chapter05/598-bad",
            description="",
        )
        issues = _lint_step_script(step, self.root)
        self.assertTrue(any("outside LFS" in issue for issue in issues))

    def test_lint_step_script_refuses_account_management_outside_chroot(self):
        script_path = os.path.join(self.tempdir, "bad-account.sh")
        with open(script_path, "w", encoding="utf-8") as handle:
            handle.write("#!/bin/bash\nuseradd demo\n")
        step = LfsBuildStep(
            name="bad",
            chapter="chapter05",
            stage="host-root-prep",
            order=1,
            script_path=script_path,
            relative_path="chapter05/596-bad-account",
            description="",
        )
        issues = _lint_step_script(step, self.root)
        self.assertTrue(any("account-management command outside chroot" in issue for issue in issues))

    def test_lint_step_script_ignores_injected_guard_wrappers(self):
        script_path = os.path.join(self.tempdir, "guarded.sh")
        with open(script_path, "w", encoding="utf-8") as handle:
            handle.write(
                "#!/bin/bash\n"
                "mkdir() {\n"
                "  command mkdir \"$@\"\n"
                "}\n"
                "# Start of LFS book script\n"
                "echo demo\n"
                "# End of LFS book script\n"
            )
        step = LfsBuildStep(
            name="guarded",
            chapter="chapter05",
            stage="lfs-user",
            order=1,
            script_path=script_path,
            relative_path="chapter05/597-guarded",
            description="",
        )
        self.assertEqual(_lint_step_script(step, self.root), [])

    def test_stage_transition_refuses_entering_lfs_user_stage_without_prior_markers(self):
        commands_root = os.path.join(self.tempdir, "commands")
        os.makedirs(os.path.join(commands_root, "chapter04"))
        os.makedirs(os.path.join(commands_root, "chapter05"))
        prep_script = os.path.join(commands_root, "chapter04", "401-creatingminlayout")
        with open(prep_script, "w", encoding="utf-8") as handle:
            handle.write("#!/bin/bash\necho prep\n")
        script_path = os.path.join(commands_root, "chapter05", "501-binutils-pass1")
        with open(script_path, "w", encoding="utf-8") as handle:
            handle.write("#!/bin/bash\nPACKAGE=binutils-2.46.0.tar.xz\necho demo\n")
        plan = LfsBuildPlan(
            book_root="",
            profiled_book="",
            commands_root=commands_root,
            target_triplet="x86_64-lfs-linux-gnu",
            source_entries=[],
            steps=[
                LfsBuildStep(
                    name="creatingminlayout",
                    chapter="chapter04",
                    stage="host-root-prep",
                    order=1,
                    script_path=prep_script,
                    relative_path="chapter04/401-creatingminlayout",
                    description="",
                ),
                LfsBuildStep(
                    name="binutils-pass1",
                    chapter="chapter05",
                    stage="lfs-user",
                    order=2,
                    script_path=script_path,
                    relative_path="chapter05/501-binutils-pass1",
                    description="",
                )
            ],
            stage_scripts={
                "host-root-prep": ["chapter04/401-creatingminlayout"],
                "lfs-user": ["chapter05/501-binutils-pass1"],
            },
        )
        executor = LfsBaseExecutor(self.config, {"build_root": self.root}, self.store)
        os.makedirs(os.path.join(self.root, "sources"), exist_ok=True)
        with open(os.path.join(self.root, "sources", "binutils-2.46.0.tar.xz"), "w", encoding="utf-8") as handle:
            handle.write("x")
        with self.assertRaises(RuntimeError):
            executor._precheck_stage_transition(plan, self.root, "lfs-user", set())

    def test_verify_step_state_checks_expected_artifacts(self):
        marker = _step_marker_path(self.root, "chapter05/501-binutils-pass1")
        os.makedirs(os.path.dirname(marker), exist_ok=True)
        with open(marker, "w", encoding="utf-8") as handle:
            handle.write("ok\n")
        tool = os.path.join(self.root, "tools", "bin")
        os.makedirs(tool, exist_ok=True)
        with open(os.path.join(tool, "x86_64-lfs-linux-gnu-ld"), "w", encoding="utf-8") as handle:
            handle.write("ld\n")
        step = LfsBuildStep(
            name="binutils-pass1",
            chapter="chapter05",
            stage="lfs-user",
            order=1,
            script_path="",
            relative_path="chapter05/501-binutils-pass1",
            description="",
        )
        _verify_step_state(
            self.root,
            step,
            marker,
            _capture_sensitive_snapshot(),
            target_triplet="x86_64-lfs-linux-gnu",
        )

    def test_verify_step_semantics_for_cleanup_removes_tester(self):
        etc_dir = os.path.join(self.root, "etc")
        os.makedirs(os.path.join(self.root, "home", "tester"), exist_ok=True)
        os.makedirs(etc_dir, exist_ok=True)
        with open(os.path.join(etc_dir, "passwd"), "w", encoding="utf-8") as handle:
            handle.write("root:x:0:0:root:/root:/bin/bash\n")
        with open(os.path.join(etc_dir, "group"), "w", encoding="utf-8") as handle:
            handle.write("root:x:0:\n")
        step = LfsBuildStep(
            name="cleanup",
            chapter="chapter08",
            stage="chroot-root",
            order=1,
            script_path="",
            relative_path="chapter08/883-cleanup",
            description="",
        )
        with self.assertRaises(RuntimeError):
            _verify_step_semantics(self.root, step)


if __name__ == "__main__":
    unittest.main()

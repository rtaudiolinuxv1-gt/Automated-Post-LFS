from __future__ import annotations

import os
import shlex
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime


class GuardedOpDeclined(Exception):
    pass


@dataclass
class GuardedOpResult:
    status: str
    changed: bool = False
    message: str = ""


class GuardedOperationRunner:
    def __init__(self, root_approval_callback=None, execution_notice_callback=None):
        self.root_approval_callback = root_approval_callback
        self.execution_notice_callback = execution_notice_callback

    def ensure_dir(self, path, target_root, env=None, owner="", mode=None, log_path="", master_log_path="", description="", require_root=None, allowed_roots=()):
        self._require_target_path(path, target_root, allowed_roots=allowed_roots)
        env = self._effective_env(env, target_root)
        if os.path.exists(path) and not os.path.isdir(path):
            raise RuntimeError("Refusing to use non-directory path: %s" % path)
        changed = False
        if not os.path.isdir(path):
            use_root = self._needs_root_for_missing_path(path) if require_root is None else require_root
            self.run_command(
                ["mkdir", "-p", path],
                env=env,
                require_root=use_root,
                context="host-root-prep",
                target_root=target_root,
                location=path,
                log_path=log_path,
                master_log_path=master_log_path,
                description=description or "create directory %s" % path,
            )
            changed = True
        if owner:
            changed = self.ensure_owner(
                path,
                owner,
                target_root=target_root,
                env=env,
                log_path=log_path,
                master_log_path=master_log_path,
                description="set ownership on %s" % path,
                require_root=require_root,
                allowed_roots=allowed_roots,
            ).changed or changed
        if mode is not None:
            changed = self.ensure_mode(
                path,
                mode,
                target_root=target_root,
                env=env,
                log_path=log_path,
                master_log_path=master_log_path,
                description="set mode on %s" % path,
                require_root=require_root,
                allowed_roots=allowed_roots,
            ).changed or changed
        return GuardedOpResult(status="performed" if changed else "skipped", changed=changed)

    def ensure_owner(self, path, owner, target_root, env=None, log_path="", master_log_path="", description="", require_root=None, allowed_roots=()):
        self._require_target_path(path, target_root, allowed_roots=allowed_roots)
        env = self._effective_env(env, target_root)
        if not os.path.exists(path):
            raise RuntimeError("Cannot set ownership on missing path: %s" % path)
        user_name, group_name = self._split_owner(owner)
        uid = self._lookup_uid(user_name)
        gid = self._lookup_gid(group_name)
        st = os.stat(path)
        if uid is not None and gid is not None and st.st_uid == uid and st.st_gid == gid:
            return GuardedOpResult(status="skipped", changed=False)
        self.run_command(
            ["chown", owner, path],
            env=env,
            require_root=True if require_root is None else require_root,
            context="host-root-prep",
            target_root=target_root,
            location=path,
            log_path=log_path,
            master_log_path=master_log_path,
            description=description or "set ownership on %s" % path,
        )
        return GuardedOpResult(status="performed", changed=True)

    def ensure_owner_many(self, paths, owner, target_root, env=None, log_path="", master_log_path="", description="", require_root=None, allowed_roots=()):
        env = self._effective_env(env, target_root)
        user_name, group_name = self._split_owner(owner)
        uid = self._lookup_uid(user_name)
        gid = self._lookup_gid(group_name)
        if uid is None or gid is None:
            raise RuntimeError("Unknown ownership target: %s" % owner)
        changed_paths = []
        for path in paths:
            self._require_target_path(path, target_root, allowed_roots=allowed_roots)
            if not os.path.exists(path):
                raise RuntimeError("Cannot set ownership on missing path: %s" % path)
            st = os.stat(path)
            if st.st_uid == uid and st.st_gid == gid:
                continue
            changed_paths.append(path)
        if not changed_paths:
            return GuardedOpResult(status="skipped", changed=False)
        self.run_command(
            ["chown", owner] + changed_paths,
            env=env,
            require_root=True if require_root is None else require_root,
            context="host-root-prep",
            target_root=target_root,
            location=", ".join(changed_paths[:3]) + (" ..." if len(changed_paths) > 3 else ""),
            log_path=log_path,
            master_log_path=master_log_path,
            description=description or "set ownership on %d path(s)" % len(changed_paths),
        )
        return GuardedOpResult(status="performed", changed=True)

    def ensure_mode(self, path, mode, target_root, env=None, log_path="", master_log_path="", description="", require_root=None, allowed_roots=()):
        self._require_target_path(path, target_root, allowed_roots=allowed_roots)
        env = self._effective_env(env, target_root)
        if not os.path.exists(path):
            raise RuntimeError("Cannot set mode on missing path: %s" % path)
        desired = int(mode, 8) if isinstance(mode, str) else int(mode)
        current = os.stat(path).st_mode & 0o7777
        if current == desired:
            return GuardedOpResult(status="skipped", changed=False)
        self.run_command(
            ["chmod", "%04o" % desired, path],
            env=env,
            require_root=self._needs_root_for_path(path) if require_root is None else require_root,
            context="host-root-prep",
            target_root=target_root,
            location=path,
            log_path=log_path,
            master_log_path=master_log_path,
            description=description or "set mode on %s" % path,
        )
        return GuardedOpResult(status="performed", changed=True)

    def remove_tree(self, path, target_root, env=None, log_path="", master_log_path="", description="", require_root=None, allowed_roots=()):
        self._require_target_path(path, target_root, allowed_roots=allowed_roots)
        env = self._effective_env(env, target_root)
        if not os.path.exists(path):
            return GuardedOpResult(status="skipped", changed=False)
        if self._is_tree_writable(path) or os.geteuid() == 0:
            shutil.rmtree(path)
            return GuardedOpResult(status="performed", changed=True)
        self.run_command(
            ["rm", "-rf", path],
            env=env,
            require_root=True if require_root is None else require_root,
            context="host-root-prep",
            target_root=target_root,
            location=path,
            log_path=log_path,
            master_log_path=master_log_path,
            description=description or "remove tree %s" % path,
        )
        return GuardedOpResult(status="performed", changed=True)

    def ensure_symlink(
        self,
        path,
        target,
        target_root,
        env=None,
        log_path="",
        master_log_path="",
        description="",
        require_root=None,
        keep_existing_nonlink=False,
        allowed_roots=(),
    ):
        self._require_target_path(path, target_root, allowed_roots=allowed_roots)
        env = self._effective_env(env, target_root)
        if os.path.islink(path):
            if os.readlink(path) == target:
                return GuardedOpResult(status="skipped", changed=False)
            if require_root is None:
                require_root = self._needs_root_for_path(path)
            self.run_command(
                ["ln", "-svf", target, path],
                env=env,
                require_root=require_root,
                context="host-root-prep",
                target_root=target_root,
                location=path,
                log_path=log_path,
                master_log_path=master_log_path,
                description=description or "refresh symlink %s" % path,
            )
            return GuardedOpResult(status="performed", changed=True)
        if os.path.exists(path):
            if keep_existing_nonlink:
                return GuardedOpResult(status="skipped", changed=False, message="keeping existing non-link")
            raise RuntimeError("Refusing to replace non-link path: %s" % path)
        if require_root is None:
            require_root = self._needs_root_for_missing_path(path)
        self.run_command(
            ["ln", "-sv", target, path],
            env=env,
            require_root=require_root,
            context="host-root-prep",
            target_root=target_root,
            location=path,
            log_path=log_path,
            master_log_path=master_log_path,
            description=description or "create symlink %s" % path,
        )
        return GuardedOpResult(status="performed", changed=True)

    def ensure_group(self, name, target_root, gid=None, env=None, log_path="", master_log_path="", description=""):
        env = self._effective_env(env, target_root)
        current_gid = self._lookup_gid(name)
        if current_gid is not None:
            return GuardedOpResult(status="skipped", changed=False)
        command = ["groupadd"]
        if gid is not None:
            command.extend(["-g", str(gid)])
        command.append(name)
        self.run_command(
            command,
            env=env,
            require_root=True,
            context="host-account",
            target_root=target_root,
            location="/etc/group",
            log_path=log_path,
            master_log_path=master_log_path,
            description=description or "create group %s" % name,
        )
        return GuardedOpResult(status="performed", changed=True)

    def ensure_user(self, name, target_root, group, shell="/bin/bash", home="", env=None, log_path="", master_log_path="", description=""):
        env = self._effective_env(env, target_root)
        current_uid = self._lookup_uid(name)
        if current_uid is not None:
            return GuardedOpResult(status="skipped", changed=False)
        command = ["useradd", "-s", shell, "-g", group]
        if home:
            command.extend(["-m", "-k", "/dev/null", "-d", home])
        else:
            command.extend(["-m", "-k", "/dev/null"])
        command.append(name)
        self.run_command(
            command,
            env=env,
            require_root=True,
            context="host-account",
            target_root=target_root,
            location="/etc/passwd",
            log_path=log_path,
            master_log_path=master_log_path,
            description=description or "create user %s" % name,
        )
        return GuardedOpResult(status="performed", changed=True)

    def write_text_file(
        self,
        path,
        content,
        target_root,
        env=None,
        log_path="",
        master_log_path="",
        description="",
        mode=None,
        owner="",
        require_root=None,
        allowed_roots=(),
    ):
        self._require_target_path(path, target_root, allowed_roots=allowed_roots)
        env = self._effective_env(env, target_root)
        parent = os.path.dirname(path)
        self.ensure_dir(parent, target_root, env=env, require_root=require_root, allowed_roots=allowed_roots)
        existing = None
        if os.path.isfile(path):
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    existing = handle.read()
            except OSError:
                existing = None
        if existing == content:
            changed = False
        else:
            if require_root is None:
                need_root = self._needs_root_for_missing_path(path) if not os.path.exists(path) else self._needs_root_for_path(path)
            else:
                need_root = require_root
            if not need_root:
                with open(path, "w", encoding="utf-8") as handle:
                    handle.write(content)
            else:
                command = "cat > %s <<'EOF'\n%sEOF" % (_shell_quote(path), content)
                self.run_command(
                    ["/bin/bash", "-lc", command],
                    env=env,
                    require_root=True,
                    context="host-file",
                    target_root=target_root,
                    location=path,
                    log_path=log_path,
                    master_log_path=master_log_path,
                    description=description or "write file %s" % path,
                )
            changed = True
        if owner:
            changed = self.ensure_owner(path, owner, target_root, env=env, log_path=log_path, master_log_path=master_log_path, require_root=require_root, allowed_roots=allowed_roots).changed or changed
        if mode is not None:
            changed = self.ensure_mode(path, mode, target_root, env=env, log_path=log_path, master_log_path=master_log_path, require_root=require_root, allowed_roots=allowed_roots).changed or changed
        return GuardedOpResult(status="performed" if changed else "skipped", changed=changed)

    def run_command(
        self,
        command,
        env=None,
        require_root=False,
        cwd="",
        context="host",
        target_root="",
        location="",
        log_path="",
        master_log_path="",
        description="",
    ):
        if isinstance(command, str):
            command = [command]
        payload = {
            "description": description or "",
            "command": list(command),
            "command_text": " ".join(shlex.quote(part) for part in command),
            "context": context or "host",
            "target_root": target_root or "",
            "location": location or "",
            "env": {key: value for key, value in (env or {}).items() if key in ("LFS", "TERM", "LC_ALL", "PATH", "CONFIG_SITE", "LFS_TGT", "MAKEFLAGS")},
        }
        if require_root:
            self._validated_lfs_env(env)
            command = self._sudo_command(command, env) if os.geteuid() != 0 else list(command)
            payload["command"] = list(command)
            payload["command_text"] = " ".join(shlex.quote(part) for part in command)
            self._confirm_root(payload, env)
        self._emit_notice(payload)
        self._run_logged(
            command,
            env=env if os.geteuid() == 0 or not require_root else None,
            cwd=cwd or None,
            log_path=log_path,
            master_log_path=master_log_path,
        )
        return GuardedOpResult(status="performed", changed=True)

    def _confirm_root(self, payload, env):
        if not self.root_approval_callback:
            return
        approved = self.root_approval_callback(
            {
                "description": payload.get("description", ""),
                "command": payload.get("command", []),
                "command_text": payload.get("command_text", ""),
                "env": {key: value for key, value in (env or {}).items() if key in ("LFS", "TERM", "LC_ALL", "PATH", "CONFIG_SITE", "LFS_TGT", "MAKEFLAGS")},
            }
        )
        if not approved:
            raise GuardedOpDeclined("Root action declined: %s" % (payload.get("description") or "root action"))

    def _emit_notice(self, payload):
        if not self.execution_notice_callback:
            return
        allowed = self.execution_notice_callback(payload)
        if allowed is False:
            raise GuardedOpDeclined("Execution declined: %s" % (payload.get("description") or "command"))

    def _run_logged(self, command, env=None, cwd=None, log_path="", master_log_path=""):
        if not log_path and not master_log_path:
            subprocess.run(command, check=True, env=env, cwd=cwd)
            return
        handles = []
        try:
            for path in (log_path, master_log_path):
                if path:
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    handles.append(open(path, "a", encoding="utf-8"))
            self._write_log_line(handles, "[%s] $ %s\n" % (_timestamp(), " ".join(shlex.quote(part) for part in command)))
            process = subprocess.Popen(
                command,
                env=env,
                cwd=cwd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                errors="replace",
                bufsize=1,
            )
            if process.stdout:
                try:
                    for line in process.stdout:
                        self._write_log_line(handles, line)
                finally:
                    process.stdout.close()
            return_code = process.wait()
            if return_code:
                raise subprocess.CalledProcessError(return_code, command)
        finally:
            for handle in handles:
                handle.close()

    def _write_log_line(self, handles, text):
        for handle in handles:
            handle.write(text)
            handle.flush()

    def _sudo_command(self, command, env=None):
        lfs = self._validated_lfs_env(env)
        forwarded = []
        for key in ("LFS", "TERM", "LC_ALL", "PATH", "CONFIG_SITE", "LFS_TGT", "MAKEFLAGS"):
            value = (env or {}).get(key)
            if value:
                forwarded.append("%s=%s" % (key, value))
        if not forwarded:
            return ["sudo"] + list(command)
        return ["sudo", "env"] + forwarded + list(command)

    def _validated_lfs_env(self, env):
        value = ((env or {}).get("LFS", "") or "").strip()
        if not value:
            raise RuntimeError("Refusing root action without non-empty LFS")
        if value == "/":
            raise RuntimeError("Refusing root action with LFS=/")
        if not os.path.isabs(value):
            raise RuntimeError("Refusing root action with non-absolute LFS=%s" % value)
        return value

    def _effective_env(self, env, target_root):
        merged = dict(env or {})
        if target_root and not merged.get("LFS"):
            merged["LFS"] = target_root
        return merged

    def _require_target_path(self, path, target_root, allowed_roots=()):
        real_root = os.path.realpath(target_root)
        real_path = os.path.realpath(path if os.path.exists(path) else os.path.dirname(path) or path)
        if real_root == "/" or real_root == os.path.realpath(os.sep):
            raise RuntimeError("Refusing to use target root resolving to /")
        for allowed in allowed_roots or ():
            real_allowed = os.path.realpath(allowed)
            if real_path == real_allowed or real_path.startswith(real_allowed + os.sep):
                return
        if real_path != real_root and not real_path.startswith(real_root + os.sep):
            raise RuntimeError("Refusing to operate outside target root: %s" % path)

    def _is_tree_writable(self, path):
        for root, dirs, files in os.walk(path):
            if not os.access(root, os.W_OK | os.X_OK):
                return False
            for name in files:
                if not os.access(os.path.join(root, name), os.W_OK):
                    return False
        return True

    def _needs_root_for_missing_path(self, path):
        probe = path
        while not os.path.exists(probe):
            parent = os.path.dirname(probe)
            if parent == probe:
                break
            probe = parent
        return not os.access(probe, os.W_OK | os.X_OK)

    def _needs_root_for_path(self, path):
        return not os.access(path, os.W_OK)

    def _split_owner(self, owner):
        if ":" in owner:
            return owner.split(":", 1)
        return owner, owner

    def _lookup_uid(self, name):
        import pwd

        try:
            return pwd.getpwnam(name).pw_uid
        except KeyError:
            return None

    def _lookup_gid(self, name):
        import grp

        try:
            return grp.getgrnam(name).gr_gid
        except KeyError:
            return None


def _timestamp():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")


def _shell_quote(value):
    return "'" + str(value).replace("'", "'\"'\"'") + "'"

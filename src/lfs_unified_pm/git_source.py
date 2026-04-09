from __future__ import annotations

import os
import subprocess


DEFAULT_T2_GIT_URL = "https://github.com/rxrbln/t2sde"
DEFAULT_BLFS_GIT_URL = "https://github.com/lfs-book/blfs"
DEFAULT_JHALFS_GIT_URL = "https://github.com/automate-lfs/jhalfs"


class GitSourceManager:
    def sync_repo(self, repo_dir, repo_url=DEFAULT_T2_GIT_URL, branch=""):
        repo_dir = os.path.abspath(repo_dir)
        if not os.path.isdir(os.path.join(repo_dir, ".git")):
            self._clone(repo_dir, repo_url or DEFAULT_T2_GIT_URL, branch)
            head = self._git(repo_dir, "rev-parse", "HEAD")
            return {
                "repo_dir": repo_dir,
                "repo_url": repo_url or DEFAULT_T2_GIT_URL,
                "branch": branch or self._current_branch(repo_dir),
                "previous_head": "",
                "current_head": head,
                "changed_files": [],
                "changed_packages": [],
                "created": True,
                "warning": "",
            }

        previous_head = self._git(repo_dir, "rev-parse", "HEAD")
        current_url = self._git(repo_dir, "remote", "get-url", "origin")
        if repo_url and current_url != repo_url:
            self._git(repo_dir, "remote", "set-url", "origin", repo_url)
        warning = ""
        if branch:
            self._git(repo_dir, "checkout", branch)
            warning = self._try_pull(repo_dir, "pull", "--ff-only", "origin", branch)
        else:
            warning = self._try_pull(repo_dir, "pull", "--ff-only")
        current_head = self._git(repo_dir, "rev-parse", "HEAD")
        changed_files = []
        if previous_head != current_head:
            changed_files = self._git_lines(
                repo_dir, "diff", "--name-only", previous_head, current_head, "--", "package"
            )
        return {
            "repo_dir": repo_dir,
            "repo_url": repo_url or current_url,
            "branch": branch or self._current_branch(repo_dir),
            "previous_head": previous_head,
            "current_head": current_head,
            "changed_files": changed_files,
            "changed_packages": sorted(_package_names_from_paths(changed_files)),
            "created": False,
            "warning": warning,
        }

    def _clone(self, repo_dir, repo_url, branch):
        os.makedirs(os.path.dirname(repo_dir), exist_ok=True)
        command = ["git", "clone"]
        if branch:
            command.extend(["--branch", branch])
        command.extend([repo_url, repo_dir])
        subprocess.run(command, check=True)

    def _current_branch(self, repo_dir):
        return self._git(repo_dir, "rev-parse", "--abbrev-ref", "HEAD")

    def _git(self, repo_dir, *args):
        result = subprocess.run(
            ["git"] + list(args),
            cwd=repo_dir,
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()

    def _git_lines(self, repo_dir, *args):
        output = self._git(repo_dir, *args)
        return [line for line in output.splitlines() if line]

    def _try_pull(self, repo_dir, *args):
        try:
            self._git(repo_dir, *args)
            return ""
        except subprocess.CalledProcessError as error:
            stderr = error.stderr.strip() if error.stderr else ""
            stdout = error.stdout.strip() if error.stdout else ""
            return stderr or stdout or "git pull failed"


def _package_names_from_paths(paths):
    names = set()
    for path in paths:
        parts = path.split("/")
        if len(parts) >= 3 and parts[0] == "package":
            names.add(parts[2])
    return names

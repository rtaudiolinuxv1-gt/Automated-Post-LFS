#!/bin/bash
set -e

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
cd "$SCRIPT_DIR"

REMOTE_URL="${1:-}"
COMMIT_MESSAGE="${2:-Initial commit}"
BRANCH="${3:-main}"

if [ -z "$REMOTE_URL" ]; then
  cat <<'EOF'
Usage:
  ./push-to-github.sh <github-repo-url> [commit-message] [branch]

Example:
  ./push-to-github.sh git@github.com:username/lfs-unified-pm.git "Initial import" main
EOF
  exit 1
fi

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  git init
fi

git symbolic-ref HEAD "refs/heads/$BRANCH"

if git remote get-url origin >/dev/null 2>&1; then
  CURRENT_REMOTE="$(git remote get-url origin)"
  if [ "$CURRENT_REMOTE" != "$REMOTE_URL" ]; then
    git remote set-url origin "$REMOTE_URL"
  fi
else
  git remote add origin "$REMOTE_URL"
fi

git add -A

if ! git diff --cached --quiet; then
  git commit -m "$COMMIT_MESSAGE"
else
  if ! git rev-parse --verify HEAD >/dev/null 2>&1; then
    git commit --allow-empty -m "$COMMIT_MESSAGE"
  fi
fi

git push -u origin "$BRANCH"

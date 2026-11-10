"""Проверка имени ветки, заголовка PR и сообщения коммита по правилам из CONTRIBUTING.md.

Используется в CI и в pre-commit (хук commit-msg). Без зависимостей.

    python scripts/check_conventions.py branch feature/PROJ-123-streaming-copy
    python scripts/check_conventions.py title "feat(pg2gp): PROJ-123 ..." --branch feature/PROJ-123-...
    python scripts/check_conventions.py commit-msg .git/COMMIT_EDITMSG
"""

from __future__ import annotations

import argparse
import re
import sys

TYPES = ("feat", "fix", "perf", "refactor", "test", "docs", "chore", "ci", "revert")
KEY = r"[A-Z][A-Z0-9]+-[0-9]+"

SLUG = r"[a-z0-9]+(?:-[a-z0-9]+)*"

BRANCH_RE = re.compile(rf"^(?:(feature|fix|hotfix)/({KEY})-{SLUG}|(chore|docs|ci)/{SLUG})$")
SUBJECT_RE = re.compile(rf"^({'|'.join(TYPES)})(\([a-z0-9_-]+\))?!?: (?:({KEY}) )?\S(?:.*[^.\s])?$")
# Сообщения, которые git создаёт сам
AUTO_SUBJECT_RE = re.compile(r"^(Merge |Revert \"|fixup! |squash! |amend! )")

FORMAT_HINT = "ожидается '<type>(<scope>): <KEY-123> <описание>', type: " + ", ".join(TYPES)


def check_branch(name: str) -> list[str]:
    if name in ("main", "master"):
        return []
    if not BRANCH_RE.match(name):
        return [
            f"имя ветки '{name}' не по правилам: feature|fix|hotfix/<KEY-123>-<desc> или chore|docs|ci/<desc>"
        ]
    return []


def branch_key(name: str) -> str | None:
    m = BRANCH_RE.match(name)
    return m.group(2) if m else None


def check_subject(subject: str, required_key: str | None = None) -> list[str]:
    m = SUBJECT_RE.match(subject)
    if not m:
        return [f"'{subject}': {FORMAT_HINT}, без точки в конце"]
    if required_key and m.group(3) != required_key:
        return [f"'{subject}': нужен ключ задачи {required_key} из имени ветки сразу после ': '"]
    return []


def check_commit_msg(path: str) -> list[str]:
    with open(path, encoding="utf-8") as f:
        lines = [ln for ln in f.read().splitlines() if not ln.startswith("#")]
    subject = next((ln for ln in lines if ln.strip()), "")
    if AUTO_SUBJECT_RE.match(subject):
        return []
    return check_subject(subject)


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("branch").add_argument("name")
    p_title = sub.add_parser("title")
    p_title.add_argument("title")
    p_title.add_argument("--branch")
    sub.add_parser("commit-msg").add_argument("path")
    args = parser.parse_args()
    sys.stderr.reconfigure(encoding="utf-8")

    if args.cmd == "branch":
        errors = check_branch(args.name)
    elif args.cmd == "title":
        errors = check_subject(args.title, branch_key(args.branch) if args.branch else None)
    else:
        errors = check_commit_msg(args.path)

    for e in errors:
        print(f"conventions: {e}", file=sys.stderr)
    if errors:
        print("Правила: CONTRIBUTING.md", file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())

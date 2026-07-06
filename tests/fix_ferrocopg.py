from __future__ import annotations

from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path

import pytest

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 fallback.
    tomllib = None  # type: ignore[assignment]


_LIBPQ_FIXTURES = frozenset({"pgconn"})
_ASYNC_FIXTURES = frozenset({"aconn", "aconn_cls", "apipeline", "acommands"})
_COMMAND_FIXTURES = frozenset({"commands"})


@dataclass(frozen=True)
class FerrocopgRule:
    glob: str
    tag: str
    reason: str
    action: str = "xfail"


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "ferrocopg_manifest(tag): expected ferrocopg compatibility gap",
    )

    if not _is_ferrocopg(config):
        return

    if config.getoption("--pq-trace"):
        raise pytest.UsageError("--pq-trace requires --impl=libpq")
    if config.getoption("--pq-debug"):
        raise pytest.UsageError("--pq-debug requires --impl=libpq")


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    if not _is_ferrocopg(config):
        return

    rules = load_manifest(config.rootpath / "tests" / "ferrocopg_manifest.toml")
    for item in items:
        fixture_names = frozenset(getattr(item, "fixturenames", ()))

        for rule in matching_rules(item.nodeid, rules):
            _mark_rule(item, rule)

        if fixture_names & _LIBPQ_FIXTURES:
            _mark_rule(
                item,
                FerrocopgRule(
                    glob=item.nodeid,
                    tag="pgconn",
                    reason="ferrocopg doesn't expose raw libpq PGconn access",
                    action="skip",
                ),
            )
        if fixture_names & _ASYNC_FIXTURES:
            _mark_rule(
                item,
                FerrocopgRule(
                    glob=item.nodeid,
                    tag="async",
                    reason="ferrocopg doesn't provide an async adapter yet",
                ),
            )
        if fixture_names & _COMMAND_FIXTURES:
            _mark_rule(
                item,
                FerrocopgRule(
                    glob=item.nodeid,
                    tag="commands",
                    reason="ferrocopg doesn't provide _exec_command yet",
                ),
            )


def load_manifest(path: Path) -> list[FerrocopgRule]:
    if not path.exists():
        return []

    text = path.read_text()
    raw = tomllib.loads(text) if tomllib else _parse_simple_manifest(text)
    rules = raw.get("rules", [])
    return [
        FerrocopgRule(
            glob=str(rule["glob"]),
            tag=str(rule["tag"]),
            reason=str(rule["reason"]),
            action=str(rule.get("action", "xfail")),
        )
        for rule in rules
    ]


def matching_rules(nodeid: str, rules: list[FerrocopgRule]) -> list[FerrocopgRule]:
    return [rule for rule in rules if fnmatch(nodeid, rule.glob)]


def _parse_simple_manifest(text: str) -> dict[str, list[dict[str, str]]]:
    rules: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line == "[[rules]]":
            current = {}
            rules.append(current)
            continue
        if current is None:
            raise ValueError("ferrocopg manifest entries must be inside [[rules]]")
        key, sep, value = line.partition("=")
        if not sep:
            raise ValueError(f"invalid ferrocopg manifest line: {line!r}")
        current[key.strip()] = value.strip().strip('"')

    return {"rules": rules}


def is_manifested_nodeid(nodeid: str, manifest: Path) -> bool:
    return bool(matching_rules(nodeid, load_manifest(manifest)))


def manifest_tags(nodeid: str, manifest: Path) -> set[str]:
    return {rule.tag for rule in matching_rules(nodeid, load_manifest(manifest))}


def _is_ferrocopg(config: pytest.Config) -> bool:
    return str(config.getoption("--impl")) == "ferrocopg"


def _mark_rule(item: pytest.Item, rule: FerrocopgRule) -> None:
    item.add_marker(pytest.mark.ferrocopg_manifest(rule.tag))
    reason = f"ferrocopg {rule.tag}: {rule.reason}"
    if rule.action == "skip":
        item.add_marker(pytest.mark.skip(reason=reason))
    elif rule.action == "xfail":
        item.add_marker(pytest.mark.xfail(reason=reason, strict=False))
    else:
        raise pytest.UsageError(f"unsupported ferrocopg manifest action: {rule.action}")

#!/bin/bash

# Run the tests in Github Action
#
# Failed tests run up to three times, to take into account flakey tests.
# Of course the random generator is not re-seeded between runs, in order to
# repeat the same result.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# Avoid the repo root shadowing installed packages.
export PYTHONSAFEPATH=1

repo_pythonpath=(
    "$repo_root/psycopg"
    "$repo_root/psycopg_pool"
)

repo_pythonpath_str="$(IFS=:; echo "${repo_pythonpath[*]}")"
export PSYCOPG_CI_REPO_ROOT="$repo_root"
export PSYCOPG_CI_REPO_PYTHONPATH="$repo_pythonpath_str"
export PYTHONPATH="$repo_pythonpath_str${PYTHONPATH:+:$PYTHONPATH}"

# Assemble a markers expression from the MARKERS and NOT_MARKERS env vars
markers=""
for m in ${MARKERS:-}; do
    [[ "$markers" != "" ]] && markers="$markers and"
    markers="$markers $m"
done
for m in ${NOT_MARKERS:-}; do
    [[ "$markers" != "" ]] && markers="$markers and"
    markers="$markers not $m"
done

pytest_runner='
import os
import sys

import pytest


def _normalized(path: str) -> str:
    return os.path.normcase(os.path.abspath(path or os.curdir))


repo_root = _normalized(os.environ["PSYCOPG_CI_REPO_ROOT"])
repo_paths = [
    _normalized(path)
    for path in os.environ.get("PSYCOPG_CI_REPO_PYTHONPATH", "").split(os.pathsep)
    if path
]

blocked = {
    repo_root,
    os.path.join(repo_root, "psycopg"),
    os.path.join(repo_root, "psycopg_c"),
    os.path.join(repo_root, "psycopg_binary"),
    os.path.join(repo_root, "psycopg_pool"),
    _normalized(os.getcwd()),
}

filtered = [path for path in sys.path if _normalized(path) not in blocked]
sys.path[:] = repo_paths + filtered
os.environ["PYTHONPATH"] = os.pathsep.join(repo_paths)

args = ["--color=yes", "-m", os.environ.get("PSYCOPG_CI_MARKERS", "")]
args.extend(sys.argv[1:])
raise SystemExit(pytest.main(args))
'

pytest=()
if [[ "${PSYCOPG_USE_UV:-1}" == "1" && -z "${VIRTUAL_ENV:-}" ]]; then
    uv_run=(uv run)
    if [[ -n "${UV_PROJECT:-}" ]]; then
        uv_run+=(--project "${UV_PROJECT}")
    fi

    lock_dir="${UV_PROJECT:-.}"
    if [[ -f "${lock_dir}/uv.lock" ]]; then
        uv_run+=(--locked)
    fi

    pytest=("${uv_run[@]}" python -bb -c "$pytest_runner")
else
    pytest=(python -bb -c "$pytest_runner")
fi

export PSYCOPG_CI_MARKERS="$markers"

"${pytest[@]}" "$@" && exit 0

"${pytest[@]}" --lf --randomly-seed=last "$@" && exit 0

"${pytest[@]}" --lf --randomly-seed=last "$@"

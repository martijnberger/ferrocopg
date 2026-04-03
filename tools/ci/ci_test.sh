#!/bin/bash

# Run the tests in Github Action
#
# Failed tests run up to three times, to take into account flakey tests.
# Of course the random generator is not re-seeded between runs, in order to
# repeat the same result.

set -euo pipefail

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

pytest=()
if [[ "${PSYCOPG_USE_UV:-1}" == "1" ]]; then
    uv_run=(uv run)
    if [[ -n "${UV_PROJECT:-}" ]]; then
        uv_run+=(--project "${UV_PROJECT}")
    fi

    lock_dir="${UV_PROJECT:-.}"
    if [[ -f "${lock_dir}/uv.lock" ]]; then
        uv_run+=(--locked)
    fi

    pytest=("${uv_run[@]}" python -bb -m pytest --color=yes)
else
    pytest=(python -bb -m pytest --color=yes)
fi

"${pytest[@]}" -m "$markers" "$@" && exit 0

"${pytest[@]}" -m "$markers" --lf --randomly-seed=last "$@" && exit 0

"${pytest[@]}" -m "$markers" --lf --randomly-seed=last "$@"

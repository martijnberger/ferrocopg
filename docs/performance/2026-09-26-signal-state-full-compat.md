# Corrected public boundary: complete local compatibility

Candidate `17c77760` was tested with the complete, unfiltered source harness on
CPython 3.14.6/macOS arm64 and PostgreSQL 15.19. All native production changes
are from `1bab4336`; the successor changes signal-state publication in Python.
No production or test files changed while the harness ran. The configured
virtual environment's tools were on PATH, addressing the earlier missing-mypy
invocation failure without modifying tests.

The native CPython 3.14 source extension SHA-256 is
`82a5d397c122f293be6200eefbdd1060cf33fb587735f3c850e0a8a15449d4d0`.
The Python adapter SHA-256 is
`39953725490b197ad638842851af834af939e081f8b28a2cd49b03646fcc0715`.
This is source compatibility evidence, separate from the 163 installed-wheel
checks and not a package-boundary or performance acceptance run.

## Verdict

The strict gate **fails**. The unmodified classifier finds 4,716/4,720 supported
sync cases passing, with four failures and zero errors. Overall pytest reports
5,209 passed, 109 failed, 11 errors, 1,285 skipped, 49 expected failures and 30
unexpected passes in 265.46 seconds. The existing manifest is unchanged.

The four supported-sync failures are:

| Test | Observed | Assertion |
| --- | --- | --- |
| Pool concurrent filling | first connection 0.110212 s | 0.100 +/- 0.010 s |
| Pool reconnect | first backoff 0.111606 s | 0.100 +/- 0.010 s |
| Pool failed-check backoff | first interval 0.108735 s | 0.100 +/- 0.005 s |
| Scheduler | first callback 0.110113 s | 0.100 +/- 0.010 s |

All other supported-sync families pass: concurrency/cancellation, connections,
COPY, cursors, notifications, pipeline, preparation, transactions, type/metadata,
and the other classifier families. Experimental native async has 493/604
non-manifested cases passing, 100 failures and 11 errors; it is not relabeled as
supported or merged into a sync acceptance claim. Both percentage floors pass,
but that does not override the separate zero-regression gate.

## Targeted diagnosis

After the full process terminated, the same four unchanged assertions ran
sequentially against pure-Python/libpq, then Rust:

| Test | Python/libpq | Rust |
| --- | --- | --- |
| Concurrent filling | Pass | Fail |
| Reconnect | Fail | Pass |
| Failed-check backoff | Fail | Fail |
| Scheduler | Fail | Fail |

Both controls finish with three failures and one pass. The scheduler test does
not perform database I/O and fails near 0.11012 seconds in both controls.
This establishes shared timing failures for reconnect/backoff/scheduling, not a
complete root-cause diagnosis or proof that all Rust timing behavior is harmless.
Concurrent filling remains a Rust-side failure in this pair. Do not replace the
failed complete run with the targeted passes, loosen tolerances, add exclusions,
or fork the pool implementation to hide these observations.

The subsequent [driver-free timer controls](2026-09-26-host-timer-diagnostic.md)
reproduce comparable lateness in two CPython builds and native C/pthreads.
They establish host-level delay without a database driver, not a passing
compatibility gate or complete attribution of every pool failure.

## Reproduction and evidence

The full command uses the owned local test server (trust authentication; literal
password retained for source-fixture assertions):

```sh
env PATH="$PWD/.venv/bin:$PATH" PSYCOPG_IMPL=python \
  DYLD_LIBRARY_PATH=/opt/homebrew/opt/postgresql@15/lib \
  .venv/bin/pytest \
  --test-dsn 'host=127.0.0.1 port=55436 dbname=phase5 user=martijnberger password=password sslmode=disable' \
  --impl=ferrocopg --randomly-dont-reorganize \
  --junitxml=/tmp/phase5-signal-state-full-compat.xml tests -q
.venv/bin/python tools/ci/ferrocopg_pass_rate.py \
  /tmp/phase5-signal-state-full-compat.xml \
  --manifest tests/ferrocopg_manifest.toml \
  --floor tests/ferrocopg_pass_rate.txt \
  --sync-floor tests/ferrocopg_sync_pass_rate.txt \
  --sync-max-regressions 0 --pytest-status 1 \
  --report /tmp/phase5-signal-state-full-compat-report.json
```

No CI baseline-count comparison is claimed for this local platform/environment.
The targeted commands substitute the four tabled node IDs for `tests`, using
`--impl=libpq` for the first control and `--impl=ferrocopg` for the second.

[Raw JUnit, logs and classified report](2026-09-26-signal-state-full-compat.tar.gz),
SHA-256:
`39de7c12d0a5bad77cf44ae83261e40e4ad258f831565b52d03345c8b3191039`.

The next action remains exact-candidate compatibility/timing diagnosis and final
performance/soak validation. Parent CI cannot establish this successor's pass.
No benchmark was run concurrently on this host. Artifact auditing during the
source run means this was not an idle-host timing experiment.

Report and analysis produced with AI assistance.

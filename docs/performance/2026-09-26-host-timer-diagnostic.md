# Host timer diagnosis, not a compatibility waiver

The four strict local timing failures on `17c77760` motivated driver-free
controls. The standalone scheduler failure already reproduces with both Rust
and pure-Python/libpq; it does not perform database I/O. This investigation asks
whether basic timed waits on the same host can exceed the unchanged 5-10 ms
tolerances without any driver or pool implementation involved.

## Controls

All controls run on macOS 26.7 arm64. Each uses main and worker threads, forward
and reverse method order, 10 ms and 100 ms requests, and five observations per
condition. Every observation is retained. No database benchmarks or builds run
concurrently with the primary controls.

The Python probe imports only standard-library modules and measures `sleep`,
`threading.Event.wait`, and timed lock acquisition. It runs with both the
repository CPython 3.14.6 build and Homebrew CPython 3.14.7. The separately
compiled C probe measures `nanosleep`, relative pthread condition waits, and
absolute pthread condition waits. It contains no Python, Rust, PostgreSQL or
pool code. These three primary processes complete sequentially.

For the sixty 100 ms observations in each primary control:

| Control | Range of condition-median lateness | More than 5 ms late | More than 10 ms late |
| --- | ---: | ---: | ---: |
| Repository CPython 3.14.6 | 3.580-10.091 ms | 46/60 | 20/60 |
| Homebrew CPython 3.14.7 | 5.069-10.066 ms | 48/60 | 25/60 |
| Native C/pthreads | 2.542-10.038 ms | 42/60 | 17/60 |

Thus host timed waits independently produce lateness large enough to fail the
existing pool/scheduler bounds. This rules out attributing that delay solely to
the Rust database client or Python/Rust integration. It does not prove that
every portion of every failed pool observation comes from the same mechanism,
or that connection setup adds no separate overhead.

The scheduler uses `threading.Event.wait` through the pool compatibility layer.
The concurrent-filling test deliberately pads connection creation to 100 ms
using `sleep`; reconnect/check-backoff tests also impose tight elapsed-time
upper bounds. These paths explain why host wait latency is directly relevant.
No pool, scheduler, backend, assertion, tolerance or manifest was changed.

## Exploratory policy controls

The macOS thread QoS reads report class 33 for the main thread and class 21 for
the worker in the first Python process. These are requested classifications,
not proof of effective CPU priority or absence of process constraints.
Apple documents QoS as affecting timer latency as well as scheduling and
throughput in its [Mac energy-efficiency guide](https://developer.apple.com/library/archive/documentation/Performance/Conceptual/power_efficiency_guidelines_osx/PrioritizeWorkAtTheTaskLevel.html).

Additional child processes used `/usr/sbin/taskpolicy -l 0` and `-l 1` to explore
latency policy. Neither establishes a remedy: lateness persists. Their
`task_policy_get` base/override reads return zero values, so no effective policy
change is claimed. The machine and parent process were not modified, and no
policy override is proposed for the backend, CI or normal benchmark commands.
The [Apple command implementation](https://github.com/apple-oss-distributions/system_cmds/blob/main/taskpolicy/taskpolicy.c)
maps these options to the corresponding latency tiers.

The native `-l 0` handle was not confirmed terminal before starting Python
`-l 1`. Those two exploratory controls may overlap and are excluded from the
primary causal evidence above. Their raw observations are still preserved.
The first two Python reports predate the addition of read-only task-policy
metadata to the probe. The final script is included in the archive.

## Disposition

Keep the complete compatibility run failed. This is a diagnosis of a
driver-independent source of timing variation, not approval to loosen the
zero-regression gate or declare the latest runtime accepted. No timing-only
pool fork or speculative Rust change is justified by these results.

Use supported-runner exact-candidate compatibility evidence and continue
isolating any backend-specific remainder. Do not repeatedly rerun the unchanged
full suite to select a lucky pass. Five observations per condition and absent
continuous host-activity recording are insufficient for stable tail estimates
or a general claim about macOS timer precision.

## Reproduction and artifacts

```sh
.venv/bin/python /tmp/phase5-timer-diagnostic.py
/opt/homebrew/bin/python3.14 /tmp/phase5-timer-diagnostic.py
env DEVELOPER_DIR=/Library/Developer/CommandLineTools \
  clang -O2 -Wall -Wextra -Werror -pthread \
  /tmp/phase5-timer-native.c -o /tmp/phase5-timer-native
/tmp/phase5-timer-native
```

Wait for each process to exit before starting the next. The archive contains
both probe sources, compiler identity, all 720 observations across primary and
exploratory controls, and the independent summary script.

[Summary](2026-09-26-host-timer-diagnostic.json) and
[raw archive](2026-09-26-host-timer-diagnostic.tar.gz), archive SHA-256:
`88c3b7a564c4d471920e87f9aef672dd8b6359d41bd75717f8fb80f7b7cfa6eb`.

Report and analysis produced with AI assistance.

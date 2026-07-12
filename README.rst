ferrocopg -- a Rust backend for Psycopg 3
==========================================

This repository is a development fork of `Psycopg 3`_, the trusted and
production-proven PostgreSQL adapter for Python. Its purpose is to build a
Rust-native PostgreSQL backend for that established library, keeping Psycopg's
Python API, adaptation system, row factories, and test suite as the
compatibility contract instead of creating a new adapter API from scratch.

.. _Psycopg 3: https://github.com/psycopg/psycopg

The Rust backend is built on the
`rust-postgres <https://github.com/rust-postgres/rust-postgres>`_ ecosystem.
The planned product is a separate ``ferrocopg`` distribution and import
namespace, with Rust as its synchronous default::

    import ferrocopg as psycopg

    with psycopg.connect(dsn) as conn:
        print(conn.execute("select 1").fetchone())

The source tree is still in transition and currently exposes Rust as an
explicit backend of its vendored ``psycopg`` package. The default will switch
before publication so ordinary development finds Rust compatibility gaps.

This work is not currently an upstream Psycopg feature or release. Whether it
should eventually be proposed upstream is deliberately undecided and does not
block development or release of the fork. The first release target is a
synchronous ``0.1.0`` beta for CPython 3.11-3.14 and PostgreSQL 14-18. See the
`Rust backend plan <plan.md>`_ for the compatibility, performance, packaging,
and release gates.


Trying the Rust backend
-----------------------

The ferrocopg backend is currently built from this source tree; no ferrocopg
wheel has been published yet. Set up the workspace and install the Rust
extension with::

    uv venv
    source .venv/bin/activate
    uv sync --dev --group rust --locked
    uv run maturin develop \
        --manifest-path crates/ferrocopg-python/Cargo.toml

The `ferrocopg development workflow <docs/ferrocopg-dev.md>`_ documents the
crate layout, focused test suites, and full compatibility harness.

Then select the backend per connection. ``psycopg.connect(...,
impl="ferrocopg")`` is the recommended entry point because it keeps the normal
Psycopg call style and transaction default explicit::

    import psycopg
    from psycopg.rows import dict_row

    dsn = "postgresql://postgres:password@localhost/postgres"

    with psycopg.connect(dsn, impl="ferrocopg", row_factory=dict_row) as conn:
        row = conn.execute(
            "select %s::int4 as answer, current_database() as database",
            (42,),
        ).fetchone()
        print(row)

The transitional ``psycopg.connect_ferrocopg()`` helper reaches the same
backend directly. If you use it, pass ``autocommit`` explicitly: its current
bootstrap default differs from ``psycopg.connect()``. The ``PSYCOPG_IMPL``
environment variable is *not* the ferrocopg selector; it selects Psycopg's
libpq wrapper implementation. Choose ``impl="ferrocopg"`` or
``impl="libpq"`` at connection time instead.

The current source-tree backend covers broad synchronous workflows:
Psycopg-style
``%s``/``%t``/``%b`` parameters, typed results, row factories, prepared
statements, transactions and savepoints, two-phase commit, cancellation,
text and binary COPY, named server cursors, LISTEN/NOTIFY, and a pipelined
batch adapter. Text and binary execution are supported by the default
ferrocopg cursor.

Named cursors use backend-native ``DECLARE``/``FETCH``/``MOVE``/``CLOSE``
commands and support scrolling and ``withhold``::

    with psycopg.connect(dsn, impl="ferrocopg") as conn:
        with conn.cursor("events", scrollable=True) as cur:
            cur.execute("select id, payload from events order by id")
            print(cur.fetchmany(100))

Binary COPY reuses Psycopg's formatters and type registry over the Rust byte
pipe::

    with psycopg.connect(dsn, impl="ferrocopg") as conn:
        with conn.cursor().copy(
            "copy events (id, payload) from stdin (format binary)"
        ) as copy:
            copy.set_types(["int4", "text"])
            copy.write_row((1, "started"))

Async applications use the thread-offload facade. Backend calls are serialized
on one connection-affine worker thread and run outside the event-loop thread::

    async with await psycopg.FerrocopgAsyncConnection.connect(
        dsn, row_factory=dict_row
    ) as conn:
        row = await (await conn.execute(
            "select %s::int4 as answer", (42,)
        )).fetchone()
        print(row)

It is not a drop-in replacement yet. The concrete cursor, COPY writer,
pipeline, timeout, and multi-host gaps below are active compatibility work for
the beta rather than accepted long-term boundaries. Keep ``impl="libpq"`` for
applications that currently require:

- Psycopg's concrete ``Cursor``, ``ClientCursor``, or ``RawCursor`` classes;
  these remain libpq-only, while backend-specific custom cursors may subclass
  ``psycopg._ferrocopg.NoTlsCursorAdapter``
- concrete ``LibpqWriter``/``QueuedLibpqWriter`` COPY writers; normal COPY,
  COPY parameters, binary row helpers, and generic custom writers are supported
- exact libpq ``PQpipelineSync``/``PIPELINE_ABORTED`` state-machine semantics;
  ferrocopg pipelines queued simple-query batches but documents this residual
  protocol boundary
- raw libpq ``PGconn``/socket access such as ``fileno()``
- libpq-specific stalled-handshake and aggregated multi-host error behavior

The dedicated CI matrix exercises all six SSL modes against SSL-enabled
PostgreSQL 14-18, including custom roots, required channel binding, and client
certificate authentication. Unsupported features raise an error and
ferrocopg never silently swaps an active connection to libpq.


Installing upstream Psycopg
---------------------------

For the normal stable Psycopg package, the quick installation remains::

    pip install "psycopg[binary,pool]"

This installs upstream Psycopg and does not add the experimental ferrocopg
backend described above.

For further information about installation please check `the documentation`__.

.. __: https://www.psycopg.org/psycopg3/docs/basic/install.html


.. _Hacking:

Hacking
-------

In order to work on the Psycopg source code, you must have the
``libpq`` PostgreSQL client library installed on the system. For instance, on
Debian systems, you can obtain it by running::

    sudo apt install libpq5

On macOS, run::

    brew install libpq

On Windows you can use EnterpriseDB's `installers`__ to obtain ``libpq``
which is included in the Command Line Tools.

.. __: https://www.enterprisedb.com/downloads/postgres-postgresql-downloads

You can then clone this repository to develop Psycopg and ferrocopg together::

    git clone https://github.com/martijnberger/ferrocopg.git
    cd ferrocopg

Please note that the repository contains the source code of several Python
packages, which may have different requirements:

- The ``psycopg`` directory contains the pure python implementation of
  ``psycopg``. The package has only a runtime dependency on the ``libpq``, the
  PostgreSQL client library, which should be installed in your system.

- The ``psycopg_c`` directory contains an optimization module written in
  C/Cython. In order to build it you will need a few development tools: please
  look at `Local installation`__ in the docs for the details.

- The ``psycopg_pool`` directory contains the `connection pools`__
  implementations. This is kept as a separate package to allow a different
  release cycle.

.. __: https://www.psycopg.org/psycopg3/docs/basic/install.html#local-installation
.. __: https://www.psycopg.org/psycopg3/docs/advanced/pool.html

You can create a local virtualenv and install the packages `in development
mode <https://www.psycopg.org/psycopg3/docs/basic/install.html#local-installation>`_,
together with their development and testing
requirements. The workspace requires Python 3.10 or newer::

    uv venv
    source .venv/bin/activate

    # Install the shared test and development environment
    uv sync --dev --locked

    # Include the Cython speedup package when working on the C backend
    uv sync --dev --group c --locked

The root ``uv sync`` command is the recommended way to get a working
development environment for the current implementation. The default locked sync
installs the local ``psycopg`` and ``psycopg_pool`` projects together with the
development and test dependencies used across the suite. Add ``--group c`` to
bring ``psycopg_c`` into the environment when you want to exercise the
Cython-backed implementation.

The active Rust extension lives in ``crates/ferrocopg-python`` and the backend
session implementation lives in ``crates/ferrocopg-postgres``. Install an
editable extension into the active uv-managed environment using::

    uv run maturin develop \
        --manifest-path crates/ferrocopg-python/Cargo.toml

To keep the current Cython backend and the Rust backend installed side by side
in one uv-managed environment, sync both optional groups::

    uv sync --dev --group c --group rust

Now hack away! You can run the tests using a local Docker database::

    tools/test-db start
    export PSYCOPG_TEST_DSN="$(tools/test-db dsn)"
    uv run pytest --test-dsn "$PSYCOPG_TEST_DSN"

If some of the tests fail on your local host, it may be helpful to exclude the
`proxy`, `subprocess`, and/or `timing` tests to get a clean test run, for
example::

     pytest  -m 'not proxy and not timing'

The project includes some `pre-commit`__ hooks to check that the code is valid
according to the project coding convention. Please make sure to install them
by running::

    pre-commit install

This will allow to check lint errors before submitting merge requests, which
will save you time and frustrations.

.. __: https://pre-commit.com/

Please follow the `conventional commits`__ specification for your commit messages.

.. __: https://www.conventionalcommits.org/en/v1.0.0/


Cross-compiling
---------------

To use cross-platform zipapps created with `shiv`__ that include Psycopg
as a dependency you must also have ``libpq`` installed. See
`the section above <Hacking_>`_ for install instructions.

.. __: https://github.com/linkedin/shiv

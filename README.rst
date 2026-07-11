ferrocopg -- a Rust backend for Psycopg 3
==========================================

This repository is a development fork of `Psycopg 3`_, the trusted and
production-proven PostgreSQL adapter for Python. Its purpose is to build a
Rust-native PostgreSQL backend *inside the existing Psycopg library*, keeping
Psycopg's Python API, adaptation system, row factories, and test suite as the
compatibility contract instead of creating a new adapter API from scratch.

.. _Psycopg 3: https://github.com/psycopg/psycopg

The Rust backend is built on the
`rust-postgres <https://github.com/rust-postgres/rust-postgres>`_ ecosystem
and is exposed as an explicit opt-in path. The normal libpq-backed Psycopg path
remains unchanged and continues to be the stable default.

This work is not currently an upstream Psycopg feature or release. Whether the
backend should eventually be proposed upstream, remain a separate fork, or be
packaged another way is deliberately undecided. The immediate job is to make
the backend useful, measure compatibility, and keep the evidence honest. See
the `Rust backend plan <plan.md>`_ for the current milestones and conditional
cutover gates.


Trying the Rust backend
-----------------------

The ferrocopg backend is experimental and currently built from this source
tree; there is no stable ferrocopg wheel release yet. Set up the workspace and
install the Rust extension with::

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

The opt-in backend already covers common synchronous workflows: Psycopg-style
``%s``/``%t``/``%b`` parameters, typed results, row factories, prepared
statements, transactions and savepoints, cancellation, text COPY,
LISTEN/NOTIFY, and a basic pipeline adapter. Text and binary execution are
supported by the default ferrocopg cursor.

It is not a drop-in replacement yet. Keep ``impl="libpq"`` for applications
that require:

- ``AsyncConnection``
- server-side, scrollable, or withhold cursors
- Psycopg's concrete ``Cursor``, ``ClientCursor``, or ``RawCursor`` classes;
  these remain libpq-only, while backend-specific custom cursors may subclass
  ``psycopg._ferrocopg.NoTlsCursorAdapter``
- binary COPY, custom COPY writers, or COPY parameters
- two-phase transactions, notice handlers, or exact libpq pipeline semantics
- raw libpq ``PGconn``/socket access such as ``fileno()``

The dedicated CI matrix exercises all six SSL modes against SSL-enabled
PostgreSQL 14-18, including custom roots, required channel binding, and client
certificate authentication. The backend remains experimental; unsupported
features raise an error and ferrocopg never silently swaps an active
connection to libpq.


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

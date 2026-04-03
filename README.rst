Psycopg 3 -- PostgreSQL database adapter for Python
===================================================

Psycopg 3 is a modern implementation of a PostgreSQL adapter for Python.


Installation
------------

Quick version::

    pip install "psycopg[binary,pool]"

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

You can then clone this repository to develop Psycopg::

    git clone https://github.com/psycopg/psycopg.git
    cd psycopg

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

You can create a local virtualenv and install the packages `in
development mode`__, together with their development and testing
requirements. The workspace requires Python 3.10 or newer::

    uv venv
    source .venv/bin/activate

    # Install the workspace, including the Cython speedup package
    uv sync

The root ``uv sync`` command is the recommended way to get a working
development environment for the current implementation. It installs the local
``psycopg``, ``psycopg_pool``, and ``psycopg_c`` projects together with the
development and test dependencies needed to run the Cython-backed test suite.

The repository is also starting the ``ferrocopg`` Rust port. The initial Rust
extension scaffold lives in ``crates/ferrocopg-python`` and is currently aimed
at the `rust-postgres <https://github.com/rust-postgres/rust-postgres>`__
stack, instead of a direct ``libpq`` wrapper. It can be installed into the
active environment using::

    uv run --with maturin maturin develop \
        --manifest-path crates/ferrocopg-python/Cargo.toml

Now hack away! You can run the tests using a local Docker database::

    tools/test-db start
    export PSYCOPG_TEST_DSN="$(tools/test-db dsn)"
    uv run pytest --test-dsn "$PSYCOPG_TEST_DSN"

The project includes some `pre-commit`__ hooks to check that the code is valid
according to the project coding convention. Please make sure to install them
by running::

    pre-commit install

This will allow to check lint errors before submitting merge requests, which
will save you time and frustrations.

.. __: https://pre-commit.com/


Cross-compiling
---------------

To use cross-platform zipapps created with `shiv`__ that include Psycopg
as a dependency you must also have ``libpq`` installed. See
`the section above <Hacking_>`_ for install instructions.

.. __: https://github.com/linkedin/shiv

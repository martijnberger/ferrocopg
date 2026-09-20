"""Count plaintext PostgreSQL messages through a diagnostic-only loopback proxy."""

from __future__ import annotations

import argparse
import collections
import hashlib
import json
import os
import re
import select
import socket
import struct
import threading
from pathlib import Path

from query_profile import installed_files
from run import BACKENDS, driver_for, metadata
from workloads import BENCHMARKS, workload

WORKLOADS = tuple(name for name in BENCHMARKS if name != "connect_tls") + ("pipeline",)
MAX_MESSAGE = 64 * 1024 * 1024


class Decoder:
    """Handle arbitrary TCP fragmentation without retaining payloads in reports."""

    def __init__(self, frontend):
        self.frontend = frontend
        self.startup = frontend
        self.buffer = bytearray()

    def feed(self, data):
        self.buffer.extend(data)
        events = []
        while len(self.buffer) >= (4 if self.startup else 5):
            offset = 0 if self.startup else 1
            length = struct.unpack_from("!I", self.buffer, offset)[0]
            if length < (8 if self.startup else 4) or length > MAX_MESSAGE:
                raise ValueError("invalid PostgreSQL message length")
            size = length + offset
            if len(self.buffer) < size:
                break
            if self.startup:
                version = struct.unpack_from("!I", self.buffer, 4)[0]
                if version >> 16 != 3:
                    raise ValueError("only plaintext protocol-v3 startup is supported")
                event = {"type": "Startup", "bytes": size}
                self.startup = False
            else:
                kind = chr(self.buffer[0])
                event = {"type": kind, "bytes": size}
                body = bytes(self.buffer[5:size])
                if self.frontend and kind == "B":
                    event.update(bind_metadata(body))
                elif not self.frontend and kind == "Z":
                    if body not in (b"I", b"T", b"E"):
                        raise ValueError("invalid ReadyForQuery transaction state")
                    event["transaction_state"] = body.decode("ascii")
            events.append(event)
            del self.buffer[:size]
        return events

    def finish(self):
        if self.buffer:
            raise ValueError("truncated PostgreSQL message")


def bind_metadata(body):
    """Expose format codes/counts only, never statement names or parameter values."""
    offset = 0
    for _ in range(2):
        end = body.find(b"\0", offset)
        if end < 0:
            raise ValueError("truncated Bind name")
        offset = end + 1

    def integer(size, signed=False):
        nonlocal offset
        if offset + size > len(body):
            raise ValueError("truncated Bind field")
        value = int.from_bytes(body[offset : offset + size], "big", signed=signed)
        offset += size
        return value

    formats = [integer(2) for _ in range(integer(2))]
    parameters = integer(2)
    if len(formats) not in (0, 1, parameters) or any(n not in (0, 1) for n in formats):
        raise ValueError("invalid Bind parameter formats")
    for _ in range(parameters):
        size = integer(4, signed=True)
        if size < -1 or offset + max(size, 0) > len(body):
            raise ValueError("invalid Bind parameter length")
        offset += max(size, 0)
    result_formats = [integer(2) for _ in range(integer(2))]
    if offset != len(body) or any(n not in (0, 1) for n in result_formats):
        raise ValueError("invalid Bind result formats")
    return {
        "parameter_count": parameters,
        "parameter_formats": formats,
        "result_formats": result_formats,
    }


class Proxy:
    def __init__(self, address):
        self.address = address
        self.lock = threading.Lock()
        self.events = []
        self.errors = []
        self.sockets = []
        self.workers = []
        self.stopping = threading.Event()

    def __enter__(self):
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.listener.settimeout(0.1)
        self.port = self.listener.getsockname()[1]
        self.acceptor = threading.Thread(target=self.accept, daemon=True)
        self.acceptor.start()
        return self

    def accept(self):
        while not self.stopping.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                if not self.stopping.is_set():
                    self.errors.append("listener failed")
                return
            connection = len(self.workers)
            worker = threading.Thread(
                target=self.relay, args=(client, connection), daemon=True
            )
            self.workers.append(worker)
            worker.start()

    def relay(self, client, connection):
        server = None
        try:
            client.settimeout(5)
            server = socket.create_connection(self.address, timeout=5)
            with self.lock:
                self.sockets.extend((client, server))
            decoders = {client: Decoder(True), server: Decoder(False)}
            startup_complete = False
            while not self.stopping.is_set():
                readable, _, _ = select.select((client, server), (), (), 0.1)
                for source in readable:
                    data = source.recv(65536)
                    if not data:
                        for decoder in decoders.values():
                            decoder.finish()
                        return
                    events = decoders[source].feed(data)
                    with self.lock:
                        for event in events:
                            if (
                                source is server
                                and event["type"] == "Z"
                                and not startup_complete
                            ):
                                event["startup_complete"] = True
                                startup_complete = True
                            self.events.append(
                                dict(
                                    event,
                                    connection=connection,
                                    direction="frontend"
                                    if source is client
                                    else "backend",
                                )
                            )
                    # Record complete frames before making their response visible to callers.
                    (server if source is client else client).sendall(data)
        except Exception as exc:
            if not self.stopping.is_set():
                with self.lock:
                    self.errors.append(f"{type(exc).__name__}: {exc}")
        finally:
            client.close()
            if server is not None:
                server.close()

    def snapshot(self):
        with self.lock:
            if self.errors:
                raise RuntimeError(f"protocol proxy failed: {self.errors}")
            return list(self.events)

    def __exit__(self, *exc):
        self.stopping.set()
        self.listener.close()
        self.acceptor.join(timeout=2)
        with self.lock:
            for stream in self.sockets:
                try:
                    stream.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
        for worker in self.workers:
            worker.join(timeout=6)
        if self.acceptor.is_alive() or any(
            worker.is_alive() for worker in self.workers
        ):
            raise RuntimeError("protocol proxy threads did not stop")
        self.snapshot()


def summarize(events):
    counts = collections.Counter(f"{e['direction']}:{e['type']}" for e in events)
    return {
        "message_counts": dict(sorted(counts.items())),
        "completed_query_cycles": sum(
            e["direction"] == "backend"
            and e["type"] == "Z"
            and not e.get("startup_complete", False)
            for e in events
        ),
        "frontend_syncs": counts["frontend:S"],
        "frontend_flushes": counts["frontend:H"],
        "connections": sorted({e["connection"] for e in events}),
        "bytes": {
            direction: sum(e["bytes"] for e in events if e["direction"] == direction)
            for direction in ("frontend", "backend")
        },
    }


def probe(args):
    os.environ["PSYCOPG_IMPL"] = "python" if args.backend == "rust" else args.backend
    os.environ["PGGSSENCMODE"] = "disable"
    os.environ.pop("PSYCOPG_SOURCE_IMPL", None)
    driver = driver_for(args.backend)
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    params = conninfo_to_dict(os.environ["PHASE5_DSN"])
    if params.get("host") not in ("127.0.0.1", "localhost") or params.get("hostaddr"):
        raise ValueError("protocol diagnostics require a single explicit loopback host")
    if (
        params.get("sslmode") != "disable"
        or params.get("gssencmode", "disable") != "disable"
    ):
        raise ValueError(
            "explicit plaintext diagnostic DSN required; TLS is never downgraded"
        )
    port = int(params.get("port", 5432))
    with driver.connect(os.environ["PHASE5_DSN"], autocommit=True) as observer:
        environment = metadata(driver, observer, args)
    operations = []
    with Proxy(("127.0.0.1", port)) as proxy:
        dsn = make_conninfo(**dict(params, host="127.0.0.1", port=str(proxy.port)))
        with workload(driver, dsn, args.workload, 1000) as operation:
            for _ in range(10):
                operation.run()
            for index in range(3):
                begin = len(proxy.snapshot())
                operation.run()
                end = len(proxy.snapshot())
                events = proxy.snapshot()[begin:end]
                operations.append(
                    dict(index=index, begin=begin, end=end, **summarize(events))
                )
        events = proxy.snapshot()
    distributions = ["ferrocopg"] if args.backend == "rust" else ["psycopg"]
    if args.backend == "c":
        distributions.append("psycopg-c")
    return {
        "mode": "protocol-diagnostic",
        "acceptance_evidence": False,
        "instrumented": True,
        "backend": args.backend,
        "workload": args.workload,
        "metadata": environment,
        "warmup": 10,
        "iterations": 3,
        "rows": 1000,
        "operations": operations,
        "events": events,
        "installed_file_sha256": {
            name: installed_files(name) for name in distributions
        },
        "probe_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "limitations": [
            "proxy changes scheduling; no latency or throughput samples are collected",
            "ReadyForQuery counts protocol cycles, not TCP packets or general network round trips",
            "pipelines can overlap cycles; inspect ordered message flow before inferring serial dependencies",
            "only explicit plaintext loopback connections; TLS workload deliberately excluded",
            "events contain message types, lengths, Bind formats, and transaction status, never payloads",
        ],
    }


def validate(report, revision, backend, name):
    if (
        report["mode"] != "protocol-diagnostic"
        or report["acceptance_evidence"] is not False
        or report["instrumented"] is not True
        or report["backend"] != backend
        or report["workload"] != name
        or report["metadata"]["revision"] != revision
        or report["warmup"] != 10
        or report["iterations"] != 3
        or report["rows"] != 1000
        or len(report["operations"]) != 3
        or not report["events"]
        or not report["installed_file_sha256"]
        or not re.fullmatch(r"[0-9a-f]{64}", report["probe_sha256"])
    ):
        raise ValueError("invalid protocol diagnostic identity or scope")
    previous = 0
    for index, operation in enumerate(report["operations"]):
        begin, end = operation["begin"], operation["end"]
        if not previous <= begin < end <= len(report["events"]):
            raise ValueError("invalid protocol operation boundaries")
        expected = dict(
            index=index, begin=begin, end=end, **summarize(report["events"][begin:end])
        )
        if operation != expected or not operation["completed_query_cycles"]:
            raise ValueError("protocol operation summary disagrees with raw messages")
        previous = end


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=BACKENDS, required=True)
    parser.add_argument("--workload", choices=WORKLOADS, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.revision) or args.output.exists():
        parser.error("full revision and unused output path required")
    result = probe(args)
    validate(result, args.revision, args.backend, args.workload)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

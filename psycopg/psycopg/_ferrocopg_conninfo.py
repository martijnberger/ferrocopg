"""libpq-free parsing for connection strings consumed by the Rust backend."""

from __future__ import annotations

import re
from urllib.parse import unquote

_KNOWN_OPTIONS = frozenset(
    {
        "application_name",
        "channel_binding",
        "client_encoding",
        "connect_timeout",
        "dbname",
        "fallback_application_name",
        "gssencmode",
        "gsslib",
        "host",
        "hostaddr",
        "keepalives",
        "keepalives_count",
        "keepalives_idle",
        "keepalives_interval",
        "keepalives_retries",
        "krbsrvname",
        "load_balance_hosts",
        "options",
        "passfile",
        "password",
        "port",
        "replication",
        "requirepeer",
        "service",
        "sslcert",
        "sslcompression",
        "sslcrl",
        "sslcrldir",
        "sslkey",
        "ssl_max_protocol_version",
        "ssl_min_protocol_version",
        "sslmode",
        "sslnegotiation",
        "sslpassword",
        "sslrootcert",
        "sslsni",
        "target_session_attrs",
        "tcp_user_timeout",
        "user",
    }
)
_BAD_PERCENT_ESCAPE = re.compile(r"%(?![0-9A-Fa-f]{2})")


def conninfo_to_dict(
    conninfo: str = "", **kwargs: str | int | None
) -> dict[str, str | int]:
    params: dict[str, str | int] = dict(parse_conninfo(conninfo))
    params.update((key, value) for key, value in kwargs.items() if value is not None)
    return params


def parse_conninfo(conninfo: str) -> list[tuple[str, str]]:
    params = (
        _parse_uri_conninfo(conninfo)
        if conninfo.startswith(("postgres://", "postgresql://"))
        else _parse_keyword_conninfo(conninfo)
    )
    if unknown := next(
        (key for key, _value in params if key not in _KNOWN_OPTIONS), None
    ):
        raise ValueError(f'invalid connection option "{unknown}"')
    return params


def _parse_keyword_conninfo(conninfo: str) -> list[tuple[str, str]]:
    params: list[tuple[str, str]] = []
    index = 0
    size = len(conninfo)

    while True:
        while index < size and conninfo[index].isspace():
            index += 1
        if index == size:
            return params

        key_start = index
        while index < size and not conninfo[index].isspace() and conninfo[index] != "=":
            index += 1
        key = conninfo[key_start:index]
        while index < size and conninfo[index].isspace():
            index += 1
        if not key or index == size or conninfo[index] != "=":
            raise ValueError(f'missing "=" after "{key or conninfo[key_start:]}"')
        index += 1
        while index < size and conninfo[index].isspace():
            index += 1

        quoted = index < size and conninfo[index] == "'"
        if quoted:
            index += 1
        chars: list[str] = []
        while index < size:
            char = conninfo[index]
            if quoted and char == "'":
                index += 1
                break
            if not quoted and char.isspace():
                break
            if char == "\\":
                index += 1
                if index == size:
                    raise ValueError("unterminated escape in connection string")
                char = conninfo[index]
            chars.append(char)
            index += 1
        else:
            if quoted:
                raise ValueError("unterminated quoted string in connection string")

        if quoted and index < size and not conninfo[index].isspace():
            raise ValueError("unexpected character after quoted connection value")
        params.append((key, "".join(chars)))


def _parse_uri_conninfo(conninfo: str) -> list[tuple[str, str]]:
    _scheme, rest = conninfo.split("://", 1)
    location, separator, query = rest.partition("?")
    authority, path_separator, path = location.partition("/")
    params: list[tuple[str, str]] = []

    if "@" in authority:
        userinfo, authority = authority.rsplit("@", 1)
        user, password_separator, password = userinfo.partition(":")
        if user:
            params.append(("user", _uri_unquote(user)))
        if password_separator:
            params.append(("password", _uri_unquote(password)))

    hosts: list[str] = []
    ports: list[str] = []
    has_port = False
    for endpoint in authority.split(",") if authority else ():
        host, port = _split_uri_endpoint(endpoint)
        hosts.append(_uri_unquote(host))
        ports.append(port)
        has_port = has_port or bool(port)
    if hosts:
        params.append(("host", ",".join(hosts)))
    if has_port:
        params.append(("port", ",".join(ports)))
    if path_separator and path:
        params.append(("dbname", _uri_unquote(path)))
    if separator:
        for item in query.split("&"):
            key, value_separator, item_value = item.partition("=")
            if not key or not value_separator:
                raise ValueError("invalid query parameter in connection URI")
            params.append((_uri_unquote(key), _uri_unquote(item_value)))
    return params


def _split_uri_endpoint(endpoint: str) -> tuple[str, str]:
    if endpoint.startswith("["):
        end = endpoint.find("]")
        if end < 0:
            raise ValueError("unterminated IPv6 address in connection URI")
        suffix = endpoint[end + 1 :]
        if suffix and not suffix.startswith(":"):
            raise ValueError("invalid host in connection URI")
        return endpoint[1:end], suffix[1:]

    if endpoint.count(":") > 1:
        raise ValueError("IPv6 addresses in connection URIs must use brackets")
    host, separator, port = endpoint.rpartition(":")
    if separator and port and not port.isdigit():
        raise ValueError("invalid port in connection URI")
    return (host, port) if separator else (endpoint, "")


def _uri_unquote(value: str) -> str:
    if _BAD_PERCENT_ESCAPE.search(value):
        raise ValueError("invalid percent escape in connection URI")
    try:
        return unquote(value, errors="strict")
    except UnicodeDecodeError as ex:
        raise ValueError("connection URI is not valid UTF-8") from ex

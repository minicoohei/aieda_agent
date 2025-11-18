"""Utilities for allocating TCP ports for marimo sessions and agents."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from typing import Iterator, Mapping


class PortValidationError(ValueError):
    """Raised when an invalid port or range is supplied."""


def _validate_port(port: int) -> None:
    if not (1 <= port <= 65535):
        raise PortValidationError(f"Port must be between 1 and 65535. Got: {port}")


@dataclass(frozen=True)
class PortRange:
    """Inclusive range of TCP ports."""

    start: int
    end: int

    def __post_init__(self) -> None:
        _validate_port(self.start)
        _validate_port(self.end)
        if self.start > self.end:
            raise PortValidationError("PortRange start must be less than or equal to end.")

    def __iter__(self) -> Iterator[int]:
        for port in range(self.start, self.end + 1):
            yield port

    def contains(self, port: int) -> bool:
        return self.start <= port <= self.end

    def __str__(self) -> str:
        return f"{self.start}-{self.end}"


DEFAULT_PORT_RANGE = PortRange(41000, 41999)


def _parse_port_value(value: str) -> int:
    try:
        port = int(value.strip())
    except (TypeError, ValueError) as exc:  # pragma: no cover - TypeError blocked at call site
        raise PortValidationError(f"Invalid port value: {value}") from exc
    _validate_port(port)
    return port


def _parse_range(value: str | None, *, default: PortRange) -> PortRange:
    if not value:
        return default
    if "-" not in value:
        port = _parse_port_value(value)
        return PortRange(port, port)
    start_raw, end_raw = value.split("-", maxsplit=1)
    start = _parse_port_value(start_raw)
    end = _parse_port_value(end_raw)
    return PortRange(start, end)


@dataclass
class PortAllocator:
    """Determines which TCP port should be used for marimo notebooks."""

    port_range: PortRange = DEFAULT_PORT_RANGE
    fixed_port: int | None = None
    host: str = "127.0.0.1"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None, *, default_range: PortRange = DEFAULT_PORT_RANGE) -> "PortAllocator":
        env = env or os.environ
        port_range = _parse_range(env.get("MARIMO_PORT_RANGE"), default=default_range)

        fixed_port_value = env.get("MARIMO_PORT") or env.get("MARIMO_PORT_FIXED")
        fixed_port = _parse_port_value(fixed_port_value) if fixed_port_value else None

        return cls(port_range=port_range, fixed_port=fixed_port)

    def allocate(self) -> int:
        if self.fixed_port is not None:
            if not self._is_available(self.fixed_port):
                raise RuntimeError(f"Requested fixed port {self.fixed_port} is already in use.")
            return self.fixed_port

        for port in self.port_range:
            if self._is_available(port):
                return port

        raise RuntimeError(f"No free port available in range {self.port_range}.")

    def _is_available(self, port: int) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind((self.host, port))
            except OSError:
                return False
        return True


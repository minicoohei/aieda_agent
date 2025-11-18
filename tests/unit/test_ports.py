"""Tests for the PortAllocator utility."""

from __future__ import annotations

import contextlib
import socket

import pytest

from ai_data_lab.ports import PortAllocator, PortRange


@pytest.fixture(autouse=True)
def _clear_port_env(monkeypatch):
    monkeypatch.delenv("MARIMO_PORT", raising=False)
    monkeypatch.delenv("MARIMO_PORT_FIXED", raising=False)
    monkeypatch.delenv("MARIMO_PORT_RANGE", raising=False)


@contextlib.contextmanager
def _occupy_port(port: int):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", port))
        sock.listen(1)
        yield
    finally:
        sock.close()


def test_port_range_validates_bounds():
    with pytest.raises(ValueError):
        PortRange(70000, 70010)

    with pytest.raises(ValueError):
        PortRange(1024, 1023)


def test_allocator_from_env_parses_range(monkeypatch):
    monkeypatch.setenv("MARIMO_PORT_RANGE", "45000-45010")

    allocator = PortAllocator.from_env()

    assert allocator.port_range.start == 45000
    assert allocator.port_range.end == 45010


def test_allocator_from_env_accepts_fixed_port(monkeypatch):
    monkeypatch.setenv("MARIMO_PORT", "4173")

    allocator = PortAllocator.from_env()

    assert allocator.fixed_port == 4173


def test_allocate_prefers_fixed_port(monkeypatch):
    monkeypatch.setenv("MARIMO_PORT", "45200")

    allocator = PortAllocator.from_env()
    port = allocator.allocate()

    assert port == 45200


def test_allocate_scans_for_free_port(monkeypatch):
    monkeypatch.setenv("MARIMO_PORT_RANGE", "45300-45302")
    allocator = PortAllocator.from_env()

    with _occupy_port(45300):
        port = allocator.allocate()

    assert port in {45301, 45302}


def test_allocate_raises_error_when_no_port_available(monkeypatch):
    monkeypatch.setenv("MARIMO_PORT_RANGE", "45400-45401")
    allocator = PortAllocator.from_env()

    with _occupy_port(45400), _occupy_port(45401):
        with pytest.raises(RuntimeError):
            allocator.allocate()


"""Tests for the SessionRegistry helper."""

from __future__ import annotations

from pathlib import Path

import pytest

from ai_data_lab.eda.session_registry import SessionInfo, SessionRegistry


def test_register_and_list_sessions(tmp_path: Path):
    registry = SessionRegistry(project_root=tmp_path)
    info = SessionInfo(notebook="demo.py", port=4173, pid=1234, url="http://localhost:4173")

    registry.register_session(info)
    sessions = registry.list_sessions()

    assert len(sessions) == 1
    assert sessions[0].notebook == "demo.py"
    assert sessions[0].port == 4173
    assert sessions[0].url == "http://localhost:4173"


def test_register_session_overwrites_same_pid(tmp_path: Path):
    registry = SessionRegistry(project_root=tmp_path)
    first = SessionInfo(notebook="a.py", port=4173, pid=1111)
    second = SessionInfo(notebook="b.py", port=4174, pid=1111)

    registry.register_session(first)
    registry.register_session(second)

    sessions = registry.list_sessions()
    assert len(sessions) == 1
    assert sessions[0].notebook == "b.py"
    assert sessions[0].port == 4174


def test_cleanup_dead_sessions(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    registry = SessionRegistry(project_root=tmp_path)
    alive = SessionInfo(notebook="alive.py", port=4173, pid=1111)
    dead = SessionInfo(notebook="dead.py", port=4174, pid=2222)
    registry.register_session(alive)
    registry.register_session(dead)

    def fake_is_alive(self, pid: int) -> bool:  # noqa: ANN001
        return pid == alive.pid

    monkeypatch.setattr(SessionRegistry, "_is_alive", fake_is_alive)

    removed = registry.cleanup_dead_sessions()
    sessions = registry.list_sessions()

    assert removed == 1
    assert len(sessions) == 1
    assert sessions[0].notebook == "alive.py"


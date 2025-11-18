"""Tests for MarimoLauncher."""

from __future__ import annotations

from pathlib import Path
from unittest import mock

import pytest

from ai_data_lab.eda.marimo_launcher import LaunchResult, MarimoLauncher
from ai_data_lab.eda.session_registry import SessionRegistry
from ai_data_lab.ports import PortAllocator, PortRange


class _FakeAllocator(PortAllocator):
    def __init__(self, port: int):
        super().__init__(port_range=PortRange(port, port))
        self._port = port

    def allocate(self) -> int:  # noqa: D401
        return self._port


def test_launch_invokes_marimo_and_registers_session(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    notebook_dir = tmp_path / "notebooks"
    notebook_dir.mkdir()
    notebook_path = notebook_dir / "demo.py"
    notebook_path.write_text("print('hello')\n")

    fake_process = mock.Mock()
    fake_process.pid = 9999

    popen_spy = mock.Mock(return_value=fake_process)
    monkeypatch.setattr("ai_data_lab.eda.marimo_launcher.subprocess.Popen", popen_spy)

    registry = SessionRegistry(project_root=tmp_path)
    allocator = _FakeAllocator(48000)
    launcher = MarimoLauncher(project_root=tmp_path, registry=registry, port_allocator=allocator, uv_executable="uv")

    result = launcher.launch("notebooks/demo.py")

    assert isinstance(result, LaunchResult)
    assert result.port == 48000
    assert result.pid == 9999
    assert result.url == "http://localhost:48000"
    popen_spy.assert_called_once()
    called_args = popen_spy.call_args[0][0]
    assert called_args[:4] == ["uv", "run", "marimo", "run"]
    assert str(notebook_path) in called_args
    assert "--port" in called_args

    sessions = registry.list_sessions()
    assert len(sessions) == 1
    assert sessions[0].port == 48000
    assert sessions[0].pid == 9999


def test_launch_raises_when_notebook_missing(tmp_path: Path):
    launcher = MarimoLauncher(project_root=tmp_path, port_allocator=_FakeAllocator(48000))

    with pytest.raises(FileNotFoundError):
        launcher.launch("notebooks/missing.py")


def test_launch_respects_extra_args(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    notebook_dir = tmp_path / "notebooks"
    notebook_dir.mkdir()
    notebook = notebook_dir / "nb.py"
    notebook.write_text("print('x')\n")

    fake_process = mock.Mock(pid=1000)
    popen_spy = mock.Mock(return_value=fake_process)
    monkeypatch.setattr("ai_data_lab.eda.marimo_launcher.subprocess.Popen", popen_spy)

    launcher = MarimoLauncher(project_root=tmp_path, port_allocator=_FakeAllocator(43000))
    launcher.launch("notebooks/nb.py", extra_args=["--theme", "dark"])

    args = popen_spy.call_args[0][0]
    assert args[-2:] == ["--theme", "dark"]


"""Lightweight registry for running marimo sessions."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


@dataclass(slots=True)
class SessionInfo:
    """Metadata about a running marimo session."""

    notebook: str
    port: int
    pid: int
    host: str = "127.0.0.1"
    url: str | None = None
    started_at: str = field(default_factory=_now_iso)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SessionInfo":
        return cls(**data)


class SessionRegistry:
    """Persists session metadata to `.marimo/sessions.json`."""

    def __init__(self, *, project_root: Path | str | None = None, filename: str = "sessions.json") -> None:
        base = Path(project_root) if project_root else Path.cwd()
        self._path = (base / ".marimo").resolve()
        self._path.mkdir(parents=True, exist_ok=True)
        self._file = self._path / filename

    @property
    def registry_path(self) -> Path:
        return self._file

    def register_session(self, info: SessionInfo) -> None:
        entries = [entry for entry in self._read_entries() if entry.get("pid") != info.pid]
        entries.append(info.to_dict())
        self._write_entries(entries)

    def list_sessions(self) -> list[SessionInfo]:
        return [SessionInfo.from_dict(entry) for entry in self._read_entries()]

    def cleanup_dead_sessions(self) -> int:
        entries = self._read_entries()
        alive_entries: list[dict[str, Any]] = []
        removed = 0
        for entry in entries:
            pid = entry.get("pid")
            if pid is None:
                removed += 1
                continue
            if not self._is_alive(int(pid)):
                removed += 1
                continue
            alive_entries.append(entry)

        if removed:
            self._write_entries(alive_entries)
        return removed

    def _read_entries(self) -> list[dict[str, Any]]:
        if not self._file.exists():
            return []
        try:
            data = json.loads(self._file.read_text())
        except json.JSONDecodeError:
            return []
        if not isinstance(data, dict):
            return []
        sessions = data.get("sessions", [])
        if not isinstance(sessions, list):
            return []
        return [entry for entry in sessions if isinstance(entry, dict)]

    def _write_entries(self, entries: list[dict[str, Any]]) -> None:
        payload = {"sessions": entries}
        self._file.write_text(json.dumps(payload, indent=2))

    def _is_alive(self, pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True


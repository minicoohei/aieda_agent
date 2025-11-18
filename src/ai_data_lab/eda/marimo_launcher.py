"""Launch marimo notebooks with automatic port allocation."""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

from ai_data_lab.ports import PortAllocator

from .session_registry import SessionInfo, SessionRegistry


@dataclass(slots=True)
class LaunchResult:
    """Information about a launched marimo process."""

    notebook: str
    port: int
    pid: int
    url: str
    command: list[str]


class MarimoLauncher:
    """High-level helper that spawns marimo notebooks via uv."""

    def __init__(
        self,
        *,
        project_root: Path | str | None = None,
        uv_executable: str = "uv",
        registry: SessionRegistry | None = None,
        port_allocator: PortAllocator | None = None,
        base_env: Mapping[str, str] | None = None,
    ) -> None:
        self._project_root = Path(project_root) if project_root else Path.cwd()
        self._uv_executable = uv_executable
        self._registry = registry or SessionRegistry(project_root=self._project_root)
        self._port_allocator = port_allocator
        self._base_env = dict(base_env) if base_env is not None else os.environ.copy()

    def launch(
        self,
        notebook: str | Path,
        *,
        host: str = "0.0.0.0",
        extra_args: Sequence[str] | None = None,
        env: Mapping[str, str] | None = None,
    ) -> LaunchResult:
        notebook_path = self._resolve_notebook(notebook)
        port = self._allocate_port()

        cmd = [
            self._uv_executable,
            "run",
            "marimo",
            "run",
            str(notebook_path),
            "--host",
            host,
            "--port",
            str(port),
        ]
        if extra_args:
            cmd.extend(extra_args)

        proc_env = self._base_env.copy()
        if env:
            proc_env.update(env)

        process = subprocess.Popen(cmd, cwd=str(self._project_root), env=proc_env)  # noqa: S603

        display_host = "localhost" if host in {"0.0.0.0", "127.0.0.1"} else host
        url = f"http://{display_host}:{port}"
        notebook_label = self._relative_notebook_label(notebook_path)

        self._registry.cleanup_dead_sessions()
        session = SessionInfo(notebook=notebook_label, port=port, pid=process.pid, host=display_host, url=url)
        self._registry.register_session(session)

        return LaunchResult(
            notebook=notebook_label,
            port=port,
            pid=process.pid,
            url=url,
            command=cmd,
        )

    def _resolve_notebook(self, notebook: str | Path) -> Path:
        path = Path(notebook)
        if not path.is_absolute():
            candidate = self._project_root / path
        else:
            candidate = path
        if not candidate.exists():
            raise FileNotFoundError(f"Notebook not found: {notebook}")
        return candidate

    def _relative_notebook_label(self, notebook_path: Path) -> str:
        try:
            return str(notebook_path.relative_to(self._project_root))
        except ValueError:
            return str(notebook_path)

    def _allocate_port(self) -> int:
        allocator = self._port_allocator or PortAllocator.from_env()
        return allocator.allocate()


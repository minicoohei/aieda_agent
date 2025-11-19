"""並列分析コーディネーター - 複数のMarimo notebookを並列実行"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ai_data_lab.eda.marimo_launcher import LaunchResult, MarimoLauncher
from ai_data_lab.eda.session_registry import SessionRegistry
from ai_data_lab.ports import PortAllocator

logger = logging.getLogger(__name__)


@dataclass
class AnalysisAgent:
    """分析エージェントの定義"""

    name: str
    notebook: str
    description: str
    depends_on: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)


@dataclass
class AgentExecution:
    """エージェント実行結果"""

    agent: AnalysisAgent
    launch_result: LaunchResult
    status: str = "running"
    outputs: list[Path] = field(default_factory=list)


class ParallelCoordinator:
    """複数の分析を並列実行・管理するコーディネーター"""

    def __init__(
        self,
        project_root: Path | str | None = None,
        reports_dir: Path | str | None = None,
    ) -> None:
        self.project_root = Path(project_root) if project_root else Path.cwd()
        self.reports_dir = Path(reports_dir) if reports_dir else self.project_root / "reports"
        self.reports_dir.mkdir(parents=True, exist_ok=True)

        self.launcher = MarimoLauncher(project_root=self.project_root)
        self.registry = SessionRegistry(project_root=self.project_root)
        self.port_allocator = PortAllocator.from_env()

        self.agents: dict[str, AnalysisAgent] = {}
        self.executions: dict[str, AgentExecution] = {}

    def register_agent(self, agent: AnalysisAgent) -> None:
        """エージェントを登録"""
        self.agents[agent.name] = agent
        logger.info(f"Registered agent: {agent.name} - {agent.description}")

    def launch_agent(self, agent_name: str) -> AgentExecution:
        """エージェントを起動"""
        agent = self.agents[agent_name]

        # 依存関係チェック
        for dep in agent.depends_on:
            if dep not in self.executions or self.executions[dep].status != "completed":
                raise RuntimeError(f"Agent {agent_name} depends on {dep}, which is not completed")

        # ポート割り当て
        port = self.port_allocator.allocate()
        logger.info(f"Allocated port {port} for agent {agent_name}")

        # 環境変数設定
        env_vars = {
            "AGENT_NAME": agent_name,
            "REPORTS_DIR": str(self.reports_dir),
            "AGENT_PORT": str(port),
            **agent.env,
        }

        # Notebook起動
        launch_result = self.launcher.launch(
            notebook=agent.notebook,
            host="0.0.0.0",
            env=env_vars,
        )

        execution = AgentExecution(
            agent=agent,
            launch_result=launch_result,
            status="running",
        )
        self.executions[agent_name] = execution

        logger.info(f"Launched {agent_name} at {launch_result.url}")
        return execution

    def launch_parallel(self, agent_names: list[str]) -> list[AgentExecution]:
        """複数のエージェントを並列起動"""
        executions = []
        for name in agent_names:
            try:
                execution = self.launch_agent(name)
                executions.append(execution)
            except Exception as e:
                logger.error(f"Failed to launch {name}: {e}")
        return executions

    def get_status_summary(self) -> dict[str, Any]:
        """実行状態のサマリーを取得"""
        return {
            "total_agents": len(self.agents),
            "running": sum(1 for e in self.executions.values() if e.status == "running"),
            "completed": sum(1 for e in self.executions.values() if e.status == "completed"),
            "failed": sum(1 for e in self.executions.values() if e.status == "failed"),
            "sessions": [
                {
                    "agent": name,
                    "url": ex.launch_result.url,
                    "port": ex.launch_result.port,
                    "status": ex.status,
                }
                for name, ex in self.executions.items()
            ],
        }


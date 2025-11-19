"""並列コーディネーターのユニットテスト"""

import pytest
from pathlib import Path
from ai_data_lab.eda.parallel_coordinator import (
    AnalysisAgent,
    ParallelCoordinator,
    AgentExecution,
)


def test_analysis_agent_creation():
    """AnalysisAgentの作成テスト"""
    agent = AnalysisAgent(
        name="test_agent",
        notebook="notebooks/test.py",
        description="テスト用エージェント",
    )
    assert agent.name == "test_agent"
    assert agent.notebook == "notebooks/test.py"
    assert agent.description == "テスト用エージェント"
    assert agent.depends_on == []
    assert agent.env == {}


def test_analysis_agent_with_dependencies():
    """依存関係を持つAnalysisAgentのテスト"""
    agent = AnalysisAgent(
        name="dependent_agent",
        notebook="notebooks/dependent.py",
        description="依存エージェント",
        depends_on=["phase1", "phase2"],
        env={"KEY": "value"},
    )
    assert agent.depends_on == ["phase1", "phase2"]
    assert agent.env == {"KEY": "value"}


def test_parallel_coordinator_initialization(tmp_path):
    """ParallelCoordinatorの初期化テスト"""
    coordinator = ParallelCoordinator(
        project_root=tmp_path,
        reports_dir=tmp_path / "reports",
    )
    
    assert coordinator.project_root == tmp_path
    assert coordinator.reports_dir == tmp_path / "reports"
    assert coordinator.reports_dir.exists()
    assert len(coordinator.agents) == 0
    assert len(coordinator.executions) == 0


def test_register_agent(tmp_path):
    """エージェント登録のテスト"""
    coordinator = ParallelCoordinator(project_root=tmp_path)
    
    agent = AnalysisAgent(
        name="test_agent",
        notebook="notebooks/test.py",
        description="テストエージェント",
    )
    
    coordinator.register_agent(agent)
    
    assert "test_agent" in coordinator.agents
    assert coordinator.agents["test_agent"] == agent


def test_get_status_summary_empty(tmp_path):
    """空の状態でのステータスサマリー取得テスト"""
    coordinator = ParallelCoordinator(project_root=tmp_path)
    
    status = coordinator.get_status_summary()
    
    assert status["total_agents"] == 0
    assert status["running"] == 0
    assert status["completed"] == 0
    assert status["failed"] == 0
    assert status["sessions"] == []


def test_get_status_summary_with_agents(tmp_path):
    """エージェント登録後のステータスサマリーテスト"""
    coordinator = ParallelCoordinator(project_root=tmp_path)
    
    for i in range(3):
        agent = AnalysisAgent(
            name=f"agent_{i}",
            notebook=f"notebooks/agent_{i}.py",
            description=f"エージェント{i}",
        )
        coordinator.register_agent(agent)
    
    status = coordinator.get_status_summary()
    assert status["total_agents"] == 3


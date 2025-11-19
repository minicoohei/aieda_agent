"""包括的分析マスターコーディネーター"""

import marimo

__generated_with = "0.10.14"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    import sys
    from pathlib import Path

    # プロジェクトルートをパスに追加
    project_root = Path.cwd().parent if Path.cwd().name == "notebooks" else Path.cwd()
    if str(project_root / "src") not in sys.path:
        sys.path.insert(0, str(project_root / "src"))

    mo.md(
        """
        # 🎯 アイドル・グループ・ファンダム包括的比較分析
        
        ## マスターコーディネーター
        
        このNotebookは、複数の分析エージェントを並列実行し、
        包括的な比較分析レポートを生成します。
        """
    )
    return mo, project_root, sys, Path


@app.cell
def __(project_root):
    from ai_data_lab.eda.parallel_coordinator import (
        AnalysisAgent,
        ParallelCoordinator,
    )

    # コーディネーター初期化
    coordinator = ParallelCoordinator(
        project_root=project_root,
        reports_dir=project_root / "reports" / "comprehensive_analysis",
    )
    coordinator
    return AnalysisAgent, ParallelCoordinator, coordinator


@app.cell
def __(AnalysisAgent, coordinator, mo):
    # Phase 1: データ収集エージェント
    phase1_agent = AnalysisAgent(
        name="phase1_data_collection",
        notebook="notebooks/phase1_data_collection.py",
        description="BigQueryからのデータ収集と前処理",
    )

    # Phase 2: 基礎統計エージェント
    phase2_agent = AnalysisAgent(
        name="phase2_basic_stats",
        notebook="notebooks/phase2_basic_statistics.py",
        description="時系列分析とユーザーセグメント分類",
        depends_on=["phase1_data_collection"],
    )

    # Phase 3: テキストマイニングエージェント
    phase3_agent = AnalysisAgent(
        name="phase3_text_mining",
        notebook="notebooks/phase3_text_mining.py",
        description="ワードクラウド、トピックモデリング、感情分析",
        depends_on=["phase1_data_collection"],
    )

    # Phase 4: 比較分析エージェント
    phase4_agent = AnalysisAgent(
        name="phase4_comparison",
        notebook="notebooks/phase4_comparison_analysis.py",
        description="グループ間・ファンダム間比較分析",
        depends_on=["phase2_basic_stats", "phase3_text_mining"],
    )

    # Phase 5: 可視化・レポートエージェント
    phase5_agent = AnalysisAgent(
        name="phase5_visualization",
        notebook="notebooks/phase5_visualization_report.py",
        description="最終レポート作成と高品質画像生成",
        depends_on=["phase4_comparison"],
    )

    agents = [phase1_agent, phase2_agent, phase3_agent, phase4_agent, phase5_agent]

    for agent in agents:
        coordinator.register_agent(agent)

    mo.md(
        f"""
        ## 📋 登録エージェント
        
        合計 **{len(agents)}** 個のエージェントを登録しました:
        
        {mo.md("\\n".join([f"- **{a.name}**: {a.description}" for a in agents]))}
        """
    )
    return (
        agent,
        agents,
        phase1_agent,
        phase2_agent,
        phase3_agent,
        phase4_agent,
        phase5_agent,
    )


@app.cell
def __(coordinator, mo):
    # Phase 1を起動（最初のエージェント）
    launch_phase1_button = mo.ui.button(
        label="🚀 Phase 1 起動: データ収集",
        on_click=lambda _: coordinator.launch_agent("phase1_data_collection"),
    )
    launch_phase1_button
    return (launch_phase1_button,)


@app.cell
def __(coordinator, mo):
    # Phase 2, 3を並列起動
    launch_parallel_button = mo.ui.button(
        label="⚡ Phase 2 & 3 並列起動",
        on_click=lambda _: coordinator.launch_parallel(
            ["phase2_basic_stats", "phase3_text_mining"]
        ),
    )
    launch_parallel_button
    return (launch_parallel_button,)


@app.cell
def __(coordinator, mo):
    # ステータス表示
    import json

    status = coordinator.get_status_summary()

    mo.md(
        f"""
        ## 📊 実行ステータス
        
        - **総エージェント数**: {status['total_agents']}
        - **実行中**: {status['running']}
        - **完了**: {status['completed']}
        - **失敗**: {status['failed']}
        
        ### セッション詳細
        
        ```json
        {json.dumps(status['sessions'], indent=2, ensure_ascii=False)}
        ```
        """
    )
    return json, status


@app.cell
def __(coordinator, mo):
    # 現在のセッション一覧
    sessions = coordinator.registry.list_sessions()

    if sessions:
        session_table = mo.ui.table(
            data=[
                {
                    "Notebook": s.notebook,
                    "Port": s.port,
                    "PID": s.pid,
                    "URL": s.url,
                    "Started": s.started_at,
                }
                for s in sessions
            ]
        )
        mo.md(f"### 🔗 実行中セッション\n\n{session_table}")
    else:
        mo.md("*現在実行中のセッションはありません*")
    return session_table, sessions


if __name__ == "__main__":
    app.run()


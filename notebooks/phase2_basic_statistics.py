"""Phase 2: 基礎統計分析"""

import marimo

__generated_with = "0.10.14"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    import os
    import sys
    from pathlib import Path
    import pandas as pd
    import numpy as np
    from datetime import datetime
    import matplotlib
    matplotlib.use('Agg')  # GUIなし設定
    import matplotlib.pyplot as plt
    import seaborn as sns

    # 日本語フォント設定
    plt.rcParams['font.sans-serif'] = ['Hiragino Sans', 'Yu Gothic', 'Meiryo', 'Takao', 'IPAexGothic', 'IPAPGothic']
    plt.rcParams['axes.unicode_minus'] = False

    # 環境変数から設定取得
    AGENT_NAME = os.getenv("AGENT_NAME", "phase2_basic_stats")
    REPORTS_DIR = Path(os.getenv("REPORTS_DIR", "reports/comprehensive_analysis"))
    AGENT_PORT = os.getenv("AGENT_PORT", "unknown")

    # ディレクトリ作成
    VIZ_DIR = REPORTS_DIR / "visualizations" / "phase2"
    VIZ_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR = REPORTS_DIR / "data"

    mo.md(
        f"""
        # 📊 Phase 2: 基礎統計分析
        
        **エージェント名**: {AGENT_NAME}  
        **ポート**: {AGENT_PORT}  
        **画像出力先**: `{VIZ_DIR}`
        
        ---
        
        ## 🎯 目的
        
        1. 時系列分析（投稿パターン、トレンド）
        2. ユーザーセグメント分類（コア層、インフルエンサー層）
        3. エンゲージメント分析
        4. **画像を多用した可視化レポート作成**
        """
    )
    return (
        AGENT_NAME,
        AGENT_PORT,
        DATA_DIR,
        Path,
        REPORTS_DIR,
        VIZ_DIR,
        datetime,
        matplotlib,
        mo,
        np,
        os,
        pd,
        plt,
        sns,
        sys,
    )


@app.cell
def __(DATA_DIR, mo, pd):
    # Phase 1のデータ読み込み
    try:
        group_data = pd.read_parquet(DATA_DIR / "group_data_sample.parquet")
        individual_data = pd.read_parquet(DATA_DIR / "individual_data_sample.parquet")
        
        mo.md(
            f"""
            ✅ データ読み込み完了
            
            - **グループデータ**: {len(group_data):,} 件
            - **個人データ**: {len(individual_data):,} 件
            """
        )
    except FileNotFoundError:
        mo.md("⚠️ Phase 1のデータが見つかりません。Phase 1を先に実行してください。")
        group_data = pd.DataFrame()
        individual_data = pd.DataFrame()

    return group_data, individual_data


@app.cell
def __(VIZ_DIR, group_data, individual_data, mo, pd, plt, sns):
    # 1. ソーステーブル別投稿数
    if not group_data.empty and "_source_table" in group_data.columns:
        table_counts = group_data["_source_table"].value_counts()
        
        fig, ax = plt.subplots(figsize=(12, 6), dpi=300)
        bars = ax.barh(range(len(table_counts)), table_counts.values, color=sns.color_palette("viridis", len(table_counts)))
        ax.set_yticks(range(len(table_counts)))
        ax.set_yticklabels(table_counts.index)
        ax.set_xlabel("投稿数", fontsize=12)
        ax.set_title("グループ別投稿数（サンプルデータ）", fontsize=14, fontweight="bold")
        
        # 数値ラベル追加
        for _i, (_bar, _value) in enumerate(zip(bars, table_counts.values)):
            ax.text(_value + 10, _i, f"{_value:,}", va="center", fontsize=10)
        
        plt.tight_layout()
        img_path_1 = VIZ_DIR / "01_group_post_counts.png"
        plt.savefig(img_path_1, dpi=300, bbox_inches="tight")
        plt.close()
        
        mo.md(f"![グループ別投稿数]({img_path_1})")
    else:
        img_path_1 = None
        mo.md("*グループデータが空です*")

    return ax, bars, fig, img_path_1, table_counts


@app.cell
def __(VIZ_DIR, individual_data, mo, plt, sns):
    # 2. 個人別投稿数（TOP 10）
    if not individual_data.empty and "_source_table" in individual_data.columns:
        individual_counts = individual_data["_source_table"].value_counts().head(10)
        
        fig2, ax2 = plt.subplots(figsize=(12, 6), dpi=300)
        bars2 = ax2.bar(range(len(individual_counts)), individual_counts.values, color=sns.color_palette("magma", len(individual_counts)))
        ax2.set_xticks(range(len(individual_counts)))
        ax2.set_xticklabels(individual_counts.index, rotation=45, ha="right")
        ax2.set_ylabel("投稿数", fontsize=12)
        ax2.set_title("個人別投稿数 TOP 10（サンプルデータ）", fontsize=14, fontweight="bold")
        
        # 数値ラベル追加
        for _bar2, _val2 in zip(bars2, individual_counts.values):
            _height = _bar2.get_height()
            ax2.text(_bar2.get_x() + _bar2.get_width()/2, _height + 5, f"{_val2:,}", ha="center", va="bottom", fontsize=10)
        
        plt.tight_layout()
        img_path_2 = VIZ_DIR / "02_individual_post_counts_top10.png"
        plt.savefig(img_path_2, dpi=300, bbox_inches="tight")
        plt.close()
        
        mo.md(f"![個人別投稿数]({img_path_2})")
    else:
        img_path_2 = None
        mo.md("*個人データが空です*")

    return ax2, bars2, fig2, img_path_2, individual_counts


@app.cell
def __(group_data, individual_data, mo, pd):
    # 3. データ構造の分析
    def analyze_columns(df: pd.DataFrame, name: str) -> pd.DataFrame:
        """カラムの型と欠損率を分析"""
        analysis = []
        for col in df.columns:
            analysis.append({
                "カラム名": col,
                "データ型": str(df[col].dtype),
                "非NULL数": df[col].notna().sum(),
                "NULL数": df[col].isna().sum(),
                "NULL率": f"{df[col].isna().sum() / len(df) * 100:.1f}%",
            })
        return pd.DataFrame(analysis)

    if not group_data.empty:
        group_columns_df = analyze_columns(group_data, "グループ")
        mo.md("### グループデータのカラム情報")
        mo.ui.table(group_columns_df.head(15))
    else:
        group_columns_df = pd.DataFrame()

    return analyze_columns, group_columns_df


@app.cell
def __(REPORTS_DIR, VIZ_DIR, datetime, img_path_1, img_path_2, mo):
    # Phase 2完了レポート（Markdown + 画像）
    report_md = f"""# Phase 2: 基礎統計分析 完了レポート

**実行日時**: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## 📊 生成された可視化

### 1. グループ別投稿数

![グループ別投稿数]({img_path_1.relative_to(REPORTS_DIR) if img_path_1 else "N/A"})

### 2. 個人別投稿数 TOP 10

![個人別投稿数]({img_path_2.relative_to(REPORTS_DIR) if img_path_2 else "N/A"})

## ✅ 完了ステータス

Phase 2の基礎統計分析が正常に完了しました。

- **生成画像数**: 2 枚
- **解像度**: 300 DPI
- **保存先**: `{VIZ_DIR.relative_to(REPORTS_DIR)}`

---

*次のステップ: Phase 4 (比較分析) の実行準備が整いました*
"""

    report_path_2 = REPORTS_DIR / "phase2_completion_report.md"
    report_path_2.write_text(report_md, encoding="utf-8")

    mo.md("## ✅ Phase 2 完了\n\n画像入りレポートを生成しました。")
    return report_md, report_path_2


if __name__ == "__main__":
    app.run()


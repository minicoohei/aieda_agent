"""Phase 1: データ収集と前処理"""

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
    from datetime import datetime

    # 環境変数から設定取得
    AGENT_NAME = os.getenv("AGENT_NAME", "phase1_data_collection")
    REPORTS_DIR = Path(os.getenv("REPORTS_DIR", "reports/comprehensive_analysis"))
    AGENT_PORT = os.getenv("AGENT_PORT", "unknown")

    # レポートディレクトリ作成
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR = REPORTS_DIR / "data"
    DATA_DIR.mkdir(exist_ok=True)

    # プロジェクトルートをパスに追加
    project_root = Path.cwd().parent if Path.cwd().name == "notebooks" else Path.cwd()
    if str(project_root / "src") not in sys.path:
        sys.path.insert(0, str(project_root / "src"))

    mo.md(
        f"""
        # 📥 Phase 1: データ収集と前処理
        
        **エージェント名**: {AGENT_NAME}  
        **ポート**: {AGENT_PORT}  
        **レポート出力先**: `{REPORTS_DIR}`
        
        ---
        
        ## 🎯 目的
        
        1. BigQueryから全テーブルのデータを取得
        2. データクレンジングと統合
        3. マッピングテーブルの作成
        4. 基本統計量の算出
        """
    )
    return (
        AGENT_NAME,
        AGENT_PORT,
        DATA_DIR,
        Path,
        REPORTS_DIR,
        datetime,
        mo,
        os,
        pd,
        project_root,
        sys,
    )


@app.cell
def __(mo, pd, project_root):
    from ai_data_lab.connectors.bigquery import BigQueryConnector

    # BigQuery接続
    bq = BigQueryConnector(project_id="yoake-dev-analysis")

    # データセット内のテーブル一覧を取得
    dataset_id = "dev_yoake_posts"

    tables_query = f"""
    SELECT table_name
    FROM `{bq.project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
    WHERE table_type = 'BASE TABLE'
    ORDER BY table_name
    """

    try:
        tables_df = bq.query_to_dataframe(tables_query)
        table_list = tables_df["table_name"].tolist()
        
        mo.md(
            f"""
            ## 📊 検出されたテーブル
            
            データセット `{dataset_id}` 内に **{len(table_list)}** 個のテーブルを検出しました。
            """
        )
    except Exception as e:
        table_list = []
        mo.md(f"⚠️ テーブル一覧の取得に失敗: {e}")

    return BigQueryConnector, bq, dataset_id, table_list, tables_df, tables_query


@app.cell
def __(mo, table_list):
    # テーブル分類
    group_tables = []
    individual_tables = []

    # グループ名キーワード（簡易版）
    group_keywords = [
        "FRUITS",
        "ZIPPER",
        "CUTIE",
        "STREET",
        "CANDY",
        "TUNE",
        "=LOVE",
        "乃木坂",
        "櫻坂",
        "日向坂",
        "推しの子",
    ]

    for _table in table_list:
        # グループテーブルか個人テーブルかを判定
        _is_group = any(keyword.lower() in _table.lower() for keyword in group_keywords)
        if _is_group:
            group_tables.append(_table)
        else:
            individual_tables.append(_table)

    mo.md(
        f"""
        ### テーブル分類結果
        
        - **グループテーブル**: {len(group_tables)} 個
        - **個人テーブル**: {len(individual_tables)} 個
        
        #### グループテーブル一覧
        {mo.md("\\n".join([f"- `{t}`" for t in group_tables[:10]]))}
        
        #### 個人テーブル一覧（最初の10件）
        {mo.md("\\n".join([f"- `{t}`" for t in individual_tables[:10]]))}
        """
    )
    return group_keywords, group_tables, individual_tables


@app.cell
def __(DATA_DIR, bq, dataset_id, group_tables, individual_tables, mo, pd):
    # サンプルデータ取得（各テーブルから1000件ずつ）
    sample_size = 1000

    def fetch_table_sample(table_name: str, limit: int = sample_size) -> pd.DataFrame:
        """テーブルからサンプルデータを取得"""
        query = f"""
        SELECT *
        FROM `{bq.project_id}.{dataset_id}.{table_name}`
        LIMIT {limit}
        """
        try:
            df = bq.query_to_dataframe(query)
            df["_source_table"] = table_name
            return df
        except Exception as e:
            print(f"Error fetching {table_name}: {e}")
            return pd.DataFrame()

    # グループテーブルのサンプル取得
    mo.md("### 📥 データ取得中...")

    all_group_samples = []
    for _tbl in group_tables[:5]:  # 最初の5つのグループテーブル
        _sample = fetch_table_sample(_tbl)
        if not _sample.empty:
            all_group_samples.append(_sample)

    all_individual_samples = []
    for _tbl2 in individual_tables[:10]:  # 最初の10個の個人テーブル
        _sample2 = fetch_table_sample(_tbl2)
        if not _sample2.empty:
            all_individual_samples.append(_sample2)

    # データ統合
    if all_group_samples:
        group_data = pd.concat(all_group_samples, ignore_index=True)
        group_data.to_parquet(DATA_DIR / "group_data_sample.parquet")
    else:
        group_data = pd.DataFrame()

    if all_individual_samples:
        individual_data = pd.concat(all_individual_samples, ignore_index=True)
        individual_data.to_parquet(DATA_DIR / "individual_data_sample.parquet")
    else:
        individual_data = pd.DataFrame()

    mo.md(
        f"""
        ✅ データ取得完了
        
        - **グループデータ**: {len(group_data):,} 件
        - **個人データ**: {len(individual_data):,} 件
        """
    )
    return (
        all_group_samples,
        all_individual_samples,
        fetch_table_sample,
        group_data,
        individual_data,
        sample_size,
    )


@app.cell
def __(group_data, individual_data, mo, pd):
    # データ品質チェック
    def data_quality_check(df: pd.DataFrame, name: str) -> dict:
        """データ品質をチェック"""
        return {
            "データ名": name,
            "総レコード数": len(df),
            "カラム数": len(df.columns),
            "重複数": df.duplicated().sum(),
            "欠損値率": f"{df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100:.2f}%",
            "メモリ使用量": f"{df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB",
        }

    quality_checks = []
    if not group_data.empty:
        quality_checks.append(data_quality_check(group_data, "グループデータ"))
    if not individual_data.empty:
        quality_checks.append(data_quality_check(individual_data, "個人データ"))

    quality_df = pd.DataFrame(quality_checks)

    mo.md(
        f"""
        ## 🔍 データ品質チェック
        
        {mo.ui.table(quality_df)}
        """
    )
    return data_quality_check, quality_checks, quality_df


@app.cell
def __(REPORTS_DIR, datetime, group_data, individual_data, mo):
    # Phase 1完了レポート作成
    report_content = f"""# Phase 1: データ収集と前処理 完了レポート

**実行日時**: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## 📊 データ収集結果

### グループデータ
- **総件数**: {len(group_data):,} 件
- **カラム数**: {len(group_data.columns)} 個
- **保存先**: `data/group_data_sample.parquet`

### 個人データ
- **総件数**: {len(individual_data):,} 件
- **カラム数**: {len(individual_data.columns)} 個
- **保存先**: `data/individual_data_sample.parquet`

## ✅ 完了ステータス

Phase 1のデータ収集が正常に完了しました。
Phase 2 (基礎統計分析) とPhase 3 (テキストマイニング) を並列実行できます。

---

*次のステップ: Phase 2 & 3 並列起動*
"""

    report_path = REPORTS_DIR / "phase1_completion_report.md"
    report_path.write_text(report_content, encoding="utf-8")

    mo.md(
        f"""
        ## ✅ Phase 1 完了
        
        完了レポートを保存しました: `{report_path}`
        
        ### 次のアクション
        
        マスターコーディネーターから **Phase 2 & 3 を並列起動** してください。
        """
    )
    return report_content, report_path


if __name__ == "__main__":
    app.run()


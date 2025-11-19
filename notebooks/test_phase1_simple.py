"""Phase 1 シンプルテスト版: データ収集と前処理"""

import marimo

__generated_with = "0.10.14"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    import os
    from pathlib import Path
    import pandas as pd
    from datetime import datetime

    # 環境変数から設定取得
    AGENT_NAME = os.getenv("AGENT_NAME", "test_phase1")
    REPORTS_DIR = Path(os.getenv("REPORTS_DIR", "reports/comprehensive_analysis"))
    AGENT_PORT = os.getenv("AGENT_PORT", "41001")

    # レポートディレクトリ作成
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR = REPORTS_DIR / "data"
    DATA_DIR.mkdir(exist_ok=True)

    mo.md(
        f"""
        # 📥 Phase 1 テスト版: データ収集と前処理
        
        **エージェント名**: {AGENT_NAME}  
        **ポート**: {AGENT_PORT}  
        **レポート出力先**: `{REPORTS_DIR}`
        
        ---
        
        ## 🎯 目的
        
        1. BigQuery接続テスト
        2. 簡易データ取得
        3. サンプルデータ作成
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
    )


@app.cell
def __(mo):
    # BigQuery接続テスト
    try:
        from ai_data_lab.connectors.bigquery import BigQueryConnector
        
        bq = BigQueryConnector(project_id="yoake-dev-analysis")
        
        # 簡単なクエリでテスト
        test_query = "SELECT 1 as test"
        test_result = bq.query_to_dataframe(test_query)
        
        mo.md(
            f"""
            ✅ BigQuery接続成功！
            
            テスト結果: {test_result['test'].iloc[0]}
            """
        )
        connection_ok = True
    except Exception as e:
        mo.md(
            f"""
            ❌ BigQuery接続エラー
            
            ```
            {str(e)}
            ```
            
            ### 解決方法
            
            ターミナルで以下を実行してください：
            
            ```bash
            unset GOOGLE_APPLICATION_CREDENTIALS
            gcloud auth application-default login
            ```
            """
        )
        bq = None
        connection_ok = False

    return BigQueryConnector, bq, connection_ok, test_query, test_result


@app.cell
def __(bq, connection_ok, mo):
    # テーブル一覧取得（接続OKの場合のみ）
    if connection_ok and bq:
        dataset_id = "dev_yoake_posts"
        
        try:
            tables_query = f"""
            SELECT table_name
            FROM `{bq.project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
            WHERE table_type = 'BASE TABLE'
            ORDER BY table_name
            LIMIT 10
            """
            
            tables_df = bq.query_to_dataframe(tables_query)
            table_list = tables_df["table_name"].tolist()
            
            mo.md(
                f"""
                ## 📊 検出されたテーブル（最初の10件）
                
                {mo.md("\\n".join([f"{i+1}. `{t}`" for i, t in enumerate(table_list)]))}
                
                合計: **{len(table_list)}** 個のテーブル
                """
            )
        except Exception as e:
            mo.md(f"⚠️ テーブル一覧の取得に失敗: {e}")
            table_list = []
            tables_df = None
    else:
        mo.md("*BigQuery接続が確立されていません*")
        dataset_id = None
        table_list = []
        tables_df = None

    return dataset_id, table_list, tables_df, tables_query


@app.cell
def __(DATA_DIR, bq, connection_ok, dataset_id, mo, pd, table_list):
    # サンプルデータ取得（最初の1テーブルのみ）
    if connection_ok and bq and table_list:
        first_table = table_list[0]
        
        try:
            sample_query = f"""
            SELECT *
            FROM `{bq.project_id}.{dataset_id}.{first_table}`
            LIMIT 100
            """
            
            sample_data = bq.query_to_dataframe(sample_query)
            sample_data["_source_table"] = first_table
            
            # 保存
            sample_data.to_parquet(DATA_DIR / "test_sample.parquet")
            
            mo.md(
                f"""
                ## ✅ データ取得成功
                
                - **テーブル**: `{first_table}`
                - **取得件数**: {len(sample_data):,} 件
                - **カラム数**: {len(sample_data.columns)} 個
                - **保存先**: `{DATA_DIR / "test_sample.parquet"}`
                
                ### データプレビュー
                
                {mo.ui.table(sample_data.head(5))}
                """
            )
        except Exception as e:
            mo.md(f"⚠️ データ取得エラー: {e}")
            sample_data = pd.DataFrame()
    else:
        mo.md("*BigQuery接続またはテーブルが利用できません*")
        sample_data = pd.DataFrame()

    return first_table, sample_data, sample_query


@app.cell
def __(REPORTS_DIR, connection_ok, datetime, mo, sample_data):
    # 完了レポート
    if connection_ok and not sample_data.empty:
        status = "✅ 成功"
        message = "Phase 1のテストが正常に完了しました。"
    else:
        status = "⚠️ 部分的に完了"
        message = "BigQuery接続に問題があります。認証を確認してください。"

    report_content = f"""# Phase 1 テスト版完了レポート

**実行日時**: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
**ステータス**: {status}

## 結果

{message}

### データ取得結果

- **取得件数**: {len(sample_data):,} 件
- **カラム数**: {len(sample_data.columns)} 個

---

*テスト完了*
"""

    report_path = REPORTS_DIR / "test_phase1_report.md"
    report_path.write_text(report_content, encoding="utf-8")

    mo.md(
        f"""
        ## {status} Phase 1 テスト完了
        
        {message}
        
        レポート保存先: `{report_path}`
        """
    )
    return message, report_content, report_path, status


if __name__ == "__main__":
    app.run()


"""BigQuery データセット俯瞰 Marimo ノートブック

このノートブックは、指定した GCP プロジェクト内の BigQuery データセット、テーブル、
スキーマを俯瞰的に確認するためのインタラクティブUIを提供します。

## 目的
- プロジェクト内の全データセット・テーブルを一覧表示
- dev_yoake_posts などの特定テーブルを素早く確認
- スキーマ定義とテーブル統計（行数、サイズ）を可視化

## 認証
- Application Default Credentials (ADC) を使用
- 事前に `gcloud auth application-default login` を実行してください

## 使い方
```bash
marimo run notebooks/bigquery_overview.py --port 4173
```
"""

import marimo

__generated_with__ = "0.9.34"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    import pandas as pd
    import sys
    import os
    from pathlib import Path

    # GOOGLE_APPLICATION_CREDENTIALS が無効な値の場合は削除
    # Application Default Credentials (ADC) を優先的に使用
    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        gac_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        if not os.path.exists(gac_path):
            # ファイルが存在しない場合は環境変数を削除して ADC にフォールバック
            del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

    # src を PYTHONPATH に追加
    root_dir = Path(__file__).parent.parent
    if str(root_dir / "src") not in sys.path:
        sys.path.insert(0, str(root_dir / "src"))

    from ai_data_lab.connectors.bigquery import BigQueryConnector

    return BigQueryConnector, Path, mo, os, pd, root_dir, sys


@app.cell
def __(mo):
    mo.md(
        """
        # 🔍 BigQuery データセット俯瞰

        GCP プロジェクト内の **全データセット・テーブル・スキーマ** を確認できます。
        """
    )
    return


@app.cell
def __(mo):
    project_id_input = mo.ui.text(
        value="yoake-dev-analysis",
        label="GCP プロジェクト ID",
        placeholder="your-project-id",
    )
    project_id_input
    return (project_id_input,)


@app.cell
def __(BigQueryConnector, mo, project_id_input):
    """データセット一覧を取得"""
    project_id = project_id_input.value

    if not project_id:
        mo.stop(
            True,
            mo.md("⚠️ プロジェクトIDを入力してください。"),
        )

    try:
        connector = BigQueryConnector(project_id=project_id)
        datasets = connector.list_datasets()

        if not datasets:
            mo.stop(
                True,
                mo.md(f"⚠️ プロジェクト `{project_id}` にデータセットが見つかりませんでした。"),
            )

    except Exception as e:
        mo.stop(
            True,
            mo.md(f"❌ データセット取得エラー: `{e}`\n\n**Application Default Credentials** が設定されているか確認してください。"),
        )

    return connector, datasets, project_id


@app.cell
def __(datasets, mo, pd):
    """データセット一覧を表示"""
    mo.md(f"## 📂 データセット一覧 ({len(datasets)} 件)")
    return


@app.cell
def __(datasets, pd):
    datasets_df = pd.DataFrame(datasets)
    datasets_df
    return (datasets_df,)


@app.cell
def __(datasets, mo):
    """データセット選択UI"""
    dataset_options = {ds["dataset_id"]: ds["dataset_id"] for ds in datasets}

    dataset_selector = mo.ui.dropdown(
        options=dataset_options,
        value=list(dataset_options.keys())[0] if dataset_options else None,
        label="📊 データセットを選択",
    )
    dataset_selector
    return dataset_options, dataset_selector


@app.cell
def __(connector, dataset_selector, mo, project_id):
    """選択されたデータセットのテーブル一覧を取得"""
    selected_dataset = dataset_selector.value

    if not selected_dataset:
        mo.stop(True, mo.md(""))

    try:
        tables = connector.list_tables(selected_dataset, project_id=project_id)

        if not tables:
            mo.stop(
                True,
                mo.md(f"⚠️ データセット `{selected_dataset}` にテーブルが見つかりませんでした。"),
            )

        # 各テーブルの詳細情報（行数含む）を取得
        table_details = []
        for table in tables:
            try:
                info = connector.get_table_info(
                    selected_dataset,
                    table["table_id"],
                    project_id=project_id,
                )
                table_details.append(
                    {
                        "table_id": info["table_id"],
                        "table_type": info.get("table_type", "N/A"),
                        "num_rows": info.get("num_rows", 0) or 0,
                        "num_bytes": info.get("num_bytes", 0) or 0,
                        "description": info.get("description", ""),
                    }
                )
            except Exception:
                # テーブル情報取得に失敗した場合は基本情報のみ
                table_details.append(
                    {
                        "table_id": table["table_id"],
                        "table_type": table.get("table_type", "N/A"),
                        "num_rows": 0,
                        "num_bytes": 0,
                        "description": "",
                    }
                )

    except Exception as e:
        mo.stop(True, mo.md(f"❌ テーブル一覧取得エラー: `{e}`"))

    return info, selected_dataset, table, table_details, tables


@app.cell
def __(mo, selected_dataset, table_details):
    """テーブル一覧を表示"""
    mo.md(f"## 📋 テーブル一覧: `{selected_dataset}` ({len(table_details)} 件)")
    return


@app.cell
def __(pd, table_details):
    tables_df = pd.DataFrame(table_details)
    # バイト数を MB に変換
    tables_df["size_mb"] = (tables_df["num_bytes"] / 1024 / 1024).round(2)
    tables_df = tables_df[["table_id", "table_type", "num_rows", "size_mb", "description"]]
    tables_df
    return (tables_df,)


@app.cell
def __(mo, table_details):
    """テーブル選択UI（dev_yoake_posts があればデフォルト選択）"""
    table_options = {t["table_id"]: t["table_id"] for t in table_details}

    # dev_yoake_posts があればそれをデフォルトに
    default_table = "dev_yoake_posts" if "dev_yoake_posts" in table_options else list(table_options.keys())[0]

    table_selector = mo.ui.dropdown(
        options=table_options,
        value=default_table,
        label="🔎 テーブルを選択",
    )
    table_selector
    return default_table, table_options, table_selector


@app.cell
def __(connector, mo, project_id, selected_dataset, table_selector):
    """選択されたテーブルのスキーマを取得"""
    selected_table = table_selector.value

    if not selected_table:
        mo.stop(True, mo.md(""))

    try:
        schema = connector.get_table_schema(
            selected_dataset,
            selected_table,
            project_id=project_id,
        )
        table_info = connector.get_table_info(
            selected_dataset,
            selected_table,
            project_id=project_id,
        )
    except Exception as e:
        mo.stop(True, mo.md(f"❌ スキーマ取得エラー: `{e}`"))

    return schema, selected_table, table_info


@app.cell
def __(mo, selected_dataset, selected_table, table_info):
    """テーブル情報サマリーを表示"""
    num_rows = table_info.get("num_rows", "N/A")
    num_bytes = table_info.get("num_bytes", 0) or 0
    size_mb = round(num_bytes / 1024 / 1024, 2)
    table_type = table_info.get("table_type", "N/A")
    description = table_info.get("description", "（説明なし）")

    mo.md(
        f"""
        ## 📊 テーブル詳細: `{selected_dataset}.{selected_table}`

        - **テーブル型**: {table_type}
        - **行数**: {num_rows:,} 行
        - **サイズ**: {size_mb} MB
        - **説明**: {description}
        """
    )
    return description, num_bytes, num_rows, size_mb, table_type


@app.cell
def __(mo, schema):
    """スキーマを表示"""
    mo.md(f"### 🔧 スキーマ定義 ({len(schema)} フィールド)")
    return


@app.cell
def __(pd, schema):
    """スキーマをフラット化して表示"""

    def flatten_schema(fields, prefix=""):
        """ネストされたスキーマをフラット化"""
        rows = []
        for field in fields:
            field_name = f"{prefix}{field['name']}" if prefix else field["name"]
            rows.append(
                {
                    "field_name": field_name,
                    "type": field["field_type"],
                    "mode": field["mode"],
                    "description": field.get("description") or "",
                }
            )
            # ネストフィールドがあれば再帰的に処理
            if field.get("fields"):
                rows.extend(flatten_schema(field["fields"], f"{field_name}."))
        return rows

    schema_rows = flatten_schema(schema)
    schema_df = pd.DataFrame(schema_rows)
    schema_df
    return flatten_schema, schema_df, schema_rows


@app.cell
def __(mo, pd, table_details):
    """テーブルごとの行数を棒グラフで可視化"""
    mo.md("### 📈 テーブル別行数")
    return


@app.cell
def __(mo, pd, table_details):
    """棒グラフ表示"""
    chart_df = pd.DataFrame(table_details)
    chart_df = chart_df[chart_df["num_rows"] > 0].sort_values("num_rows", ascending=False)

    if chart_df.empty:
        mo.md("（行数情報がありません）")
    else:
        # Marimo のプロット機能を使用
        mo.ui.table(
            chart_df[["table_id", "num_rows"]],
            selection=None,
        )
    return (chart_df,)


if __name__ == "__main__":
    app.run()


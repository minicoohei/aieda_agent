"""ユーザーID検索 Marimo ノートブック（本番環境）

## 目的
本番環境（yoake-prod-analysis）から、ユーザーID一覧を抽出・検索する。

### 抽出項目
- HandleName（Xハンドル名）
- AccountID（XユーザーID）
- PrivyID（Privy認証ID）
- ユーザネーム

## データソース
- プロジェクト: yoake-prod-analysis
- 投稿データセット: prod_yoake_posts
- ユーザーテーブル: prod_yoake_db.users

## 認証
Application Default Credentials (ADC) を使用
事前に `gcloud auth application-default login` を実行してください

## 使い方
```bash
marimo edit notebooks/10_user_id_lookup.py --port 4175
```
"""

import marimo

__generated_with = "0.17.8"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import sys
    import os
    from pathlib import Path
    from dotenv import load_dotenv

    # .env ファイルを読み込み
    root_dir = Path(__file__).parent.parent
    load_dotenv(root_dir / ".env")

    # GOOGLE_APPLICATION_CREDENTIALS が無効な値の場合は削除
    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        gac_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        if not os.path.exists(gac_path):
            del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

    # src を PYTHONPATH に追加
    if str(root_dir / "src") not in sys.path:
        sys.path.insert(0, str(root_dir / "src"))

    from ai_data_lab.connectors.bigquery import BigQueryConnector
    return BigQueryConnector, mo


@app.cell
def _(mo):
    mo.md("""
    # 🔍 ユーザーID検索（本番環境）

    **yoake-prod-analysis** から、ユーザーID一覧を抽出・検索します。

    ## 抽出項目
    - HandleName（Xハンドル名）
    - AccountID（XユーザーID）
    - PrivyID（Privy認証ID）
    - ユーザネーム
    """)
    return


@app.cell
def _():
    # BigQuery 設定（本番環境）
    PROJECT_ID = "yoake-prod-analysis"
    POSTS_DATASET_ID = "prod_yoake_posts"
    DB_DATASET_ID = "prod_yoake_db"
    return DB_DATASET_ID, POSTS_DATASET_ID, PROJECT_ID


@app.cell
def _(BigQueryConnector, POSTS_DATASET_ID, PROJECT_ID, mo):
    """BigQueryコネクタ初期化とテーブル一覧取得"""
    try:
        connector = BigQueryConnector(project_id=PROJECT_ID)
        tables = connector.list_tables(POSTS_DATASET_ID, project_id=PROJECT_ID)

        table_names = [t["table_id"] for t in tables]
        mo.md(f"✅ 接続成功: **{len(table_names)}** テーブルを検出（{POSTS_DATASET_ID}）")
    except Exception as e:
        mo.stop(True, mo.md(f"❌ BigQuery接続エラー: `{e}`"))
    return connector, table_names


@app.cell
def _(DB_DATASET_ID, POSTS_DATASET_ID, PROJECT_ID, mo, table_names):
    """ユーザー一覧取得クエリを生成"""

    def build_user_lookup_query(project_id, posts_dataset_id, db_dataset_id, tables):
        """投稿データからユニークユーザーを抽出し、usersテーブルとJOINするクエリを生成"""
        queries = []
        for table_name in tables:
            query = f"""
            SELECT DISTINCT
                REGEXP_EXTRACT(post.xPostUrl, r'^https://x\\.com/([^/]+)/status') AS handle_name,
                CAST(user.xPostUserId AS STRING) AS account_id,
                user.xPostUserName AS user_name
            FROM `{project_id}.{posts_dataset_id}.{table_name}`
            WHERE _PARTITIONTIME IS NOT NULL
              AND user.xPostUserId IS NOT NULL
            """
            queries.append(query)

        union_sql = "\nUNION DISTINCT\n".join(queries)

        # usersテーブルとJOINしてPrivy IDを取得
        final_query = f"""
        WITH all_users AS (
            {union_sql}
        ),
        unique_users AS (
            SELECT DISTINCT
                handle_name,
                account_id,
                user_name
            FROM all_users
            WHERE handle_name IS NOT NULL
        )
        SELECT 
            u.handle_name AS HandleName,
            u.account_id AS AccountID,
            users.id AS PrivyID,
            u.user_name AS UserName
        FROM unique_users u
        LEFT JOIN `{project_id}.{db_dataset_id}.users` users
            ON u.account_id = users.xUserId
        ORDER BY u.handle_name
        """
        return final_query

    user_lookup_sql = build_user_lookup_query(PROJECT_ID, POSTS_DATASET_ID, DB_DATASET_ID, table_names)
    mo.md("✅ ユーザー検索クエリを生成しました")
    return (user_lookup_sql,)


@app.cell
def _(mo):
    mo.md("""
    ## 🔍 データ取得中...
    """)
    return


@app.cell
def _(connector, mo, user_lookup_sql):
    """ユーザー一覧を取得"""
    try:
        df_users = connector.query(user_lookup_sql)
        user_count = len(df_users)
        mo.md(f"✅ 取得完了: **{user_count:,}** 件のユニークユーザーが見つかりました")
    except Exception as e:
        mo.stop(True, mo.md(f"❌ クエリ実行エラー: `{e}`"))
    return df_users, user_count


@app.cell
def _(df_users, mo, user_count):
    """結果サマリー"""
    if user_count == 0:
        summary_md = mo.md("⚠️ ユーザーが見つかりませんでした。")
    else:
        privy_linked = df_users["PrivyID"].notna().sum()
        privy_unlinked = user_count - privy_linked

        summary_md = mo.md(
            f"""
            ## 📊 ユーザーサマリー

            | 項目 | 値 |
            |------|-----|
            | 総ユーザー数 | **{user_count:,}** 人 |
            | Privy連携済み | **{privy_linked:,}** 人 |
            | Privy未連携 | **{privy_unlinked:,}** 人 |
            """
        )
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 🔍 ユーザーID検索

    HandleName / AccountID / PrivyID / ユーザネーム で検索できます。
    """)
    return


@app.cell
def _(mo):
    """ユーザーID検索用の入力フィールド"""
    user_search_input = mo.ui.text(
        value="",
        label="🔎 検索（HandleName / AccountID / PrivyID / ユーザネームで検索）",
        placeholder="検索キーワードを入力...",
        full_width=True,
    )
    user_search_input
    return (user_search_input,)


@app.cell
def _(df_users, mo, user_count, user_search_input):
    """ユーザー一覧を検索可能なテーブルで表示"""
    if user_count == 0:
        user_lookup_output = mo.md("⚠️ ユーザーデータがありません。")
        filtered_df = None
    else:
        # 検索フィルタ適用
        search_term = user_search_input.value.strip().lower()
        if search_term:
            mask = (
                df_users["HandleName"].astype(str).str.lower().str.contains(search_term, na=False) |
                df_users["AccountID"].astype(str).str.lower().str.contains(search_term, na=False) |
                df_users["PrivyID"].astype(str).str.lower().str.contains(search_term, na=False) |
                df_users["UserName"].astype(str).str.lower().str.contains(search_term, na=False)
            )
            filtered_df = df_users[mask]
        else:
            filtered_df = df_users

        user_lookup_output = mo.vstack([
            mo.md(f"### ユーザー一覧（{len(filtered_df):,} / {user_count:,} 件）"),
            mo.ui.table(filtered_df, selection=None, page_size=50),
        ])

    user_lookup_output
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 💾 ユーザーID一覧 CSV / TSV エクスポート

    ユーザーID一覧（HandleName / AccountID / PrivyID / ユーザネーム）をダウンロードできます。
    """)
    return


@app.cell
def _(df_users, mo, user_count):
    """ユーザーID一覧のCSV/TSVダウンロードボタン"""
    if user_count == 0:
        user_download_btn = mo.md("（ダウンロードするデータがありません）")
    else:
        csv_user_data = df_users.to_csv(index=False)
        tsv_user_data = df_users.to_csv(index=False, sep="\t")

        user_download_btn = mo.hstack([
            mo.download(
                data=csv_user_data.encode("utf-8-sig"),
                filename="user_id_lookup_prod.csv",
                label="📥 ユーザーID一覧CSVダウンロード",
            ),
            mo.download(
                data=tsv_user_data.encode("utf-8-sig"),
                filename="user_id_lookup_prod.tsv",
                label="📥 ユーザーID一覧TSVダウンロード",
            ),
        ], gap=1)

    user_download_btn
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()















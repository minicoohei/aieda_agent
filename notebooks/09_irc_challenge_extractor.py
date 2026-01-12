"""推し活ハッシュタグ投稿抽出 Marimo ノートブック

## 目的
X投稿データから以下のハッシュタグを含む投稿を抽出し、
以下の情報を一覧表示する：
- `#推しとこの冬の予定`
- `#冬の推しここが尊い`

### 抽出項目
- POSTID（投稿ID）
- accountid（アカウントID）
- ユーザネーム
- いいね数
- RT数
- Reply数

## データソース
- プロジェクト: yoake-dev-analysis
- データセット: dev_yoake_posts
- 対象: データセット内の全テーブル

## 認証
Application Default Credentials (ADC) を使用
事前に `gcloud auth application-default login` を実行してください

## 使い方
```bash
marimo edit notebooks/09_irc_challenge_extractor.py --port 4173
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
    import requests
    import time
    from pathlib import Path
    from dotenv import load_dotenv
    from tqdm import tqdm

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

    # ScrapingDog API Key
    SCRAPINGDOG_API_KEY = os.getenv("SCRAPINGDOG_API_KEY")
    return BigQueryConnector, SCRAPINGDOG_API_KEY, mo, pd, requests, time, tqdm


@app.cell
def _(mo):
    mo.md("""
    # 🏷️ 推し活ハッシュタグ投稿抽出

    `dev_yoake_posts` データセット内の **全テーブル** を横断検索し、
    以下のハッシュタグを含む投稿を抽出します。

    ## 対象ハッシュタグ
    - `#推しとこの冬の予定`
    - `#冬の推しここが尊い`

    ## 抽出項目
    - POSTID（投稿ID）
    - accountid（アカウントID）
    - ユーザネーム
    - いいね数
    - RT数
    - Reply数
    """)
    return


@app.cell
def _():
    # BigQuery 設定
    PROJECT_ID = "yoake-dev-analysis"
    DATASET_ID = "dev_yoake_posts"
    HASHTAGS = [
        "#2025年推し活を振り返る",
    ]
    return DATASET_ID, HASHTAGS, PROJECT_ID


@app.cell
def _(BigQueryConnector, DATASET_ID, PROJECT_ID, mo):
    """BigQueryコネクタ初期化とテーブル一覧取得"""
    try:
        connector = BigQueryConnector(project_id=PROJECT_ID)
        tables = connector.list_tables(DATASET_ID, project_id=PROJECT_ID)

        table_names = [t["table_id"] for t in tables]
        mo.md(f"✅ 接続成功: **{len(table_names)}** テーブルを検出")
    except Exception as e:
        mo.stop(True, mo.md(f"❌ BigQuery接続エラー: `{e}`"))
    return connector, table_names


@app.cell
def _(mo, table_names):
    """検出されたテーブル一覧を表示"""
    table_list = "\n".join([f"- {name}" for name in table_names])
    mo.md(
        f"""
        ## 📂 検索対象テーブル一覧

        {table_list}
        """
    )
    return


@app.cell
def _(mo):
    """userオブジェクトのスキーマを確認（Privy IDフィールド特定用）"""
    mo.md("""
    ## 🔧 userオブジェクトのスキーマ確認

    BigQueryテーブル内の `user` フィールドの構造を確認します。
    """)
    return


@app.cell
def _(DATASET_ID, PROJECT_ID, connector, mo, table_names):
    """userフィールドのスキーマを取得して表示"""
    try:
        # 最初のテーブルからスキーマを取得
        sample_table = table_names[0] if table_names else None
        if sample_table:
            schema = connector.get_table_schema(DATASET_ID, sample_table, project_id=PROJECT_ID)

            # userフィールドを探す
            user_fields = []
            for field in schema:
                if field["name"] == "user":
                    user_fields = field.get("fields", [])
                    break

            if user_fields:
                field_list = "\n".join([f"- `{f['name']}` ({f['field_type']})" for f in user_fields])
                schema_output = mo.md(f"""
    ### user オブジェクトのフィールド一覧

    {field_list}

    **注意**: `userId` フィールドがあれば、それがPrivy IDの可能性があります。
                """)
            else:
                schema_output = mo.md("⚠️ userフィールドが見つかりませんでした。")
        else:
            schema_output = mo.md("⚠️ テーブルが見つかりませんでした。")
    except Exception as e:
        schema_output = mo.md(f"❌ スキーマ取得エラー: `{e}`")

    schema_output
    return


@app.cell
def _(DATASET_ID, HASHTAGS, PROJECT_ID, mo, table_names):
    """全テーブルを横断検索するUNION ALLクエリを生成"""

    def build_union_query(project_id, dataset_id, tables, hashtags):
        """全テーブルをUNION ALLで結合するSQLを生成"""
        queries = []
        for table_name in tables:
            # 複数ハッシュタグをOR条件で結合
            hashtag_conditions = " OR ".join(
                [f"post.xPostContent LIKE '%{tag}%'" for tag in hashtags]
            )
            # NOTE: Privy IDは別テーブルにある可能性があるため、現在は除外
            query = f"""
            SELECT
                '{table_name}' AS source_table,
                post.xPostId AS post_id,
                REGEXP_EXTRACT(post.xPostUrl, r'^https://x\\.com/([^/]+)/status') AS user_handle,
                user.xPostUserId AS account_id,
                user.xPostUserName AS user_name,
                post.xPostLikedCount AS like_count,
                post.xPostRepostedCount AS rt_count,
                post.xPostRepliedCount AS reply_count,
                post.xPostContent AS content,
                TIMESTAMP_SECONDS(post.xPostCreatedAt) AS created_at,
                post.xPostUrl AS post_url
            FROM `{project_id}.{dataset_id}.{table_name}`
            WHERE _PARTITIONTIME IS NOT NULL
              AND ({hashtag_conditions})
            """
            queries.append(query)

        return "\nUNION ALL\n".join(queries)

    union_sql = build_union_query(PROJECT_ID, DATASET_ID, table_names, HASHTAGS)

    # 最終クエリ（重複除去付き）
    final_query = f"""
    WITH all_posts AS (
        {union_sql}
    ),
    deduplicated AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY created_at DESC) AS row_num
        FROM all_posts
    )
    SELECT
        source_table,
        post_id,
        user_handle,
        account_id,
        user_name,
        like_count,
        rt_count,
        reply_count,
        content,
        created_at,
        post_url
    FROM deduplicated
    WHERE row_num = 1
    ORDER BY created_at DESC
    """

    mo.md("✅ UNION ALL クエリを生成しました")
    return (final_query,)


@app.cell
def _(mo):
    """クエリ実行とデータ取得"""
    mo.md("## 🔍 データ取得中...")
    return


@app.cell
def _(connector, final_query, mo):
    """#IRCチャレンジ 投稿を抽出"""
    try:
        df_irc = connector.query(final_query)
        result_count = len(df_irc)
        mo.md(f"✅ 抽出完了: **{result_count:,}** 件の投稿が見つかりました")
    except Exception as e:
        mo.stop(True, mo.md(f"❌ クエリ実行エラー: `{e}`"))
    return df_irc, result_count


@app.cell
def _(df_irc, mo, result_count):
    """結果サマリー"""
    if result_count == 0:
        summary_md = mo.md("⚠️ 対象ハッシュタグを含む投稿は見つかりませんでした。")
    else:
        unique_users = df_irc["account_id"].nunique()
        total_likes = df_irc["like_count"].sum()
        total_rts = df_irc["rt_count"].sum()
        total_replies = df_irc["reply_count"].sum()

        summary_md = mo.md(
            f"""
            ## 📊 抽出結果サマリー

            | 項目 | 値 |
            |------|-----|
            | 総投稿数 | **{result_count:,}** 件 |
            | ユニークユーザー数 | **{unique_users:,}** 人 |
            | 総いいね数 | **{total_likes:,}** |
            | 総RT数 | **{total_rts:,}** |
            | 総Reply数 | **{total_replies:,}** |
            """
        )
    return


@app.cell
def _(mo):
    mo.md("""
    ## 👀 データプレビュー（上位30件）

    抽出されたデータの先頭30件を表示します。
    """)
    return


@app.cell
def _(df_irc, mo, result_count):
    """データフレームのプレビュー（上位30件）"""
    if result_count > 0:
        preview_df = df_irc.head(30)
        preview_table = mo.ui.table(preview_df, selection=None)
    else:
        preview_table = mo.md("（データなし）")
    preview_table
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 📋 抽出データ一覧（整形版）

    投稿日時の新しい順に表示しています。
    """)
    return


@app.cell
def _(df_irc, mo, result_count):
    """抽出結果をテーブル表示"""
    if result_count > 0:
        # 表示用カラムを選択・リネーム
        display_df = df_irc[
            [
                "post_id",
                "user_handle",
                "account_id",
                "user_name",
                "like_count",
                "rt_count",
                "reply_count",
                "content",
                "created_at",
                "post_url",
            ]
        ].copy()

        display_df.columns = [
            "POSTID",
            "UserHandle",
            "AccountID",
            "ユーザネーム",
            "いいね",
            "RT",
            "Reply",
            "投稿内容",
            "投稿日時",
            "URL",
        ]

        table_output = mo.ui.table(display_df, selection=None)
    else:
        table_output = mo.md("（データなし）")

    table_output
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 📈 ユーザー別集計

    投稿数が多いユーザーTop20
    """)
    return


@app.cell
def _(df_irc, mo, result_count):
    """ユーザー別投稿数とエンゲージメント集計"""
    if result_count > 0:
        user_stats = (
            df_irc.groupby(["user_handle", "account_id", "user_name"])
            .agg(
                投稿数=("post_id", "count"),
                総いいね=("like_count", "sum"),
                総RT=("rt_count", "sum"),
                総Reply=("reply_count", "sum"),
            )
            .reset_index()
            .sort_values("投稿数", ascending=False)
        )

        user_stats.columns = [
            "UserHandle",
            "AccountID",
            "ユーザネーム",
            "投稿数",
            "総いいね",
            "総RT",
            "総Reply",
        ]

        user_table = mo.ui.table(user_stats, selection=None)
    else:
        user_table = mo.md("（データなし）")
    user_table
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 💾 CSV / TSV エクスポート

    抽出データをCSVまたはTSVファイルとしてダウンロードできます。
    """)
    return


@app.cell
def _(df_irc, mo, result_count):
    """CSV/TSVダウンロードボタン"""
    if result_count > 0:
        # エクスポート用DataFrame
        export_df = df_irc[
            [
                "post_id",
                "user_handle",
                "account_id",
                "user_name",
                "like_count",
                "rt_count",
                "reply_count",
                "content",
                "created_at",
                "post_url",
                "source_table",
            ]
        ].copy()

        export_df.columns = [
            "POSTID",
            "UserHandle",
            "AccountID",
            "ユーザネーム",
            "いいね",
            "RT",
            "Reply",
            "投稿内容",
            "投稿日時",
            "URL",
            "ソーステーブル",
        ]

        csv_data = export_df.to_csv(index=False)
        tsv_data = export_df.to_csv(index=False, sep="\t")

        download_btn = mo.hstack([
            mo.download(
                data=csv_data.encode("utf-8-sig"),
                filename="oshi_hashtag_posts.csv",
                label="📥 CSVダウンロード",
            ),
            mo.download(
                data=tsv_data.encode("utf-8-sig"),
                filename="oshi_hashtag_posts.tsv",
                label="📥 TSVダウンロード",
            ),
        ], gap=1)
    else:
        download_btn = mo.md("（ダウンロードするデータがありません）")
    download_btn
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 👤 プロフィール情報取得

    ユニークユーザーのXプロフィール情報（フォロワー数等）を取得します。

    **注意**: API呼び出しにはレート制限があります。「取得開始」ボタンをクリックして実行してください。
    """)
    return


@app.cell
def _(mo):
    """プロフィール取得ボタン"""
    fetch_profile_btn = mo.ui.run_button(label="🔄 プロフィール情報を取得")
    fetch_profile_btn
    return (fetch_profile_btn,)


@app.cell
def _(
    SCRAPINGDOG_API_KEY,
    df_irc,
    fetch_profile_btn,
    mo,
    pd,
    requests,
    result_count,
    time,
    tqdm,
):
    """プロフィール情報を取得"""

    def get_x_profile(profile_id: str, api_key: str, max_retries: int = 2) -> dict:
        """ScrapingDog APIでXプロフィール情報を取得（リトライ機能付き）"""
        url = "https://api.scrapingdog.com/x/profile"
        params = {
            "api_key": api_key,
            "profileId": profile_id,
            "parsed": "true"
        }
        for attempt in range(max_retries):
            try:
                response = requests.get(url, params=params, timeout=15)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    # レート制限の場合は待機してリトライ
                    print(f"Rate limited for {profile_id}, waiting...")
                    time.sleep(2)
                    continue
            except requests.exceptions.Timeout:
                print(f"Timeout for {profile_id} (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
            except Exception as e:
                print(f"Error fetching {profile_id}: {e}")
                break
        return None

    # ボタンがクリックされた場合のみ実行
    if not fetch_profile_btn.value:
        profile_result = mo.md("👆 上のボタンをクリックしてプロフィール情報を取得してください。")
    elif result_count == 0:
        profile_result = mo.md("⚠️ 抽出データがありません。")
    elif not SCRAPINGDOG_API_KEY:
        profile_result = mo.md("❌ SCRAPINGDOG_API_KEY が設定されていません。`.env` ファイルを確認してください。")
    else:
        # ユニークなuser_handleを取得
        unique_handles = df_irc["user_handle"].dropna().unique().tolist()
        total_users = len(unique_handles)

        profiles = []
        for handle in tqdm(unique_handles, desc="プロフィール取得中", unit="user"):
            profile_data = get_x_profile(handle, SCRAPINGDOG_API_KEY)
            if profile_data:
                # APIレスポンスが "user" オブジェクト内にある場合に対応
                user_data = profile_data.get("user", profile_data)
                profiles.append({
                    "UserHandle": handle,
                    "フォロワー数": user_data.get("followers_count", 0),
                    "フォロー数": user_data.get("following_count", 0),
                    "総投稿数": user_data.get("statuses_count", 0),
                    "Blue認証": user_data.get("is_blue_verified", False),
                })
            else:
                profiles.append({
                    "UserHandle": handle,
                    "フォロワー数": None,
                    "フォロー数": None,
                    "総投稿数": None,
                    "Blue認証": None,
                })
            # レート制限対策: 0.5秒待機
            time.sleep(0.5)

        profile_df = pd.DataFrame(profiles)
        profile_df = profile_df.sort_values("フォロワー数", ascending=False, na_position="last")

        profile_result = mo.vstack([
            mo.md(f"✅ **{len(profiles)}** ユーザーのプロフィールを取得しました"),
            mo.ui.table(profile_df, selection=None)
        ])

    # profile_dfが定義されていない場合はNoneを返す
    if "profile_df" not in dir():
        profile_df = None

    profile_result
    return (profile_df,)


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 📥 プロフィール結合 CSV / TSV エクスポート

    投稿データにプロフィール情報（フォロワー数、フォロー数等）を結合してダウンロードできます。
    """)
    return


@app.cell
def _(df_irc, mo, pd, profile_df, result_count):
    """プロフィール情報を結合したCSVエクスポート"""
    # profile_dfが存在するかチェック
    if result_count == 0:
        merged_csv_output = mo.md("⚠️ 抽出データがありません。")
    elif profile_df is None or (isinstance(profile_df, pd.DataFrame) and profile_df.empty):
        merged_csv_output = mo.md("⚠️ プロフィール情報が取得されていません。上の「プロフィール情報を取得」ボタンをクリックしてください。")
    else:
        # 投稿データとプロフィールを結合
        merged_df = df_irc.merge(
            profile_df,
            left_on="user_handle",
            right_on="UserHandle",
            how="left"
        )

        # エクスポート用DataFrame
        export_merged_df = merged_df[
            [
                "post_id",
                "user_handle",
                "account_id",
                "user_name",
                "like_count",
                "rt_count",
                "reply_count",
                "content",
                "created_at",
                "post_url",
                "フォロワー数",
                "フォロー数",
                "総投稿数",
                "Blue認証",
            ]
        ].copy()

        export_merged_df.columns = [
            "POSTID",
            "UserHandle",
            "AccountID",
            "ユーザネーム",
            "いいね",
            "RT",
            "Reply",
            "投稿内容",
            "投稿日時",
            "URL",
            "フォロワー数",
            "フォロー数",
            "総投稿数",
            "Blue認証",
        ]

        csv_merged_data = export_merged_df.to_csv(index=False)
        tsv_merged_data = export_merged_df.to_csv(index=False, sep="\t")

        merged_csv_output = mo.vstack([
            mo.md(f"✅ **{len(export_merged_df):,}** 件のデータを結合しました"),
            mo.hstack([
                mo.download(
                    data=csv_merged_data.encode("utf-8-sig"),
                    filename="oshi_hashtag_posts_with_profile.csv",
                    label="📥 プロフィール結合CSVダウンロード",
                ),
                mo.download(
                    data=tsv_merged_data.encode("utf-8-sig"),
                    filename="oshi_hashtag_posts_with_profile.tsv",
                    label="📥 プロフィール結合TSVダウンロード",
                ),
            ], gap=1),
            mo.md("### プレビュー（上位10件）"),
            mo.ui.table(export_merged_df.head(10), selection=None),
        ])

    merged_csv_output
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 🔍 ユーザーID検索

    AccountID / UserHandle / ユーザネーム の一覧を検索・フィルタできます。
    """)
    return


@app.cell
def _(mo):
    """ユーザーID検索用の入力フィールド"""
    user_search_input = mo.ui.text(
        value="",
        label="🔎 検索（AccountID / UserHandle / ユーザネームで検索）",
        placeholder="検索キーワードを入力...",
        full_width=True,
    )
    user_search_input
    return (user_search_input,)


@app.cell
def _(df_irc, mo, result_count, user_search_input):
    """ユニークユーザー一覧を検索可能なテーブルで表示"""
    if result_count == 0:
        user_lookup_output = mo.md("⚠️ 抽出データがありません。")
        user_lookup_df = None
    else:
        # ユニークユーザー一覧を作成（重複除去）
        user_lookup_df = (
            df_irc[["account_id", "user_handle", "user_name"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )

        # カラム名をリネーム
        user_lookup_df.columns = ["AccountID", "UserHandle", "ユーザネーム"]

        # 検索フィルタ適用
        search_term = user_search_input.value.strip().lower()
        if search_term:
            mask = (
                user_lookup_df["AccountID"].astype(str).str.lower().str.contains(search_term, na=False) |
                user_lookup_df["UserHandle"].astype(str).str.lower().str.contains(search_term, na=False) |
                user_lookup_df["ユーザネーム"].astype(str).str.lower().str.contains(search_term, na=False)
            )
            filtered_df = user_lookup_df[mask]
        else:
            filtered_df = user_lookup_df

        user_lookup_output = mo.vstack([
            mo.md(f"### ユーザー一覧（{len(filtered_df):,} / {len(user_lookup_df):,} 件）"),
            mo.ui.table(filtered_df, selection=None, page_size=50),
        ])

    user_lookup_output
    return (user_lookup_df,)


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 💾 ユーザーID一覧 CSV / TSV エクスポート

    ユーザーID一覧（AccountID / UserHandle / ユーザネーム）をダウンロードできます。
    """)
    return


@app.cell
def _(mo, user_lookup_df):
    """ユーザーID一覧のCSV/TSVダウンロードボタン"""
    if user_lookup_df is None or len(user_lookup_df) == 0:
        user_download_btn = mo.md("（ダウンロードするデータがありません）")
    else:
        csv_user_data = user_lookup_df.to_csv(index=False)
        tsv_user_data = user_lookup_df.to_csv(index=False, sep="\t")

        user_download_btn = mo.hstack([
            mo.download(
                data=csv_user_data.encode("utf-8-sig"),
                filename="user_id_lookup.csv",
                label="📥 ユーザーID一覧CSVダウンロード",
            ),
            mo.download(
                data=tsv_user_data.encode("utf-8-sig"),
                filename="user_id_lookup.tsv",
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

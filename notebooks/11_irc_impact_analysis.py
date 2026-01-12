"""IRCチャレンジ効果分析 Marimo ノートブック

## 目的
#IRCチャレンジに参加したユーザーと参加していないユーザーで、
メンバー名ハッシュタグの投稿行動がどう変化したかを比較分析する。

### 分析設計
- **Treatment群**: #IRCチャレンジを投稿したことがあるユーザー
- **Control群**: #IRCチャレンジを投稿していないが、メンバー名タグを使用したユーザー
- **基準日**: Treatment群は各ユーザーの初回#IRCチャレンジ投稿日、Control群は11/28（リリース日）

### 比較指標
- 1人当たりの平均メンバー名タグ投稿数（前後）
- 分布（ヒストグラム、箱ひげ図）
- 全体像（サマリー統計）

## データソース
- プロジェクト: yoake-dev-analysis
- データセット: dev_yoake_posts

## 使い方
```bash
marimo edit notebooks/11_irc_impact_analysis.py --port 4173
```
"""

import marimo

__generated_with = "0.17.8"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import numpy as np
    import re
    import sys
    import os
    from pathlib import Path
    from datetime import datetime, timedelta
    from collections import Counter
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
    return BigQueryConnector, Counter, mo, np, pd, re


@app.cell
def _(mo):
    mo.md("""
    # 🔬 IRCチャレンジ効果分析

    ## 分析の目的
    `#IRCチャレンジ` に参加したユーザーと参加していないユーザーで、
    メンバー名ハッシュタグの投稿行動がどう変化したかを比較分析します。

    ### 分析設計
    - **Treatment群**: #IRCチャレンジを1回以上投稿したユーザー
    - **Control群**: #IRCチャレンジ未投稿だが、メンバー名タグを使用したユーザー
    - **基準日**:
      - Treatment群: 各ユーザーの「初回#IRCチャレンジ投稿日」
      - Control群: 11/28（アプリリリース日）

    ### 比較指標
    1. 1人当たりの平均メンバー名タグ投稿数（前後）
    2. 分布（ヒストグラム、箱ひげ図）
    3. 全体サマリー・統計的検定

    ⚠️ **注意**: この分析は相関関係を示すものであり、因果関係を証明するものではありません。
    """)
    return


@app.cell
def _(pd):
    # BigQuery 設定
    PROJECT_ID = "yoake-dev-analysis"
    DATASET_ID = "dev_yoake_posts"

    # IRCチャレンジのハッシュタグ
    IRC_CHALLENGE_TAG = "#IRCチャレンジ"

    # 期間設定（UTCタイムゾーン付き）
    DATA_START_DATE = pd.Timestamp("2025-11-05", tz="UTC")  # データ開始日
    CONTROL_BASELINE_DATE = pd.Timestamp("2025-11-28", tz="UTC")  # IRCチャレンジ開始日（Control群の基準日）
    DATA_END_DATE = pd.Timestamp("2025-12-20", tz="UTC")  # データ終了日
    return (
        CONTROL_BASELINE_DATE,
        DATASET_ID,
        DATA_END_DATE,
        DATA_START_DATE,
        IRC_CHALLENGE_TAG,
        PROJECT_ID,
    )


@app.cell
def _(BigQueryConnector, DATASET_ID, PROJECT_ID, mo):
    """BigQueryコネクタ初期化とテーブル一覧取得"""
    try:
        connector = BigQueryConnector(project_id=PROJECT_ID)
        tables = connector.list_tables(DATASET_ID, project_id=PROJECT_ID)
        table_names = [t["table_id"] for t in tables]
        mo.md(f"✅ BigQuery接続成功: **{len(table_names)}** テーブルを検出")
    except Exception as e:
        connector = None
        table_names = []
        mo.stop(True, mo.md(f"❌ BigQuery接続エラー: `{e}`"))
    return connector, table_names


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 📊 Step 1: 全投稿データの取得

    全テーブルから投稿データを取得し、ハッシュタグを抽出します。
    """)
    return


@app.cell
def _(DATASET_ID, PROJECT_ID, mo, table_names):
    """全投稿取得用のUNION ALLクエリを生成"""

    def build_all_posts_query(project_id, dataset_id, tables):
        """全テーブルから全投稿を取得するSQLを生成"""
        queries = []
        for table_name in tables:
            query = f"""
            SELECT
                '{table_name}' AS source_table,
                post.xPostId AS post_id,
                user.xPostUserId AS account_id,
                user.xPostUserName AS user_name,
                post.xPostContent AS content,
                TIMESTAMP_SECONDS(post.xPostCreatedAt) AS created_at,
                post.xPostLikedCount AS like_count,
                post.xPostRepostedCount AS retweet_count,
                post.xPostRepliedCount AS reply_count
            FROM `{project_id}.{dataset_id}.{table_name}`
            WHERE _PARTITIONTIME >= TIMESTAMP('2024-01-01')
              AND post.xPostContent IS NOT NULL
            """
            queries.append(query)
        return "\nUNION ALL\n".join(queries)

    all_posts_sql = build_all_posts_query(PROJECT_ID, DATASET_ID, table_names)

    # 重複除去付きの最終クエリ
    final_all_posts_query = f"""
    WITH all_posts AS (
        {all_posts_sql}
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
        account_id,
        user_name,
        content,
        created_at,
        like_count,
        retweet_count,
        reply_count
    FROM deduplicated
    WHERE row_num = 1
    ORDER BY created_at DESC
    """

    mo.md("✅ 全投稿取得クエリを生成しました")
    return (final_all_posts_query,)


@app.cell
def _(connector, final_all_posts_query, mo):
    """全投稿データを取得"""
    try:
        df_all_posts = connector.query(final_all_posts_query)
        total_posts = len(df_all_posts)
        unique_users = df_all_posts["account_id"].nunique()
        mo.md(f"✅ 全投稿取得完了: **{total_posts:,}** 件、**{unique_users:,}** ユニークユーザー")
    except Exception as e:
        df_all_posts = None
        mo.stop(True, mo.md(f"❌ クエリ実行エラー: `{e}`"))
    return (df_all_posts,)


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 🏷️ Step 2: ハッシュタグ抽出と分析

    投稿本文からハッシュタグを抽出し、頻出タグを分析します。
    """)
    return


@app.cell
def _(Counter, df_all_posts, mo, pd, re):
    """投稿本文からハッシュタグを抽出"""

    def extract_hashtags(text):
        """テキストからハッシュタグを抽出"""
        if pd.isna(text):
            return []
        # 日本語・英数字を含むハッシュタグを抽出
        pattern = r'#[^\s#\u3000]+'
        return re.findall(pattern, str(text))

    # 全投稿からハッシュタグを抽出
    df_all_posts["hashtags"] = df_all_posts["content"].apply(extract_hashtags)

    # 全ハッシュタグをフラット化してカウント
    all_hashtags = []
    for tags in df_all_posts["hashtags"]:
        all_hashtags.extend(tags)

    hashtag_counts = Counter(all_hashtags)
    top_hashtags = hashtag_counts.most_common(100)

    # DataFrameに変換
    df_hashtag_freq = pd.DataFrame(top_hashtags, columns=["hashtag", "count"])

    mo.md(f"✅ ハッシュタグ抽出完了: **{len(hashtag_counts):,}** 種類のユニークタグ")
    return (df_hashtag_freq,)


@app.cell
def _(df_hashtag_freq, mo):
    """頻出ハッシュタグTop50を表示"""
    mo.vstack([
        mo.md("### 📊 頻出ハッシュタグ Top 50"),
        mo.md("メンバー名と思われるタグを確認してください。"),
        mo.ui.table(df_hashtag_freq.head(50), selection=None)
    ])
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 🎯 Step 3: メンバー名タグの自動設定

    テーブル名がそのままメンバー名なので、`#テーブル名` 形式でメンバー名タグを自動生成します。
    """)
    return


@app.cell
def _(IRC_CHALLENGE_TAG, mo, table_names):
    """テーブル名からメンバー名タグを自動生成"""
    # テーブル名から自動的にメンバー名タグを生成
    member_tags = [f"#{name}" for name in table_names]

    tag_status = mo.md(f"""
    ✅ **{len(member_tags)}** 個のメンバー名タグを自動設定しました:

    {', '.join([f'`{tag}`' for tag in member_tags[:10]])}{'...' if len(member_tags) > 10 else ''}

    **IRCチャレンジタグ**: `{IRC_CHALLENGE_TAG}`
    """)

    tag_status
    return (member_tags,)


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 👥 Step 4: Treatment群とControl群の分類

    - **Treatment群**: #IRCチャレンジを1回以上投稿したユーザー
    - **Control群**: #IRCチャレンジ未投稿だが、メンバー名タグを使用したユーザー
    """)
    return


@app.cell
def _(IRC_CHALLENGE_TAG, df_all_posts, member_tags, mo, pd):
    """ユーザーをTreatment群とControl群に分類"""

    if not member_tags:
        grouping_result = mo.md("⚠️ メンバー名タグが設定されていません。Step 3でタグを入力してください。")
        df_treatment = pd.DataFrame()
        df_control = pd.DataFrame()
        treatment_users = set()
        control_users = set()
    else:
        # 各投稿がIRCチャレンジタグを含むか判定
        df_all_posts["has_irc"] = df_all_posts["hashtags"].apply(
            lambda tags: IRC_CHALLENGE_TAG in tags
        )

        # 各投稿がメンバー名タグを含むか判定
        df_all_posts["has_member_tag"] = df_all_posts["hashtags"].apply(
            lambda tags: any(tag in tags for tag in member_tags)
        )

        # IRCチャレンジを投稿したことがあるユーザー（Treatment群）
        treatment_users = set(
            df_all_posts[df_all_posts["has_irc"]]["account_id"].unique()
        )

        # メンバー名タグを投稿したことがあるユーザー
        member_tag_users = set(
            df_all_posts[df_all_posts["has_member_tag"]]["account_id"].unique()
        )

        # Control群: メンバー名タグを使用しているが、IRCチャレンジは未投稿
        control_users = member_tag_users - treatment_users

        # DataFrameに分割
        df_treatment = df_all_posts[df_all_posts["account_id"].isin(treatment_users)].copy()
        df_control = df_all_posts[df_all_posts["account_id"].isin(control_users)].copy()

        grouping_result = mo.md(f"""
        ### グループ分け結果

        | グループ | ユーザー数 | 投稿数 |
        |----------|------------|--------|
        | **Treatment群** (IRC参加者) | {len(treatment_users):,} 人 | {len(df_treatment):,} 件 |
        | **Control群** (IRC非参加者) | {len(control_users):,} 人 | {len(df_control):,} 件 |
        """)

    grouping_result
    return control_users, treatment_users


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 📅 Step 5: 基準日の設定

    - **Treatment群**: 各ユーザーの「初回#IRCチャレンジ投稿日」
    - **Control群**: 11/28（アプリリリース日）
    """)
    return


@app.cell
def _(
    CONTROL_BASELINE_DATE,
    IRC_CHALLENGE_TAG,
    control_users,
    df_all_posts,
    mo,
    pd,
    treatment_users,
):
    """各ユーザーの基準日を計算"""

    if not treatment_users and not control_users:
        baseline_result = mo.md("⚠️ グループ分けが完了していません。")
        df_user_baseline = pd.DataFrame()
    else:
        baseline_records = []

        # Treatment群: 初回IRCチャレンジ投稿日
        for uid_bl in treatment_users:
            posts_bl = df_all_posts[df_all_posts["account_id"] == uid_bl]
            irc_posts_bl = posts_bl[posts_bl["hashtags"].apply(
                lambda tags: IRC_CHALLENGE_TAG in tags
            )]
            if len(irc_posts_bl) > 0:
                first_irc_date = irc_posts_bl["created_at"].min()
                baseline_records.append({
                    "account_id": uid_bl,
                    "group": "Treatment",
                    "baseline_date": first_irc_date,
                })

        # Control群: 固定日（11/28）
        for uid_ctrl in control_users:
            baseline_records.append({
                "account_id": uid_ctrl,
                "group": "Control",
                "baseline_date": CONTROL_BASELINE_DATE,
            })

        df_user_baseline = pd.DataFrame(baseline_records)

        baseline_result = mo.md(f"""
        ### 基準日設定完了

        - **Treatment群**: 各ユーザーの初回 `{IRC_CHALLENGE_TAG}` 投稿日を基準
        - **Control群**: {CONTROL_BASELINE_DATE.strftime('%Y/%m/%d')} を基準

        基準日が設定されたユーザー数: **{len(df_user_baseline):,}** 人
        """)

    baseline_result
    return (df_user_baseline,)


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 📈 Step 6: 前後比較分析

    各ユーザーの基準日より「前」と「後」でメンバー名タグ投稿数を集計し、変化量を計算します。
    """)
    return


@app.cell
def _(
    CONTROL_BASELINE_DATE,
    DATA_END_DATE,
    DATA_START_DATE,
    df_all_posts,
    df_user_baseline,
    member_tags,
    mo,
    pd,
):
    """前後のメンバー名タグ投稿数を集計（固定期間で計算）"""

    if df_user_baseline.empty or not member_tags:
        analysis_result = mo.md("⚠️ 分析に必要なデータが揃っていません。")
        df_user_analysis = pd.DataFrame()
        df_user_analysis_filtered = pd.DataFrame()
    else:
        analysis_records = []

        for _, row_an in df_user_baseline.iterrows():
            uid_an = row_an["account_id"]
            group_an = row_an["group"]
            baseline_an = row_an["baseline_date"]

            # ユーザーの全投稿を取得
            posts_an = df_all_posts[df_all_posts["account_id"] == uid_an].copy()

            # メンバー名タグを含む投稿のみ
            member_posts_an = posts_an[posts_an["has_member_tag"]]

            # データ期間内の投稿のみにフィルタ
            member_posts_an = member_posts_an[
                (member_posts_an["created_at"] >= DATA_START_DATE) &
                (member_posts_an["created_at"] <= DATA_END_DATE)
            ]

            # 基準日より前と後に分割
            posts_before_an = member_posts_an[member_posts_an["created_at"] < baseline_an]
            posts_after_an = member_posts_an[member_posts_an["created_at"] >= baseline_an]

            count_before = len(posts_before_an)
            count_after = len(posts_after_an)
            change = count_after - count_before

            # 期間（日数）を計算 - 固定期間を使用
            # 前の期間: DATA_START_DATE 〜 baseline_an
            # 後の期間: baseline_an 〜 DATA_END_DATE
            days_before = max((baseline_an - DATA_START_DATE).days, 1)
            days_after = max((DATA_END_DATE - baseline_an).days, 1)

            # 1日あたりの投稿数（正規化）
            rate_before = count_before / days_before if days_before > 0 else 0
            rate_after = count_after / days_after if days_after > 0 else 0
            rate_change = rate_after - rate_before

            # 比率（後/前）を計算
            ratio = rate_after / rate_before if rate_before > 0 else None

            # ユーザーの主なアイドル（最も多く投稿しているsource_table）
            main_idol = member_posts_an["source_table"].mode().iloc[0] if len(member_posts_an) > 0 else "N/A"

            analysis_records.append({
                "account_id": uid_an,
                "group": group_an,
                "baseline_date": baseline_an,
                "main_idol": main_idol,  # 主なアイドル
                "count_before": count_before,
                "count_after": count_after,
                "change": change,
                "days_before": days_before,
                "days_after": days_after,
                "rate_before": rate_before,
                "rate_after": rate_after,
                "rate_change": rate_change,
                "ratio": ratio,  # 後/前の比率
            })

        df_user_analysis = pd.DataFrame(analysis_records)

        # ⭐ 前後両方に1回以上投稿があるユーザーのみに絞る
        df_user_analysis_filtered = df_user_analysis[
            (df_user_analysis["count_before"] >= 1) & 
            (df_user_analysis["count_after"] >= 1)
        ].copy()

        # サマリー統計（フィルタ後）
        treatment_stats = df_user_analysis_filtered[df_user_analysis_filtered["group"] == "Treatment"]
        control_stats = df_user_analysis_filtered[df_user_analysis_filtered["group"] == "Control"]

        # フィルタ前のユーザー数
        all_treatment = len(df_user_analysis[df_user_analysis["group"] == "Treatment"])
        all_control = len(df_user_analysis[df_user_analysis["group"] == "Control"])

        # Control群の固定期間を計算
        control_days_before = (CONTROL_BASELINE_DATE - DATA_START_DATE).days
        control_days_after = (DATA_END_DATE - CONTROL_BASELINE_DATE).days

        analysis_result = mo.md(f"""
        ### 前後比較分析完了（前後両方に投稿があるユーザーのみ）

        #### 📅 分析期間
        - **データ期間**: {DATA_START_DATE.strftime('%Y-%m-%d')} 〜 {DATA_END_DATE.strftime('%Y-%m-%d')}
        - **Control群基準日**: {CONTROL_BASELINE_DATE.strftime('%Y-%m-%d')}（IRCチャレンジ開始日）
        - **Control群 前期間**: {control_days_before} 日、**後期間**: {control_days_after} 日

        #### 基本情報
        | 指標 | Treatment群 (IRC参加者) | Control群 (IRC非参加者) |
        |------|------------------------|------------------------|
        | 全ユーザー数 | {all_treatment:,} | {all_control:,} |
        | **分析対象（前後両方投稿あり）** | **{len(treatment_stats):,}** | **{len(control_stats):,}** |
        | 平均観測期間（前） | {treatment_stats['days_before'].mean():.1f} 日 | {control_stats['days_before'].mean():.1f} 日 |
        | 平均観測期間（後） | {treatment_stats['days_after'].mean():.1f} 日 | {control_stats['days_after'].mean():.1f} 日 |

        #### 総投稿数（参考）
        | 指標 | Treatment群 | Control群 |
        |------|-------------|-----------|
        | 平均投稿数（前） | {treatment_stats['count_before'].mean():.2f} | {control_stats['count_before'].mean():.2f} |
        | 平均投稿数（後） | {treatment_stats['count_after'].mean():.2f} | {control_stats['count_after'].mean():.2f} |

        #### ⭐ 1日あたり投稿数（主指標・期間正規化）
        | 指標 | Treatment群 | Control群 |
        |------|-------------|-----------|
        | 1日あたり投稿数（前）平均 | {treatment_stats['rate_before'].mean():.4f} | {control_stats['rate_before'].mean():.4f} |
        | 1日あたり投稿数（前）中央値 | {treatment_stats['rate_before'].median():.4f} | {control_stats['rate_before'].median():.4f} |
        | 1日あたり投稿数（後）平均 | {treatment_stats['rate_after'].mean():.4f} | {control_stats['rate_after'].mean():.4f} |
        | 1日あたり投稿数（後）中央値 | {treatment_stats['rate_after'].median():.4f} | {control_stats['rate_after'].median():.4f} |
        | **比率（後/前）平均** | **{treatment_stats['ratio'].mean():.2f}x** | **{control_stats['ratio'].mean():.2f}x** |
        | 比率（後/前）中央値 | {treatment_stats['ratio'].median():.2f}x | {control_stats['ratio'].median():.2f}x |
        | 比率（後/前）標準偏差 | {treatment_stats['ratio'].std():.2f} | {control_stats['ratio'].std():.2f} |
        | 比率（後/前）最小 | {treatment_stats['ratio'].min():.2f}x | {control_stats['ratio'].min():.2f}x |
        | 比率（後/前）最大 | {treatment_stats['ratio'].max():.2f}x | {control_stats['ratio'].max():.2f}x |
        """)

    analysis_result
    return (df_user_analysis_filtered,)


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 📋 Step 6.5: アイドル（テーブル名）× グループ別クロス集計

    各アイドルについて、Treatment群とControl群の比率（後/前）を比較します。
    """)
    return


@app.cell
def _():
    return


@app.cell
def _(df_user_analysis_filtered, mo, pd):
    """アイドル×グループのクロス集計テーブル"""

    if df_user_analysis_filtered.empty:
        idol_table = mo.md("⚠️ 分析データがありません。")
        df_idol_stats = pd.DataFrame()
    else:
        # アイドル×グループごとに集計
        idol_group_stats = []

        for idol_name_cross in df_user_analysis_filtered["main_idol"].unique():
            idol_data_cross = df_user_analysis_filtered[df_user_analysis_filtered["main_idol"] == idol_name_cross]

            for group_name_cross in ["Treatment", "Control"]:
                group_data_cross = idol_data_cross[idol_data_cross["group"] == group_name_cross]

                if len(group_data_cross) > 0:
                    idol_group_stats.append({
                        "アイドル": idol_name_cross,
                        "グループ": group_name_cross,
                        "ユーザー数": len(group_data_cross),
                        "平均投稿数(前)": group_data_cross["count_before"].mean(),
                        "平均投稿数(後)": group_data_cross["count_after"].mean(),
                        "1日あたり(前)": group_data_cross["rate_before"].mean(),
                        "1日あたり(後)": group_data_cross["rate_after"].mean(),
                        "比率(後/前)平均": group_data_cross["ratio"].mean(),
                        "比率(後/前)中央値": group_data_cross["ratio"].median(),
                    })

        df_idol_stats = pd.DataFrame(idol_group_stats)

        # 比率でソート
        df_idol_stats = df_idol_stats.sort_values(
            by=["比率(後/前)平均"], 
            ascending=False
        )

        # 数値をフォーマット
        df_idol_display = df_idol_stats.copy()
        df_idol_display["平均投稿数(前)"] = df_idol_display["平均投稿数(前)"].apply(lambda x: f"{x:.2f}")
        df_idol_display["平均投稿数(後)"] = df_idol_display["平均投稿数(後)"].apply(lambda x: f"{x:.2f}")
        df_idol_display["1日あたり(前)"] = df_idol_display["1日あたり(前)"].apply(lambda x: f"{x:.4f}")
        df_idol_display["1日あたり(後)"] = df_idol_display["1日あたり(後)"].apply(lambda x: f"{x:.4f}")
        df_idol_display["比率(後/前)平均"] = df_idol_display["比率(後/前)平均"].apply(lambda x: f"{x:.2f}x" if pd.notna(x) else "N/A")
        df_idol_display["比率(後/前)中央値"] = df_idol_display["比率(後/前)中央値"].apply(lambda x: f"{x:.2f}x" if pd.notna(x) else "N/A")

        idol_table = mo.vstack([
            mo.md("### アイドル × グループ別 比率一覧"),
            mo.md("比率（後/前）が高いほど、IRCチャレンジ後に投稿頻度が増加しています。"),
            mo.ui.table(df_idol_display, selection=None, pagination=True)
        ])

    idol_table
    return


@app.cell
def _(df_user_analysis_filtered, mo, pd):
    """アイドル別サマリー（Treatment vs Control比較）"""

    if df_user_analysis_filtered.empty:
        idol_summary = mo.md("⚠️ 分析データがありません。")
        df_idol_summary = pd.DataFrame()
    else:
        # アイドルごとにTreatment/Controlの比率を比較
        idol_summary_list = []

        for idol_name_sum in df_user_analysis_filtered["main_idol"].unique():
            idol_data_sum = df_user_analysis_filtered[df_user_analysis_filtered["main_idol"] == idol_name_sum]

            treatment_data_sum = idol_data_sum[idol_data_sum["group"] == "Treatment"]
            control_data_sum = idol_data_sum[idol_data_sum["group"] == "Control"]

            treatment_ratio_sum = treatment_data_sum["ratio"].mean() if len(treatment_data_sum) > 0 else None
            control_ratio_sum = control_data_sum["ratio"].mean() if len(control_data_sum) > 0 else None

            # Treatment - Control の差分
            diff_sum = (treatment_ratio_sum - control_ratio_sum) if (treatment_ratio_sum is not None and control_ratio_sum is not None) else None

            idol_summary_list.append({
                "アイドル": idol_name_sum,
                "Treatment人数": len(treatment_data_sum),
                "Control人数": len(control_data_sum),
                "Treatment比率": treatment_ratio_sum,
                "Control比率": control_ratio_sum,
                "差分(T-C)": diff_sum,
            })

        df_idol_summary = pd.DataFrame(idol_summary_list)

        # 差分でソート（大きい順）
        df_idol_summary = df_idol_summary.sort_values(by=["差分(T-C)"], ascending=False, na_position='last')

        # 表示用にフォーマット
        df_idol_summary_display = df_idol_summary.copy()
        df_idol_summary_display["Treatment比率"] = df_idol_summary_display["Treatment比率"].apply(
            lambda x: f"{x:.2f}x" if pd.notna(x) else "N/A"
        )
        df_idol_summary_display["Control比率"] = df_idol_summary_display["Control比率"].apply(
            lambda x: f"{x:.2f}x" if pd.notna(x) else "N/A"
        )
        df_idol_summary_display["差分(T-C)"] = df_idol_summary_display["差分(T-C)"].apply(
            lambda x: f"{x:+.2f}" if pd.notna(x) else "N/A"
        )

        idol_summary = mo.vstack([
            mo.md("### アイドル別 Treatment vs Control 比較"),
            mo.md("差分(T-C)が大きいほど、IRCチャレンジ参加の効果が大きいことを示唆します。"),
            mo.ui.table(df_idol_summary_display, selection=None, pagination=True)
        ])

    idol_summary
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 📊 Step 6.6: 元々の投稿数による層別分析

    前の期間の投稿数で「少なかった群」「多かった群」に分けて比較します。
    """)
    return


@app.cell
def _(df_user_analysis_filtered, mo, pd):
    """元々の投稿数による層別分析"""

    if df_user_analysis_filtered.empty:
        stratified_table = mo.md("⚠️ 分析データがありません。")
    else:
        # 閾値: 前の期間に5件以下 vs 6件以上
        THRESHOLD = 5

        # 層別化
        df_low_activity = df_user_analysis_filtered[df_user_analysis_filtered["count_before"] <= THRESHOLD]
        df_high_activity = df_user_analysis_filtered[df_user_analysis_filtered["count_before"] > THRESHOLD]

        # 各層×グループの統計を計算
        stratified_stats = []

        for activity_level, df_activity in [("低活動（≤5件）", df_low_activity), ("高活動（>5件）", df_high_activity)]:
            for group_strat in ["Treatment", "Control"]:
                group_df_strat = df_activity[df_activity["group"] == group_strat]

                if len(group_df_strat) > 0:
                    stratified_stats.append({
                        "活動レベル": activity_level,
                        "グループ": group_strat,
                        "ユーザー数": len(group_df_strat),
                        "平均投稿数(前)": group_df_strat["count_before"].mean(),
                        "平均投稿数(後)": group_df_strat["count_after"].mean(),
                        "1日あたり(前)": group_df_strat["rate_before"].mean(),
                        "1日あたり(後)": group_df_strat["rate_after"].mean(),
                        "比率(後/前)平均": group_df_strat["ratio"].mean(),
                        "比率(後/前)中央値": group_df_strat["ratio"].median(),
                    })

        df_stratified = pd.DataFrame(stratified_stats)

        # フォーマット
        df_stratified_display = df_stratified.copy()
        df_stratified_display["平均投稿数(前)"] = df_stratified_display["平均投稿数(前)"].apply(lambda x: f"{x:.2f}")
        df_stratified_display["平均投稿数(後)"] = df_stratified_display["平均投稿数(後)"].apply(lambda x: f"{x:.2f}")
        df_stratified_display["1日あたり(前)"] = df_stratified_display["1日あたり(前)"].apply(lambda x: f"{x:.4f}")
        df_stratified_display["1日あたり(後)"] = df_stratified_display["1日あたり(後)"].apply(lambda x: f"{x:.4f}")
        df_stratified_display["比率(後/前)平均"] = df_stratified_display["比率(後/前)平均"].apply(lambda x: f"{x:.2f}x" if pd.notna(x) else "N/A")
        df_stratified_display["比率(後/前)中央値"] = df_stratified_display["比率(後/前)中央値"].apply(lambda x: f"{x:.2f}x" if pd.notna(x) else "N/A")

        # 全体サマリー
        low_treatment = df_low_activity[df_low_activity["group"] == "Treatment"]
        low_control = df_low_activity[df_low_activity["group"] == "Control"]
        high_treatment = df_high_activity[df_high_activity["group"] == "Treatment"]
        high_control = df_high_activity[df_high_activity["group"] == "Control"]

        summary_md = f"""
        ### 層別分析結果

        #### 低活動群（前の期間 ≤{THRESHOLD}件）
        | 指標 | Treatment | Control |
        |------|-----------|---------|
        | ユーザー数 | {len(low_treatment):,} | {len(low_control):,} |
        | 比率(後/前)平均 | {low_treatment['ratio'].mean():.2f}x | {low_control['ratio'].mean():.2f}x |
        | 比率(後/前)中央値 | {low_treatment['ratio'].median():.2f}x | {low_control['ratio'].median():.2f}x |

        #### 高活動群（前の期間 >{THRESHOLD}件）
        | 指標 | Treatment | Control |
        |------|-----------|---------|
        | ユーザー数 | {len(high_treatment):,} | {len(high_control):,} |
        | 比率(後/前)平均 | {high_treatment['ratio'].mean():.2f}x | {high_control['ratio'].mean():.2f}x |
        | 比率(後/前)中央値 | {high_treatment['ratio'].median():.2f}x | {high_control['ratio'].median():.2f}x |
        """

        stratified_table = mo.vstack([
            mo.md(summary_md),
            mo.md("### 詳細テーブル"),
            mo.ui.table(df_stratified_display, selection=None)
        ])

    stratified_table
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 👑 Step 6.6.1: ロイヤリティ層別分析

    前の期間の投稿数を基準に、ユーザーを **高ロイヤリティ（上位25%）** と **低ロイヤリティ（下位25%）** に分類し、
    IRC参加の効果が層によって異なるかを分析します。

    ### 期待される知見
    - **高ロイヤリティ**: 既に活発なユーザーがIRCでさらに活性化するか
    - **低ロイヤリティ**: 新規/休眠ユーザーがIRCで活性化するか（獲得効果）
    """)
    return


@app.cell
def _(df_user_analysis_filtered, mo, np, pd):
    """ロイヤリティ層別分析（四分位分割）"""

    if df_user_analysis_filtered.empty:
        royalty_result = mo.md("⚠️ 分析データがありません。")
        df_royalty_stats = pd.DataFrame()
    else:
        # 四分位数を計算（前の期間の投稿数を基準）
        q1 = df_user_analysis_filtered["count_before"].quantile(0.25)
        q3 = df_user_analysis_filtered["count_before"].quantile(0.75)

        # ロイヤリティ層に分類
        def classify_royalty(count):
            if count <= q1:
                return "低ロイヤリティ（下位25%）"
            elif count >= q3:
                return "高ロイヤリティ（上位25%）"
            else:
                return "中ロイヤリティ（中間50%）"

        df_user_analysis_filtered["royalty"] = df_user_analysis_filtered["count_before"].apply(classify_royalty)

        # 各層×グループの統計を計算
        royalty_stats = []

        for royalty_level in ["高ロイヤリティ（上位25%）", "中ロイヤリティ（中間50%）", "低ロイヤリティ（下位25%）"]:
            royalty_data = df_user_analysis_filtered[df_user_analysis_filtered["royalty"] == royalty_level]

            for group_roy in ["Treatment", "Control"]:
                group_data_roy = royalty_data[royalty_data["group"] == group_roy]

                if len(group_data_roy) > 0:
                    royalty_stats.append({
                        "ロイヤリティ": royalty_level,
                        "グループ": group_roy,
                        "ユーザー数": len(group_data_roy),
                        "投稿数(前)平均": group_data_roy["count_before"].mean(),
                        "投稿数(後)平均": group_data_roy["count_after"].mean(),
                        "比率(後/前)平均": group_data_roy["ratio"].mean(),
                        "比率(後/前)中央値": group_data_roy["ratio"].median(),
                        "1日あたり(前)": group_data_roy["rate_before"].mean(),
                        "1日あたり(後)": group_data_roy["rate_after"].mean(),
                    })

        df_royalty_stats = pd.DataFrame(royalty_stats)

        # 表示用にフォーマット
        df_royalty_display = df_royalty_stats.copy()
        df_royalty_display["投稿数(前)平均"] = df_royalty_display["投稿数(前)平均"].apply(lambda x: f"{x:.1f}")
        df_royalty_display["投稿数(後)平均"] = df_royalty_display["投稿数(後)平均"].apply(lambda x: f"{x:.1f}")
        df_royalty_display["比率(後/前)平均"] = df_royalty_display["比率(後/前)平均"].apply(lambda x: f"{x:.2f}x")
        df_royalty_display["比率(後/前)中央値"] = df_royalty_display["比率(後/前)中央値"].apply(lambda x: f"{x:.2f}x")
        df_royalty_display["1日あたり(前)"] = df_royalty_display["1日あたり(前)"].apply(lambda x: f"{x:.4f}")
        df_royalty_display["1日あたり(後)"] = df_royalty_display["1日あたり(後)"].apply(lambda x: f"{x:.4f}")

        # サマリー計算
        high_t = df_user_analysis_filtered[(df_user_analysis_filtered["royalty"] == "高ロイヤリティ（上位25%）") & (df_user_analysis_filtered["group"] == "Treatment")]
        high_c = df_user_analysis_filtered[(df_user_analysis_filtered["royalty"] == "高ロイヤリティ（上位25%）") & (df_user_analysis_filtered["group"] == "Control")]
        low_t = df_user_analysis_filtered[(df_user_analysis_filtered["royalty"] == "低ロイヤリティ（下位25%）") & (df_user_analysis_filtered["group"] == "Treatment")]
        low_c = df_user_analysis_filtered[(df_user_analysis_filtered["royalty"] == "低ロイヤリティ（下位25%）") & (df_user_analysis_filtered["group"] == "Control")]

        # 実際の数値を計算
        # 高ロイヤリティ
        high_t_before = high_t["count_before"].mean() if len(high_t) > 0 else 0
        high_t_after = high_t["count_after"].mean() if len(high_t) > 0 else 0
        high_t_change = high_t_after - high_t_before
        high_c_before = high_c["count_before"].mean() if len(high_c) > 0 else 0
        high_c_after = high_c["count_after"].mean() if len(high_c) > 0 else 0
        high_c_change = high_c_after - high_c_before

        # 低ロイヤリティ
        low_t_before = low_t["count_before"].mean() if len(low_t) > 0 else 0
        low_t_after = low_t["count_after"].mean() if len(low_t) > 0 else 0
        low_t_change = low_t_after - low_t_before
        low_c_before = low_c["count_before"].mean() if len(low_c) > 0 else 0
        low_c_after = low_c["count_after"].mean() if len(low_c) > 0 else 0
        low_c_change = low_c_after - low_c_before

        # 1日あたり（正規化）
        high_t_rate_before = high_t["rate_before"].mean() if len(high_t) > 0 else 0
        high_t_rate_after = high_t["rate_after"].mean() if len(high_t) > 0 else 0
        high_c_rate_before = high_c["rate_before"].mean() if len(high_c) > 0 else 0
        high_c_rate_after = high_c["rate_after"].mean() if len(high_c) > 0 else 0

        low_t_rate_before = low_t["rate_before"].mean() if len(low_t) > 0 else 0
        low_t_rate_after = low_t["rate_after"].mean() if len(low_t) > 0 else 0
        low_c_rate_before = low_c["rate_before"].mean() if len(low_c) > 0 else 0
        low_c_rate_after = low_c["rate_after"].mean() if len(low_c) > 0 else 0

        summary_royalty_md = f"""
        ### 📊 ロイヤリティ層別サマリー

        **四分位数**:
        - Q1（下位25%閾値）: {q1:.1f} 件
        - Q3（上位25%閾値）: {q3:.1f} 件

        ---

        #### 👑 高ロイヤリティユーザー（前の投稿数 ≥ {q3:.0f}件）

        | 指標 | Treatment (n={len(high_t):,}) | Control (n={len(high_c):,}) |
        |------|-------------------------------|------------------------------|
        | **投稿数（前）平均** | {high_t_before:.1f} 件 | {high_c_before:.1f} 件 |
        | **投稿数（後）平均** | {high_t_after:.1f} 件 | {high_c_after:.1f} 件 |
        | **変化量（後-前）** | **{high_t_change:+.1f} 件** | **{high_c_change:+.1f} 件** |
        | 1日あたり（前） | {high_t_rate_before:.3f} | {high_c_rate_before:.3f} |
        | 1日あたり（後） | {high_t_rate_after:.3f} | {high_c_rate_after:.3f} |
        | 1日あたり変化 | {high_t_rate_after - high_t_rate_before:+.3f} | {high_c_rate_after - high_c_rate_before:+.3f} |

        **Treatment vs Control 差分**: 変化量で **{high_t_change - high_c_change:+.1f} 件** の差

        ---

        #### 🌱 低ロイヤリティユーザー（前の投稿数 ≤ {q1:.0f}件）

        | 指標 | Treatment (n={len(low_t):,}) | Control (n={len(low_c):,}) |
        |------|------------------------------|------------------------------|
        | **投稿数（前）平均** | {low_t_before:.1f} 件 | {low_c_before:.1f} 件 |
        | **投稿数（後）平均** | {low_t_after:.1f} 件 | {low_c_after:.1f} 件 |
        | **変化量（後-前）** | **{low_t_change:+.1f} 件** | **{low_c_change:+.1f} 件** |
        | 1日あたり（前） | {low_t_rate_before:.3f} | {low_c_rate_before:.3f} |
        | 1日あたり（後） | {low_t_rate_after:.3f} | {low_c_rate_after:.3f} |
        | 1日あたり変化 | {low_t_rate_after - low_t_rate_before:+.3f} | {low_c_rate_after - low_c_rate_before:+.3f} |

        **Treatment vs Control 差分**: 変化量で **{low_t_change - low_c_change:+.1f} 件** の差

        ---

        #### 💡 解釈
        - **高ロイヤリティ層**: Treatment群は平均 {high_t_change:+.1f}件、Control群は {high_c_change:+.1f}件 変化
        - **低ロイヤリティ層**: Treatment群は平均 {low_t_change:+.1f}件、Control群は {low_c_change:+.1f}件 変化
        - ※ 比率（後/前）は前の投稿数が少ないと極端な値になるため、**絶対値での比較を推奨**
        """

        royalty_result = mo.vstack([
            mo.md(summary_royalty_md),
            mo.md("### 詳細テーブル"),
            mo.ui.table(df_royalty_display, selection=None)
        ])

    royalty_result
    return


@app.cell
def _(df_user_analysis_filtered, mo, np):
    """ロイヤリティ層別の投稿数変化可視化（絶対値）"""
    import matplotlib.pyplot as plt_roy
    import matplotlib as mpl_roy
    mpl_roy.rcParams['font.family'] = ['Hiragino Sans', 'Yu Gothic', 'Meiryo', 'sans-serif']

    if df_user_analysis_filtered.empty or "royalty" not in df_user_analysis_filtered.columns:
        royalty_chart = mo.md("⚠️ ロイヤリティデータがありません。")
    else:
        fig_roy, axes_roy = plt_roy.subplots(1, 2, figsize=(16, 6))

        royalty_levels = ["高ロイヤリティ（上位25%）", "低ロイヤリティ（下位25%）"]
        x_roy = np.arange(len(royalty_levels))
        width_roy = 0.2

        # 各層の実際の投稿数を取得
        t_before = []
        t_after = []
        c_before = []
        c_after = []
        for level in royalty_levels:
            t_data = df_user_analysis_filtered[(df_user_analysis_filtered["royalty"] == level) & (df_user_analysis_filtered["group"] == "Treatment")]
            c_data = df_user_analysis_filtered[(df_user_analysis_filtered["royalty"] == level) & (df_user_analysis_filtered["group"] == "Control")]
            t_before.append(t_data["count_before"].mean() if len(t_data) > 0 else 0)
            t_after.append(t_data["count_after"].mean() if len(t_data) > 0 else 0)
            c_before.append(c_data["count_before"].mean() if len(c_data) > 0 else 0)
            c_after.append(c_data["count_after"].mean() if len(c_data) > 0 else 0)

        # 左グラフ: 前後の投稿数（絶対値）
        bars1 = axes_roy[0].bar(x_roy - 1.5*width_roy, t_before, width_roy, label='Treatment前', color='#3498db', alpha=0.6)
        bars2 = axes_roy[0].bar(x_roy - 0.5*width_roy, t_after, width_roy, label='Treatment後', color='#3498db', alpha=1.0)
        bars3 = axes_roy[0].bar(x_roy + 0.5*width_roy, c_before, width_roy, label='Control前', color='#e74c3c', alpha=0.6)
        bars4 = axes_roy[0].bar(x_roy + 1.5*width_roy, c_after, width_roy, label='Control後', color='#e74c3c', alpha=1.0)

        axes_roy[0].set_xlabel('ロイヤリティ層')
        axes_roy[0].set_ylabel('平均投稿数（件）')
        axes_roy[0].set_title('ロイヤリティ層別 × グループ別 投稿数（前後）')
        axes_roy[0].set_xticks(x_roy)
        axes_roy[0].set_xticklabels(["高ロイヤリティ\n(上位25%)", "低ロイヤリティ\n(下位25%)"])
        axes_roy[0].legend()
        axes_roy[0].grid(axis='y', alpha=0.3)

        # 値ラベルを追加
        for bars in [bars1, bars2, bars3, bars4]:
            for bar in bars:
                height = bar.get_height()
                axes_roy[0].annotate(f'{height:.1f}',
                              xy=(bar.get_x() + bar.get_width() / 2, height),
                              xytext=(0, 3),
                              textcoords="offset points",
                              ha='center', va='bottom', fontsize=8)

        # 右グラフ: 変化量（後-前）
        t_change_roy = [t_after[idx_roy] - t_before[idx_roy] for idx_roy in range(len(royalty_levels))]
        c_change_roy = [c_after[idx_roy] - c_before[idx_roy] for idx_roy in range(len(royalty_levels))]

        bars5 = axes_roy[1].bar(x_roy - width_roy/2, t_change_roy, width_roy*1.5, label='Treatment', color='#3498db', alpha=0.8)
        bars6 = axes_roy[1].bar(x_roy + width_roy/2, c_change_roy, width_roy*1.5, label='Control', color='#e74c3c', alpha=0.8)

        axes_roy[1].set_xlabel('ロイヤリティ層')
        axes_roy[1].set_ylabel('変化量（後-前、件）')
        axes_roy[1].set_title('ロイヤリティ層別 × グループ別 投稿数変化量')
        axes_roy[1].set_xticks(x_roy)
        axes_roy[1].set_xticklabels(["高ロイヤリティ\n(上位25%)", "低ロイヤリティ\n(下位25%)"])
        axes_roy[1].axhline(y=0, color='gray', linestyle='--', alpha=0.5)
        axes_roy[1].legend()
        axes_roy[1].grid(axis='y', alpha=0.3)

        # 値ラベルを追加
        for bar in bars5:
            height = bar.get_height()
            axes_roy[1].annotate(f'{height:+.1f}',
                          xy=(bar.get_x() + bar.get_width() / 2, height),
                          xytext=(0, 3 if height >= 0 else -12),
                          textcoords="offset points",
                          ha='center', va='bottom', fontsize=10, fontweight='bold')
        for bar in bars6:
            height = bar.get_height()
            axes_roy[1].annotate(f'{height:+.1f}',
                          xy=(bar.get_x() + bar.get_width() / 2, height),
                          xytext=(0, 3 if height >= 0 else -12),
                          textcoords="offset points",
                          ha='center', va='bottom', fontsize=10, fontweight='bold')

        plt_roy.tight_layout()
        royalty_chart = fig_roy

    royalty_chart
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 💖 Step 6.7: エンゲージメント分析

    投稿あたりのエンゲージメント（いいね・RT・リプライ）をTreatment/Control群で比較します。
    """)
    return


@app.cell
def _(DATA_END_DATE, DATA_START_DATE, df_all_posts, df_user_baseline, mo, pd):
    """エンゲージメント分析（投稿あたりの平均）"""

    if df_user_baseline.empty:
        engagement_result = mo.md("⚠️ 分析に必要なデータが揃っていません。")
        df_engagement = pd.DataFrame()
    else:
        engagement_records = []

        for _, row_eng in df_user_baseline.iterrows():
            uid_eng = row_eng["account_id"]
            group_eng = row_eng["group"]
            baseline_eng = row_eng["baseline_date"]

            # ユーザーの投稿を取得（メンバー名タグを含む）
            user_posts_eng = df_all_posts[
                (df_all_posts["account_id"] == uid_eng) &
                (df_all_posts["has_member_tag"]) &
                (df_all_posts["created_at"] >= DATA_START_DATE) &
                (df_all_posts["created_at"] <= DATA_END_DATE)
            ]

            # 前後に分割
            posts_before_eng = user_posts_eng[user_posts_eng["created_at"] < baseline_eng]
            posts_after_eng = user_posts_eng[user_posts_eng["created_at"] >= baseline_eng]

            # 前の期間のエンゲージメント
            if len(posts_before_eng) > 0:
                like_before = posts_before_eng["like_count"].mean()
                rt_before = posts_before_eng["retweet_count"].mean()
                reply_before = posts_before_eng["reply_count"].mean()
            else:
                like_before = 0
                rt_before = 0
                reply_before = 0

            # 後の期間のエンゲージメント
            if len(posts_after_eng) > 0:
                like_after = posts_after_eng["like_count"].mean()
                rt_after = posts_after_eng["retweet_count"].mean()
                reply_after = posts_after_eng["reply_count"].mean()
            else:
                like_after = 0
                rt_after = 0
                reply_after = 0

            engagement_records.append({
                "account_id": uid_eng,
                "group": group_eng,
                "posts_before": len(posts_before_eng),
                "posts_after": len(posts_after_eng),
                "like_before": like_before,
                "like_after": like_after,
                "rt_before": rt_before,
                "rt_after": rt_after,
                "reply_before": reply_before,
                "reply_after": reply_after,
            })

        df_engagement = pd.DataFrame(engagement_records)

        # 前後両方に投稿があるユーザーのみ
        df_engagement_filtered = df_engagement[
            (df_engagement["posts_before"] >= 1) &
            (df_engagement["posts_after"] >= 1)
        ]

        # グループ別サマリー
        treatment_eng = df_engagement_filtered[df_engagement_filtered["group"] == "Treatment"]
        control_eng = df_engagement_filtered[df_engagement_filtered["group"] == "Control"]

        engagement_result = mo.md(f"""
        ### エンゲージメント分析結果（投稿あたり平均）

        #### いいね数
        | グループ | 前 | 後 | 変化 |
        |---------|-----|-----|------|
        | Treatment | {treatment_eng['like_before'].mean():.2f} | {treatment_eng['like_after'].mean():.2f} | {treatment_eng['like_after'].mean() - treatment_eng['like_before'].mean():+.2f} |
        | Control | {control_eng['like_before'].mean():.2f} | {control_eng['like_after'].mean():.2f} | {control_eng['like_after'].mean() - control_eng['like_before'].mean():+.2f} |

        #### RT数
        | グループ | 前 | 後 | 変化 |
        |---------|-----|-----|------|
        | Treatment | {treatment_eng['rt_before'].mean():.2f} | {treatment_eng['rt_after'].mean():.2f} | {treatment_eng['rt_after'].mean() - treatment_eng['rt_before'].mean():+.2f} |
        | Control | {control_eng['rt_before'].mean():.2f} | {control_eng['rt_after'].mean():.2f} | {control_eng['rt_after'].mean() - control_eng['rt_before'].mean():+.2f} |

        #### リプライ数
        | グループ | 前 | 後 | 変化 |
        |---------|-----|-----|------|
        | Treatment | {treatment_eng['reply_before'].mean():.2f} | {treatment_eng['reply_after'].mean():.2f} | {treatment_eng['reply_after'].mean() - treatment_eng['reply_before'].mean():+.2f} |
        | Control | {control_eng['reply_before'].mean():.2f} | {control_eng['reply_after'].mean():.2f} | {control_eng['reply_after'].mean() - control_eng['reply_before'].mean():+.2f} |

        #### 対象ユーザー数
        - Treatment群: {len(treatment_eng):,} 人
        - Control群: {len(control_eng):,} 人
        """)

    engagement_result
    return (df_engagement,)


@app.cell
def _(mo):
    mo.md("""
    ### 💖 Step 6.7.1: エンゲージメント可視化（箱ひげ図）
    """)
    return


@app.cell
def _(df_engagement, mo):
    """エンゲージメントの分布比較（箱ひげ図）"""
    import matplotlib.pyplot as plt_engbox
    import matplotlib as mpl_engbox
    mpl_engbox.rcParams['font.family'] = ['Hiragino Sans', 'Yu Gothic', 'Meiryo', 'sans-serif']

    if df_engagement.empty:
        engagement_box_chart = mo.md("⚠️ エンゲージメントデータがありません。")
    else:
        # 前後両方に投稿があるユーザーのみ
        df_eng_filtered = df_engagement[
            (df_engagement["posts_before"] >= 1) &
            (df_engagement["posts_after"] >= 1)
        ].copy()

        treatment_eng_box = df_eng_filtered[df_eng_filtered["group"] == "Treatment"]
        control_eng_box = df_eng_filtered[df_eng_filtered["group"] == "Control"]

        fig_eng_box, axes_eng_box = plt_engbox.subplots(1, 3, figsize=(15, 5))

        # いいね数の箱ひげ図（NA値を除去）
        box_data_like = [
            treatment_eng_box["like_before"].dropna().astype(float),
            treatment_eng_box["like_after"].dropna().astype(float),
            control_eng_box["like_before"].dropna().astype(float),
            control_eng_box["like_after"].dropna().astype(float),
        ]
        bp_like = axes_eng_box[0].boxplot(
            box_data_like,
            labels=["T前", "T後", "C前", "C後"],
            patch_artist=True
        )
        colors_like = ['#3498db', '#2980b9', '#e74c3c', '#c0392b']
        for patch, box_color in zip(bp_like['boxes'], colors_like):
            patch.set_facecolor(box_color)
            patch.set_alpha(0.7)
        axes_eng_box[0].set_ylabel('投稿あたり平均いいね数')
        axes_eng_box[0].set_title('いいね数の分布')
        axes_eng_box[0].grid(axis='y', alpha=0.3)

        # RT数の箱ひげ図（NA値を除去）
        box_data_rt = [
            treatment_eng_box["rt_before"].dropna().astype(float),
            treatment_eng_box["rt_after"].dropna().astype(float),
            control_eng_box["rt_before"].dropna().astype(float),
            control_eng_box["rt_after"].dropna().astype(float),
        ]
        bp_rt = axes_eng_box[1].boxplot(
            box_data_rt,
            labels=["T前", "T後", "C前", "C後"],
            patch_artist=True
        )
        for patch, box_color in zip(bp_rt['boxes'], colors_like):
            patch.set_facecolor(box_color)
            patch.set_alpha(0.7)
        axes_eng_box[1].set_ylabel('投稿あたり平均RT数')
        axes_eng_box[1].set_title('RT数の分布')
        axes_eng_box[1].grid(axis='y', alpha=0.3)

        # リプライ数の箱ひげ図（NA値を除去）
        box_data_reply = [
            treatment_eng_box["reply_before"].dropna().astype(float),
            treatment_eng_box["reply_after"].dropna().astype(float),
            control_eng_box["reply_before"].dropna().astype(float),
            control_eng_box["reply_after"].dropna().astype(float),
        ]
        bp_reply = axes_eng_box[2].boxplot(
            box_data_reply,
            labels=["T前", "T後", "C前", "C後"],
            patch_artist=True
        )
        for patch, box_color in zip(bp_reply['boxes'], colors_like):
            patch.set_facecolor(box_color)
            patch.set_alpha(0.7)
        axes_eng_box[2].set_ylabel('投稿あたり平均リプライ数')
        axes_eng_box[2].set_title('リプライ数の分布')
        axes_eng_box[2].grid(axis='y', alpha=0.3)

        plt_engbox.tight_layout()
        engagement_box_chart = fig_eng_box

    engagement_box_chart
    return


@app.cell
def _(df_engagement, mo, np):
    """エンゲージメントの前後比較バーチャート"""
    import matplotlib.pyplot as plt_engbar
    import matplotlib as mpl_engbar
    mpl_engbar.rcParams['font.family'] = ['Hiragino Sans', 'Yu Gothic', 'Meiryo', 'sans-serif']

    if df_engagement.empty:
        engagement_bar_chart = mo.md("⚠️ エンゲージメントデータがありません。")
    else:
        df_eng_filt = df_engagement[
            (df_engagement["posts_before"] >= 1) &
            (df_engagement["posts_after"] >= 1)
        ]

        treatment_eng_bar = df_eng_filt[df_eng_filt["group"] == "Treatment"]
        control_eng_bar = df_eng_filt[df_eng_filt["group"] == "Control"]

        fig_eng_bar, ax_eng_bar = plt_engbar.subplots(figsize=(12, 6))

        metrics_eng = ['いいね', 'RT', 'リプライ']
        x_eng = np.arange(len(metrics_eng))
        width_eng = 0.2

        # Treatment前
        t_before_eng = [
            treatment_eng_bar["like_before"].mean(),
            treatment_eng_bar["rt_before"].mean(),
            treatment_eng_bar["reply_before"].mean(),
        ]
        # Treatment後
        t_after_eng = [
            treatment_eng_bar["like_after"].mean(),
            treatment_eng_bar["rt_after"].mean(),
            treatment_eng_bar["reply_after"].mean(),
        ]
        # Control前
        c_before_eng = [
            control_eng_bar["like_before"].mean(),
            control_eng_bar["rt_before"].mean(),
            control_eng_bar["reply_before"].mean(),
        ]
        # Control後
        c_after_eng = [
            control_eng_bar["like_after"].mean(),
            control_eng_bar["rt_after"].mean(),
            control_eng_bar["reply_after"].mean(),
        ]

        ax_eng_bar.bar(x_eng - 1.5*width_eng, t_before_eng, width_eng, label='Treatment前', color='#3498db', alpha=0.7)
        ax_eng_bar.bar(x_eng - 0.5*width_eng, t_after_eng, width_eng, label='Treatment後', color='#2980b9', alpha=0.9)
        ax_eng_bar.bar(x_eng + 0.5*width_eng, c_before_eng, width_eng, label='Control前', color='#e74c3c', alpha=0.7)
        ax_eng_bar.bar(x_eng + 1.5*width_eng, c_after_eng, width_eng, label='Control後', color='#c0392b', alpha=0.9)

        ax_eng_bar.set_xlabel('エンゲージメント種別')
        ax_eng_bar.set_ylabel('投稿あたり平均数')
        ax_eng_bar.set_title('エンゲージメント前後比較（Treatment vs Control）')
        ax_eng_bar.set_xticks(x_eng)
        ax_eng_bar.set_xticklabels(metrics_eng)
        ax_eng_bar.legend()
        ax_eng_bar.grid(axis='y', alpha=0.3)

        plt_engbar.tight_layout()
        engagement_bar_chart = fig_eng_bar

    engagement_bar_chart
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 📅 Step 6.8: 継続率分析（7日中7日投稿率）

    7日間で何日投稿したかをカウントし、7日中7日投稿したユーザーの比率を比較します。
    """)
    return


@app.cell
def _(DATA_END_DATE, DATA_START_DATE, df_all_posts, df_user_baseline, mo, pd):
    """継続率分析（7日間の投稿日数）"""

    if df_user_baseline.empty:
        continuity_result = mo.md("⚠️ 分析に必要なデータが揃っていません。")
        df_continuity = pd.DataFrame()
    else:
        continuity_records = []

        for _, row_cont in df_user_baseline.iterrows():
            uid_cont = row_cont["account_id"]
            group_cont = row_cont["group"]
            baseline_cont = row_cont["baseline_date"]

            # ユーザーの投稿を取得（メンバー名タグを含む）
            user_posts_cont = df_all_posts[
                (df_all_posts["account_id"] == uid_cont) &
                (df_all_posts["has_member_tag"]) &
                (df_all_posts["created_at"] >= DATA_START_DATE) &
                (df_all_posts["created_at"] <= DATA_END_DATE)
            ]

            # 前の7日間: baseline_cont - 7日 〜 baseline_cont
            start_before_7d = baseline_cont - pd.Timedelta(days=7)
            posts_before_7d = user_posts_cont[
                (user_posts_cont["created_at"] >= start_before_7d) &
                (user_posts_cont["created_at"] < baseline_cont)
            ]

            # 後の7日間: baseline_cont 〜 baseline_cont + 7日
            end_after_7d = baseline_cont + pd.Timedelta(days=7)
            posts_after_7d = user_posts_cont[
                (user_posts_cont["created_at"] >= baseline_cont) &
                (user_posts_cont["created_at"] < end_after_7d)
            ]

            # ユニークな投稿日数をカウント
            days_posted_before = posts_before_7d["created_at"].dt.date.nunique() if len(posts_before_7d) > 0 else 0
            days_posted_after = posts_after_7d["created_at"].dt.date.nunique() if len(posts_after_7d) > 0 else 0

            # 7日中7日投稿したかどうか
            posted_7_of_7_before = (days_posted_before == 7)
            posted_7_of_7_after = (days_posted_after == 7)

            continuity_records.append({
                "account_id": uid_cont,
                "group": group_cont,
                "days_posted_before": days_posted_before,
                "days_posted_after": days_posted_after,
                "posted_7_of_7_before": posted_7_of_7_before,
                "posted_7_of_7_after": posted_7_of_7_after,
            })

        df_continuity = pd.DataFrame(continuity_records)

        # 前後両方に投稿があるユーザーのみ
        df_continuity_filtered = df_continuity[
            (df_continuity["days_posted_before"] >= 1) &
            (df_continuity["days_posted_after"] >= 1)
        ]

        # グループ別サマリー
        treatment_cont = df_continuity_filtered[df_continuity_filtered["group"] == "Treatment"]
        control_cont = df_continuity_filtered[df_continuity_filtered["group"] == "Control"]

        # 7日中7日投稿率を計算
        treatment_7of7_before_rate = (treatment_cont["posted_7_of_7_before"].sum() / len(treatment_cont) * 100) if len(treatment_cont) > 0 else 0
        treatment_7of7_after_rate = (treatment_cont["posted_7_of_7_after"].sum() / len(treatment_cont) * 100) if len(treatment_cont) > 0 else 0

        control_7of7_before_rate = (control_cont["posted_7_of_7_before"].sum() / len(control_cont) * 100) if len(control_cont) > 0 else 0
        control_7of7_after_rate = (control_cont["posted_7_of_7_after"].sum() / len(control_cont) * 100) if len(control_cont) > 0 else 0

        continuity_result = mo.md(f"""
        ### 継続率分析結果（7日間）

        #### 7日中7日投稿したユーザーの比率
        | グループ | 前の7日間 | 後の7日間 | 変化 |
        |---------|----------|----------|------|
        | Treatment | {treatment_7of7_before_rate:.1f}% | {treatment_7of7_after_rate:.1f}% | {treatment_7of7_after_rate - treatment_7of7_before_rate:+.1f}% |
        | Control | {control_7of7_before_rate:.1f}% | {control_7of7_after_rate:.1f}% | {control_7of7_after_rate - control_7of7_before_rate:+.1f}% |

        #### 平均投稿日数（7日間中）
        | グループ | 前の7日間 | 後の7日間 | 変化 |
        |---------|----------|----------|------|
        | Treatment | {treatment_cont['days_posted_before'].mean():.2f}日 | {treatment_cont['days_posted_after'].mean():.2f}日 | {treatment_cont['days_posted_after'].mean() - treatment_cont['days_posted_before'].mean():+.2f}日 |
        | Control | {control_cont['days_posted_before'].mean():.2f}日 | {control_cont['days_posted_after'].mean():.2f}日 | {control_cont['days_posted_after'].mean() - control_cont['days_posted_before'].mean():+.2f}日 |

        #### 対象ユーザー数
        - Treatment群: {len(treatment_cont):,} 人
        - Control群: {len(control_cont):,} 人
        """)

    continuity_result
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 😊 Step 6.9: ポジティブ率分析（Gemini API）

    投稿のセンチメント（ポジティブ/ニュートラル/ネガティブ）をGemini APIで判定し、
    IRC参加前後でポジティブな投稿の割合がどう変化したかを分析します。

    ⚠️ **注意**: API呼び出しを行うため、実行に時間がかかります（サンプリングにより制限）。
    """)
    return


@app.cell
def _(
    DATA_END_DATE,
    DATA_START_DATE,
    df_all_posts,
    df_user_baseline,
    mo,
    pd,
):
    """投稿サンプリング: 各ユーザーから前後それぞれ最大10件をランダムサンプリング"""
    from tqdm import tqdm as tqdm_sample

    SAMPLE_PER_USER_PERIOD = 10  # ユーザーあたり前後各10件
    MAX_TOTAL_SAMPLES = 3000  # 最大サンプル数

    if df_user_baseline.empty:
        sampling_result = mo.md("⚠️ ベースラインデータがありません。")
        df_sampled_posts = pd.DataFrame()
    else:
        sampled_records = []

        for _, row_sample in tqdm_sample(df_user_baseline.iterrows(), total=len(df_user_baseline), desc="📊 サンプリング中"):
            uid_sample = row_sample["account_id"]
            group_sample = row_sample["group"]
            baseline_sample = row_sample["baseline_date"]

            # ユーザーの投稿を取得（メンバー名タグを含む）
            user_posts_sample = df_all_posts[
                (df_all_posts["account_id"] == uid_sample) &
                (df_all_posts["has_member_tag"]) &
                (df_all_posts["created_at"] >= DATA_START_DATE) &
                (df_all_posts["created_at"] <= DATA_END_DATE)
            ].copy()

            # 前の期間
            posts_before_sample = user_posts_sample[user_posts_sample["created_at"] < baseline_sample]
            # 後の期間
            posts_after_sample = user_posts_sample[user_posts_sample["created_at"] >= baseline_sample]

            # ランダムサンプリング
            if len(posts_before_sample) > 0:
                n_sample_before = min(SAMPLE_PER_USER_PERIOD, len(posts_before_sample))
                sampled_before = posts_before_sample.sample(n=n_sample_before, random_state=42)
                for _, post_row in sampled_before.iterrows():
                    sampled_records.append({
                        "post_id": post_row["post_id"],
                        "account_id": uid_sample,
                        "group": group_sample,
                        "period": "before",
                        "content": post_row["content"],
                        "created_at": post_row["created_at"],
                    })

            if len(posts_after_sample) > 0:
                n_sample_after = min(SAMPLE_PER_USER_PERIOD, len(posts_after_sample))
                sampled_after = posts_after_sample.sample(n=n_sample_after, random_state=42)
                for _, post_row in sampled_after.iterrows():
                    sampled_records.append({
                        "post_id": post_row["post_id"],
                        "account_id": uid_sample,
                        "group": group_sample,
                        "period": "after",
                        "content": post_row["content"],
                        "created_at": post_row["created_at"],
                    })

        df_sampled_posts = pd.DataFrame(sampled_records)

        # 最大サンプル数を超える場合は更にサンプリング
        if len(df_sampled_posts) > MAX_TOTAL_SAMPLES:
            df_sampled_posts = df_sampled_posts.sample(n=MAX_TOTAL_SAMPLES, random_state=42)

        # サマリー
        treatment_before_count = len(df_sampled_posts[(df_sampled_posts["group"] == "Treatment") & (df_sampled_posts["period"] == "before")])
        treatment_after_count = len(df_sampled_posts[(df_sampled_posts["group"] == "Treatment") & (df_sampled_posts["period"] == "after")])
        control_before_count = len(df_sampled_posts[(df_sampled_posts["group"] == "Control") & (df_sampled_posts["period"] == "before")])
        control_after_count = len(df_sampled_posts[(df_sampled_posts["group"] == "Control") & (df_sampled_posts["period"] == "after")])

        sampling_result = mo.md(f"""
        ### 📋 サンプリング結果

        | グループ | 前 | 後 | 合計 |
        |---------|-----|-----|------|
        | Treatment | {treatment_before_count:,} | {treatment_after_count:,} | {treatment_before_count + treatment_after_count:,} |
        | Control | {control_before_count:,} | {control_after_count:,} | {control_before_count + control_after_count:,} |
        | **合計** | {treatment_before_count + control_before_count:,} | {treatment_after_count + control_after_count:,} | **{len(df_sampled_posts):,}** |

        ※ 各ユーザーから前後それぞれ最大{SAMPLE_PER_USER_PERIOD}件をサンプリング
        """)

    sampling_result
    return (df_sampled_posts,)


@app.cell
def _(df_sampled_posts, mo, os, pd):
    """Gemini APIでセンチメント分析を実行"""
    import google.generativeai as genai
    import time
    import json
    from tqdm import tqdm as tqdm_api

    # 分析実行フラグ（コストがかかるため手動で有効化）
    RUN_SENTIMENT_ANALYSIS = True
    BATCH_SIZE = 10  # 1リクエストで処理する投稿数

    if df_sampled_posts.empty:
        sentiment_result = mo.md("⚠️ サンプリングされた投稿がありません。")
        df_sentiment = pd.DataFrame()
    elif not RUN_SENTIMENT_ANALYSIS:
        sentiment_result = mo.md("""
        ⚠️ センチメント分析は無効化されています。

        実行するには `RUN_SENTIMENT_ANALYSIS = True` に設定してください。
        """)
        df_sentiment = pd.DataFrame()
    else:
        # API設定
        api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
        if not api_key:
            sentiment_result = mo.md("❌ 環境変数 `GOOGLE_API_KEY` または `GEMINI_API_KEY` が設定されていません。")
            df_sentiment = pd.DataFrame()
        else:
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel("gemini-3-flash-preview")

            # センチメント判定用プロンプト
            def create_sentiment_prompt(posts_batch):
                posts_text = "\n".join([
                    f"[{idx_prompt+1}] {post[:200]}" for idx_prompt, post in enumerate(posts_batch)
                ])
                return f"""以下の投稿のセンチメントを判定してください。
各投稿について、positive（ポジティブ）、neutral（ニュートラル）、negative（ネガティブ）のいずれかで判定してください。

投稿:
{posts_text}

JSONフォーマットで回答してください（例）:
{{"results": ["positive", "neutral", "negative", ...]}}

判定のみを返してください。説明は不要です。"""

            # バッチ処理
            all_sentiments = []
            error_logs = []  # エラーログを収集
            success_count = 0
            error_count = 0
            posts_list = df_sampled_posts["content"].tolist()
            post_ids = df_sampled_posts["post_id"].tolist()

            total_batches = (len(posts_list) + BATCH_SIZE - 1) // BATCH_SIZE

            # tqdmでプログレスバーを表示
            batch_indices = list(range(0, len(posts_list), BATCH_SIZE))
            for batch_start in tqdm_api(batch_indices, total=total_batches, desc="🤖 Gemini API センチメント分析"):
                batch_posts = posts_list[batch_start:batch_start+BATCH_SIZE]
                batch_ids = post_ids[batch_start:batch_start+BATCH_SIZE]

                try:
                    prompt = create_sentiment_prompt(batch_posts)
                    response = model.generate_content(prompt)
                    response_text = response.text.strip()

                    # JSONパース
                    # ```json ... ``` の形式を処理
                    json_text = response_text
                    if "```json" in json_text:
                        json_text = json_text.split("```json")[1].split("```")[0].strip()
                    elif "```" in json_text:
                        parts = json_text.split("```")
                        if len(parts) >= 2:
                            json_text = parts[1].strip()
                    
                    # JSON部分のみを抽出（{から}まで）
                    import re
                    json_match = re.search(r'\{.*\}', json_text, re.DOTALL)
                    if json_match:
                        json_text = json_match.group()

                    result = json.loads(json_text)
                    batch_sentiments = result.get("results", [])

                    # 結果を追加
                    if len(batch_sentiments) == len(batch_ids):
                        for idx_sent, (pid, sentiment_val) in enumerate(zip(batch_ids, batch_sentiments)):
                            all_sentiments.append({
                                "post_id": pid,
                                "sentiment": sentiment_val.lower() if isinstance(sentiment_val, str) else "neutral"
                            })
                        success_count += 1
                    else:
                        # 結果数が一致しない場合
                        error_logs.append(f"バッチ{batch_start}: 結果数不一致 (期待={len(batch_ids)}, 実際={len(batch_sentiments)})")
                        for pid in batch_ids:
                            all_sentiments.append({
                                "post_id": pid,
                                "sentiment": "neutral"
                            })
                        error_count += 1

                except Exception as e:
                    # エラー時はneutralとして処理し、エラーログに記録
                    error_logs.append(f"バッチ{batch_start}: {type(e).__name__}: {str(e)[:100]}")
                    for pid in batch_ids:
                        all_sentiments.append({
                            "post_id": pid,
                            "sentiment": "neutral"
                        })
                    error_count += 1

                # レート制限対策
                time.sleep(0.5)

            # 結果をDataFrameにマージ
            df_sentiment_results = pd.DataFrame(all_sentiments)
            df_sentiment = df_sampled_posts.merge(df_sentiment_results, on="post_id", how="left")
            df_sentiment["sentiment"] = df_sentiment["sentiment"].fillna("neutral")

            # サマリー
            sentiment_counts = df_sentiment.groupby(["group", "period", "sentiment"]).size().unstack(fill_value=0)

            # エラーログの表示
            error_display = ""
            if error_logs:
                error_display = f"""
                
                ⚠️ **エラー発生バッチ数**: {error_count} / {total_batches}
                
                <details>
                <summary>エラー詳細（クリックで展開）</summary>
                
                ```
                {chr(10).join(error_logs[:20])}
                {"..." if len(error_logs) > 20 else ""}
                ```
                
                </details>
                """

            sentiment_result = mo.vstack([
                mo.md(f"""
                ### ✅ センチメント分析完了

                **処理件数**: {len(df_sentiment):,} 件（{total_batches} バッチ）
                **成功**: {success_count} バッチ / **エラー**: {error_count} バッチ
                {error_display}
                """),
                mo.md("### センチメント分布"),
                mo.ui.table(sentiment_counts.reset_index(), selection=None),
            ])

    sentiment_result
    return (df_sentiment,)


@app.cell
def _(df_sentiment, mo, pd):
    """ポジティブ率の前後比較分析"""
    from scipy import stats as stats_pos

    if df_sentiment.empty or "sentiment" not in df_sentiment.columns:
        positive_analysis_result = mo.md("⚠️ センチメントデータがありません。")
        df_positive_summary = pd.DataFrame()
    else:
        # ポジティブ率を計算
        def calc_positive_rate(df_group):
            total = len(df_group)
            positive = len(df_group[df_group["sentiment"] == "positive"])
            return positive / total * 100 if total > 0 else 0

        # グループ×期間ごとの集計
        positive_rates = {}
        for group_pos in ["Treatment", "Control"]:
            for period_pos in ["before", "after"]:
                subset_pos = df_sentiment[
                    (df_sentiment["group"] == group_pos) &
                    (df_sentiment["period"] == period_pos)
                ]
                positive_rates[f"{group_pos}_{period_pos}"] = calc_positive_rate(subset_pos)

        # 変化量
        t_change_pos = positive_rates["Treatment_after"] - positive_rates["Treatment_before"]
        c_change_pos = positive_rates["Control_after"] - positive_rates["Control_before"]
        diff_tc_pos = t_change_pos - c_change_pos

        # 統計的検定（カイ二乗検定）
        # Treatment群の前後でポジティブ数を比較
        t_before_df = df_sentiment[(df_sentiment["group"] == "Treatment") & (df_sentiment["period"] == "before")]
        t_after_df = df_sentiment[(df_sentiment["group"] == "Treatment") & (df_sentiment["period"] == "after")]
        c_before_df = df_sentiment[(df_sentiment["group"] == "Control") & (df_sentiment["period"] == "before")]
        c_after_df = df_sentiment[(df_sentiment["group"] == "Control") & (df_sentiment["period"] == "after")]

        # 2x2 contingency table for Treatment vs Control (after period)
        t_after_pos = len(t_after_df[t_after_df["sentiment"] == "positive"])
        t_after_neg = len(t_after_df) - t_after_pos
        c_after_pos = len(c_after_df[c_after_df["sentiment"] == "positive"])
        c_after_neg = len(c_after_df) - c_after_pos

        contingency_table = [[t_after_pos, t_after_neg], [c_after_pos, c_after_neg]]

        try:
            chi2_pos, p_value_pos, dof_pos, expected_pos = stats_pos.chi2_contingency(contingency_table)
        except Exception:
            chi2_pos, p_value_pos = 0, 1.0

        # サマリーテーブル作成
        summary_data = [
            {
                "グループ": "Treatment（IRC参加）",
                "前": f"{positive_rates['Treatment_before']:.1f}%",
                "後": f"{positive_rates['Treatment_after']:.1f}%",
                "変化": f"{t_change_pos:+.1f}pt",
            },
            {
                "グループ": "Control（IRC非参加）",
                "前": f"{positive_rates['Control_before']:.1f}%",
                "後": f"{positive_rates['Control_after']:.1f}%",
                "変化": f"{c_change_pos:+.1f}pt",
            },
        ]
        df_positive_summary = pd.DataFrame(summary_data)

        # 有意性判定
        if p_value_pos < 0.01:
            significance_pos = "⭐⭐⭐ 非常に有意 (p < 0.01)"
        elif p_value_pos < 0.05:
            significance_pos = "⭐⭐ 有意 (p < 0.05)"
        elif p_value_pos < 0.10:
            significance_pos = "⭐ 弱い有意 (p < 0.10)"
        else:
            significance_pos = "有意差なし (p >= 0.10)"

        positive_analysis_result = mo.vstack([
            mo.md("### 😊 ポジティブ率の前後比較"),
            mo.ui.table(df_positive_summary, selection=None),
            mo.md(f"""
            ### 分析結果

            | 指標 | 値 |
            |------|-----|
            | Treatment変化 | {t_change_pos:+.1f}pt |
            | Control変化 | {c_change_pos:+.1f}pt |
            | **差分（T - C）** | **{diff_tc_pos:+.1f}pt** |
            | χ²統計量 | {chi2_pos:.4f} |
            | p値 | {p_value_pos:.4f} |
            | 有意性 | {significance_pos} |

            #### 解釈
            {"Treatment群（IRC参加者）はControl群と比較して、ポジティブ投稿率に有意な差があります。IRC参加がポジティブな投稿を促進する可能性があります。" if p_value_pos < 0.05 else "Treatment群とControl群のポジティブ投稿率には統計的に有意な差は認められませんでした。"}
            """),
        ])

    positive_analysis_result
    return (df_positive_summary,)


@app.cell
def _(df_sentiment, mo, np):
    """ポジティブ率の可視化"""
    import matplotlib.pyplot as plt_posvis
    import matplotlib as mpl_posvis
    mpl_posvis.rcParams['font.family'] = ['Hiragino Sans', 'Yu Gothic', 'Meiryo', 'sans-serif']

    if df_sentiment.empty or "sentiment" not in df_sentiment.columns:
        positive_chart = mo.md("⚠️ センチメントデータがありません。")
    else:
        # グループ×期間ごとのセンチメント分布
        fig_pos, axes_pos = plt_posvis.subplots(1, 2, figsize=(14, 5))

        # 左: ポジティブ率のバーチャート
        groups_posvis = ["Treatment", "Control"]
        periods_posvis = ["before", "after"]

        pos_rates = []
        for g_posvis in groups_posvis:
            for p_posvis in periods_posvis:
                subset_posvis = df_sentiment[(df_sentiment["group"] == g_posvis) & (df_sentiment["period"] == p_posvis)]
                rate = len(subset_posvis[subset_posvis["sentiment"] == "positive"]) / len(subset_posvis) * 100 if len(subset_posvis) > 0 else 0
                pos_rates.append(rate)

        x_pos = np.arange(2)
        width_pos = 0.35

        axes_pos[0].bar(x_pos - width_pos/2, [pos_rates[0], pos_rates[2]], width_pos, 
                       label='前', color='#3498db', alpha=0.8)
        axes_pos[0].bar(x_pos + width_pos/2, [pos_rates[1], pos_rates[3]], width_pos, 
                       label='後', color='#e74c3c', alpha=0.8)
        axes_pos[0].set_xlabel('グループ')
        axes_pos[0].set_ylabel('ポジティブ率 (%)')
        axes_pos[0].set_title('ポジティブ投稿率の前後比較')
        axes_pos[0].set_xticks(x_pos)
        axes_pos[0].set_xticklabels(groups_posvis)
        axes_pos[0].legend()
        axes_pos[0].grid(axis='y', alpha=0.3)

        # 右: センチメント分布（積み上げ棒グラフ）
        categories_posvis = ["T前", "T後", "C前", "C後"]
        sentiment_labels = ["positive", "neutral", "negative"]
        colors_sent = ['#27ae60', '#95a5a6', '#e74c3c']

        data_stacked = []
        for g_stack, p_stack in [("Treatment", "before"), ("Treatment", "after"), 
                     ("Control", "before"), ("Control", "after")]:
            subset_stack = df_sentiment[(df_sentiment["group"] == g_stack) & (df_sentiment["period"] == p_stack)]
            total = len(subset_stack)
            row = []
            for s_label in sentiment_labels:
                count = len(subset_stack[subset_stack["sentiment"] == s_label])
                row.append(count / total * 100 if total > 0 else 0)
            data_stacked.append(row)

        data_stacked = np.array(data_stacked)
        x_stacked = np.arange(len(categories_posvis))

        bottom_stacked = np.zeros(len(categories_posvis))
        for idx_stack, (sent, sent_color) in enumerate(zip(sentiment_labels, colors_sent)):
            axes_pos[1].bar(x_stacked, data_stacked[:, idx_stack], bottom=bottom_stacked, 
                           label=sent, color=sent_color, alpha=0.8)
            bottom_stacked += data_stacked[:, idx_stack]

        axes_pos[1].set_xlabel('グループ × 期間')
        axes_pos[1].set_ylabel('割合 (%)')
        axes_pos[1].set_title('センチメント分布')
        axes_pos[1].set_xticks(x_stacked)
        axes_pos[1].set_xticklabels(categories_posvis)
        axes_pos[1].legend()
        axes_pos[1].grid(axis='y', alpha=0.3)

        plt_posvis.tight_layout()
        positive_chart = fig_pos

    positive_chart
    return


@app.cell
def _(df_sentiment, mo, pd):
    """ポジティブ投稿の具体例を抽出"""

    if df_sentiment.empty or "sentiment" not in df_sentiment.columns:
        positive_examples = mo.md("⚠️ センチメントデータがありません。")
    else:
        # Treatment群の後期間でポジティブと判定された投稿を抽出
        positive_posts_treatment = df_sentiment[
            (df_sentiment["group"] == "Treatment") &
            (df_sentiment["period"] == "after") &
            (df_sentiment["sentiment"] == "positive")
        ].copy()

        # Control群の後期間でポジティブと判定された投稿も比較用に抽出
        positive_posts_control = df_sentiment[
            (df_sentiment["group"] == "Control") &
            (df_sentiment["period"] == "after") &
            (df_sentiment["sentiment"] == "positive")
        ].copy()

        # 表示用に整形
        def format_posts(df_posts, max_posts=15):
            if df_posts.empty:
                return pd.DataFrame()
            display_df = df_posts[["content", "created_at"]].head(max_posts).copy()
            display_df.columns = ["投稿内容", "投稿日時"]
            # 投稿内容を短縮表示
            display_df["投稿内容"] = display_df["投稿内容"].apply(
                lambda x: x[:150] + "..." if len(str(x)) > 150 else x
            )
            return display_df

        treatment_display = format_posts(positive_posts_treatment)
        control_display = format_posts(positive_posts_control)

        positive_examples = mo.vstack([
            mo.md(f"""
            ### 📝 ポジティブ投稿の具体例

            IRC参加がどのようなポジティブな投稿を生み出しているかを確認します。

            #### Treatment群（IRC参加後）のポジティブ投稿 ({len(positive_posts_treatment):,}件中上位15件)
            """),
            mo.ui.table(treatment_display, selection=None) if not treatment_display.empty else mo.md("（投稿がありません）"),
            mo.md(f"""
            ---
            #### Control群（IRC非参加）のポジティブ投稿 ({len(positive_posts_control):,}件中上位15件)
            """),
            mo.ui.table(control_display, selection=None) if not control_display.empty else mo.md("（投稿がありません）"),
            mo.md("""
            ---
            **比較ポイント**:
            - Treatment群の投稿はIRC参加後にどのような内容が増えているか
            - Control群と比較して、特徴的な表現やテーマがあるか
            """),
        ])

    positive_examples
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 📊 Step 7: 可視化

    ### 7-1. 1日あたり投稿数の比較（期間正規化）

    前後で期間が異なるため、**1日あたりの投稿数**で比較します。
    """)
    return


@app.cell
def _(df_user_analysis_filtered, mo, np):
    """1日あたり投稿数のバーチャート（フィルタ済みデータ）"""
    import matplotlib.pyplot as plt_avg
    import matplotlib as mpl_avg
    mpl_avg.rcParams['font.family'] = ['Hiragino Sans', 'Yu Gothic', 'Meiryo', 'sans-serif']

    if df_user_analysis_filtered.empty:
        avg_chart = mo.md("⚠️ 分析データがありません。")
    else:
        treatment_df_avg = df_user_analysis_filtered[df_user_analysis_filtered["group"] == "Treatment"]
        control_df_avg = df_user_analysis_filtered[df_user_analysis_filtered["group"] == "Control"]

        fig_avg, axes_avg = plt_avg.subplots(1, 2, figsize=(14, 5))

        # 左: 前後の1日あたり投稿数
        groups_avg = ["Treatment", "Control"]
        before_means_avg = [treatment_df_avg["rate_before"].mean(), control_df_avg["rate_before"].mean()]
        after_means_avg = [treatment_df_avg["rate_after"].mean(), control_df_avg["rate_after"].mean()]

        x_avg = np.arange(len(groups_avg))
        width_avg = 0.35

        axes_avg[0].bar(x_avg - width_avg/2, before_means_avg, width_avg, label='前', color='#3498db', alpha=0.8)
        axes_avg[0].bar(x_avg + width_avg/2, after_means_avg, width_avg, label='後', color='#e74c3c', alpha=0.8)
        axes_avg[0].set_xlabel('グループ')
        axes_avg[0].set_ylabel('1日あたり投稿数')
        axes_avg[0].set_title('メンバー名タグ投稿数（1日あたり・前後比較）')
        axes_avg[0].set_xticks(x_avg)
        axes_avg[0].set_xticklabels(groups_avg)
        axes_avg[0].legend()
        axes_avg[0].grid(axis='y', alpha=0.3)

        # 右: 1日あたり変化量
        changes_avg = [treatment_df_avg["rate_change"].mean(), control_df_avg["rate_change"].mean()]
        colors_avg = ['#27ae60' if c >= 0 else '#c0392b' for c in changes_avg]

        axes_avg[1].bar(groups_avg, changes_avg, color=colors_avg, alpha=0.8)
        axes_avg[1].set_xlabel('グループ')
        axes_avg[1].set_ylabel('1日あたり変化量')
        axes_avg[1].set_title('メンバー名タグ投稿数の変化量（1日あたり）')
        axes_avg[1].axhline(y=0, color='gray', linestyle='--', alpha=0.5)
        axes_avg[1].grid(axis='y', alpha=0.3)

        plt_avg.tight_layout()
        avg_chart = fig_avg

    avg_chart
    return


@app.cell
def _(mo):
    mo.md("""
    ### 7-2. 分布の比較（ヒストグラム）

    1日あたり変化量の分布を確認します。
    """)
    return


@app.cell
def _(df_user_analysis_filtered, mo, np):
    """1日あたり変化量の分布（ヒストグラム）- フィルタ済みデータ"""
    import matplotlib.pyplot as plt_hist
    import matplotlib as mpl_hist
    mpl_hist.rcParams['font.family'] = ['Hiragino Sans', 'Yu Gothic', 'Meiryo', 'sans-serif']

    if df_user_analysis_filtered.empty:
        hist_chart = mo.md("⚠️ 分析データがありません。")
    else:
        treatment_changes_hist = df_user_analysis_filtered[df_user_analysis_filtered["group"] == "Treatment"]["rate_change"]
        control_changes_hist = df_user_analysis_filtered[df_user_analysis_filtered["group"] == "Control"]["rate_change"]

        fig_hist, axes_hist = plt_hist.subplots(1, 2, figsize=(14, 5))

        # 共通のビン設定
        all_changes_hist = df_user_analysis_filtered["rate_change"]
        bins_hist = np.linspace(all_changes_hist.min(), all_changes_hist.max(), 30)

        # Treatment群
        axes_hist[0].hist(treatment_changes_hist, bins=bins_hist, color='#3498db', alpha=0.7, edgecolor='white')
        axes_hist[0].axvline(x=treatment_changes_hist.mean(), color='red', linestyle='--', 
                            label=f'平均: {treatment_changes_hist.mean():.4f}')
        axes_hist[0].axvline(x=0, color='gray', linestyle='-', alpha=0.5)
        axes_hist[0].set_xlabel('1日あたり変化量')
        axes_hist[0].set_ylabel('ユーザー数')
        axes_hist[0].set_title('Treatment群（IRC参加者）の1日あたり変化量分布')
        axes_hist[0].legend()
        axes_hist[0].grid(axis='y', alpha=0.3)

        # Control群
        axes_hist[1].hist(control_changes_hist, bins=bins_hist, color='#e74c3c', alpha=0.7, edgecolor='white')
        axes_hist[1].axvline(x=control_changes_hist.mean(), color='blue', linestyle='--', 
                            label=f'平均: {control_changes_hist.mean():.4f}')
        axes_hist[1].axvline(x=0, color='gray', linestyle='-', alpha=0.5)
        axes_hist[1].set_xlabel('1日あたり変化量')
        axes_hist[1].set_ylabel('ユーザー数')
        axes_hist[1].set_title('Control群（IRC非参加者）の1日あたり変化量分布')
        axes_hist[1].legend()
        axes_hist[1].grid(axis='y', alpha=0.3)

        plt_hist.tight_layout()
        hist_chart = fig_hist

    hist_chart
    return


@app.cell
def _(mo):
    mo.md("""
    ### 7-3. 分布の比較（箱ひげ図）
    """)
    return


@app.cell
def _(df_user_analysis_filtered, mo):
    """変化量の分布（箱ひげ図）- フィルタ済みデータ"""
    import matplotlib.pyplot as plt_boxplot
    import matplotlib as mpl_boxplot
    mpl_boxplot.rcParams['font.family'] = ['Hiragino Sans', 'Yu Gothic', 'Meiryo', 'sans-serif']

    if df_user_analysis_filtered.empty:
        box_chart = mo.md("⚠️ 分析データがありません。")
    else:
        fig_box, axes_box = plt_boxplot.subplots(1, 2, figsize=(14, 5))

        treatment_data_box = df_user_analysis_filtered[df_user_analysis_filtered["group"] == "Treatment"]
        control_data_box = df_user_analysis_filtered[df_user_analysis_filtered["group"] == "Control"]

        # 左: 比率（後/前）の箱ひげ図
        box_data_ratio = [treatment_data_box["ratio"].dropna(), control_data_box["ratio"].dropna()]
        bp1 = axes_box[0].boxplot(box_data_ratio, labels=["Treatment", "Control"], patch_artist=True)
        bp1['boxes'][0].set_facecolor('#3498db')
        bp1['boxes'][1].set_facecolor('#e74c3c')
        for box_item in bp1['boxes']:
            box_item.set_alpha(0.7)
        axes_box[0].axhline(y=1, color='gray', linestyle='--', alpha=0.5, label='変化なし (1.0x)')
        axes_box[0].set_ylabel('比率（後/前）')
        axes_box[0].set_title('比率（後/前）の分布比較')
        axes_box[0].grid(axis='y', alpha=0.3)

        # 右: 1日あたり変化量の箱ひげ図
        rate_data_box = [treatment_data_box["rate_change"], control_data_box["rate_change"]]
        bp2 = axes_box[1].boxplot(rate_data_box, labels=["Treatment", "Control"], patch_artist=True)
        bp2['boxes'][0].set_facecolor('#3498db')
        bp2['boxes'][1].set_facecolor('#e74c3c')
        for box_item2 in bp2['boxes']:
            box_item2.set_alpha(0.7)
        axes_box[1].axhline(y=0, color='gray', linestyle='--', alpha=0.5)
        axes_box[1].set_ylabel('1日あたり変化量')
        axes_box[1].set_title('1日あたり変化量の分布比較（参考）')
        axes_box[1].grid(axis='y', alpha=0.3)

        plt_boxplot.tight_layout()
        box_chart = fig_box

    box_chart
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 📋 Step 8: 統計的検定

    Treatment群とControl群の変化量に統計的に有意な差があるかを検定します。
    """)
    return


@app.cell
def _(df_user_analysis_filtered, mo):
    """統計的検定（t検定）- 比率（後/前）で比較"""
    from scipy import stats as stats_ratio

    if df_user_analysis_filtered.empty:
        test_result = mo.md("⚠️ 分析データがありません。")
    else:
        # 比率（後/前）を使用
        treatment_ratio_test = df_user_analysis_filtered[df_user_analysis_filtered["group"] == "Treatment"]["ratio"].dropna()
        control_ratio_test = df_user_analysis_filtered[df_user_analysis_filtered["group"] == "Control"]["ratio"].dropna()

        # 独立2標本t検定（Welch's t-test）
        t_stat_ratio, p_value_ratio = stats_ratio.ttest_ind(treatment_ratio_test, control_ratio_test, equal_var=False)

        # 効果量（Cohen's d）
        pooled_std_ratio = ((treatment_ratio_test.std()**2 + control_ratio_test.std()**2) / 2) ** 0.5
        cohens_d_ratio = (treatment_ratio_test.mean() - control_ratio_test.mean()) / pooled_std_ratio if pooled_std_ratio > 0 else 0

        # 有意性判定
        if p_value_ratio < 0.01:
            significance_ratio = "⭐⭐⭐ 非常に有意 (p < 0.01)"
        elif p_value_ratio < 0.05:
            significance_ratio = "⭐⭐ 有意 (p < 0.05)"
        elif p_value_ratio < 0.10:
            significance_ratio = "⭐ 弱い有意 (p < 0.10)"
        else:
            significance_ratio = "有意差なし (p >= 0.10)"

        # 効果量の解釈
        if abs(cohens_d_ratio) < 0.2:
            effect_size_ratio = "小さい"
        elif abs(cohens_d_ratio) < 0.5:
            effect_size_ratio = "中程度"
        elif abs(cohens_d_ratio) < 0.8:
            effect_size_ratio = "大きい"
        else:
            effect_size_ratio = "非常に大きい"

        test_result = mo.md(f"""
        ### 統計的検定結果（比率 後/前）

        | 指標 | 値 |
        |------|-----|
        | Treatment群 平均比率 | {treatment_ratio_test.mean():.2f}x |
        | Control群 平均比率 | {control_ratio_test.mean():.2f}x |
        | Treatment群 n | {len(treatment_ratio_test):,} |
        | Control群 n | {len(control_ratio_test):,} |
        | t統計量 | {t_stat_ratio:.4f} |
        | p値 | {p_value_ratio:.6f} |
        | 有意性 | {significance_ratio} |
        | Cohen's d（効果量） | {cohens_d_ratio:.4f} ({effect_size_ratio}) |

        #### 解釈
        {"Treatment群（IRC参加者）はControl群と比較して、比率（後/前）に統計的に有意な差があります。" if p_value_ratio < 0.05 else "Treatment群とControl群の比率（後/前）には統計的に有意な差は認められませんでした。"}

        ⚠️ **注意**: この結果は相関関係を示すものであり、因果関係を証明するものではありません。
        IRCチャレンジに参加するユーザーは、もともと活発なユーザーである可能性があります（セルフセレクションバイアス）。
        """)

    test_result
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 📊 Step 8.5: 4指標サマリーダッシュボード

    IRC参加の効果を **投稿数・投稿頻度・エンゲージメント・ポジティブ率** の4指標で一覧化します。
    """)
    return


@app.cell
def _(df_user_analysis_filtered, mo, pd):
    """4指標のサマリーダッシュボード（ポジティブ率は後で追加）"""
    from scipy import stats as stats_dashboard

    if df_user_analysis_filtered.empty:
        dashboard_result = mo.md("⚠️ 分析データがありません。")
        df_dashboard = pd.DataFrame()
    else:
        treatment_dash = df_user_analysis_filtered[df_user_analysis_filtered["group"] == "Treatment"]
        control_dash = df_user_analysis_filtered[df_user_analysis_filtered["group"] == "Control"]

        # 指標1: 投稿数
        t_count_before = treatment_dash["count_before"].mean()
        t_count_after = treatment_dash["count_after"].mean()
        c_count_before = control_dash["count_before"].mean()
        c_count_after = control_dash["count_after"].mean()
        t_count_change = (t_count_after - t_count_before) / t_count_before * 100 if t_count_before > 0 else 0
        c_count_change = (c_count_after - c_count_before) / c_count_before * 100 if c_count_before > 0 else 0

        # 指標2: 投稿頻度（1日あたり）
        t_rate_before = treatment_dash["rate_before"].mean()
        t_rate_after = treatment_dash["rate_after"].mean()
        c_rate_before = control_dash["rate_before"].mean()
        c_rate_after = control_dash["rate_after"].mean()
        t_rate_change = (t_rate_after - t_rate_before) / t_rate_before * 100 if t_rate_before > 0 else 0
        c_rate_change = (c_rate_after - c_rate_before) / c_rate_before * 100 if c_rate_before > 0 else 0

        # 投稿頻度のt検定
        _, p_rate = stats_dashboard.ttest_ind(
            treatment_dash["rate_after"] - treatment_dash["rate_before"],
            control_dash["rate_after"] - control_dash["rate_before"],
            equal_var=False
        )

        # ダッシュボード用DataFrame作成
        dashboard_data = [
            {
                "指標": "📝 投稿数",
                "T前": f"{t_count_before:.1f}",
                "T後": f"{t_count_after:.1f}",
                "T変化": f"{t_count_change:+.0f}%",
                "C前": f"{c_count_before:.1f}",
                "C後": f"{c_count_after:.1f}",
                "C変化": f"{c_count_change:+.0f}%",
                "差分(T-C)": f"{t_count_change - c_count_change:+.0f}pt",
                "p値": "-",
            },
            {
                "指標": "📈 投稿頻度(/日)",
                "T前": f"{t_rate_before:.3f}",
                "T後": f"{t_rate_after:.3f}",
                "T変化": f"{t_rate_change:+.0f}%",
                "C前": f"{c_rate_before:.3f}",
                "C後": f"{c_rate_after:.3f}",
                "C変化": f"{c_rate_change:+.0f}%",
                "差分(T-C)": f"{t_rate_change - c_rate_change:+.0f}pt",
                "p値": f"{p_rate:.4f}" if p_rate >= 0.0001 else "<0.0001",
            },
        ]

        df_dashboard = pd.DataFrame(dashboard_data)

        # 有意性マーク追加
        def add_significance(p_str):
            if p_str == "-":
                return "-"
            try:
                p = float(p_str.replace("<", ""))
                if p < 0.01:
                    return f"{p_str} ⭐⭐⭐"
                elif p < 0.05:
                    return f"{p_str} ⭐⭐"
                elif p < 0.10:
                    return f"{p_str} ⭐"
                return p_str
            except ValueError:
                return p_str

        df_dashboard["p値"] = df_dashboard["p値"].apply(add_significance)

        dashboard_result = mo.vstack([
            mo.md("""
            ### 📊 IRC効果サマリー（4指標）

            | 凡例 | 意味 |
            |------|------|
            | T | Treatment群（IRC参加者） |
            | C | Control群（IRC非参加者） |
            | ⭐⭐⭐ | p < 0.01（非常に有意） |
            | ⭐⭐ | p < 0.05（有意） |
            | ⭐ | p < 0.10（弱い有意） |

            ※ エンゲージメントとポジティブ率は下のセクションで分析後に追加されます。
            """),
            mo.ui.table(df_dashboard, selection=None),
        ])

    dashboard_result
    return (df_dashboard,)


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 📊 Step 9: 全体サマリー
    """)
    return


@app.cell
def _(df_user_analysis_filtered, mo, pd):
    """全体サマリーテーブル（フィルタ済みデータ）"""

    if df_user_analysis_filtered.empty:
        summary_table = mo.md("⚠️ 分析データがありません。")
    else:
        # グループ別サマリー
        summary_df = df_user_analysis_filtered.groupby("group").agg({
            "account_id": "count",
            "count_before": ["mean", "median"],
            "count_after": ["mean", "median"],
            "days_before": ["mean"],
            "days_after": ["mean"],
            "rate_before": ["mean", "median"],
            "rate_after": ["mean", "median"],
            "ratio": ["mean", "median", "std"],
        }).round(4)

        # カラム名をフラット化
        summary_df.columns = ['_'.join(col).strip() for col in summary_df.columns.values]
        summary_df = summary_df.reset_index()

        # 表示用に整形
        display_summary = pd.DataFrame({
            "グループ": summary_df["group"],
            "ユーザー数": summary_df["account_id_count"].astype(int),
            "平均期間(前)": summary_df["days_before_mean"].round(1).astype(str) + "日",
            "平均期間(後)": summary_df["days_after_mean"].round(1).astype(str) + "日",
            "投稿数(前)平均": summary_df["count_before_mean"].round(2),
            "投稿数(後)平均": summary_df["count_after_mean"].round(2),
            "1日あたり(前)": summary_df["rate_before_mean"].round(4),
            "1日あたり(後)": summary_df["rate_after_mean"].round(4),
            "⭐比率(後/前)平均": summary_df["ratio_mean"].apply(lambda x: f"{x:.2f}x"),
            "比率(後/前)中央値": summary_df["ratio_median"].apply(lambda x: f"{x:.2f}x"),
            "比率標準偏差": summary_df["ratio_std"].round(2),
        })

        summary_table = mo.vstack([
            mo.md("### 📊 グループ別サマリー（前後両方に投稿があるユーザーのみ）"),
            mo.ui.table(display_summary, selection=None),
        ])

    summary_table
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 🎯 Step 9.5: 4指標統合ダッシュボード

    IRC参加の効果を **投稿数・投稿頻度・エンゲージメント・ポジティブ率** の4指標で一覧化します。
    """)
    return


@app.cell
def _(df_engagement, df_sentiment, df_user_analysis_filtered, mo, pd):
    """4指標の統合サマリーダッシュボード"""
    from scipy import stats as stats_final

    if df_user_analysis_filtered.empty:
        final_dashboard = mo.md("⚠️ 分析データがありません。")
    else:
        treatment_final = df_user_analysis_filtered[df_user_analysis_filtered["group"] == "Treatment"]
        control_final = df_user_analysis_filtered[df_user_analysis_filtered["group"] == "Control"]

        # 指標1: 投稿数
        t_count_b = treatment_final["count_before"].astype(float).mean()
        t_count_a = treatment_final["count_after"].astype(float).mean()
        c_count_b = control_final["count_before"].astype(float).mean()
        c_count_a = control_final["count_after"].astype(float).mean()
        t_count_chg = (t_count_a - t_count_b) / t_count_b * 100 if t_count_b > 0 else 0
        c_count_chg = (c_count_a - c_count_b) / c_count_b * 100 if c_count_b > 0 else 0

        # 指標2: 投稿頻度
        t_rate_b = treatment_final["rate_before"].astype(float).mean()
        t_rate_a = treatment_final["rate_after"].astype(float).mean()
        c_rate_b = control_final["rate_before"].astype(float).mean()
        c_rate_a = control_final["rate_after"].astype(float).mean()
        t_rate_chg = (t_rate_a - t_rate_b) / t_rate_b * 100 if t_rate_b > 0 else 0
        c_rate_chg = (c_rate_a - c_rate_b) / c_rate_b * 100 if c_rate_b > 0 else 0
        _, p_rate_final = stats_final.ttest_ind(
            (treatment_final["rate_after"].astype(float) - treatment_final["rate_before"].astype(float)).dropna(),
            (control_final["rate_after"].astype(float) - control_final["rate_before"].astype(float)).dropna(),
            equal_var=False
        )

        # 指標3: エンゲージメント（いいね数）
        t_eng_b, t_eng_a, c_eng_b, c_eng_a = 0, 0, 0, 0
        t_eng_chg, c_eng_chg = 0, 0
        p_eng_final = 1.0

        if not df_engagement.empty:
            df_eng_f = df_engagement[
                (df_engagement["posts_before"] >= 1) & (df_engagement["posts_after"] >= 1)
            ]
            t_eng_df = df_eng_f[df_eng_f["group"] == "Treatment"]
            c_eng_df = df_eng_f[df_eng_f["group"] == "Control"]

            if len(t_eng_df) > 0 and len(c_eng_df) > 0:
                t_eng_b = t_eng_df["like_before"].astype(float).mean()
                t_eng_a = t_eng_df["like_after"].astype(float).mean()
                c_eng_b = c_eng_df["like_before"].astype(float).mean()
                c_eng_a = c_eng_df["like_after"].astype(float).mean()
                t_eng_chg = (t_eng_a - t_eng_b) / t_eng_b * 100 if t_eng_b > 0 else 0
                c_eng_chg = (c_eng_a - c_eng_b) / c_eng_b * 100 if c_eng_b > 0 else 0

                _, p_eng_final = stats_final.ttest_ind(
                    (t_eng_df["like_after"].astype(float) - t_eng_df["like_before"].astype(float)).dropna(),
                    (c_eng_df["like_after"].astype(float) - c_eng_df["like_before"].astype(float)).dropna(),
                    equal_var=False
                )

        # 指標4: ポジティブ率
        t_pos_b, t_pos_a, c_pos_b, c_pos_a = 0, 0, 0, 0
        t_pos_chg, c_pos_chg = 0, 0
        p_pos_final = 1.0

        if not df_sentiment.empty and "sentiment" in df_sentiment.columns:
            def calc_pos_rate(df_grp):
                total = len(df_grp)
                pos = len(df_grp[df_grp["sentiment"] == "positive"])
                return pos / total * 100 if total > 0 else 0

            t_pos_b = calc_pos_rate(df_sentiment[(df_sentiment["group"] == "Treatment") & (df_sentiment["period"] == "before")])
            t_pos_a = calc_pos_rate(df_sentiment[(df_sentiment["group"] == "Treatment") & (df_sentiment["period"] == "after")])
            c_pos_b = calc_pos_rate(df_sentiment[(df_sentiment["group"] == "Control") & (df_sentiment["period"] == "before")])
            c_pos_a = calc_pos_rate(df_sentiment[(df_sentiment["group"] == "Control") & (df_sentiment["period"] == "after")])
            t_pos_chg = t_pos_a - t_pos_b
            c_pos_chg = c_pos_a - c_pos_b

            # カイ二乗検定
            t_aft = df_sentiment[(df_sentiment["group"] == "Treatment") & (df_sentiment["period"] == "after")]
            c_aft = df_sentiment[(df_sentiment["group"] == "Control") & (df_sentiment["period"] == "after")]
            t_aft_pos = len(t_aft[t_aft["sentiment"] == "positive"])
            t_aft_neg = len(t_aft) - t_aft_pos
            c_aft_pos = len(c_aft[c_aft["sentiment"] == "positive"])
            c_aft_neg = len(c_aft) - c_aft_pos

            if t_aft_pos + t_aft_neg > 0 and c_aft_pos + c_aft_neg > 0:
                try:
                    _, p_pos_final, _, _ = stats_final.chi2_contingency(
                        [[t_aft_pos, t_aft_neg], [c_aft_pos, c_aft_neg]]
                    )
                except Exception:
                    pass

        # 有意性マーク関数
        def sig_mark(p):
            if p < 0.01:
                return "⭐⭐⭐"
            elif p < 0.05:
                return "⭐⭐"
            elif p < 0.10:
                return "⭐"
            return ""

        # ダッシュボード作成
        dashboard_rows = [
            {
                "指標": "📝 投稿数",
                "T前": f"{t_count_b:.1f}",
                "T後": f"{t_count_a:.1f}",
                "T変化": f"{t_count_chg:+.0f}%",
                "C前": f"{c_count_b:.1f}",
                "C後": f"{c_count_a:.1f}",
                "C変化": f"{c_count_chg:+.0f}%",
                "差分(T-C)": f"{t_count_chg - c_count_chg:+.0f}pt",
                "有意性": "-",
            },
            {
                "指標": "📈 投稿頻度(/日)",
                "T前": f"{t_rate_b:.3f}",
                "T後": f"{t_rate_a:.3f}",
                "T変化": f"{t_rate_chg:+.0f}%",
                "C前": f"{c_rate_b:.3f}",
                "C後": f"{c_rate_a:.3f}",
                "C変化": f"{c_rate_chg:+.0f}%",
                "差分(T-C)": f"{t_rate_chg - c_rate_chg:+.0f}pt",
                "有意性": f"p={p_rate_final:.3f} {sig_mark(p_rate_final)}",
            },
            {
                "指標": "💖 エンゲージメント(いいね)",
                "T前": f"{t_eng_b:.1f}",
                "T後": f"{t_eng_a:.1f}",
                "T変化": f"{t_eng_chg:+.0f}%" if t_eng_b > 0 else "-",
                "C前": f"{c_eng_b:.1f}",
                "C後": f"{c_eng_a:.1f}",
                "C変化": f"{c_eng_chg:+.0f}%" if c_eng_b > 0 else "-",
                "差分(T-C)": f"{t_eng_chg - c_eng_chg:+.0f}pt" if t_eng_b > 0 and c_eng_b > 0 else "-",
                "有意性": f"p={p_eng_final:.3f} {sig_mark(p_eng_final)}" if p_eng_final < 1.0 else "-",
            },
            {
                "指標": "😊 ポジティブ率",
                "T前": f"{t_pos_b:.1f}%",
                "T後": f"{t_pos_a:.1f}%",
                "T変化": f"{t_pos_chg:+.1f}pt",
                "C前": f"{c_pos_b:.1f}%",
                "C後": f"{c_pos_a:.1f}%",
                "C変化": f"{c_pos_chg:+.1f}pt",
                "差分(T-C)": f"{t_pos_chg - c_pos_chg:+.1f}pt",
                "有意性": f"p={p_pos_final:.3f} {sig_mark(p_pos_final)}" if p_pos_final < 1.0 else "-",
            },
        ]

        df_final_dashboard = pd.DataFrame(dashboard_rows)

        final_dashboard = mo.vstack([
            mo.md("""
            ## 🎯 IRC効果 4指標サマリー

            | 凡例 | 意味 |
            |------|------|
            | T | Treatment群（IRC参加者） |
            | C | Control群（IRC非参加者） |
            | ⭐⭐⭐ | p < 0.01（非常に有意） |
            | ⭐⭐ | p < 0.05（有意） |
            | ⭐ | p < 0.10（弱い有意） |
            """),
            mo.ui.table(df_final_dashboard, selection=None),
            mo.md("""
            ### 💡 結果の解釈

            - **差分(T-C)** が正の値: IRC参加者の方が効果が大きい
            - **有意性** に⭐がある場合: 統計的に意味のある差がある
            - 投稿数・投稿頻度・エンゲージメント・ポジティブ率の全てでTreatment群の変化が大きい場合、IRCチャレンジの効果が示唆される
            """),
        ])

    final_dashboard
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 💾 CSVエクスポート

    分析結果をCSVファイルとしてダウンロードできます。
    """)
    return


@app.cell
def _(df_user_analysis_filtered, mo):
    """分析結果のCSVダウンロード"""

    if df_user_analysis_filtered.empty:
        download_btn = mo.md("（ダウンロードするデータがありません）")
    else:
        # エクスポート用DataFrame
        export_df = df_user_analysis_filtered.copy()
        export_df.columns = [
            "アカウントID",
            "グループ",
            "基準日",
            "主なアイドル",
            "投稿数（前）",
            "投稿数（後）",
            "変化量",
            "日数（前）",
            "日数（後）",
            "1日あたり投稿数（前）",
            "1日あたり投稿数（後）",
            "1日あたり変化量",
            "比率（後/前）",
        ]

        csv_data = export_df.to_csv(index=False)

        download_btn = mo.download(
            data=csv_data.encode("utf-8-sig"),
            filename="irc_impact_analysis.csv",
            label="📥 分析結果CSVダウンロード",
        )

    download_btn
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## ⚠️ 分析の限界と注意点

    ### Limitations

    1. **因果関係 vs 相関関係**
       - この分析は相関関係を示すものであり、因果関係を証明するものではありません。
       - 「IRCチャレンジに参加したから投稿が増えた」とは断定できません。

    2. **セルフセレクションバイアス**
       - IRCチャレンジに参加するユーザーは、もともと活発なユーザーである可能性があります。
       - 参加/非参加の決定には観測されていない要因が影響している可能性があります。

    3. **時間的要因**
       - Treatment群とControl群で観測期間が異なる可能性があります。
       - 季節性やトレンドの影響を完全には除去できていません。

    4. **サンプルサイズ**
       - サンプルサイズによっては、統計的検出力が不足している可能性があります。

    ### 推奨される追加分析

    - マッチング手法（傾向スコアマッチング）による比較
    - 時系列分析（Event Study）
    - 差分の差分法（Difference-in-Differences）
    """)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

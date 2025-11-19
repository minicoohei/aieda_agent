import marimo

__generated_with = "0.17.8"
app = marimo.App(width="full")


@app.cell
def _(mo):
    mo.md("""
    # 🌏 グロス市場規模分析 (Gross Market Analysis)

    `dev_yoake_posts` データセット全体を対象とした、アイドルファン市場の規模と成長性の包括的分析。

    ## 🎯 目的
    1. **市場規模の把握**: 総投稿数、アクティブユーザー数(DAU)、総エンゲージメント数の推移。
    2. **成長性の可視化**: 累積ユニークユーザー数(Cumulative UU)の推移。
    3. **シェア比較**: アイドル/グループごとのシェア・オブ・ボイスとエンゲージメント効率。
    4. **ユーザー構造**: ファン層の集中度と行動特性。

    ## 🛠️ 設定
    - **データソース**: `yoake-dev-analysis.dev_yoake_posts.*`
    - **分析期間**: 2025年9月1日以降
    - **除外対象**: 公式アカウント（運営、メンバー本人）を除外し、純粋なファン活動を計測。
    """)
    return


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import seaborn as sns
    import sys
    import os
    from pathlib import Path
    from datetime import datetime, timedelta

    # 日本語フォント設定 (Mac用)
    import matplotlib.font_manager as fm

    # プロジェクトルートをパスに追加
    project_root = Path.cwd().parent if Path.cwd().name == "notebooks" else Path.cwd()
    if str(project_root / "src") not in sys.path:
        sys.path.insert(0, str(project_root / "src"))

    from ai_data_lab.connectors.bigquery import BigQueryConnector

    # 高解像度プロット設定
    plt.rcParams['figure.dpi'] = 300
    plt.rcParams['savefig.dpi'] = 300
    sns.set_context("notebook", font_scale=1.2)
    sns.set_style("whitegrid")

    # Macの日本語フォント設定
    font_candidates = ['Hiragino Sans', 'Hiragino Kaku Gothic ProN', 'AppleGothic']
    for font in font_candidates:
        try:
            fm.findfont(font, fallback_to_default=False)
            plt.rcParams['font.family'] = font
            break
        except:
            continue

    # GOOGLE_APPLICATION_CREDENTIALS が無効な値の場合は削除 (ADCフォールバック)
    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        gac_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        if not os.path.exists(gac_path):
            print(f"⚠️ Credential file not found at {gac_path}. Removing env var to use ADC.")
            del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    return BigQueryConnector, mdates, mo, np, pd, plt, project_root, sns


@app.cell
def _():
    # 公式アカウント除外リスト
    EXCLUDED_HANDLES = [
        'FRUITS_ZIPPER', 'amane_fz1026', 'suzuka_fz1124', 'yui_fz0221',
        'luna_fz0703', 'manafy_fz0422', 'karen_fz0328', 'noel_fz1229',
        'CUTIE_STREET_', 'aika_cs1126', 'risa_cs1108', 'ayano_cs0526',
        'emiru_cs0422', 'kana_cs1111', 'haruka_cs0129', 'miyu_cs0913',
        'nagisa_cs0628', 'candy_tune_', 'mizuki_ct0221', 'rino_ct1224',
        'nachico_ct1001', 'natsu_ct0317', 'kotomi_ct0525', 'shizuka_ct0530',
        'bibian_ct1203', 'SWEET_STEADY', 'rise_ss0731', 'ayu_ss0107',
        'sakina_ss0229', 'nagisa_ss1029', 'natsuka_ss0719', 'mayumi_ss1227',
        'yui_ss0109', 'nogizaka46', 'takanenofficial', 'nao_kizuki',
        'hina_hinahata', 'Mikuru_hositani', 'erisahigasiyama', 'momonamatsumoto',
        'MomokoHashimoto', 'su_suzumi_', 'himeri_momiyama', 'saara_hazuki',
        'Equal_LOVE_12', 'otani_emiri', 'hana_oba', 'otoshima_risa',
        'saitou_kiara', 'sasaki_maika', 'takamatsuhitomi', 'shoko_takiwaki',
        'noguchi_iori', 'morohashi_sana', 'yamamoto_anna_'
    ]

    EXCLUDED_HANDLES_STR = ", ".join([f"'{h}'" for h in EXCLUDED_HANDLES])
    return (EXCLUDED_HANDLES_STR,)


@app.cell
def _(BigQueryConnector, EXCLUDED_HANDLES_STR, mo):
    # BigQueryデータ取得
    bq = BigQueryConnector(project_id="yoake-dev-analysis")
    DATASET_ID = "dev_yoake_posts"

    mo.md("## 📥 データ抽出 (BigQuery)")

    # 1. 日次・全体指標 (Gross Daily Metrics)
    query_daily_global = f"""
    WITH deduplicated AS (
        SELECT
            DATE(TIMESTAMP_SECONDS(post.xPostCreatedAt)) as date,
            post.xPostId as xPostId,
            user.xPostUserId as user_id,
            post.xPostLikedCount + post.xPostRepostedCount + post.xPostRepliedCount + post.xPostQuotedCount as total_engagement,
            post.xPostLikedCount as like_count,
            post.xPostRepostedCount as repost_count,
            post.xPostRepliedCount as reply_count,
            post.xPostQuotedCount as quoted_count,
            REGEXP_EXTRACT(post.xPostUrl, r'^https://x\\.com/([^/]+)/status') as handle,
            ROW_NUMBER() OVER (PARTITION BY post.xPostId ORDER BY _PARTITIONTIME DESC) as row_num
        FROM `{bq.project_id}.{DATASET_ID}.*`
        WHERE _TABLE_SUFFIX IS NOT NULL 
            AND _PARTITIONTIME IS NOT NULL
            AND DATE(TIMESTAMP_SECONDS(post.xPostCreatedAt)) >= '2025-09-01'
    ),
    base AS (
        SELECT * FROM deduplicated WHERE row_num = 1
    )
    SELECT
        date,
        COUNT(DISTINCT xPostId) as post_count,
        COUNT(DISTINCT user_id) as dau,
        SUM(total_engagement) as total_engagement,
        SUM(like_count) as total_likes,
        SUM(repost_count) as total_reposts,
        SUM(reply_count) as total_replies,
        SUM(quoted_count) as total_quotes
    FROM base
    WHERE handle NOT IN ({EXCLUDED_HANDLES_STR})
    GROUP BY date
    ORDER BY date
    """

    # 2. 日次・アイドル別指標 (Idol Daily Metrics)
    query_daily_idol = f"""
    WITH deduplicated AS (
        SELECT
            DATE(TIMESTAMP_SECONDS(post.xPostCreatedAt)) as date,
            _TABLE_SUFFIX as idol_name,
            post.xPostId as xPostId,
            user.xPostUserId as user_id,
            post.xPostLikedCount + post.xPostRepostedCount + post.xPostRepliedCount + post.xPostQuotedCount as total_engagement,
            REGEXP_EXTRACT(post.xPostUrl, r'^https://x\\.com/([^/]+)/status') as handle,
            ROW_NUMBER() OVER (PARTITION BY post.xPostId ORDER BY _PARTITIONTIME DESC) as row_num
        FROM `{bq.project_id}.{DATASET_ID}.*`
        WHERE _TABLE_SUFFIX IS NOT NULL 
            AND _PARTITIONTIME IS NOT NULL
            AND DATE(TIMESTAMP_SECONDS(post.xPostCreatedAt)) >= '2025-09-01'
    ),
    base AS (
        SELECT * FROM deduplicated WHERE row_num = 1
    )
    SELECT
        date,
        idol_name,
        COUNT(DISTINCT xPostId) as post_count,
        COUNT(DISTINCT user_id) as unique_user_count,
        SUM(total_engagement) as total_engagement
    FROM base
    WHERE handle NOT IN ({EXCLUDED_HANDLES_STR})
    GROUP BY date, idol_name
    ORDER BY date, post_count DESC
    """

    # 3. ユーザーコホート・成長指標 (User Growth & Segmentation)
    query_user_growth = f"""
    WITH deduplicated AS (
        SELECT
            user.xPostUserId as user_id,
            post.xPostId as xPostId,
            TIMESTAMP_SECONDS(post.xPostCreatedAt) as created_at,
            post.xPostLikedCount + post.xPostRepostedCount + post.xPostRepliedCount + post.xPostQuotedCount as total_engagement,
            REGEXP_EXTRACT(post.xPostUrl, r'^https://x\\.com/([^/]+)/status') as handle,
            ROW_NUMBER() OVER (PARTITION BY post.xPostId ORDER BY _PARTITIONTIME DESC) as row_num
        FROM `{bq.project_id}.{DATASET_ID}.*`
        WHERE _TABLE_SUFFIX IS NOT NULL 
            AND _PARTITIONTIME IS NOT NULL
            AND DATE(TIMESTAMP_SECONDS(post.xPostCreatedAt)) >= '2025-09-01'
    ),
    base AS (
        SELECT * FROM deduplicated WHERE row_num = 1
    )
    SELECT
        user_id,
        MIN(created_at) as first_post_at,
        COUNT(DISTINCT xPostId) as total_posts,
        SUM(total_engagement) as total_engagement_sum,
        AVG(total_engagement) as avg_engagement,
        EXTRACT(HOUR FROM MAX(created_at)) as last_post_hour,
        EXTRACT(DAYOFWEEK FROM MAX(created_at)) as last_post_dow
    FROM base
    WHERE handle NOT IN ({EXCLUDED_HANDLES_STR})
    GROUP BY user_id
    """

    # 4. 月次集計（MAU計算用）
    query_monthly = f"""
    WITH deduplicated AS (
        SELECT
            FORMAT_DATE('%Y-%m', DATE(TIMESTAMP_SECONDS(post.xPostCreatedAt))) as year_month,
            user.xPostUserId as user_id,
            post.xPostId as xPostId,
            REGEXP_EXTRACT(post.xPostUrl, r'^https://x\\.com/([^/]+)/status') as handle,
            ROW_NUMBER() OVER (PARTITION BY post.xPostId ORDER BY _PARTITIONTIME DESC) as row_num
        FROM `{bq.project_id}.{DATASET_ID}.*`
        WHERE _TABLE_SUFFIX IS NOT NULL 
            AND _PARTITIONTIME IS NOT NULL
            AND DATE(TIMESTAMP_SECONDS(post.xPostCreatedAt)) >= '2025-09-01'
    ),
    base AS (
        SELECT * FROM deduplicated WHERE row_num = 1
    )
    SELECT
        year_month,
        COUNT(DISTINCT user_id) as mau,
        COUNT(DISTINCT xPostId) as monthly_posts
    FROM base
    WHERE handle NOT IN ({EXCLUDED_HANDLES_STR})
    GROUP BY year_month
    ORDER BY year_month
    """

    # 5. 時間帯・曜日ヒートマップ用データ
    query_heatmap = f"""
    WITH deduplicated AS (
        SELECT
            post.xPostId as xPostId,
            TIMESTAMP_SECONDS(post.xPostCreatedAt) as created_at,
            REGEXP_EXTRACT(post.xPostUrl, r'^https://x\\.com/([^/]+)/status') as handle,
            ROW_NUMBER() OVER (PARTITION BY post.xPostId ORDER BY _PARTITIONTIME DESC) as row_num
        FROM `{bq.project_id}.{DATASET_ID}.*`
        WHERE _TABLE_SUFFIX IS NOT NULL 
            AND _PARTITIONTIME IS NOT NULL
            AND DATE(TIMESTAMP_SECONDS(post.xPostCreatedAt)) >= '2025-09-01'
    ),
    base AS (
        SELECT * FROM deduplicated WHERE row_num = 1
    )
    SELECT
        EXTRACT(DAYOFWEEK FROM created_at) as day_of_week, -- 1=Sunday, 7=Saturday
        EXTRACT(HOUR FROM created_at) as hour_of_day,
        COUNT(DISTINCT xPostId) as post_count
    FROM base
    WHERE handle NOT IN ({EXCLUDED_HANDLES_STR})
    GROUP BY day_of_week, hour_of_day
    ORDER BY day_of_week, hour_of_day
    """

    try:
        # Use .query() instead of .query_to_dataframe()
        df_daily_global = bq.query(query_daily_global)
        df_daily_idol = bq.query(query_daily_idol)
        df_user_growth = bq.query(query_user_growth)
        df_monthly = bq.query(query_monthly)
        df_heatmap = bq.query(query_heatmap)

        mo.md(f"""
        ✅ データ取得完了
        - **Daily Global**: {len(df_daily_global):,} rows
        - **Daily Idol**: {len(df_daily_idol):,} rows
        - **User Growth**: {len(df_user_growth):,} rows (Unique Users)
        - **Monthly (MAU)**: {len(df_monthly):,} rows
        - **Heatmap Data**: {len(df_heatmap):,} rows
        """)
    except Exception as e:
        mo.stop(True, mo.md(f"❌ Query Error: {e}"))
    return (
        df_daily_global,
        df_daily_idol,
        df_heatmap,
        df_monthly,
        df_user_growth,
    )


@app.cell
def _(
    df_daily_global,
    df_daily_idol,
    df_heatmap,
    df_monthly,
    df_user_growth,
    mo,
    pd,
):
    # データ前処理

    # df_daily_globalのコピーを作成して処理
    df_global_processed = df_daily_global.copy()

    # 数値型のキャスト (BigQueryのDecimal対応)
    numeric_cols_global = ['post_count', 'dau', 'total_engagement', 'total_likes', 'total_reposts', 'total_replies', 'total_quotes']
    for col in numeric_cols_global:
        if col in df_global_processed.columns:
            df_global_processed[col] = df_global_processed[col].astype(float)

    # idol/heatmap/user_growth/monthlyもコピーして処理
    df_idol_processed = df_daily_idol.copy()
    df_heatmap_processed = df_heatmap.copy()
    df_user_processed = df_user_growth.copy()
    df_monthly_processed = df_monthly.copy()

    numeric_cols_idol = ['post_count', 'unique_user_count', 'total_engagement']
    for col in numeric_cols_idol:
        if col in df_idol_processed.columns:
            df_idol_processed[col] = df_idol_processed[col].astype(float)

    numeric_cols_user = ['total_posts', 'total_engagement_sum', 'avg_engagement']
    for col in numeric_cols_user:
        if col in df_user_processed.columns:
            df_user_processed[col] = df_user_processed[col].astype(float)

    if not df_heatmap_processed.empty:
        df_heatmap_processed['post_count'] = df_heatmap_processed['post_count'].astype(float)

    # 月次データの処理
    if not df_monthly_processed.empty:
        df_monthly_processed['mau'] = df_monthly_processed['mau'].astype(float)
        df_monthly_processed['monthly_posts'] = df_monthly_processed['monthly_posts'].astype(float)

    # 日付型変換
    df_global_processed['date'] = pd.to_datetime(df_global_processed['date'])

    # 月次フィールド追加
    df_global_processed['year_month'] = df_global_processed['date'].dt.to_period('M')

    # 累積ユニークユーザー数 (Cumulative UU) の計算
    # ユーザーごとの初回投稿日を集計して累積和をとる
    user_first_dates = pd.to_datetime(df_user_processed['first_post_at']).dt.date.value_counts().sort_index().cumsum()

    # df_global_processed にマージ (日付範囲を合わせる)
    df_global_processed = df_global_processed.set_index('date')
    df_global_processed['cumulative_uu'] = user_first_dates
    # 欠損値補完 (投稿がない日でも累積UUは前の値を維持)
    df_global_processed['cumulative_uu'] = df_global_processed['cumulative_uu'].ffill()
    df_global_processed = df_global_processed.reset_index()

    # MAU計算用に元データからユーザーIDと月を抽出
    df_user_processed['year_month'] = pd.to_datetime(df_user_processed['first_post_at']).dt.to_period('M')

    mo.md("🛠️ 前処理完了: 累積UU・MAU計算準備完了")
    return (
        df_global_processed,
        df_heatmap_processed,
        df_idol_processed,
        df_monthly_processed,
        df_user_processed,
    )


@app.cell
def _(df_global_processed, df_monthly_processed, mo):
    mo.md(f"""
    ## 📊 DAU & MAU サマリー

    ### 📈 Daily Active Users (DAU)
    - **平均 DAU**: {df_global_processed['dau'].mean():,.0f} 人/日
    - **最大 DAU**: {df_global_processed['dau'].max():,.0f} 人/日
    - **最小 DAU**: {df_global_processed['dau'].min():,.0f} 人/日

    ### 📅 Monthly Active Users (MAU)
    - **平均 MAU**: {df_monthly_processed['mau'].mean():,.0f} 人/月
    - **最大 MAU**: {df_monthly_processed['mau'].max():,.0f} 人/月
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 📊 1. Gross Market Dynamics (市場の鼓動)
    """)
    return


@app.cell
def _(df_global_processed, mdates, plt, project_root):
    # 1-1. Combined Time Series (Posts vs DAU)

    # Restart with pure matplotlib for better dual-axis date control
    fig1, ax1 = plt.subplots(figsize=(15, 8))

    ax1.bar(df_global_processed['date'], df_global_processed['post_count'], color='skyblue', alpha=0.6, label='Daily Posts')
    ax1_twin = ax1.twinx()
    ax1_twin.plot(df_global_processed['date'], df_global_processed['dau'], color='navy', linewidth=3, label='DAU')

    # Rolling Average (7-day)
    df_global_processed['dau_7ma'] = df_global_processed['dau'].rolling(7).mean()
    ax1_twin.plot(df_global_processed['date'], df_global_processed['dau_7ma'], color='orange', linewidth=2, linestyle='--', label='DAU (7-day MA)')

    ax1.set_title('Gross Market Dynamics: Posts vs Active Users', fontsize=20, pad=20)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=3))
    plt.xticks(rotation=45)

    # Legends
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax1_twin.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=12)

    plt.tight_layout()

    # Save
    save_dir = project_root / "reports" / "visualizations"
    save_dir.mkdir(parents=True, exist_ok=True)
    plt.savefig(save_dir / "gross_market_pulse.png")
    return (save_dir,)


@app.cell
def _(df_global_processed, plt, save_dir):
    # 1-2. Engagement Composition (Stacked Area)
    fig2, ax2 = plt.subplots(figsize=(15, 8))

    colors = ['#FF9999', '#66B2FF', '#99FF99', '#FFCC99'] # Likes, Reposts, Replies, Quotes
    labels = ['Likes', 'Reposts', 'Replies', 'Quotes']

    y_data = [
        df_global_processed['total_likes'],
        df_global_processed['total_reposts'],
        df_global_processed['total_replies'],
        df_global_processed['total_quotes']
    ]

    ax2.stackplot(df_global_processed['date'], y_data, labels=labels, colors=colors, alpha=0.8)

    ax2.set_title('Daily Engagement Composition', fontsize=20, pad=20)
    ax2.set_ylabel('Total Engagement Count', fontsize=14)
    ax2.legend(loc='upper left', fontsize=12)
    plt.xticks(rotation=45)

    plt.tight_layout()
    plt.savefig(save_dir / "gross_engagement_composition.png")
    return


@app.cell
def _(df_global_processed, plt, save_dir):
    # 1-3. Growth Trajectory (Cumulative UU)
    fig3, ax3 = plt.subplots(figsize=(15, 8))

    ax3.fill_between(df_global_processed['date'], df_global_processed['cumulative_uu'], color='purple', alpha=0.3)
    ax3.plot(df_global_processed['date'], df_global_processed['cumulative_uu'], color='purple', linewidth=3)

    # Annotate last value
    last_date = df_global_processed['date'].iloc[-1]
    last_val = df_global_processed['cumulative_uu'].iloc[-1]
    ax3.annotate(f'Total Reach: {last_val:,} Users', 
                xy=(last_date, last_val), 
                xytext=(last_date, last_val*1.1),
                arrowprops=dict(facecolor='black', shrink=0.05),
                fontsize=14, fontweight='bold')

    ax3.set_title('Market Growth Trajectory: Cumulative Unique Users', fontsize=20, pad=20)
    ax3.set_ylabel('Cumulative Users', fontsize=14)
    plt.grid(True, which='major', linestyle='--', alpha=0.7)

    plt.tight_layout()
    plt.savefig(save_dir / "gross_user_growth.png")
    return


@app.cell
def _(mo):
    mo.md("""
    ## 📊 2. Idol/Group Comparison (市場シェア)
    """)
    return


@app.cell
def _(df_idol_processed, plt, save_dir):
    # 2-1. Share of Voice (Stacked 100% Area Chart)
    # Top 10 Idols by total posts
    top_idols = df_idol_processed.groupby('idol_name')['post_count'].sum().nlargest(10).index.tolist()

    # Pivot data
    df_pivot = df_idol_processed[df_idol_processed['idol_name'].isin(top_idols)].pivot(index='date', columns='idol_name', values='post_count').fillna(0)

    # Convert to percentage
    df_pct = df_pivot.div(df_pivot.sum(axis=1), axis=0) * 100

    fig4, ax4 = plt.subplots(figsize=(15, 8))
    df_pct.plot(kind='area', stacked=True, ax=ax4, colormap='tab20', alpha=0.8)

    ax4.set_title('Share of Voice: Daily Post Volume Share (Top 10 Idols)', fontsize=20, pad=20)
    ax4.set_ylabel('Share of Posts (%)', fontsize=14)
    ax4.legend(bbox_to_anchor=(1.02, 1), loc='upper left', borderaxespad=0, title='Idol')
    ax4.margins(x=0, y=0)

    plt.tight_layout()
    plt.savefig(save_dir / "market_share_of_voice.png")
    return (top_idols,)


@app.cell
def _(df_idol_processed, plt, save_dir):
    # 2-2. Leaderboard (Engagement Efficiency)
    # Aggregate stats
    idol_stats = df_idol_processed.groupby('idol_name').agg({
        'total_engagement': 'sum',
        'post_count': 'sum',
        'unique_user_count': 'mean' # Average DAU
    }).reset_index()

    # Calculate Engagement Rate
    idol_stats['engagement_rate'] = idol_stats['total_engagement'] / idol_stats['post_count']

    # Sort by Total Engagement for display (Top 20)
    top_eng_idols = idol_stats.nlargest(20, 'total_engagement').sort_values('total_engagement', ascending=True)

    fig5, ax5 = plt.subplots(figsize=(12, 10))

    # Plot bars colored by Engagement Rate
    norm = plt.Normalize(idol_stats['engagement_rate'].min(), idol_stats['engagement_rate'].max())
    sm = plt.cm.ScalarMappable(cmap='viridis', norm=norm)
    sm.set_array([])

    bars = ax5.barh(top_eng_idols['idol_name'], top_eng_idols['total_engagement'], color=sm.to_rgba(top_eng_idols['engagement_rate']))

    # Colorbar
    cbar = plt.colorbar(sm, ax=ax5)
    cbar.set_label('Engagement Rate (Avg Eng/Post)', rotation=270, labelpad=15)

    ax5.set_title('Engagement Leaderboard (Total Engagement)', fontsize=20, pad=20)
    ax5.set_xlabel('Total Engagement', fontsize=14)

    # Annotate bars
    for bar in bars:
        width = bar.get_width()
        ax5.text(width * 1.02, bar.get_y() + bar.get_height()/2, f'{int(width):,}', 
                va='center', ha='left', fontsize=10)

    plt.tight_layout()
    plt.savefig(save_dir / "market_leaderboard.png")
    return


@app.cell
def _(df_idol_processed, plt, save_dir, sns, top_idols):
    # 2-3. Small Multiples (全アイドル Daily Trend)
    # 全アイドルを表示（投稿数順）
    df_all_idols = df_idol_processed[df_idol_processed['idol_name'].isin(top_idols)]

    # アイドル数に応じてグリッドサイズを調整
    num_idols = len(top_idols)
    col_wrap = 4  # 4列表示
    height = 2.5  # 各グラフの高さを少し小さく

    g = sns.FacetGrid(df_all_idols, col="idol_name", col_wrap=col_wrap, height=height, aspect=2, sharey=False)
    g.map(sns.lineplot, "date", "post_count", color="skyblue", label="Posts")

    # Add DAU on secondary axis (Tricky with FacetGrid, simpler to just show Posts for now or overlay normalized)
    # Let's stick to Posts trend to see activity spikes

    g.set_titles("{col_name}")
    g.set_axis_labels("Date", "Daily Posts")

    # Improve date formatting
    for ax_grid in g.axes.flat:
        ax_grid.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%m/%d'))
        plt.setp(ax_grid.get_xticklabels(), rotation=45)

    plt.suptitle(f'Activity Trends: All Idols (Top {num_idols})', fontsize=20, y=1.02)
    plt.tight_layout()
    plt.savefig(save_dir / "market_small_multiples.png")
    plt.show()  # FacetGrid needs explicit show
    return


@app.cell
def _(mo):
    mo.md("""
    ## 📊 3. User Behavior & Concentration (ユーザー深掘り)
    """)
    return


@app.cell
def _(df_user_processed, np, plt, save_dir):
    # 3-1. Power Law Distribution (Lorenz Curve / Gini)
    # Sort users by post count (descending)
    posts_by_user = df_user_processed['total_posts'].sort_values(ascending=False).values

    # Cumulative calculations
    cum_users_pct = np.linspace(0, 100, len(posts_by_user))
    cum_posts = np.cumsum(posts_by_user)
    cum_posts_pct = cum_posts / cum_posts[-1] * 100

    fig7, ax7 = plt.subplots(figsize=(10, 10))

    # Lorenz curve
    ax7.plot(cum_users_pct, cum_posts_pct, linewidth=3, color='crimson', label='Actual Distribution')
    # Equality line
    ax7.plot([0, 100], [0, 100], linestyle='--', color='gray', label='Perfect Equality')

    # Find 80/20 point (Pareto principle)
    # How many users create 80% of posts?
    top_users_idx = np.searchsorted(cum_posts_pct, 80) # Index where posts reach 80% (actually this logic is slightly off for standard Lorenz reading)

    # Let's flip for standard "Top X% users create Y% posts" reading
    # Users are sorted High to Low. 
    # cum_users_pct[i] is "Top i% of users"
    # cum_posts_pct[i] is "Create i% of posts"

    idx_20pct_users = int(len(posts_by_user) * 0.2)
    posts_by_top20 = cum_posts_pct[idx_20pct_users]

    ax7.plot([20, 20], [0, posts_by_top20], 'k--', alpha=0.5)
    ax7.plot([0, 20], [posts_by_top20, posts_by_top20], 'k--', alpha=0.5)

    ax7.annotate(f'Top 20% users create\n{posts_by_top20:.1f}% of posts', 
                xy=(20, posts_by_top20), 
                xytext=(30, 60),
                arrowprops=dict(facecolor='black', arrowstyle='->'),
                fontsize=12)

    ax7.set_title('User Concentration (Power Law)', fontsize=20, pad=20)
    ax7.set_xlabel('Cumulative % of Users (Sorted by Activity)', fontsize=14)
    ax7.set_ylabel('Cumulative % of Posts', fontsize=14)
    ax7.legend()

    plt.tight_layout()
    plt.savefig(save_dir / "market_power_law.png")
    return (posts_by_top20,)


@app.cell
def _(df_heatmap_processed, plt, save_dir, sns):
    # 3-2. Activity Heatmap
    # Pivot: Day of Week (Index) x Hour (Column)
    heatmap_data = df_heatmap_processed.pivot(index='day_of_week', columns='hour_of_day', values='post_count').fillna(0)

    # Rename index to Day Names
    days = {1: 'Sunday', 2: 'Monday', 3: 'Tuesday', 4: 'Wednesday', 5: 'Thursday', 6: 'Friday', 7: 'Saturday'}
    heatmap_data.index = heatmap_data.index.map(days)

    # Reorder rows
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    heatmap_data = heatmap_data.reindex(day_order)

    fig8, ax8 = plt.subplots(figsize=(15, 8))
    sns.heatmap(heatmap_data, cmap='YlOrRd', annot=False, fmt='.0f', cbar_kws={'label': 'Post Count'}, ax=ax8)

    ax8.set_title('Activity Heatmap: When do fans post?', fontsize=20, pad=20)
    ax8.set_xlabel('Hour of Day', fontsize=14)
    ax8.set_ylabel('Day of Week', fontsize=14)

    plt.tight_layout()
    plt.savefig(save_dir / "market_activity_heatmap.png")
    return


@app.cell
def _(df_user_processed, plt, save_dir, sns):
    # 3-3. User Segmentation Matrix (Scatter)
    # X: Total Posts (Log Scale), Y: Avg Engagement (Log Scale)

    fig9, ax9 = plt.subplots(figsize=(12, 12))

    sns.scatterplot(data=df_user_processed, x='total_posts', y='avg_engagement', 
                    alpha=0.3, color='teal', size='total_engagement_sum', sizes=(20, 500), legend=False, ax=ax9)

    ax9.set_xscale('log')
    ax9.set_yscale('log')

    ax9.set_title('User Segmentation: Volume vs Impact', fontsize=20, pad=20)
    ax9.set_xlabel('Total Posts (Log Scale)', fontsize=14)
    ax9.set_ylabel('Avg Engagement per Post (Log Scale)', fontsize=14)

    # Quadrant annotations
    ax9.text(0.95, 0.95, 'Influencers\n(High Eng, Variable Vol)', transform=ax9.transAxes, ha='right', va='top', fontsize=14, color='darkred')
    ax9.text(0.95, 0.05, 'Super Fans\n(High Vol, Low Eng)', transform=ax9.transAxes, ha='right', va='bottom', fontsize=14, color='navy')

    plt.tight_layout()
    plt.savefig(save_dir / "market_user_segmentation.png")
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 🎯 個別アイドル分析（インタラクティブ）

    プルダウンからアイドルを選択して、詳細な分析を確認できます。
    """)
    return


@app.cell
def _(df_idol_processed, mo):
    # アイドル一覧を取得（投稿数順）
    idol_list = df_idol_processed.groupby('idol_name')['post_count'].sum().sort_values(ascending=False).index.tolist()

    # ドロップダウンUI作成
    idol_selector = mo.ui.dropdown(
        options=idol_list,
        value=idol_list[0] if idol_list else None,
        label="アイドルを選択："
    )

    idol_selector
    return (idol_selector,)


@app.cell
def _(df_idol_processed, idol_selector, mo):
    # 選択されたアイドルのデータをフィルタ
    if idol_selector.value is None:
        mo.stop(True, mo.md("アイドルを選択してください"))

    selected_idol = idol_selector.value
    df_selected = df_idol_processed[df_idol_processed['idol_name'] == selected_idol].copy()
    df_selected = df_selected.sort_values('date')

    # 統計計算
    total_posts_idol = df_selected['post_count'].sum()
    total_engagement_idol = df_selected['total_engagement'].sum()
    avg_dau_idol = df_selected['unique_user_count'].mean()
    max_dau_idol = df_selected['unique_user_count'].max()
    avg_engagement_per_post = total_engagement_idol / total_posts_idol if total_posts_idol > 0 else 0

    mo.md(f"""
    ## 📊 {selected_idol} の統計

    - **総投稿数**: {total_posts_idol:,.0f} 件
    - **総エンゲージメント**: {total_engagement_idol:,.0f} 件
    - **平均DAU**: {avg_dau_idol:,.0f} 人/日
    - **最大DAU**: {max_dau_idol:,.0f} 人/日
    - **投稿あたり平均エンゲージメント**: {avg_engagement_per_post:,.1f} 件/投稿
    """)
    return df_selected, selected_idol


@app.cell
def _(df_selected, plt, save_dir, selected_idol):
    # 個別アイドルの時系列グラフ
    fig_idol, (ax_idol1, ax_idol2) = plt.subplots(2, 1, figsize=(15, 10))

    # 投稿数推移
    ax_idol1.plot(df_selected['date'], df_selected['post_count'], linewidth=2, color='steelblue', marker='o')
    ax_idol1.set_title(f'{selected_idol} - Daily Posts', fontsize=16)
    ax_idol1.set_ylabel('Posts', fontsize=12)
    ax_idol1.grid(True, alpha=0.3)

    # DAU推移
    ax_idol2.plot(df_selected['date'], df_selected['unique_user_count'], linewidth=2, color='coral', marker='o')
    ax_idol2.set_title(f'{selected_idol} - Daily Active Users', fontsize=16)
    ax_idol2.set_ylabel('DAU', fontsize=12)
    ax_idol2.set_xlabel('Date', fontsize=12)
    ax_idol2.grid(True, alpha=0.3)

    # X軸の日付フォーマット
    for ax_subplot in [ax_idol1, ax_idol2]:
        ax_subplot.tick_params(axis='x', rotation=45)

    plt.tight_layout()
    plt.savefig(save_dir / f"individual_{selected_idol.replace(' ', '_')}_trends.png")
    return


@app.cell
def _(df_selected, plt, save_dir, selected_idol):
    # エンゲージメント詳細グラフ
    fig_idol_eng, ax_idol_eng = plt.subplots(figsize=(15, 8))

    # 日別のエンゲージメント推移
    ax_idol_eng.plot(df_selected['date'], df_selected['total_engagement'], linewidth=2, color='purple', marker='o', label='Total Engagement')
    ax_idol_eng.set_title(f'{selected_idol} - Daily Engagement', fontsize=16)
    ax_idol_eng.set_ylabel('Total Engagement', fontsize=12)
    ax_idol_eng.set_xlabel('Date', fontsize=12)
    ax_idol_eng.legend()
    ax_idol_eng.grid(True, alpha=0.3)
    ax_idol_eng.tick_params(axis='x', rotation=45)

    plt.tight_layout()
    plt.savefig(save_dir / f"individual_{selected_idol.replace(' ', '_')}_engagement.png")
    return


@app.cell
def _(
    df_global_processed,
    df_idol_processed,
    df_monthly_processed,
    df_user_processed,
    mo,
    posts_by_top20,
    project_root,
    save_dir,
):
    # レポート生成

    # 統計値
    total_uu = len(df_user_processed)
    total_posts = df_global_processed['post_count'].sum()
    total_eng = df_global_processed['total_engagement'].sum()
    max_dau = df_global_processed['dau'].max()
    avg_dau = df_global_processed['dau'].mean()

    # MAU統計
    avg_mau = df_monthly_processed['mau'].mean() if not df_monthly_processed.empty else 0
    max_mau = df_monthly_processed['mau'].max() if not df_monthly_processed.empty else 0

    top_idol = df_idol_processed.groupby('idol_name')['post_count'].sum().idxmax()
    top_idol_posts = df_idol_processed.groupby('idol_name')['post_count'].sum().max()

    report_md = f"""
    # 🌏 Gross Market Analysis Report

    **生成日時**: {mo.ui.button("Update").value if False else ""}

    **分析期間**: 2025年9月1日以降

    ## 📊 市場規模サマリー
    - **累積ユニークユーザー数 (Reach)**: {total_uu:,} 人
    - **総投稿数**: {total_posts:,} 件
    - **総エンゲージメント**: {total_eng:,} 件

    ### 👥 ユーザー活動指標
    - **DAU (平均)**: {avg_dau:,.0f} 人/日 / **(最大)**: {max_dau:,} 人/日
    - **MAU (平均)**: {avg_mau:,.0f} 人/月 / **(最大)**: {max_mau:,} 人/月

    ### 🎯 その他指標
    - **トップアイドル (Volume)**: {top_idol} ({top_idol_posts:,} 件)
    - **パレート法則**: 上位20%のユーザーが全投稿の {posts_by_top20:.1f}% を生成

    ## 📈 Visualizations

    ### 1. Market Pulse
    ![](visualizations/gross_market_pulse.png)

    ### 2. Engagement Composition
    ![](visualizations/gross_engagement_composition.png)

    ### 3. User Growth (Reach)
    ![](visualizations/gross_user_growth.png)

    ### 4. Share of Voice
    ![](visualizations/market_share_of_voice.png)

    ### 5. Engagement Leaderboard
    ![](visualizations/market_leaderboard.png)

    ### 6. Activity Trends
    ![](visualizations/market_small_multiples.png)

    ### 7. User Concentration
    ![](visualizations/market_power_law.png)

    ### 8. Activity Heatmap
    ![](visualizations/market_activity_heatmap.png)

    ### 9. User Segmentation
    ![](visualizations/market_user_segmentation.png)
    """

    # Save Report
    report_path = project_root / "reports" / "gross_market_analysis.md"
    report_path.write_text(report_md, encoding='utf-8')

    mo.md(f"""
    ## ✅ レポート生成完了

    レポートファイル: `{report_path}`

    画像保存先: `{save_dir}`
    """)
    return


if __name__ == "__main__":
    app.run()

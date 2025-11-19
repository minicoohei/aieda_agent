import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import matplotlib.font_manager as fm

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent
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

def main():
    print("🚀 Starting Gross Market Analysis...")
    
    # GOOGLE_APPLICATION_CREDENTIALS が無効な値の場合は削除 (ADCフォールバック)
    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        gac_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        if not os.path.exists(gac_path):
            print(f"⚠️ Credential file not found at {gac_path}. Removing env var to use ADC.")
            del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

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

    # BigQueryデータ取得
    bq = BigQueryConnector(project_id="yoake-dev-analysis")
    DATASET_ID = "dev_yoake_posts"

    print("📥 Fetching data from BigQuery...")

    # 1. 日次・全体指標 (Gross Daily Metrics)
    query_daily_global = f"""
    WITH base AS (
        SELECT
            DATE(TIMESTAMP_SECONDS(post.xPostCreatedAt)) as date,
            post.xPostId as xPostId,
            user.xPostUserId as user_id,
            post.xPostLikedCount + post.xPostRepostedCount + post.xPostRepliedCount + post.xPostQuotedCount as total_engagement,
            post.xPostLikedCount as like_count,
            post.xPostRepostedCount as repost_count,
            post.xPostRepliedCount as reply_count,
            post.xPostQuotedCount as quoted_count,
            REGEXP_EXTRACT(post.xPostUrl, r'^https://x\\.com/([^/]+)/status') as handle
        FROM `{bq.project_id}.{DATASET_ID}.*`
        WHERE _TABLE_SUFFIX IS NOT NULL AND _PARTITIONTIME IS NOT NULL
    )
    SELECT
        date,
        COUNT(DISTINCT xPostId) as post_count,
        COUNT(DISTINCT user_id) as unique_user_count,
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
    WITH base AS (
        SELECT
            DATE(TIMESTAMP_SECONDS(post.xPostCreatedAt)) as date,
            _TABLE_SUFFIX as idol_name,
            post.xPostId as xPostId,
            user.xPostUserId as user_id,
            post.xPostLikedCount + post.xPostRepostedCount + post.xPostRepliedCount + post.xPostQuotedCount as total_engagement,
            REGEXP_EXTRACT(post.xPostUrl, r'^https://x\\.com/([^/]+)/status') as handle
        FROM `{bq.project_id}.{DATASET_ID}.*`
        WHERE _TABLE_SUFFIX IS NOT NULL AND _PARTITIONTIME IS NOT NULL
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
    WITH base AS (
        SELECT
            user.xPostUserId as user_id,
            TIMESTAMP_SECONDS(post.xPostCreatedAt) as created_at,
            post.xPostLikedCount + post.xPostRepostedCount + post.xPostRepliedCount + post.xPostQuotedCount as total_engagement,
            REGEXP_EXTRACT(post.xPostUrl, r'^https://x\\.com/([^/]+)/status') as handle
        FROM `{bq.project_id}.{DATASET_ID}.*`
        WHERE _TABLE_SUFFIX IS NOT NULL AND _PARTITIONTIME IS NOT NULL
    )
    SELECT
        user_id,
        MIN(created_at) as first_post_at,
        COUNT(*) as total_posts,
        SUM(total_engagement) as total_engagement_sum,
        AVG(total_engagement) as avg_engagement,
        EXTRACT(HOUR FROM MAX(created_at)) as last_post_hour,
        EXTRACT(DAYOFWEEK FROM MAX(created_at)) as last_post_dow
    FROM base
    WHERE handle NOT IN ({EXCLUDED_HANDLES_STR})
    GROUP BY user_id
    """
    
    # 4. 時間帯・曜日ヒートマップ用データ
    query_heatmap = f"""
    WITH base AS (
        SELECT
            TIMESTAMP_SECONDS(post.xPostCreatedAt) as created_at,
            REGEXP_EXTRACT(post.xPostUrl, r'^https://x\\.com/([^/]+)/status') as handle
        FROM `{bq.project_id}.{DATASET_ID}.*`
        WHERE _TABLE_SUFFIX IS NOT NULL AND _PARTITIONTIME IS NOT NULL
    )
    SELECT
        EXTRACT(DAYOFWEEK FROM created_at) as day_of_week, -- 1=Sunday, 7=Saturday
        EXTRACT(HOUR FROM created_at) as hour_of_day,
        COUNT(*) as post_count
    FROM base
    WHERE handle NOT IN ({EXCLUDED_HANDLES_STR})
    GROUP BY day_of_week, hour_of_day
    ORDER BY day_of_week, hour_of_day
    """

    try:
        # Use .query() instead of .query_to_dataframe()
        df_daily_global = bq.query(query_daily_global)
        print(f"  - Daily Global: {len(df_daily_global)} rows")
        
        df_daily_idol = bq.query(query_daily_idol)
        print(f"  - Daily Idol: {len(df_daily_idol)} rows")
        
        df_user_growth = bq.query(query_user_growth)
        print(f"  - User Growth: {len(df_user_growth)} rows")
        
        df_heatmap = bq.query(query_heatmap)
        print(f"  - Heatmap Data: {len(df_heatmap)} rows")
        
    except Exception as e:
        print(f"❌ Query Error: {e}")
        return

    # 前処理
    print("🛠️ Processing data...")
    
    # 数値型のキャスト (BigQueryのDecimal対応)
    numeric_cols_global = ['post_count', 'unique_user_count', 'total_engagement', 'total_likes', 'total_reposts', 'total_replies', 'total_quotes']
    for col in numeric_cols_global:
        df_daily_global[col] = df_daily_global[col].astype(float)
        
    numeric_cols_idol = ['post_count', 'unique_user_count', 'total_engagement']
    for col in numeric_cols_idol:
        df_daily_idol[col] = df_daily_idol[col].astype(float)
        
    numeric_cols_user = ['total_posts', 'total_engagement_sum', 'avg_engagement']
    for col in numeric_cols_user:
        df_user_growth[col] = df_user_growth[col].astype(float)
        
    if not df_heatmap.empty:
        df_heatmap['post_count'] = df_heatmap['post_count'].astype(float)

    # 日付型変換
    
    # 累積UU計算
    user_first_dates = pd.to_datetime(df_user_growth['first_post_at']).dt.date.value_counts().sort_index().cumsum()
    df_daily_global = df_daily_global.set_index('date')
    df_daily_global['cumulative_uu'] = user_first_dates
    df_daily_global['cumulative_uu'] = df_daily_global['cumulative_uu'].ffill()
    df_daily_global = df_daily_global.reset_index()

    # 保存先
    save_dir = project_root / "reports" / "visualizations"
    save_dir.mkdir(parents=True, exist_ok=True)

    # --- Visualizations ---
    print("🎨 Generating visualizations...")

    # 1. Market Pulse
    fig, ax1 = plt.subplots(figsize=(15, 8))
    ax1.bar(df_daily_global['date'], df_daily_global['post_count'], color='skyblue', alpha=0.6, label='Daily Posts')
    ax2 = ax1.twinx()
    ax2.plot(df_daily_global['date'], df_daily_global['unique_user_count'], color='navy', linewidth=3, label='DAU')
    df_daily_global['dau_7ma'] = df_daily_global['unique_user_count'].rolling(7).mean()
    ax2.plot(df_daily_global['date'], df_daily_global['dau_7ma'], color='orange', linewidth=2, linestyle='--', label='DAU (7-day MA)')
    ax1.set_title('Gross Market Dynamics: Posts vs Active Users', fontsize=20, pad=20)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=3))
    plt.xticks(rotation=45)
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=12)
    plt.tight_layout()
    plt.savefig(save_dir / "gross_market_pulse.png")
    plt.close()

    # 2. Engagement Composition
    fig, ax = plt.subplots(figsize=(15, 8))
    colors = ['#FF9999', '#66B2FF', '#99FF99', '#FFCC99']
    labels = ['Likes', 'Reposts', 'Replies', 'Quotes']
    y_data = [df_daily_global['total_likes'], df_daily_global['total_reposts'], df_daily_global['total_replies'], df_daily_global['total_quotes']]
    ax.stackplot(df_daily_global['date'], y_data, labels=labels, colors=colors, alpha=0.8)
    ax.set_title('Daily Engagement Composition', fontsize=20, pad=20)
    ax.legend(loc='upper left', fontsize=12)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(save_dir / "gross_engagement_composition.png")
    plt.close()

    # 3. User Growth
    fig, ax = plt.subplots(figsize=(15, 8))
    ax.fill_between(df_daily_global['date'], df_daily_global['cumulative_uu'], color='purple', alpha=0.3)
    ax.plot(df_daily_global['date'], df_daily_global['cumulative_uu'], color='purple', linewidth=3)
    last_date = df_daily_global['date'].iloc[-1]
    last_val = df_daily_global['cumulative_uu'].iloc[-1]
    ax.annotate(f'Total Reach: {last_val:,.0f} Users', xy=(last_date, last_val), xytext=(last_date, last_val*1.1),
                arrowprops=dict(facecolor='black', shrink=0.05), fontsize=14, fontweight='bold')
    ax.set_title('Market Growth Trajectory: Cumulative Unique Users', fontsize=20, pad=20)
    plt.tight_layout()
    plt.savefig(save_dir / "gross_user_growth.png")
    plt.close()

    # 4. Share of Voice
    top_idols = df_daily_idol.groupby('idol_name')['post_count'].sum().nlargest(10).index.tolist()
    df_pivot = df_daily_idol[df_daily_idol['idol_name'].isin(top_idols)].pivot(index='date', columns='idol_name', values='post_count').fillna(0)
    df_pct = df_pivot.div(df_pivot.sum(axis=1), axis=0) * 100
    fig, ax = plt.subplots(figsize=(15, 8))
    df_pct.plot(kind='area', stacked=True, ax=ax, colormap='tab20', alpha=0.8)
    ax.set_title('Share of Voice: Daily Post Volume Share (Top 10 Idols)', fontsize=20, pad=20)
    ax.legend(bbox_to_anchor=(1.02, 1), loc='upper left', borderaxespad=0, title='Idol')
    ax.margins(x=0, y=0)
    plt.tight_layout()
    plt.savefig(save_dir / "market_share_of_voice.png")
    plt.close()

    # 5. Leaderboard
    idol_stats = df_daily_idol.groupby('idol_name').agg({
        'total_engagement': 'sum',
        'post_count': 'sum'
    }).reset_index()
    idol_stats['engagement_rate'] = idol_stats['total_engagement'] / idol_stats['post_count']
    top_eng_idols = idol_stats.nlargest(20, 'total_engagement').sort_values('total_engagement', ascending=True)
    
    fig, ax = plt.subplots(figsize=(12, 10))
    norm = plt.Normalize(idol_stats['engagement_rate'].min(), idol_stats['engagement_rate'].max())
    sm = plt.cm.ScalarMappable(cmap='viridis', norm=norm)
    sm.set_array([])
    bars = ax.barh(top_eng_idols['idol_name'], top_eng_idols['total_engagement'], color=sm.to_rgba(top_eng_idols['engagement_rate']))
    cbar = plt.colorbar(sm, ax=ax)
    cbar.set_label('Engagement Rate (Avg Eng/Post)', rotation=270, labelpad=15)
    ax.set_title('Engagement Leaderboard (Total Engagement)', fontsize=20, pad=20)
    for bar in bars:
        width = bar.get_width()
        ax.text(width * 1.02, bar.get_y() + bar.get_height()/2, f'{int(width):,}', va='center', ha='left', fontsize=10)
    plt.tight_layout()
    plt.savefig(save_dir / "market_leaderboard.png")
    plt.close()

    # 6. Small Multiples
    top_9 = top_idols[:9]
    df_top9 = df_daily_idol[df_daily_idol['idol_name'].isin(top_9)]
    g = sns.FacetGrid(df_top9, col="idol_name", col_wrap=3, height=3, aspect=2, sharey=False)
    g.map(sns.lineplot, "date", "post_count", color="skyblue", label="Posts")
    for ax in g.axes.flat:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.setp(ax.get_xticklabels(), rotation=45)
    plt.suptitle('Activity Trends: Top 9 Idols', fontsize=20, y=1.02)
    plt.tight_layout()
    plt.savefig(save_dir / "market_small_multiples.png")
    plt.close()

    # 7. Power Law
    posts_by_user = df_user_growth['total_posts'].sort_values(ascending=False).values
    cum_users_pct = np.linspace(0, 100, len(posts_by_user))
    cum_posts = np.cumsum(posts_by_user)
    cum_posts_pct = cum_posts / cum_posts[-1] * 100
    fig, ax = plt.subplots(figsize=(10, 10))
    ax.plot(cum_users_pct, cum_posts_pct, linewidth=3, color='crimson', label='Actual Distribution')
    ax.plot([0, 100], [0, 100], linestyle='--', color='gray', label='Perfect Equality')
    
    idx_20pct_users = int(len(posts_by_user) * 0.2)
    posts_by_top20 = cum_posts_pct[idx_20pct_users]
    
    ax.plot([20, 20], [0, posts_by_top20], 'k--', alpha=0.5)
    ax.plot([0, 20], [posts_by_top20, posts_by_top20], 'k--', alpha=0.5)
    ax.annotate(f'Top 20% users create\n{posts_by_top20:.1f}% of posts', 
                xy=(20, posts_by_top20), xytext=(30, 60),
                arrowprops=dict(facecolor='black', arrowstyle='->'), fontsize=12)
    ax.set_title('User Concentration (Power Law)', fontsize=20, pad=20)
    ax.set_xlabel('Cumulative % of Users', fontsize=14)
    ax.set_ylabel('Cumulative % of Posts', fontsize=14)
    plt.tight_layout()
    plt.savefig(save_dir / "market_power_law.png")
    plt.close()

    # 8. Heatmap
    heatmap_data = df_heatmap.pivot(index='day_of_week', columns='hour_of_day', values='post_count').fillna(0)
    days = {1: 'Sunday', 2: 'Monday', 3: 'Tuesday', 4: 'Wednesday', 5: 'Thursday', 6: 'Friday', 7: 'Saturday'}
    heatmap_data.index = heatmap_data.index.map(days)
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    heatmap_data = heatmap_data.reindex(day_order)
    fig, ax = plt.subplots(figsize=(15, 8))
    sns.heatmap(heatmap_data, cmap='YlOrRd', annot=False, fmt='.0f', cbar_kws={'label': 'Post Count'}, ax=ax)
    ax.set_title('Activity Heatmap', fontsize=20, pad=20)
    plt.tight_layout()
    plt.savefig(save_dir / "market_activity_heatmap.png")
    plt.close()

    # 9. User Segmentation
    fig, ax = plt.subplots(figsize=(12, 12))
    sns.scatterplot(data=df_user_growth, x='total_posts', y='avg_engagement', 
                    alpha=0.3, color='teal', size='total_engagement_sum', sizes=(20, 500), legend=False)
    ax.set_xscale('log')
    ax.set_yscale('log')
    ax.set_title('User Segmentation: Volume vs Impact', fontsize=20, pad=20)
    plt.tight_layout()
    plt.savefig(save_dir / "market_user_segmentation.png")
    plt.close()

    # Report generation
    print("📝 Generating report markdown...")
    
    total_uu = len(df_user_growth)
    total_posts = df_daily_global['post_count'].sum()
    total_eng = df_daily_global['total_engagement'].sum()
    max_dau = df_daily_global['unique_user_count'].max()
    avg_dau = df_daily_global['unique_user_count'].mean()
    top_idol = df_daily_idol.groupby('idol_name')['post_count'].sum().idxmax()
    top_idol_posts = df_daily_idol.groupby('idol_name')['post_count'].sum().max()

    report_md = f"""
# 🌏 Gross Market Analysis Report

**分析日時**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 📊 市場規模サマリー
- **累積ユニークユーザー数 (Reach)**: {total_uu:,} 人
- **総投稿数**: {total_posts:,} 件
- **総エンゲージメント**: {total_eng:,} 件
- **DAU (最大)**: {max_dau:,} 人 / **(平均)**: {avg_dau:,.0f} 人
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

    report_path = project_root / "reports" / "gross_market_analysis.md"
    report_path.write_text(report_md, encoding='utf-8')
    print(f"✅ Report saved to {report_path}")

if __name__ == "__main__":
    main()


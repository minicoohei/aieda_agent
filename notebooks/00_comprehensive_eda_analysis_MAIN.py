"""櫻井優衣 包括的EDA分析（YData Profiling + AutoViz + 詳細Pandas分析）

このスクリプトは以下を実行します：
1. YData Profilingによる包括的HTMLレポート生成
2. AutoVizによる自動可視化
3. 詳細なPandas分析（crosstab、pivot_table、groupby等）
4. 全結果をreports/ディレクトリに保存
"""

import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# 環境設定
if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
    gac_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    if not os.path.exists(gac_path):
        del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

root_dir = Path(__file__).parent.parent
if str(root_dir / "src") not in sys.path:
    sys.path.insert(0, str(root_dir / "src"))

from ai_data_lab.connectors.bigquery import BigQueryConnector

# BigQuery 設定
PROJECT_ID = "yoake-dev-analysis"
DATASET_ID = "dev_yoake_posts"
TABLE_ID = "櫻井優衣"

# レポート出力ディレクトリ
reports_dir = root_dir / "reports"
reports_dir.mkdir(exist_ok=True)

print("=" * 80)
print("🎀 櫻井優衣 包括的EDA分析実行")
print("=" * 80)
print()

# ================================================================================
# 1. データ読み込み
# ================================================================================
print("📥 1. データ読み込み中...")
connector = BigQueryConnector(project_id=PROJECT_ID)

base_query = f"""
WITH deduplicated AS (
  SELECT
    keyword,
    talentId,
    post.xPostId as post_id,
    TIMESTAMP_SECONDS(post.xPostCreatedAt) as created_at,
    post.xPostUrl as post_url,
    post.xPostContent as content,
    post.xPostQuotedCount as quoted_count,
    post.xPostRepostedCount as repost_count,
    post.xPostRepliedCount as reply_count,
    post.xPostLikedCount as like_count,
    ARRAY_LENGTH(post.xPostMediaList) as media_count,
    user.xPostUserId as user_id,
    user.xPostUserName as user_name,
    user.xPostUserBadge as user_badge,
    user.xProfileImageUrl as user_profile_image,
    ROW_NUMBER() OVER (PARTITION BY post.xPostId ORDER BY _PARTITIONTIME DESC) as row_num
  FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
  WHERE _PARTITIONTIME IS NOT NULL
)
SELECT
    keyword,
    talentId,
    post_id,
    created_at,
    post_url,
    content,
    quoted_count,
    repost_count,
    reply_count,
    like_count,
    media_count,
    user_id,
    user_name,
    user_badge,
    user_profile_image
FROM deduplicated
WHERE row_num = 1
"""

df = connector.query(base_query)
print(f"✅ データ読み込み完了: {len(df):,} 行 × {len(df.columns)} 列\n")

# データ加工
print("🔧 データ加工中...")
df['created_at'] = pd.to_datetime(df['created_at'])
df['date'] = df['created_at'].dt.date
df['hour'] = df['created_at'].dt.hour
df['day_of_week'] = df['created_at'].dt.dayofweek
df['weekday_name'] = df['created_at'].dt.day_name()
df['has_media'] = df['media_count'] > 0
df['total_engagement'] = df['like_count'] + df['repost_count'] + df['reply_count'] + df['quoted_count']
df['content_length'] = df['content'].fillna('').str.len()
df['has_url'] = df['content'].fillna('').str.contains('http')
df['engagement_per_char'] = df['total_engagement'] / (df['content_length'] + 1)  # ゼロ除算回避
print("✅ データ加工完了\n")

# ================================================================================
# 2. YData Profiling レポート生成
# ================================================================================
print("=" * 80)
print("📊 2. YData Profiling レポート生成中...")
print("=" * 80)

try:
    from ydata_profiling import ProfileReport
    
    # 軽量化されたプロファイル（大規模データ対応）
    profile = ProfileReport(
        df,
        title="櫻井優衣 投稿データ プロファイリングレポート",
        minimal=False,
        explorative=True,
        progress_bar=True
    )
    
    output_path = reports_dir / "櫻井優衣_ydata_profiling_report.html"
    profile.to_file(output_path)
    print(f"✅ YData Profiling レポート保存: {output_path}")
    print(f"   ブラウザで開く: open {output_path}")
    print()
except Exception as e:
    print(f"⚠️ YData Profiling エラー: {e}")
    print("   スキップして次の分析に進みます...\n")

# ================================================================================
# 3. 詳細Pandas分析
# ================================================================================
print("=" * 80)
print("📈 3. 詳細Pandas分析実行中...")
print("=" * 80)

# 3.1 曜日×時間帯のクロス集計
print("\n📅 3.1 曜日 × 時間帯 クロス集計")
weekday_hour_crosstab = pd.crosstab(df['weekday_name'], df['hour'], margins=True)
print(weekday_hour_crosstab)
weekday_hour_crosstab.to_csv(reports_dir / "crosstab_weekday_hour.csv")
print(f"✅ 保存: crosstab_weekday_hour.csv")

# 3.2 メディア有無×バッジ有無のクロス集計
print("\n🎬 3.2 メディア有無 × バッジ有無 クロス集計")
media_badge_crosstab = pd.crosstab(
    df['has_media'], 
    df['user_badge'], 
    values=df['total_engagement'], 
    aggfunc='mean',
    margins=True
)
print(media_badge_crosstab)
media_badge_crosstab.to_csv(reports_dir / "crosstab_media_badge_engagement.csv")
print(f"✅ 保存: crosstab_media_badge_engagement.csv")

# 3.3 日別×メディア有無のピボットテーブル
print("\n📊 3.3 日別 × メディア有無 ピボットテーブル（投稿数・平均エンゲージメント）")
daily_media_pivot = df.pivot_table(
    index='date',
    columns='has_media',
    values=['post_id', 'total_engagement'],
    aggfunc={'post_id': 'count', 'total_engagement': 'mean'}
)
print(daily_media_pivot.head(10))
daily_media_pivot.to_csv(reports_dir / "pivot_daily_media.csv")
print(f"✅ 保存: pivot_daily_media.csv")

# 3.4 時間帯別の詳細統計
print("\n🕐 3.4 時間帯別詳細統計")
hourly_stats = df.groupby('hour').agg({
    'post_id': 'count',
    'like_count': ['mean', 'median', 'std', 'max'],
    'repost_count': ['mean', 'median', 'std', 'max'],
    'total_engagement': ['mean', 'median', 'std', 'max'],
    'content_length': ['mean', 'median'],
    'has_media': 'mean'
}).round(2)
hourly_stats.columns = ['_'.join(col).strip() for col in hourly_stats.columns.values]
print(hourly_stats)
hourly_stats.to_csv(reports_dir / "stats_hourly_detailed.csv")
print(f"✅ 保存: stats_hourly_detailed.csv")

# 3.5 曜日別の詳細統計
print("\n📆 3.5 曜日別詳細統計")
weekday_stats = df.groupby('weekday_name').agg({
    'post_id': 'count',
    'like_count': ['mean', 'median', 'std', 'max'],
    'repost_count': ['mean', 'median', 'std', 'max'],
    'total_engagement': ['mean', 'median', 'std', 'max'],
    'has_media': 'mean'
}).round(2)
weekday_stats.columns = ['_'.join(col).strip() for col in weekday_stats.columns.values]
weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
weekday_stats = weekday_stats.reindex(weekday_order)
print(weekday_stats)
weekday_stats.to_csv(reports_dir / "stats_weekday_detailed.csv")
print(f"✅ 保存: stats_weekday_detailed.csv")

# 3.6 ユーザーバッジ有無別の詳細比較
print("\n👥 3.6 ユーザーバッジ有無別詳細比較")
badge_comparison = df.groupby('user_badge').agg({
    'post_id': 'count',
    'user_id': 'nunique',
    'like_count': ['mean', 'median', 'std', 'max'],
    'repost_count': ['mean', 'median', 'std', 'max'],
    'reply_count': ['mean', 'median', 'std', 'max'],
    'quoted_count': ['mean', 'median', 'std', 'max'],
    'total_engagement': ['mean', 'median', 'std', 'max'],
    'content_length': ['mean', 'median'],
    'has_media': 'mean'
}).round(2)
badge_comparison.columns = ['_'.join(col).strip() for col in badge_comparison.columns.values]
print(badge_comparison)
badge_comparison.to_csv(reports_dir / "stats_badge_comparison.csv")
print(f"✅ 保存: stats_badge_comparison.csv")

# 3.7 エンゲージメント四分位数分析
print("\n💬 3.7 エンゲージメント四分位数分析")
try:
    # 重複値が多い場合は自動でビン数を調整
    df['engagement_quartile'] = pd.qcut(df['total_engagement'], q=4, labels=False, duplicates='drop')
    # カテゴリ名を追加
    df['engagement_quartile'] = df['engagement_quartile'].map({0: 'Q1(低)', 1: 'Q2(中下)', 2: 'Q3(中上)', 3: 'Q4(高)'})
    
    quartile_analysis = df.groupby('engagement_quartile').agg({
        'post_id': 'count',
        'like_count': ['min', 'mean', 'max'],
        'repost_count': ['min', 'mean', 'max'],
        'total_engagement': ['min', 'mean', 'max'],
        'has_media': 'mean',
        'content_length': 'mean',
        'user_badge': lambda x: x.sum()
    }).round(2)
    quartile_analysis.columns = ['_'.join(col).strip() for col in quartile_analysis.columns.values]
    print(quartile_analysis)
    quartile_analysis.to_csv(reports_dir / "stats_engagement_quartiles.csv")
    print("✅ 保存: stats_engagement_quartiles.csv")
except Exception as e:
    print(f"⚠️ 四分位数分析エラー: {e}")
    print("   多数のゼロ値のため、代替分析を実施...")
    # 代替: ゼロと非ゼロで分割
    df['engagement_category'] = df['total_engagement'].apply(lambda x: 'ゼロ' if x == 0 else ('低(1-10)' if x <= 10 else ('中(11-100)' if x <= 100 else '高(100+)')))
    alt_analysis = df.groupby('engagement_category').agg({
        'post_id': 'count',
        'like_count': ['min', 'mean', 'max'],
        'total_engagement': ['min', 'mean', 'max'],
        'has_media': 'mean'
    }).round(2)
    alt_analysis.columns = ['_'.join(col).strip() for col in alt_analysis.columns.values]
    print(alt_analysis)
    alt_analysis.to_csv(reports_dir / "stats_engagement_categories.csv")
    print("✅ 保存: stats_engagement_categories.csv")

# 3.8 メディア数別分析
print("\n🎬 3.8 メディア数別分析")
media_count_analysis = df.groupby('media_count').agg({
    'post_id': 'count',
    'like_count': ['mean', 'median'],
    'repost_count': ['mean', 'median'],
    'total_engagement': ['mean', 'median']
}).round(2)
media_count_analysis.columns = ['_'.join(col).strip() for col in media_count_analysis.columns.values]
print(media_count_analysis)
media_count_analysis.to_csv(reports_dir / "stats_media_count.csv")
print(f"✅ 保存: stats_media_count.csv")

# 3.9 コンテンツ長別分析（ビン分割）
print("\n📝 3.9 コンテンツ長別分析")
df['content_length_bin'] = pd.cut(df['content_length'], bins=[0, 50, 100, 150, 200, 300], labels=['短(0-50)', '中(51-100)', '長(101-150)', '超長(151-200)', '極長(200+)'])
content_length_analysis = df.groupby('content_length_bin').agg({
    'post_id': 'count',
    'total_engagement': ['mean', 'median'],
    'has_media': 'mean',
    'like_count': 'mean'
}).round(2)
content_length_analysis.columns = ['_'.join(col).strip() for col in content_length_analysis.columns.values]
print(content_length_analysis)
content_length_analysis.to_csv(reports_dir / "stats_content_length.csv")
print(f"✅ 保存: stats_content_length.csv")

# 3.10 トップユーザー詳細分析
print("\n🏆 3.10 トップユーザー詳細分析（TOP 20）")
top_users_analysis = df.groupby(['user_id', 'user_name', 'user_badge']).agg({
    'post_id': 'count',
    'like_count': ['sum', 'mean', 'median', 'max'],
    'repost_count': ['sum', 'mean', 'max'],
    'total_engagement': ['sum', 'mean', 'max'],
    'has_media': 'mean',
    'content_length': 'mean'
}).round(2)
top_users_analysis.columns = ['_'.join(col).strip() for col in top_users_analysis.columns.values]
top_users_analysis = top_users_analysis.sort_values('post_id_count', ascending=False).head(20)
print(top_users_analysis)
top_users_analysis.to_csv(reports_dir / "stats_top_users_detailed.csv")
print(f"✅ 保存: stats_top_users_detailed.csv")

# 3.11 相関行列（数値カラム）
print("\n🔗 3.11 相関行列")
numeric_cols = ['like_count', 'repost_count', 'reply_count', 'quoted_count', 'total_engagement', 'content_length', 'media_count']
correlation_matrix = df[numeric_cols].corr().round(3)
print(correlation_matrix)
correlation_matrix.to_csv(reports_dir / "correlation_matrix.csv")
print(f"✅ 保存: correlation_matrix.csv")

# 3.12 日別エンゲージメント推移
print("\n📈 3.12 日別エンゲージメント推移")
daily_engagement = df.groupby('date').agg({
    'post_id': 'count',
    'like_count': ['sum', 'mean'],
    'repost_count': ['sum', 'mean'],
    'total_engagement': ['sum', 'mean'],
    'has_media': 'mean'
}).round(2)
daily_engagement.columns = ['_'.join(col).strip() for col in daily_engagement.columns.values]
print(daily_engagement.head(10))
print("...")
print(daily_engagement.tail(10))
daily_engagement.to_csv(reports_dir / "timeseries_daily_engagement.csv")
print(f"✅ 保存: timeseries_daily_engagement.csv")

print("\n✅ 詳細Pandas分析完了！\n")

# ================================================================================
# 4. AutoViz 自動可視化
# ================================================================================
print("=" * 80)
print("🎨 4. AutoViz 自動可視化実行中...")
print("=" * 80)

try:
    from autoviz.AutoViz_Class import AutoViz_Class
    
    # AutoVizは数値・カテゴリカルデータを自動認識して可視化
    # 大規模データの場合はサンプリング
    sample_size = min(5000, len(df))
    df_sample = df.sample(n=sample_size, random_state=42) if len(df) > sample_size else df
    
    # AutoViz用にデータ準備（datetimeをstrに変換）
    df_autoviz = df_sample.copy()
    df_autoviz['created_at'] = df_autoviz['created_at'].astype(str)
    df_autoviz['date'] = df_autoviz['date'].astype(str)
    
    # 一時CSVに保存（AutoVizはファイルパス必要）
    temp_csv = reports_dir / "temp_autoviz_data.csv"
    df_autoviz.to_csv(temp_csv, index=False)
    
    AV = AutoViz_Class()
    
    # AutoViz実行（HTMLレポート生成）
    autoviz_output = reports_dir / "autoviz_visualizations"
    autoviz_output.mkdir(exist_ok=True)
    
    print(f"   サンプルサイズ: {len(df_autoviz):,} 行")
    print(f"   出力先: {autoviz_output}")
    
    dft = AV.AutoViz(
        filename=str(temp_csv),
        sep=',',
        depVar='total_engagement',
        dfte=None,
        header=0,
        verbose=1,
        lowess=False,
        chart_format='html',
        max_rows_analyzed=5000,
        max_cols_analyzed=30,
        save_plot_dir=str(autoviz_output)
    )
    
    # 一時ファイル削除
    temp_csv.unlink()
    
    print(f"✅ AutoViz 可視化完了: {autoviz_output}")
    print(f"   HTMLファイルをブラウザで確認してください")
    print()
except Exception as e:
    print(f"⚠️ AutoViz エラー: {e}")
    print("   スキップして次の分析に進みます...\n")

# ================================================================================
# 5. サマリーレポート生成
# ================================================================================
print("=" * 80)
print("📄 5. サマリーレポート生成中...")
print("=" * 80)

# ゼロ除算ガード付きでメディア倍率とバッジ影響力を計算
media_with_eng = df[df['has_media']]['total_engagement'].mean()
media_without_eng = df[~df['has_media']]['total_engagement'].mean()
media_ratio = media_with_eng / media_without_eng if media_without_eng != 0 else float('nan')

badge_with_eng = df[df['user_badge']]['total_engagement'].mean()
badge_without_eng = df[~df['user_badge']]['total_engagement'].mean()
badge_ratio = badge_with_eng / badge_without_eng if badge_without_eng != 0 else float('nan')

summary_report = f"""
# 櫻井優衣 包括的EDA分析サマリー

**実施日時**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**データ件数**: {len(df):,} 件

## 生成ファイル一覧

### 1. YData Profiling レポート
- `櫻井優衣_ydata_profiling_report.html` - 包括的データプロファイリング

### 2. AutoViz 可視化
- `autoviz_visualizations/` ディレクトリ - 自動生成された可視化HTML

### 3. 詳細Pandas分析CSV

#### クロス集計
- `crosstab_weekday_hour.csv` - 曜日×時間帯の投稿数
- `crosstab_media_badge_engagement.csv` - メディア×バッジ別平均エンゲージメント

#### ピボットテーブル
- `pivot_daily_media.csv` - 日別×メディア有無の投稿数・エンゲージメント

#### 統計分析
- `stats_hourly_detailed.csv` - 時間帯別詳細統計
- `stats_weekday_detailed.csv` - 曜日別詳細統計
- `stats_badge_comparison.csv` - バッジ有無別比較
- `stats_engagement_quartiles.csv` - エンゲージメント四分位数分析
- `stats_media_count.csv` - メディア数別分析
- `stats_content_length.csv` - コンテンツ長別分析
- `stats_top_users_detailed.csv` - トップユーザー詳細（TOP 20）

#### 相関・時系列
- `correlation_matrix.csv` - 相関行列
- `timeseries_daily_engagement.csv` - 日別エンゲージメント推移

## 主要発見（再掲）

1. **総投稿数**: {len(df):,} 件（重複除去済み）
2. **ユニークユーザー**: {df['user_id'].nunique():,} 人
3. **平均エンゲージメント**: {df['total_engagement'].mean():.2f}
4. **メディア倍率**: {media_ratio:.2f}x
5. **バッジユーザー影響力**: {badge_ratio:.2f}x

## 次のステップ

1. YData Profiling HTMLレポートを開いて、全体像を把握
2. AutoViz可視化で、パターンを視覚的に確認
3. CSV分析結果をExcelやPandasで深堀り
4. 特定の仮説（例：木曜日の影響、メディアタイプ別効果）を追加検証

---
"""

summary_path = reports_dir / "包括的EDA分析_サマリー.md"
with open(summary_path, 'w', encoding='utf-8') as f:
    f.write(summary_report)

print(f"✅ サマリーレポート保存: {summary_path}")
print()

print("=" * 80)
print("✅ 包括的EDA分析完了！")
print("=" * 80)
print()
print("📂 生成ファイル:")
print(f"   1. YData Profiling: {reports_dir}/櫻井優衣_ydata_profiling_report.html")
print(f"   2. AutoViz: {reports_dir}/autoviz_visualizations/")
print(f"   3. 詳細CSV: {reports_dir}/*.csv (12ファイル)")
print(f"   4. サマリー: {reports_dir}/包括的EDA分析_サマリー.md")
print()
print("🎉 全ての分析が完了しました！")


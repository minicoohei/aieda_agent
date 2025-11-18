"""櫻井優衣 わかりやすい可視化レポート

目的：
- index番号ではなく、意味のあるラベル（ユーザー名、日付など）を使用
- 適切なグラフタイプを選択（棒グラフ、折れ線グラフ、ヒートマップ等）
- 日本語ラベルで見やすく表示
"""

import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # GUIなしのバックエンドを使用
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# 日本語フォント設定
try:
    import japanize_matplotlib
except:
    print("⚠️ japanize_matplotlib がインストールされていません")

# 環境設定
if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
    gac_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    if not os.path.exists(gac_path):
        del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

root_dir = Path(__file__).parent.parent
if str(root_dir / "src") not in sys.path:
    sys.path.insert(0, str(root_dir / "src"))

from ai_data_lab.connectors.bigquery import BigQueryConnector

# 設定
PROJECT_ID = "yoake-dev-analysis"
DATASET_ID = "dev_yoake_posts"
TABLE_ID = "櫻井優衣"
viz_dir = root_dir / "reports" / "visualizations"
viz_dir.mkdir(parents=True, exist_ok=True)

# スタイル設定
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

print("=" * 80)
print("🎨 櫻井優衣 わかりやすい可視化レポート生成")
print("=" * 80)
print()

# データ読み込み
print("📥 データ読み込み中...")
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
print(f"✅ データ読み込み完了: {len(df):,} 行\n")

# データ加工
df['created_at'] = pd.to_datetime(df['created_at'])
df['date'] = df['created_at'].dt.date
df['hour'] = df['created_at'].dt.hour
df['day_of_week'] = df['created_at'].dt.dayofweek
df['weekday_name'] = df['created_at'].dt.day_name()
df['weekday_jp'] = df['weekday_name'].map({
    'Monday': '月曜日',
    'Tuesday': '火曜日',
    'Wednesday': '水曜日',
    'Thursday': '木曜日',
    'Friday': '金曜日',
    'Saturday': '土曜日',
    'Sunday': '日曜日'
})
df['has_media'] = df['media_count'] > 0
df['media_label'] = df['has_media'].map({True: 'メディアあり', False: 'メディアなし'})
df['total_engagement'] = df['like_count'] + df['repost_count'] + df['reply_count'] + df['quoted_count']
df['content_length'] = df['content'].fillna('').str.len()

print("=" * 80)
print("📊 1. 曜日別投稿数（棒グラフ）")
print("=" * 80)

# 曜日順に並べ替え
weekday_order_jp = ['月曜日', '火曜日', '水曜日', '木曜日', '金曜日', '土曜日', '日曜日']
weekday_counts = df['weekday_jp'].value_counts().reindex(weekday_order_jp)

fig, ax = plt.subplots(figsize=(12, 6))
bars = ax.bar(weekday_counts.index, weekday_counts.values, color='skyblue', edgecolor='navy', alpha=0.7)

# 数値ラベルを追加
for bar in bars:
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., height,
            f'{int(height):,}件',
            ha='center', va='bottom', fontsize=10, fontweight='bold')

ax.set_xlabel('曜日', fontsize=12, fontweight='bold')
ax.set_ylabel('投稿数', fontsize=12, fontweight='bold')
ax.set_title('櫻井優衣 曜日別投稿数', fontsize=14, fontweight='bold', pad=20)
ax.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.savefig(viz_dir / '01_weekday_posts_bar.png', dpi=300, bbox_inches='tight')
print(f"✅ 保存: 01_weekday_posts_bar.png")
plt.close()

print("\n" + "=" * 80)
print("📊 2. 時間帯別投稿数（折れ線グラフ）")
print("=" * 80)

hourly_counts = df['hour'].value_counts().sort_index()

fig, ax = plt.subplots(figsize=(14, 6))
ax.plot(hourly_counts.index, hourly_counts.values, marker='o', linewidth=2, 
        markersize=8, color='coral', markeredgecolor='darkred', markeredgewidth=1.5)
ax.fill_between(hourly_counts.index, hourly_counts.values, alpha=0.3, color='coral')

# ピーク時間をハイライト
peak_hour = hourly_counts.idxmax()
ax.axvline(peak_hour, color='red', linestyle='--', linewidth=2, alpha=0.7, label=f'ピーク: {peak_hour}時')

ax.set_xlabel('時間帯', fontsize=12, fontweight='bold')
ax.set_ylabel('投稿数', fontsize=12, fontweight='bold')
ax.set_title('櫻井優衣 時間帯別投稿数', fontsize=14, fontweight='bold', pad=20)
ax.set_xticks(range(0, 24))
ax.grid(True, alpha=0.3)
ax.legend(fontsize=11)
plt.tight_layout()
plt.savefig(viz_dir / '02_hourly_posts_line.png', dpi=300, bbox_inches='tight')
print(f"✅ 保存: 02_hourly_posts_line.png")
plt.close()

print("\n" + "=" * 80)
print("📊 3. 日別投稿数推移（折れ線グラフ）")
print("=" * 80)

daily_counts = df.groupby('date').size().reset_index(name='count')
daily_counts['date'] = pd.to_datetime(daily_counts['date'])

fig, ax = plt.subplots(figsize=(16, 6))
ax.plot(daily_counts['date'], daily_counts['count'], marker='o', linewidth=2,
        markersize=5, color='green', markeredgecolor='darkgreen')

# ピーク日をハイライト
peak_date = daily_counts.loc[daily_counts['count'].idxmax()]
ax.scatter([peak_date['date']], [peak_date['count']], color='red', s=200, zorder=5, 
           label=f"ピーク: {peak_date['date'].strftime('%m/%d')} ({int(peak_date['count'])}件)")

ax.set_xlabel('日付', fontsize=12, fontweight='bold')
ax.set_ylabel('投稿数', fontsize=12, fontweight='bold')
ax.set_title('櫻井優衣 日別投稿数推移（35日間）', fontsize=14, fontweight='bold', pad=20)
ax.grid(True, alpha=0.3)
ax.legend(fontsize=11)
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(viz_dir / '03_daily_posts_timeline.png', dpi=300, bbox_inches='tight')
print(f"✅ 保存: 03_daily_posts_timeline.png")
plt.close()

print("\n" + "=" * 80)
print("📊 4. メディア有無別エンゲージメント比較（棒グラフ）")
print("=" * 80)

media_engagement = df.groupby('media_label')['total_engagement'].mean().sort_values(ascending=False)

fig, ax = plt.subplots(figsize=(10, 6))
bars = ax.bar(media_engagement.index, media_engagement.values, 
              color=['#ff6b6b', '#4ecdc4'], edgecolor='black', alpha=0.8)

# 数値ラベル
for bar in bars:
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., height,
            f'{height:.1f}',
            ha='center', va='bottom', fontsize=12, fontweight='bold')

ax.set_ylabel('平均エンゲージメント', fontsize=12, fontweight='bold')
ax.set_title('メディア有無別 平均エンゲージメント比較', fontsize=14, fontweight='bold', pad=20)
ax.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.savefig(viz_dir / '04_media_engagement_comparison.png', dpi=300, bbox_inches='tight')
print(f"✅ 保存: 04_media_engagement_comparison.png")
plt.close()

print("\n" + "=" * 80)
print("📊 5. TOP 20 ユーザー投稿数（横棒グラフ）")
print("=" * 80)

top_users = df.groupby('user_name').size().sort_values(ascending=True).tail(20)

fig, ax = plt.subplots(figsize=(10, 10))
bars = ax.barh(range(len(top_users)), top_users.values, color='purple', edgecolor='darkviolet', alpha=0.7)

# ユーザー名をラベルに
ax.set_yticks(range(len(top_users)))
ax.set_yticklabels(top_users.index, fontsize=10)

# 数値ラベル
for i, (bar, value) in enumerate(zip(bars, top_users.values)):
    ax.text(value, i, f' {value}件', va='center', fontsize=9, fontweight='bold')

ax.set_xlabel('投稿数', fontsize=12, fontweight='bold')
ax.set_title('TOP 20 投稿数の多いユーザー', fontsize=14, fontweight='bold', pad=20)
ax.grid(axis='x', alpha=0.3)
plt.tight_layout()
plt.savefig(viz_dir / '05_top_users_bar.png', dpi=300, bbox_inches='tight')
print(f"✅ 保存: 05_top_users_bar.png")
plt.close()

print("\n" + "=" * 80)
print("📊 6. 曜日×時間帯ヒートマップ")
print("=" * 80)

heatmap_data = pd.crosstab(df['weekday_jp'], df['hour'])
heatmap_data = heatmap_data.reindex(weekday_order_jp)

fig, ax = plt.subplots(figsize=(16, 8))
sns.heatmap(heatmap_data, annot=True, fmt='d', cmap='YlOrRd', cbar_kws={'label': '投稿数'},
            linewidths=0.5, linecolor='gray', ax=ax)

ax.set_xlabel('時間帯', fontsize=12, fontweight='bold')
ax.set_ylabel('曜日', fontsize=12, fontweight='bold')
ax.set_title('曜日×時間帯 投稿数ヒートマップ', fontsize=14, fontweight='bold', pad=20)
plt.tight_layout()
plt.savefig(viz_dir / '06_weekday_hour_heatmap.png', dpi=300, bbox_inches='tight')
print(f"✅ 保存: 06_weekday_hour_heatmap.png")
plt.close()

print("\n" + "=" * 80)
print("📊 7. エンゲージメント種別比較（積み上げ棒グラフ）")
print("=" * 80)

engagement_by_weekday = df.groupby('weekday_jp')[['like_count', 'repost_count', 'reply_count', 'quoted_count']].sum()
engagement_by_weekday = engagement_by_weekday.reindex(weekday_order_jp)

fig, ax = plt.subplots(figsize=(12, 6))
engagement_by_weekday.plot(kind='bar', stacked=True, ax=ax, 
                           color=['#ff6b6b', '#4ecdc4', '#ffd93d', '#a8e6cf'],
                           edgecolor='black', alpha=0.8)

ax.set_xlabel('曜日', fontsize=12, fontweight='bold')
ax.set_ylabel('エンゲージメント総数', fontsize=12, fontweight='bold')
ax.set_title('曜日別エンゲージメント内訳（積み上げ）', fontsize=14, fontweight='bold', pad=20)
ax.legend(['いいね', 'リポスト', '返信', '引用'], fontsize=10, loc='upper left')
ax.grid(axis='y', alpha=0.3)
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(viz_dir / '07_engagement_breakdown_stacked.png', dpi=300, bbox_inches='tight')
print(f"✅ 保存: 07_engagement_breakdown_stacked.png")
plt.close()

print("\n" + "=" * 80)
print("📊 8. コンテンツ長別平均エンゲージメント（棒グラフ）")
print("=" * 80)

df['length_category'] = pd.cut(df['content_length'], 
                                bins=[0, 50, 100, 150, 200, 300],
                                labels=['短\n(0-50)', '中\n(51-100)', '長\n(101-150)', '超長\n(151-200)', '極長\n(200+)'])

length_engagement = df.groupby('length_category')['total_engagement'].mean()

fig, ax = plt.subplots(figsize=(12, 6))
bars = ax.bar(length_engagement.index.astype(str), length_engagement.values,
              color=['#ffadad', '#ffd6a5', '#fdffb6', '#caffbf', '#9bf6ff'],
              edgecolor='black', alpha=0.8)

# 数値ラベル
for bar in bars:
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., height,
            f'{height:.1f}',
            ha='center', va='bottom', fontsize=11, fontweight='bold')

ax.set_xlabel('コンテンツ長カテゴリ', fontsize=12, fontweight='bold')
ax.set_ylabel('平均エンゲージメント', fontsize=12, fontweight='bold')
ax.set_title('コンテンツ長別 平均エンゲージメント', fontsize=14, fontweight='bold', pad=20)
ax.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.savefig(viz_dir / '08_content_length_engagement.png', dpi=300, bbox_inches='tight')
print(f"✅ 保存: 08_content_length_engagement.png")
plt.close()

print("\n" + "=" * 80)
print("📊 9. TOP 10 高エンゲージメント投稿（横棒グラフ）")
print("=" * 80)

top_posts = df.nlargest(10, 'total_engagement')[['user_name', 'total_engagement', 'content']].copy()
top_posts['label'] = top_posts.apply(lambda x: f"{x['user_name'][:15]}...", axis=1)
top_posts = top_posts.sort_values('total_engagement', ascending=True)

fig, ax = plt.subplots(figsize=(12, 8))
bars = ax.barh(range(len(top_posts)), top_posts['total_engagement'].values,
              color='gold', edgecolor='orange', alpha=0.8)

ax.set_yticks(range(len(top_posts)))
ax.set_yticklabels(top_posts['label'].values, fontsize=10)

# 数値ラベル
for i, (bar, value) in enumerate(zip(bars, top_posts['total_engagement'].values)):
    ax.text(value, i, f' {int(value):,}', va='center', fontsize=9, fontweight='bold')

ax.set_xlabel('総エンゲージメント', fontsize=12, fontweight='bold')
ax.set_title('TOP 10 エンゲージメントの高い投稿', fontsize=14, fontweight='bold', pad=20)
ax.grid(axis='x', alpha=0.3)
plt.tight_layout()
plt.savefig(viz_dir / '09_top_engagement_posts.png', dpi=300, bbox_inches='tight')
print(f"✅ 保存: 09_top_engagement_posts.png")
plt.close()

print("\n" + "=" * 80)
print("📊 10. メディア枚数別エンゲージメント（棒グラフ）")
print("=" * 80)

media_count_engagement = df.groupby('media_count')['total_engagement'].mean()
media_count_labels = [f'{int(i)}枚' for i in media_count_engagement.index]

fig, ax = plt.subplots(figsize=(10, 6))
bars = ax.bar(media_count_labels, media_count_engagement.values,
              color=['#e63946', '#f1faee', '#a8dadc', '#457b9d', '#1d3557'],
              edgecolor='black', alpha=0.8)

# 数値ラベル
for bar in bars:
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., height,
            f'{height:.1f}',
            ha='center', va='bottom', fontsize=11, fontweight='bold')

ax.set_xlabel('メディア枚数', fontsize=12, fontweight='bold')
ax.set_ylabel('平均エンゲージメント', fontsize=12, fontweight='bold')
ax.set_title('メディア枚数別 平均エンゲージメント', fontsize=14, fontweight='bold', pad=20)
ax.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.savefig(viz_dir / '10_media_count_engagement.png', dpi=300, bbox_inches='tight')
print(f"✅ 保存: 10_media_count_engagement.png")
plt.close()

print("\n" + "=" * 80)
print("✅ 全ての可視化完了！")
print("=" * 80)
print()
print(f"📂 保存先: {viz_dir}")
print()
print("生成されたグラフ:")
print("  1. 01_weekday_posts_bar.png - 曜日別投稿数（棒グラフ）")
print("  2. 02_hourly_posts_line.png - 時間帯別投稿数（折れ線グラフ）")
print("  3. 03_daily_posts_timeline.png - 日別投稿数推移（折れ線グラフ）")
print("  4. 04_media_engagement_comparison.png - メディア有無別比較")
print("  5. 05_top_users_bar.png - TOP 20ユーザー（横棒グラフ）")
print("  6. 06_weekday_hour_heatmap.png - 曜日×時間帯ヒートマップ")
print("  7. 07_engagement_breakdown_stacked.png - エンゲージメント内訳")
print("  8. 08_content_length_engagement.png - コンテンツ長別")
print("  9. 09_top_engagement_posts.png - TOP 10投稿")
print(" 10. 10_media_count_engagement.png - メディア枚数別")
print()
print("🎉 全てのグラフにわかりやすいラベルと適切なグラフタイプを使用しました！")


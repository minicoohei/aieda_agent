"""櫻井優衣 EDA 分析実行スクリプト

このスクリプトは全てのEDA分析を実行し、結果を表示・保存します。
"""

import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
import re
from collections import Counter
import json

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

print("=" * 80)
print("🎀 櫻井優衣 投稿データ EDA 分析実行")
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
    REGEXP_EXTRACT(
      post.xPostUrl,
      r'^https://x\\.com/([^/]+)/status'
    ) as post_user_handle,
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
    post_user_handle,
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
WHERE
  row_num = 1
  AND post_user_handle NOT IN (
    'FRUITS_ZIPPER',
    'amane_fz1026',
    'suzuka_fz1124',
    'yui_fz0221',
    'luna_fz0703',
    'manafy_fz0422',
    'karen_fz0328',
    'noel_fz1229',
    'CUTIE_STREET_',
    'aika_cs1126',
    'risa_cs1108',
    'ayano_cs0526',
    'emiru_cs0422',
    'kana_cs1111',
    'haruka_cs0129',
    'miyu_cs0913',
    'nagisa_cs0628',
    'candy_tune_',
    'mizuki_ct0221',
    'rino_ct1224',
    'nachico_ct1001',
    'natsu_ct0317',
    'kotomi_ct0525',
    'shizuka_ct0530',
    'bibian_ct1203',
    'SWEET_STEADY',
    'rise_ss0731',
    'ayu_ss0107',
    'sakina_ss0229',
    'nagisa_ss1029',
    'natsuka_ss0719',
    'mayumi_ss1227',
    'yui_ss0109',
    'nogizaka46',
    'takanenofficial',
    'nao_kizuki',
    'hina_hinahata',
    'Mikuru_hositani',
    'erisahigasiyama',
    'momonamatsumoto',
    'MomokoHashimoto',
    'su_suzumi_',
    'himeri_momiyama',
    'saara_hazuki',
    'Equal_LOVE_12',
    'otani_emiri',
    'hana_oba',
    'otoshima_risa',
    'saitou_kiara',
    'sasaki_maika',
    'takamatsuhitomi',
    'shoko_takiwaki',
    'noguchi_iori',
    'morohashi_sana',
    'yamamoto_anna_'
  )
"""

df = connector.query(base_query)
print(f"✅ データ読み込み完了: {len(df):,} 行 × {len(df.columns)} 列\n")

# データ加工
print("🔧 データ加工中...")
df['date'] = pd.to_datetime(df['created_at']).dt.date
df['hour'] = pd.to_datetime(df['created_at']).dt.hour
df['day_of_week'] = pd.to_datetime(df['created_at']).dt.dayofweek
df['weekday_name'] = pd.to_datetime(df['created_at']).dt.day_name()
df['has_media'] = df['media_count'] > 0
df['total_engagement'] = df['like_count'] + df['repost_count'] + df['reply_count'] + df['quoted_count']
df['content_length'] = df['content'].fillna('').str.len()
df['has_url'] = df['content'].fillna('').str.contains('http')
print("✅ データ加工完了\n")

# 分析結果を格納する辞書
results = {}

print("=" * 80)
print("📊 1. 基本統計")
print("=" * 80)
results['basic_stats'] = {
    '総投稿数': len(df),
    'ユニークユーザー数': int(df['user_id'].nunique()),
    'ユニーク投稿ID数': int(df['post_id'].nunique()),
    '期間_開始': str(df['created_at'].min()),
    '期間_終了': str(df['created_at'].max()),
    '日数': int((df['created_at'].max() - df['created_at'].min()).days),
}
for key, value in results['basic_stats'].items():
    print(f"  {key}: {value}")
print()

print("=" * 80)
print("📅 2. 投稿パターン分析")
print("=" * 80)

# 日別投稿数
daily_posts = df.groupby('date').size().reset_index(name='post_count')
results['daily_posts'] = {
    '平均投稿数_日': float(daily_posts['post_count'].mean()),
    '中央値投稿数_日': float(daily_posts['post_count'].median()),
    '最大投稿数_日': int(daily_posts['post_count'].max()),
    '最小投稿数_日': int(daily_posts['post_count'].min()),
}
print("\n📈 日別投稿数:")
for key, value in results['daily_posts'].items():
    print(f"  {key}: {value}")

# 曜日別投稿数
weekday_posts = df.groupby('weekday_name').size().reset_index(name='post_count')
weekday_posts = weekday_posts.sort_values('post_count', ascending=False)
results['weekday_posts'] = weekday_posts.to_dict('records')
print("\n📆 曜日別投稿数 (TOP 3):")
for i, row in weekday_posts.head(3).iterrows():
    print(f"  {row['weekday_name']}: {row['post_count']:,} 件")

# 時間帯別投稿数
hourly_posts = df.groupby('hour').size().reset_index(name='post_count')
hourly_posts = hourly_posts.sort_values('post_count', ascending=False)
results['hourly_posts'] = hourly_posts.to_dict('records')
print("\n🕐 時間帯別投稿数 (TOP 5):")
for i, row in hourly_posts.head(5).iterrows():
    print(f"  {row['hour']:02d}時: {row['post_count']:,} 件")
print()

print("=" * 80)
print("💬 3. エンゲージメント分析")
print("=" * 80)

engagement_cols = ['like_count', 'repost_count', 'reply_count', 'quoted_count', 'total_engagement']
engagement_stats = df[engagement_cols].describe()
results['engagement_stats'] = {
    'いいね_平均': float(df['like_count'].mean()),
    'いいね_中央値': float(df['like_count'].median()),
    'いいね_最大': int(df['like_count'].max()),
    'リポスト_平均': float(df['repost_count'].mean()),
    'リポスト_中央値': float(df['repost_count'].median()),
    '返信_平均': float(df['reply_count'].mean()),
    '返信_中央値': float(df['reply_count'].median()),
    '引用_平均': float(df['quoted_count'].mean()),
    '引用_中央値': float(df['quoted_count'].median()),
    '総エンゲージメント_平均': float(df['total_engagement'].mean()),
    '総エンゲージメント_中央値': float(df['total_engagement'].median()),
    '総エンゲージメント_最大': int(df['total_engagement'].max()),
}

print("\n📊 エンゲージメント統計:")
for key, value in results['engagement_stats'].items():
    if isinstance(value, float):
        print(f"  {key}: {value:.2f}")
    else:
        print(f"  {key}: {value:,}")

# 相関行列
engagement_corr = df[['like_count', 'repost_count', 'reply_count', 'quoted_count']].corr()
print("\n🔗 エンゲージメント相関:")
print(engagement_corr.to_string())

# 上位エンゲージメント投稿
top_engagement = df.nlargest(10, 'total_engagement')[
    ['created_at', 'user_name', 'content', 'like_count', 'repost_count', 'reply_count', 'quoted_count', 'total_engagement', 'has_media']
]
results['top_engagement_posts'] = top_engagement.to_dict('records')
print("\n🏆 TOP 10 エンゲージメント投稿:")
for i, row in top_engagement.iterrows():
    # content が None/NaN の場合にも安全にプレビュー生成
    if pd.isna(row["content"]):
        content = ""
    else:
        content = str(row["content"])
    content_preview = content[:50] + "..." if len(content) > 50 else content
    print(f"  {i+1}. {row['user_name']}: {content_preview}")
    print(f"     いいね: {row['like_count']}, リポスト: {row['repost_count']}, 返信: {row['reply_count']}, 総計: {row['total_engagement']}")
print()

print("=" * 80)
print("🎬 4. メディア分析")
print("=" * 80)

posts_with_media = df['has_media'].sum()
posts_without_media = (~df['has_media']).sum()

media_comparison = df.groupby('has_media').agg({
    'post_id': 'count',
    'like_count': 'mean',
    'repost_count': 'mean',
    'reply_count': 'mean',
    'quoted_count': 'mean',
    'total_engagement': 'mean'
}).round(2)

media_eng_with = df[df['has_media']]['total_engagement'].mean()
media_eng_without = df[~df['has_media']]['total_engagement'].mean()
media_ratio = media_eng_with / media_eng_without if media_eng_without != 0 else float('nan')

results['media_stats'] = {
    'メディアあり投稿数': int(posts_with_media),
    'メディアあり割合_%': float(posts_with_media / len(df) * 100),
    'メディアなし投稿数': int(posts_without_media),
    'メディアなし割合_%': float(posts_without_media / len(df) * 100),
    'メディアあり_平均エンゲージメント': float(media_eng_with),
    'メディアなし_平均エンゲージメント': float(media_eng_without),
    'エンゲージメント倍率': float(media_ratio),
}

print("\n📊 メディア統計:")
print(f"  メディアあり: {posts_with_media:,} 件 ({posts_with_media/len(df)*100:.1f}%)")
print(f"  メディアなし: {posts_without_media:,} 件 ({posts_without_media/len(df)*100:.1f}%)")
print(f"\n  メディアあり平均エンゲージメント: {results['media_stats']['メディアあり_平均エンゲージメント']:.2f}")
print(f"  メディアなし平均エンゲージメント: {results['media_stats']['メディアなし_平均エンゲージメント']:.2f}")
print(f"  エンゲージメント倍率: {results['media_stats']['エンゲージメント倍率']:.2f}x")

print("\n🎬 メディア有無別エンゲージメント詳細:")
print(media_comparison.to_string())
print()

print("=" * 80)
print("👥 5. ユーザー分析")
print("=" * 80)

user_stats = df.groupby('user_id').agg({
    'post_id': 'count',
    'user_name': 'first',
    'user_badge': 'first',
    'like_count': 'sum',
    'repost_count': 'sum',
    'reply_count': 'sum',
    'quoted_count': 'sum',
    'total_engagement': 'sum'
}).reset_index()

user_stats.columns = ['user_id', 'post_count', 'user_name', 'user_badge', 'total_likes', 'total_reposts', 'total_replies', 'total_quotes', 'total_engagement']
user_stats = user_stats.sort_values('post_count', ascending=False)

badge_users = user_stats[user_stats['user_badge'] == True]
non_badge_users = user_stats[user_stats['user_badge'] != True]

results['user_stats'] = {
    '総ユーザー数': int(len(user_stats)),
    'バッジ付きユーザー数': int(len(badge_users)),
    'バッジ付き割合_%': float(len(badge_users) / len(user_stats) * 100),
    '平均投稿数_ユーザー': float(user_stats['post_count'].mean()),
    '中央値投稿数_ユーザー': float(user_stats['post_count'].median()),
    '最大投稿数': int(user_stats['post_count'].max()),
    '最多投稿ユーザー': str(user_stats.iloc[0]['user_name']),
    'バッジあり_平均エンゲージメント_投稿': float(badge_users['total_engagement'].sum() / badge_users['post_count'].sum()) if len(badge_users) > 0 else 0,
    'バッジなし_平均エンゲージメント_投稿': float(non_badge_users['total_engagement'].sum() / non_badge_users['post_count'].sum()) if len(non_badge_users) > 0 else 0,
}

print("\n📊 ユーザー統計:")
for key, value in results['user_stats'].items():
    if isinstance(value, float):
        print(f"  {key}: {value:.2f}")
    else:
        print(f"  {key}: {value}")

print("\n🏆 TOP 10 投稿数ユーザー:")
top_users = user_stats.head(10)
for i, row in top_users.iterrows():
    badge = "✓" if row['user_badge'] else ""
    print(f"  {row['user_name']}{badge}: {row['post_count']} 件 (総エンゲージメント: {row['total_engagement']:,})")
print()

print("=" * 80)
print("📝 6. コンテンツ分析")
print("=" * 80)

results['content_stats'] = {
    '平均文字数': float(df['content_length'].mean()),
    '中央値文字数': float(df['content_length'].median()),
    '最大文字数': int(df['content_length'].max()),
    '最小文字数': int(df['content_length'].min()),
    'URL含有率_%': float(df['has_url'].sum() / len(df) * 100),
}

print("\n📏 コンテンツ長統計:")
for key, value in results['content_stats'].items():
    if isinstance(value, float):
        print(f"  {key}: {value:.2f}")
    else:
        print(f"  {key}: {value}")

# ハッシュタグ抽出
all_content = ' '.join(df['content'].fillna(''))
hashtags = re.findall(r'#\w+', all_content)
hashtag_freq = Counter(hashtags).most_common(20)
results['top_hashtags'] = [{'hashtag': h, 'count': c} for h, c in hashtag_freq]

print("\n🏷️ TOP 20 ハッシュタグ:")
for i, (tag, count) in enumerate(hashtag_freq, 1):
    print(f"  {i:2d}. {tag}: {count:,} 回")

# メンション抽出
mentions = re.findall(r'@\w+', all_content)
mention_freq = Counter(mentions).most_common(20)
results['top_mentions'] = [{'mention': m, 'count': c} for m, c in mention_freq]

print("\n👤 TOP 20 メンション:")
for i, (mention, count) in enumerate(mention_freq, 1):
    print(f"  {i:2d}. {mention}: {count:,} 回")
print()

# 結果をJSONで保存
print("=" * 80)
print("💾 結果保存中...")
output_file = root_dir / "reports" / "sakurai_yui_eda_results.json"
output_file.parent.mkdir(parents=True, exist_ok=True)

with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(results, f, ensure_ascii=False, indent=2, default=str)

print(f"✅ 結果を保存: {output_file}")
print()

print("=" * 80)
print("✅ EDA 分析完了！")
print("=" * 80)


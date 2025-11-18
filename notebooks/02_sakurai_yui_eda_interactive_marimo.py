"""櫻井優衣 EDA（探索的データ分析）ノートブック

## 目的
櫻井優衣に関するXの投稿データ（約10万件）を分析し、以下を明らかにする：
1. 投稿パターン（時系列・ボリューム）
2. エンゲージメント特性（いいね・リポスト・引用・返信）
3. メディアの影響（種別・有無別のエンゲージメント）
4. ユーザー分布（投稿数・バッジ）
5. コンテンツ特性（長さ・キーワード）

## データソース
- プロジェクト: yoake-dev-analysis
- データセット: dev_yoake_posts
- テーブル: 櫻井優衣
- 期間: 2025-10-13 〜 2025-11-17（約1ヶ月）
- 行数: 106,057行

## 認証
Application Default Credentials (ADC) を使用
"""

import marimo

__generated_with__ = "0.9.34"
app = marimo.App(width="full")


@app.cell
def __():
    import marimo as mo
    import pandas as pd
    import numpy as np
    import sys
    import os
    from pathlib import Path
    from datetime import datetime, timedelta
    import re
    from collections import Counter
    
    # GOOGLE_APPLICATION_CREDENTIALS が無効な値の場合は削除
    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        gac_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        if not os.path.exists(gac_path):
            del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    
    # src を PYTHONPATH に追加
    root_dir = Path(__file__).parent.parent
    if str(root_dir / "src") not in sys.path:
        sys.path.insert(0, str(root_dir / "src"))
    
    from ai_data_lab.connectors.bigquery import BigQueryConnector
    
    return BigQueryConnector, Counter, Path, datetime, mo, np, os, pd, re, root_dir, sys, timedelta


@app.cell
def __(mo):
    mo.md(
        """
        # 🎀 櫻井優衣 投稿データ EDA

        **FRUITS ZIPPER** 櫻井優衣に関する X（Twitter）投稿データの探索的データ分析
        
        📊 データ期間: 2025-10-13 〜 2025-11-17（約1ヶ月）  
        📈 総投稿数: 106,057件
        """
    )
    return


@app.cell
def __():
    # BigQuery 設定
    PROJECT_ID = "yoake-dev-analysis"
    DATASET_ID = "dev_yoake_posts"
    TABLE_ID = "櫻井優衣"
    
    return DATASET_ID, PROJECT_ID, TABLE_ID


@app.cell
def __(mo):
    mo.md("## 🔌 データ読み込み")
    return


@app.cell
def __(BigQueryConnector, DATASET_ID, PROJECT_ID, TABLE_ID, mo):
    # BigQueryコネクタ初期化
    try:
        connector = BigQueryConnector(project_id=PROJECT_ID)
        
        # 全データ取得（展開形式・重複除去済み）
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
        
        mo.md(f"✅ データ読み込み完了: **{len(df):,}** 行 × **{len(df.columns)}** 列")
    except Exception as e:
        mo.stop(True, mo.md(f"❌ データ読み込みエラー: {e}"))
    
    return base_query, connector, df


@app.cell
def __(df, mo):
    mo.md(f"""
    ## 📊 基本統計
    
    - **総投稿数**: {len(df):,} 件
    - **ユニークユーザー数**: {df['user_id'].nunique():,} 人
    - **ユニーク投稿ID数**: {df['post_id'].nunique():,} 件
    - **期間**: {df['created_at'].min():%Y-%m-%d} 〜 {df['created_at'].max():%Y-%m-%d}
    - **日数**: {(df['created_at'].max() - df['created_at'].min()).days} 日
    """)
    return


@app.cell
def __(df, pd):
    # データ加工
    df_clean = df.copy()
    
    # 日付関連の特徴量追加
    df_clean['date'] = pd.to_datetime(df_clean['created_at']).dt.date
    df_clean['hour'] = pd.to_datetime(df_clean['created_at']).dt.hour
    df_clean['day_of_week'] = pd.to_datetime(df_clean['created_at']).dt.dayofweek
    df_clean['weekday_name'] = pd.to_datetime(df_clean['created_at']).dt.day_name()
    
    # メディアフラグ
    df_clean['has_media'] = df_clean['media_count'] > 0
    
    # 総エンゲージメント
    df_clean['total_engagement'] = (
        df_clean['like_count'] + 
        df_clean['repost_count'] + 
        df_clean['reply_count'] + 
        df_clean['quoted_count']
    )
    
    # コンテンツ長
    df_clean['content_length'] = df_clean['content'].fillna('').str.len()
    
    # URL含有フラグ（後続セル・サマリーで共通利用）
    df_clean['has_url'] = df_clean['content'].fillna('').str.contains('http')
    
    return (df_clean,)


@app.cell
def __(df_clean, mo):
    mo.md("""
    ---
    ## 📅 投稿パターン分析
    
    時系列での投稿ボリューム、曜日・時間帯別の傾向を確認
    """)
    return


@app.cell
def __(df_clean, pd):
    # 日別投稿数
    daily_posts = df_clean.groupby('date').size().reset_index(name='post_count')
    daily_posts['date'] = pd.to_datetime(daily_posts['date'])
    
    daily_posts
    return (daily_posts,)


@app.cell
def __(df_clean, pd):
    # 曜日別投稿数
    weekday_posts = df_clean.groupby('weekday_name').size().reset_index(name='post_count')
    # 曜日順にソート
    weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    weekday_posts['weekday_name'] = pd.Categorical(weekday_posts['weekday_name'], categories=weekday_order, ordered=True)
    weekday_posts = weekday_posts.sort_values('weekday_name')
    
    weekday_posts
    return weekday_order, weekday_posts


@app.cell
def __(df_clean):
    # 時間帯別投稿数
    hourly_posts = df_clean.groupby('hour').size().reset_index(name='post_count')
    hourly_posts = hourly_posts.sort_values('hour')
    
    hourly_posts
    return (hourly_posts,)


@app.cell
def __(daily_posts, mo):
    mo.md(f"""
    ### 📈 日別投稿数サマリー
    
    - **平均**: {daily_posts['post_count'].mean():.1f} 件/日
    - **中央値**: {daily_posts['post_count'].median():.0f} 件/日
    - **最大**: {daily_posts['post_count'].max()} 件/日
    - **最小**: {daily_posts['post_count'].min()} 件/日
    """)
    return


@app.cell
def __(mo):
    mo.md("""
    ---
    ## 💬 エンゲージメント分析
    
    いいね、リポスト、返信、引用の分布と相関を確認
    """)
    return


@app.cell
def __(df_clean):
    # エンゲージメント統計
    engagement_stats = df_clean[['like_count', 'repost_count', 'reply_count', 'quoted_count', 'total_engagement']].describe()
    
    engagement_stats
    return (engagement_stats,)


@app.cell
def __(df_clean, mo):
    mo.md(f"""
    ### 📊 エンゲージメント統計サマリー
    
    - **平均いいね数**: {df_clean['like_count'].mean():.1f}
    - **平均リポスト数**: {df_clean['repost_count'].mean():.1f}
    - **平均返信数**: {df_clean['reply_count'].mean():.1f}
    - **平均引用数**: {df_clean['quoted_count'].mean():.1f}
    - **平均総エンゲージメント**: {df_clean['total_engagement'].mean():.1f}
    
    ---
    
    - **中央値いいね数**: {df_clean['like_count'].median():.0f}
    - **中央値総エンゲージメント**: {df_clean['total_engagement'].median():.0f}
    """)
    return


@app.cell
def __(df_clean):
    # 相関行列
    engagement_corr = df_clean[['like_count', 'repost_count', 'reply_count', 'quoted_count']].corr()
    
    engagement_corr
    return (engagement_corr,)


@app.cell
def __(df_clean):
    # 上位エンゲージメント投稿（TOP 20）
    top_engagement = df_clean.nlargest(20, 'total_engagement')[
        ['created_at', 'user_name', 'content', 'like_count', 'repost_count', 'reply_count', 'quoted_count', 'total_engagement', 'has_media']
    ]
    
    top_engagement
    return (top_engagement,)


@app.cell
def __(mo):
    mo.md("""
    ---
    ## 🎬 メディア分析
    
    メディア有無別のエンゲージメントと投稿分布
    """)
    return


@app.cell
def __(df_clean):
    # メディア有無別の統計
    media_comparison = df_clean.groupby('has_media').agg({
        'post_id': 'count',
        'like_count': ['mean', 'median'],
        'repost_count': ['mean', 'median'],
        'reply_count': ['mean', 'median'],
        'quoted_count': ['mean', 'median'],
        'total_engagement': ['mean', 'median']
    }).round(2)
    
    media_comparison.columns = ['_'.join(col).strip() for col in media_comparison.columns.values]
    media_comparison = media_comparison.reset_index()
    media_comparison
    return (media_comparison,)


@app.cell
def __(df_clean, mo):
    posts_with_media = int(df_clean['has_media'].sum())
    posts_without_media = int((~df_clean['has_media']).sum())
    total_posts = len(df_clean)
    
    media_eng_with = df_clean[df_clean['has_media']]['total_engagement'].mean()
    media_eng_without = df_clean[~df_clean['has_media']]['total_engagement'].mean()
    media_ratio = media_eng_with / media_eng_without if media_eng_without not in (0, 0.0) else float('nan')
    
    share_with = posts_with_media / total_posts * 100 if total_posts else float('nan')
    share_without = posts_without_media / total_posts * 100 if total_posts else float('nan')
    
    mo.md(f"""
    ### 📊 メディア統計
    
    - **メディアあり**: {posts_with_media:,} 件 ({share_with:.1f}%)
    - **メディアなし**: {posts_without_media:,} 件 ({share_without:.1f}%)
    
    メディアありの投稿は平均的に **{media_ratio:.2f}倍** のエンゲージメントを獲得
    """)
    # サマリー用に media_ratio も返しておく
    return media_ratio, posts_with_media, posts_without_media


@app.cell
def __(mo):
    mo.md("""
    ---
    ## 👥 ユーザー分析
    
    投稿者（ユーザー）単位での集計と分布
    """)
    return


@app.cell
def __(df_clean):
    # ユーザー単位の集計
    user_stats = df_clean.groupby('user_id').agg({
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
    
    # TOP 50 ユーザー
    top_users = user_stats.head(50)
    top_users
    return top_users, user_stats


@app.cell
def __(df_clean, mo, user_stats):
    badge_users = user_stats[user_stats['user_badge'] == True]
    non_badge_users = user_stats[user_stats['user_badge'] != True]
    total_users = len(user_stats)
    
    badge_posts = badge_users['post_count'].sum()
    non_badge_posts = non_badge_users['post_count'].sum()
    badge_avg_eng = (
        badge_users['total_engagement'].sum() / badge_posts if badge_posts else float('nan')
    )
    non_badge_avg_eng = (
        non_badge_users['total_engagement'].sum() / non_badge_posts if non_badge_posts else float('nan')
    )
    
    mo.md(f"""
    ### 👥 ユーザー統計
    
    - **総ユーザー数**: {len(user_stats):,} 人
    - **バッジ付きユーザー数**: {len(badge_users):,} 人 ({(len(badge_users)/total_users*100 if total_users else float('nan')):.1f}%)
    - **平均投稿数/ユーザー**: {user_stats['post_count'].mean():.1f} 件
    - **中央値投稿数**: {user_stats['post_count'].median():.0f} 件
    - **最大投稿数**: {user_stats['post_count'].max()} 件（ユーザー: {user_stats.iloc[0]['user_name']}）
    
    ---
    
    - **バッジ付きユーザーの平均エンゲージメント**: {badge_avg_eng:.1f}
    - **バッジなしユーザーの平均エンゲージメント**: {non_badge_avg_eng:.1f}
    """)
    return (badge_users,)


@app.cell
def __(mo):
    mo.md("""
    ---
    ## 📝 コンテンツ分析
    
    投稿内容の長さ、キーワード頻度
    """)
    return


@app.cell
def __(df_clean):
    # コンテンツ長の分布
    content_length_stats = df_clean['content_length'].describe()
    
    content_length_stats
    return (content_length_stats,)


@app.cell
def __(df_clean, mo):
    mo.md(f"""
    ### 📏 コンテンツ長統計
    
    - **平均文字数**: {df_clean['content_length'].mean():.1f} 文字
    - **中央値文字数**: {df_clean['content_length'].median():.0f} 文字
    - **最大文字数**: {df_clean['content_length'].max()} 文字
    - **最小文字数**: {df_clean['content_length'].min()} 文字
    """)
    return


@app.cell
def __(Counter, df_clean, pd, re):
    # キーワード抽出（ハッシュタグ）
    all_content = ' '.join(df_clean['content'].fillna(''))
    
    # ハッシュタグ抽出
    hashtags = re.findall(r'#\w+', all_content)
    hashtag_freq = Counter(hashtags).most_common(30)
    
    hashtag_df = pd.DataFrame(hashtag_freq, columns=['hashtag', 'count'])
    hashtag_df
    return all_content, hashtag_df, hashtag_freq, hashtags


@app.cell
def __(Counter, all_content, pd, re):
    # メンション抽出
    mentions = re.findall(r'@\w+', all_content)
    mention_freq = Counter(mentions).most_common(30)
    
    mention_df = pd.DataFrame(mention_freq, columns=['mention', 'count'])
    mention_df
    return mention_df, mention_freq, mentions


@app.cell
def __(df_clean):
    # URL含有率（has_url は前処理セルで付与済み）
    url_stats = df_clean['has_url'].value_counts()
    
    url_stats
    return (url_stats,)


@app.cell
def __(mo):
    mo.md("""
    ---
    ## 📊 総合サマリー
    
    分析結果の要点
    """)
    return


@app.cell
def __(badge_users, daily_posts, df_clean, hashtag_df, media_ratio, mo, posts_with_media, user_stats):
    total_clean = len(df_clean)
    mo.md(f"""
    # 🎯 櫻井優衣 投稿データ分析結果サマリー
    
    ## 📈 投稿パターン
    - **総投稿数**: {total_clean:,} 件
    - **期間**: {df_clean['created_at'].min():%Y-%m-%d} 〜 {df_clean['created_at'].max():%Y-%m-%d} ({(df_clean['created_at'].max() - df_clean['created_at'].min()).days} 日間)
    - **1日平均投稿数**: {daily_posts['post_count'].mean():.1f} 件
    - **最も投稿が多い曜日**: （データから算出）
    - **最も投稿が多い時間帯**: （データから算出）
    
    ## 💬 エンゲージメント特性
    - **平均いいね数**: {df_clean['like_count'].mean():.1f} / **中央値**: {df_clean['like_count'].median():.0f}
    - **平均リポスト数**: {df_clean['repost_count'].mean():.1f} / **中央値**: {df_clean['repost_count'].median():.0f}
    - **平均返信数**: {df_clean['reply_count'].mean():.1f} / **中央値**: {df_clean['reply_count'].median():.0f}
    - **平均引用数**: {df_clean['quoted_count'].mean():.1f} / **中央値**: {df_clean['quoted_count'].median():.0f}
    - **総エンゲージメント最大**: {df_clean['total_engagement'].max()} 件
    
    ## 🎬 メディア効果
    - **メディアあり投稿**: {posts_with_media:,} 件 ({(posts_with_media/total_clean*100 if total_clean else float('nan')):.1f}%)
    - **メディアなし投稿**: {total_clean-posts_with_media:,} 件 ({((total_clean-posts_with_media)/total_clean*100 if total_clean else float('nan')):.1f}%)
    - **メディアありのエンゲージメント倍率**: {media_ratio:.2f}倍
    
    ## 👥 ユーザー特性
    - **総ユーザー数**: {len(user_stats):,} 人
    - **バッジ付きユーザー**: {len(badge_users):,} 人 ({(len(badge_users)/len(user_stats)*100 if len(user_stats) else float('nan')):.1f}%)
    - **1ユーザー平均投稿数**: {user_stats['post_count'].mean():.1f} 件
    - **最多投稿ユーザー**: {user_stats.iloc[0]['user_name']} ({user_stats.iloc[0]['post_count']} 件)
    
    ## 📝 コンテンツ特性
    - **平均文字数**: {df_clean['content_length'].mean():.1f} 文字
    - **TOP 3 ハッシュタグ**: {', '.join([h[0] for h in hashtag_df.head(3).values.tolist()])}
    - **URL含有率**: {df_clean['has_url'].sum() / len(df_clean) * 100:.1f}%
    """)
    return


if __name__ == "__main__":
    app.run()


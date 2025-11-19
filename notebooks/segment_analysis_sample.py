"""
ユーザーセグメント分析サンプル実装
コア層・インフルエンサー層の判定ロジック
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta

class UserSegmentAnalyzer:
    """ユーザーセグメント分析クラス"""
    
    def __init__(self, df: pd.DataFrame):
        """
        Parameters:
        -----------
        df: pd.DataFrame
            必須カラム: user_id, post_id, created_at, like_count, repost_count,
                      quote_count, reply_count, user_followers_count, verified_badge
        """
        self.df = df
        self.user_stats = None
        self.segments = None
        
    def calculate_user_stats(self) -> pd.DataFrame:
        """ユーザー別統計量を計算"""
        
        # 日付型に変換
        self.df['created_at'] = pd.to_datetime(self.df['created_at'])
        self.df['date'] = self.df['created_at'].dt.date
        
        # 分析期間
        date_range = (self.df['date'].max() - self.df['date'].min()).days + 1
        
        # ユーザー別集計
        user_stats = self.df.groupby('user_id').agg({
            'post_id': 'count',  # 投稿数
            'like_count': ['mean', 'sum', 'max'],  # いいね統計
            'repost_count': ['mean', 'sum'],  # RT統計
            'quote_count': ['mean', 'sum'],  # 引用統計
            'reply_count': ['mean', 'sum'],  # リプライ統計
            'date': lambda x: x.nunique(),  # 投稿日数
            'user_followers_count': 'max',  # フォロワー数（最大値）
            'verified_badge': 'max'  # 認証バッジ
        }).reset_index()
        
        # カラム名を整理
        user_stats.columns = [
            'user_id', 'post_count', 'avg_likes', 'total_likes', 'max_likes',
            'avg_reposts', 'total_reposts', 'avg_quotes', 'total_quotes',
            'avg_replies', 'total_replies', 'active_days', 'followers_count', 
            'is_verified'
        ]
        
        # 派生指標の計算
        user_stats['posting_frequency'] = user_stats['post_count'] / date_range
        user_stats['continuity_rate'] = user_stats['active_days'] / date_range
        user_stats['avg_engagement'] = (
            user_stats['avg_likes'] + 
            user_stats['avg_reposts'] * 2 + 
            user_stats['avg_quotes'] * 3
        )
        user_stats['engagement_rate'] = np.where(
            user_stats['followers_count'] > 0,
            user_stats['avg_engagement'] / user_stats['followers_count'],
            0
        )
        user_stats['total_rt'] = user_stats['total_reposts'] + user_stats['total_quotes']
        
        # エンゲージメント多様性スコア
        engagement_types = ['total_likes', 'total_reposts', 'total_quotes', 'total_replies']
        for col in engagement_types:
            user_stats[f'{col}_has'] = (user_stats[col] > 0).astype(int)
        
        user_stats['engagement_diversity'] = (
            user_stats[[f'{col}_has' for col in engagement_types]].sum(axis=1) / 4
        )
        
        self.user_stats = user_stats
        return user_stats
    
    def identify_core_users(self) -> pd.DataFrame:
        """コア層（ロイヤルカスタマー）を判定"""
        
        if self.user_stats is None:
            self.calculate_user_stats()
        
        stats = self.user_stats.copy()
        
        # 判定基準1: 高頻度投稿層（上位10%）
        stats['is_high_frequency'] = (
            stats['post_count'] >= stats['post_count'].quantile(0.9)
        )
        
        # 判定基準2: 高エンゲージメント獲得層（上位10%）
        stats['is_high_engagement'] = (
            (stats['avg_likes'] >= stats['avg_likes'].quantile(0.9)) |
            (stats['avg_reposts'] >= stats['avg_reposts'].quantile(0.9))
        )
        
        # 判定基準3: 継続投稿層（80%以上の日数）
        stats['is_continuous'] = stats['continuity_rate'] >= 0.8
        
        # 判定基準4: マルチエンゲージメント層（多様性スコア上位20%）
        stats['is_multi_engagement'] = (
            stats['engagement_diversity'] >= stats['engagement_diversity'].quantile(0.8)
        )
        
        # コア層判定（2つ以上の条件を満たす）
        core_criteria = [
            'is_high_frequency', 'is_high_engagement', 
            'is_continuous', 'is_multi_engagement'
        ]
        stats['core_score'] = stats[core_criteria].sum(axis=1)
        stats['is_core'] = stats['core_score'] >= 2
        
        # コア層の詳細分類
        stats['core_type'] = 'non-core'
        stats.loc[stats['is_core'], 'core_type'] = stats.loc[stats['is_core'], core_criteria].apply(
            lambda x: '+'.join([col.replace('is_', '') for col in core_criteria if x[col]]),
            axis=1
        )
        
        return stats
    
    def identify_influencers(self) -> pd.DataFrame:
        """インフルエンサー層を判定"""
        
        if self.user_stats is None:
            self.calculate_user_stats()
        
        stats = self.user_stats.copy()
        
        # 判定基準1: フォロワー規模
        stats['has_followers'] = (
            (stats['followers_count'] >= 1000) |
            (stats['followers_count'] / stats['post_count'] >= 100)  # フォロワー/投稿比
        )
        
        # 判定基準2: 高エンゲージメント率（5%以上）
        stats['has_high_engagement_rate'] = stats['engagement_rate'] >= 0.05
        
        # 判定基準3: 公式認証
        stats['has_verification'] = stats['is_verified'] == 1
        
        # 判定基準4: 高拡散力（RT数上位5%）
        stats['has_viral_power'] = (
            stats['total_rt'] >= stats['total_rt'].quantile(0.95)
        )
        
        # インフルエンサー判定（2つ以上の条件を満たす）
        influencer_criteria = [
            'has_followers', 'has_high_engagement_rate',
            'has_verification', 'has_viral_power'
        ]
        stats['influencer_score'] = stats[influencer_criteria].sum(axis=1)
        stats['is_influencer'] = stats['influencer_score'] >= 2
        
        # インフルエンサーの詳細分類
        stats['influencer_type'] = 'non-influencer'
        stats.loc[stats['is_influencer'], 'influencer_type'] = stats.loc[
            stats['is_influencer'], influencer_criteria
        ].apply(
            lambda x: '+'.join([col.replace('has_', '') for col in influencer_criteria if x[col]]),
            axis=1
        )
        
        return stats
    
    def segment_users(self) -> pd.DataFrame:
        """全ユーザーをセグメント分類"""
        
        # コア層判定
        core_stats = self.identify_core_users()
        
        # インフルエンサー判定
        influencer_stats = self.identify_influencers()
        
        # 統合
        segments = core_stats.copy()
        segments['is_influencer'] = influencer_stats['is_influencer']
        segments['influencer_score'] = influencer_stats['influencer_score']
        segments['influencer_type'] = influencer_stats['influencer_type']
        
        # 最終セグメント分類
        segments['user_segment'] = 'casual'  # デフォルト
        
        # 週1回以上投稿
        segments.loc[segments['posting_frequency'] >= 1/7, 'user_segment'] = 'active'
        
        # コア層
        segments.loc[segments['is_core'], 'user_segment'] = 'core'
        
        # インフルエンサー
        segments.loc[segments['is_influencer'], 'user_segment'] = 'influencer'
        
        # コア＆インフルエンサー
        segments.loc[
            (segments['is_core']) & (segments['is_influencer']), 
            'user_segment'
        ] = 'core_influencer'
        
        self.segments = segments
        return segments
    
    def get_segment_summary(self) -> pd.DataFrame:
        """セグメント別サマリー統計"""
        
        if self.segments is None:
            self.segment_users()
        
        summary = self.segments.groupby('user_segment').agg({
            'user_id': 'count',
            'post_count': ['mean', 'median', 'sum'],
            'avg_likes': 'mean',
            'avg_reposts': 'mean',
            'followers_count': 'median',
            'continuity_rate': 'mean'
        }).round(2)
        
        summary.columns = [
            'user_count', 'avg_posts', 'median_posts', 'total_posts',
            'avg_likes', 'avg_reposts', 'median_followers', 'avg_continuity'
        ]
        
        # 構成比を追加
        summary['user_ratio'] = (
            summary['user_count'] / summary['user_count'].sum() * 100
        ).round(1)
        
        summary['post_ratio'] = (
            summary['total_posts'] / summary['total_posts'].sum() * 100
        ).round(1)
        
        return summary
    
    def visualize_segments(self):
        """セグメント分析の可視化"""
        
        if self.segments is None:
            self.segment_users()
        
        fig, axes = plt.subplots(2, 3, figsize=(15, 10))
        
        # 1. セグメント構成比（ユーザー数）
        segment_counts = self.segments['user_segment'].value_counts()
        axes[0, 0].pie(segment_counts.values, labels=segment_counts.index, 
                      autopct='%1.1f%%')
        axes[0, 0].set_title('ユーザーセグメント構成比')
        
        # 2. セグメント別投稿数分布
        self.segments.boxplot(column='post_count', by='user_segment', ax=axes[0, 1])
        axes[0, 1].set_title('セグメント別投稿数分布')
        axes[0, 1].set_ylabel('投稿数')
        axes[0, 1].set_xlabel('セグメント')
        
        # 3. セグメント別エンゲージメント
        segment_engagement = self.segments.groupby('user_segment')['avg_engagement'].mean()
        axes[0, 2].bar(segment_engagement.index, segment_engagement.values)
        axes[0, 2].set_title('セグメント別平均エンゲージメント')
        axes[0, 2].set_ylabel('平均エンゲージメント')
        axes[0, 2].set_xlabel('セグメント')
        axes[0, 2].tick_params(axis='x', rotation=45)
        
        # 4. コア層スコア分布
        axes[1, 0].hist(self.segments['core_score'], bins=5, edgecolor='black')
        axes[1, 0].set_title('コア層スコア分布')
        axes[1, 0].set_xlabel('スコア（0-4）')
        axes[1, 0].set_ylabel('ユーザー数')
        
        # 5. インフルエンサースコア分布
        axes[1, 1].hist(self.segments['influencer_score'], bins=5, edgecolor='black')
        axes[1, 1].set_title('インフルエンサースコア分布')
        axes[1, 1].set_xlabel('スコア（0-4）')
        axes[1, 1].set_ylabel('ユーザー数')
        
        # 6. セグメント×フォロワー数散布図
        for segment in self.segments['user_segment'].unique():
            segment_data = self.segments[self.segments['user_segment'] == segment]
            axes[1, 2].scatter(segment_data['followers_count'], 
                             segment_data['avg_engagement'],
                             label=segment, alpha=0.6)
        axes[1, 2].set_title('フォロワー数×エンゲージメント')
        axes[1, 2].set_xlabel('フォロワー数')
        axes[1, 2].set_ylabel('平均エンゲージメント')
        axes[1, 2].set_xscale('log')
        axes[1, 2].legend()
        
        plt.tight_layout()
        plt.show()
        
    def export_segment_definitions(self, output_path: str = 'user_segments.csv'):
        """セグメント定義をCSVエクスポート"""
        
        if self.segments is None:
            self.segment_users()
        
        # エクスポート用に主要カラムを選択
        export_columns = [
            'user_id', 'user_segment', 'post_count', 'avg_likes', 
            'followers_count', 'is_core', 'core_score', 'core_type',
            'is_influencer', 'influencer_score', 'influencer_type'
        ]
        
        self.segments[export_columns].to_csv(output_path, index=False)
        print(f"セグメント定義を {output_path} に保存しました")


def analyze_group_comparison(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    複数グループ/個人のセグメント比較分析
    
    Parameters:
    -----------
    dfs: Dict[str, pd.DataFrame]
        グループ名/個人名をキー、データフレームを値とする辞書
    
    Returns:
    --------
    pd.DataFrame: グループ別セグメント比較結果
    """
    
    comparison_results = []
    
    for name, df in dfs.items():
        # セグメント分析実行
        analyzer = UserSegmentAnalyzer(df)
        segments = analyzer.segment_users()
        summary = analyzer.get_segment_summary()
        
        # グループ別サマリー作成
        result = {
            'name': name,
            'total_users': len(segments),
            'total_posts': segments['post_count'].sum(),
            'core_ratio': (segments['is_core'].sum() / len(segments) * 100),
            'influencer_ratio': (segments['is_influencer'].sum() / len(segments) * 100),
            'avg_posts_per_user': segments['post_count'].mean(),
            'median_followers': segments['followers_count'].median(),
            'avg_engagement': segments['avg_engagement'].mean()
        }
        
        # セグメント別ユーザー比率を追加
        for segment in ['casual', 'active', 'core', 'influencer', 'core_influencer']:
            ratio = (segments['user_segment'] == segment).sum() / len(segments) * 100
            result[f'{segment}_ratio'] = ratio
        
        comparison_results.append(result)
    
    return pd.DataFrame(comparison_results)


# 使用例
if __name__ == "__main__":
    
    # サンプルデータ生成（実際はBigQueryから取得）
    np.random.seed(42)
    n_users = 1000
    n_posts = 10000
    
    # ユーザーごとの投稿数を生成（パレート分布に近い）
    user_post_counts = np.random.pareto(1.5, n_users) * 10 + 1
    user_post_counts = user_post_counts.astype(int)
    
    # サンプルデータフレーム作成
    sample_data = []
    for user_idx, post_count in enumerate(user_post_counts[:100]):  # 100ユーザー分
        user_id = f"user_{user_idx:04d}"
        
        # ユーザータイプに応じてパラメータ設定
        if user_idx < 10:  # インフルエンサー
            followers = np.random.randint(1000, 10000)
            avg_likes = np.random.randint(50, 200)
        elif user_idx < 30:  # コア層
            followers = np.random.randint(100, 1000)
            avg_likes = np.random.randint(10, 50)
        else:  # 一般層
            followers = np.random.randint(10, 500)
            avg_likes = np.random.randint(1, 20)
        
        for post_idx in range(min(post_count, 50)):  # 最大50投稿
            sample_data.append({
                'user_id': user_id,
                'post_id': f"post_{user_idx:04d}_{post_idx:03d}",
                'created_at': datetime.now() - timedelta(days=np.random.randint(0, 30)),
                'like_count': np.random.poisson(avg_likes),
                'repost_count': np.random.poisson(avg_likes / 5),
                'quote_count': np.random.poisson(avg_likes / 10),
                'reply_count': np.random.poisson(avg_likes / 7),
                'user_followers_count': followers,
                'verified_badge': 1 if user_idx < 5 else 0
            })
    
    df = pd.DataFrame(sample_data)
    
    # 分析実行
    print("=" * 60)
    print("ユーザーセグメント分析デモ")
    print("=" * 60)
    
    analyzer = UserSegmentAnalyzer(df)
    segments = analyzer.segment_users()
    summary = analyzer.get_segment_summary()
    
    print("\n📊 セグメント別サマリー:")
    print(summary)
    
    print("\n🏆 コア層ユーザー（TOP5）:")
    core_users = segments[segments['is_core']].nlargest(5, 'core_score')
    print(core_users[['user_id', 'post_count', 'avg_likes', 'core_score', 'core_type']])
    
    print("\n⭐ インフルエンサー層（TOP5）:")
    influencers = segments[segments['is_influencer']].nlargest(5, 'influencer_score')
    print(influencers[['user_id', 'followers_count', 'engagement_rate', 
                       'influencer_score', 'influencer_type']])
    
    # 可視化
    # analyzer.visualize_segments()
    
    print("\n✅ 分析完了")


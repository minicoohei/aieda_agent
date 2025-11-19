# ⚡ クイックスタートガイド

## 包括的分析を5分で開始する

### 1. 前提条件

- ✅ `uv` インストール済み
- ✅ GCP認証設定済み（`gcloud auth application-default login`）
- ✅ BigQueryへのアクセス権限あり

### 2. ワンコマンド起動

```bash
./scripts/start_comprehensive_analysis.sh
```

または、手動で：

```bash
cd /Users/kou1904/.cursor/worktrees/aieda_agent/Cauws
uv run marimo edit notebooks/05_comprehensive_analysis_coordinator.py --port 40000
```

### 3. ブラウザを開く

```
http://localhost:40000
```

### 4. 分析を実行

#### Step 1: Phase 1起動

**「🚀 Phase 1 起動: データ収集」** ボタンをクリック

- BigQueryからデータを取得
- サンプルデータを保存
- 約3-5分で完了

#### Step 2: Phase 2 & 3並列起動

**「⚡ Phase 2 & 3 並列起動」** ボタンをクリック

- **Phase 2**: 基礎統計分析（グラフ2枚生成）
- **Phase 3**: テキストマイニング（グラフ3枚生成）
- 並列実行で時間を短縮

#### Step 3: ステータス確認

「📊 実行ステータス」セクションで進捗を確認：

- **実行中**: 現在処理中のPhase数
- **完了**: 完了したPhase数
- **セッション詳細**: 各エージェントのURL、ポート、ステータス

#### Step 4: レポート確認

全Phase完了後、以下のレポートが生成されます：

```
reports/comprehensive_analysis/
├── final_comprehensive_report.md     # Markdownレポート
├── final_comprehensive_report.html   # HTMLレポート（画像埋め込み）
└── visualizations/                   # 高解像度画像（300 DPI）
    ├── phase2/
    ├── phase3/
    └── phase4/
```

### 5. HTMLレポートを開く

```bash
open reports/comprehensive_analysis/final_comprehensive_report.html
```

または、ブラウザで直接開く：

```
file:///Users/kou1904/.cursor/worktrees/aieda_agent/Cauws/reports/comprehensive_analysis/final_comprehensive_report.html
```

## 🎨 カスタマイズ

### ポート範囲を変更

```bash
export MARIMO_PORT_RANGE="42000-42999"
./scripts/start_comprehensive_analysis.sh
```

### サンプルサイズを変更

`notebooks/phase1_data_collection.py` を編集：

```python
sample_size = 5000  # デフォルト: 1000
```

### 分析対象テーブルを変更

`notebooks/phase1_data_collection.py` のグループキーワードを編集：

```python
group_keywords = [
    "FRUITS",
    "ZIPPER",
    "YOUR_GROUP",  # 追加
]
```

## 🔧 トラブルシューティング

### ポートが使用中

```bash
# 使用中のポートを確認
lsof -i :40000

# プロセスを終了
kill -9 <PID>
```

### BigQuery認証エラー

```bash
gcloud auth application-default login
```

### メモリ不足

サンプルサイズを減らす：

```python
sample_size = 500  # 少なくする
```

## 📊 生成される画像例

| Phase | 画像 | 説明 |
|-------|------|------|
| Phase 2 | `01_group_post_counts.png` | グループ別投稿数（横棒グラフ） |
| Phase 2 | `02_individual_post_counts_top10.png` | 個人別投稿数TOP10（縦棒グラフ） |
| Phase 3 | `03_text_length_distribution.png` | 文字数分布（ヒストグラム） |
| Phase 3 | `04_top_hashtags.png` | 頻出ハッシュタグTOP20（横棒グラフ） |
| Phase 3 | `05_group_time_distribution.png` | 時間帯分布（積み上げ棒グラフ） |
| Phase 4 | `06_group_radar_comparison.png` | グループ比較（レーダーチャート） |
| Phase 4 | `07_individual_performance_heatmap.png` | パフォーマンス（ヒートマップ） |
| Phase 4 | `08_group_vs_individual_pie.png` | 投稿量比較（円グラフ） |

すべて **300 DPI** の高解像度PNG形式で保存されます。

## 🚀 次のステップ

1. 生成されたレポートをプレゼン資料に活用
2. 各PhaseのNotebookを編集してカスタマイズ
3. 新しい分析指標を追加
4. 定期実行スケジュールの設定

詳細は [包括的分析実行ガイド](COMPREHENSIVE_ANALYSIS_GUIDE.md) を参照してください。

---

*作成日: 2025-11-18*


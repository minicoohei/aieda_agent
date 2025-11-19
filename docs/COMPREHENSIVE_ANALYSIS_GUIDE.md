# 📘 包括的分析実行ガイド

## 概要

このガイドでは、アイドル・グループ・ファンダムの包括的比較分析を実行する方法を説明します。

## 🎯 分析の目的

- 複数のアイドルグループと個人メンバーのソーシャルメディアデータを多角的に分析
- 投稿パターン、ファンダム特性、エンゲージメント指標を可視化
- **高解像度（300 DPI）の画像を多用したエビデンスレポート作成**

## 🏗️ アーキテクチャ

### 並列実行フレームワーク

```
マスターコーディネーター (port: 自動割り当て)
├── Phase 1: データ収集 (port: 41000)
├── Phase 2: 基礎統計分析 (port: 41001) ─┐
├── Phase 3: テキストマイニング (port: 41002) ─┤ 並列実行
│                                              │
├── Phase 4: 比較分析 (port: 41003) ←─────────┘
└── Phase 5: 最終レポート作成 (port: 41004)
```

### ポート自動割り当て

各エージェントは `PortAllocator` を使用して、利用可能なポートを自動的に割り当てます。

- **デフォルト範囲**: 41000-41999
- **環境変数で変更可能**: `MARIMO_PORT_RANGE=42000-42999`

## 📋 実行手順

### 1. マスターコーディネーターの起動

```bash
cd /Users/kou1904/.cursor/worktrees/aieda_agent/Cauws
uv run marimo edit notebooks/05_comprehensive_analysis_coordinator.py --port 40000
```

ブラウザで `http://localhost:40000` を開きます。

### 2. Phase 1の実行

マスターコーディネーターのUIから **「🚀 Phase 1 起動: データ収集」** ボタンをクリック。

Phase 1が完了するまで待ちます（約3-5分）。

### 3. Phase 2 & 3の並列実行

**「⚡ Phase 2 & 3 並列起動」** ボタンをクリック。

2つのエージェントが同時に動作します：
- Phase 2: 基礎統計分析（ポート自動割り当て）
- Phase 3: テキストマイニング（ポート自動割り当て）

### 4. Phase 4の実行

Phase 2と3が完了後、Phase 4を起動します。

### 5. Phase 5の実行

最後に、Phase 5で全ての結果を統合します。

## 📊 生成される成果物

### レポート

```
reports/comprehensive_analysis/
├── phase1_completion_report.md
├── phase2_completion_report.md
├── phase3_completion_report.md
├── phase4_completion_report.md
├── final_comprehensive_report.md
├── final_comprehensive_report.html
└── COMPLETION_SUMMARY.md
```

### 画像（高解像度 300 DPI）

```
reports/comprehensive_analysis/visualizations/
├── phase2/
│   ├── 01_group_post_counts.png
│   └── 02_individual_post_counts_top10.png
├── phase3/
│   ├── 03_text_length_distribution.png
│   ├── 04_top_hashtags.png
│   └── 05_group_time_distribution.png
└── phase4/
    ├── 06_group_radar_comparison.png
    ├── 07_individual_performance_heatmap.png
    └── 08_group_vs_individual_pie.png
```

### データセット

```
reports/comprehensive_analysis/data/
├── group_data_sample.parquet
└── individual_data_sample.parquet
```

## 🔧 トラブルシューティング

### ポートが既に使用されている

**エラー**: `RuntimeError: Requested fixed port XXXXX is already in use.`

**解決策**:
```bash
# 使用中のポートを確認
lsof -i :41000

# プロセスを終了
kill -9 <PID>
```

または、環境変数でポート範囲を変更：
```bash
export MARIMO_PORT_RANGE=42000-42999
```

### BigQueryへの接続エラー

**エラー**: `google.auth.exceptions.DefaultCredentialsError`

**解決策**:
```bash
# GCPの認証を設定
gcloud auth application-default login
```

### メモリ不足エラー

大量のデータを扱う場合、メモリ不足になることがあります。

**解決策**:
1. サンプルサイズを減らす（`phase1_data_collection.py` の `sample_size` 変数）
2. バッチ処理に変更
3. DuckDBを使用してディスク上で処理

## 🎨 カスタマイズ

### 分析対象テーブルの変更

`notebooks/phase1_data_collection.py` の以下の部分を編集：

```python
# グループ名キーワード（簡易版）
group_keywords = [
    "FRUITS",
    "ZIPPER",
    # ... 追加のキーワード
]
```

### 可視化スタイルの変更

各Phaseのnotebook内で、matplotlibの設定を変更：

```python
plt.style.use('seaborn-v0_8-darkgrid')  # スタイル変更
plt.rcParams['figure.dpi'] = 300  # 解像度変更
```

### メトリクスの追加

Phase 4の `notebooks/phase4_comparison_analysis.py` で、レーダーチャートのメトリクスを追加：

```python
metrics = ["投稿量", "ユニーク性", "エンゲージメント", "新メトリクス"]
```

## 📚 参考資料

- [Marimo公式ドキュメント](https://docs.marimo.io/)
- [包括的分析計画書](../notebooks/comprehensive_analysis_plan.md)
- [可視化ルール](./.cursor/rules/visualization.mdc)
- [データ分析ルール](./.cursor/rules/data_analysis.mdc)

## 🆘 サポート

問題が発生した場合は、以下を確認してください：

1. `uv sync` で依存関係を最新化
2. Python環境が正しく設定されているか
3. BigQueryへのアクセス権限があるか
4. ポートが競合していないか

---

*最終更新: 2025-11-18*


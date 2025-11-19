# 📚 Notebooks 一覧

このディレクトリには、包括的データ分析のためのMarimoノートブックが格納されています。

---

## 🎯 包括的分析フレームワーク

### マスターコーディネーター

```bash
uv run marimo edit notebooks/05_comprehensive_analysis_coordinator.py --port 40000
```

**または、クイックスタート:**

```bash
./scripts/start_comprehensive_analysis.sh
```

このノートブックから、5つの分析Phaseを並列実行できます。

---

## 📊 分析Phase一覧

### Phase 1: データ収集と前処理
📄 `phase1_data_collection.py`

- BigQueryからの全テーブル取得
- グループ/個人の自動分類
- サンプルデータの保存
- データ品質チェック

**依存関係**: なし（最初に実行）

---

### Phase 2: 基礎統計分析
📄 `phase2_basic_statistics.py`

- グループ別投稿数可視化
- 個人別投稿数TOP10
- 時系列分析
- 📸 **画像2枚生成**（300 DPI）

**依存関係**: Phase 1完了後

---

### Phase 3: テキストマイニング
📄 `phase3_text_mining.py`

- 文字数分布分析
- ハッシュタグ頻出分析
- 時間帯別投稿パターン
- 📸 **画像3枚生成**（300 DPI）

**依存関係**: Phase 1完了後

**💡 Phase 2と並列実行可能！**

---

### Phase 4: 比較分析
📄 `phase4_comparison_analysis.py`

- グループ間比較（レーダーチャート）
- 個人別パフォーマンス（ヒートマップ）
- グループ vs 個人比較（円グラフ）
- 📸 **画像3枚生成**（300 DPI）

**依存関係**: Phase 2, 3完了後

---

### Phase 5: 最終レポート統合
📄 `phase5_visualization_report.py`

- 全Phaseのレポート統合
- Markdown最終レポート
- HTML版レポート（画像埋め込み）
- 完了サマリー

**依存関係**: Phase 4完了後

---

## 🔧 その他のノートブック

### BigQueryデータセット俯瞰
📄 `bigquery_overview.py`

```bash
uv run marimo run notebooks/bigquery_overview.py --port 4173
```

- データセット一覧表示
- テーブル情報表示
- スキーマ定義表示

---

### ポータルランチャー
📄 `portal_marimo_launcher.py`

```bash
uv run marimo run notebooks/portal_marimo_launcher.py --port 4174
```

- 各種ノートブックへのリンク集

---

## 📚 詳細ドキュメント

- **クイックスタート**: [docs/QUICK_START.md](../docs/QUICK_START.md)
- **実行ガイド**: [docs/COMPREHENSIVE_ANALYSIS_GUIDE.md](../docs/COMPREHENSIVE_ANALYSIS_GUIDE.md)
- **実装サマリー**: [docs/IMPLEMENTATION_SUMMARY.md](../docs/IMPLEMENTATION_SUMMARY.md)
- **分析計画書**: [comprehensive_analysis_plan.md](comprehensive_analysis_plan.md)

---

## 🚀 推奨実行順序

```
1. マスターコーディネーター起動
   ↓
2. Phase 1: データ収集
   ↓
3. Phase 2 & 3: 並列実行 ⚡
   ↓
4. Phase 4: 比較分析
   ↓
5. Phase 5: レポート統合
   ↓
6. HTMLレポート確認
```

---

## 📊 生成される成果物

### レポート
- `reports/comprehensive_analysis/final_comprehensive_report.md`
- `reports/comprehensive_analysis/final_comprehensive_report.html`

### 画像（8枚、300 DPI）
- `reports/comprehensive_analysis/visualizations/phase2/*.png` (2枚)
- `reports/comprehensive_analysis/visualizations/phase3/*.png` (3枚)
- `reports/comprehensive_analysis/visualizations/phase4/*.png` (3枚)

### データセット
- `reports/comprehensive_analysis/data/group_data_sample.parquet`
- `reports/comprehensive_analysis/data/individual_data_sample.parquet`

---

**最終更新**: 2025-11-18


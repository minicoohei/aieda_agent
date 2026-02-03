# 🎓 AI Data Lab 完全学習カリキュラム

**version: 1.0**
**対象者**: データ分析初心者～上級者
**最終更新**: 2026年2月

---

## 📖 このカリキュラムについて

このドキュメントは、**AI Data Lab（アイデータラボ）**を習得するための包括的な学習パスを提供します。

### プロジェクト概要

AI Data Lab は、AI と協調しながら BigQuery / S3 / Google Sheets / DuckDB を横断し、**marimo** で再現性の高いデータ分析を行うための統合プラットフォームです。

| 項目 | 説明 |
|------|------|
| **主要技術** | marimo（Reactive Python Notebook）、uv（パッケージ管理）、Docker |
| **データ接続** | BigQuery、S3、Google Sheets、DuckDB、Filesystem |
| **EDA ツール** | YData Profiling、AutoViz、Squarify、Colab Charts 風ユーティリティ |
| **実行環境** | Python 3.12+、Docker / Docker Compose |

---

## 🎯 学習到達度マトリックス

### 修了後に実現可能なスキル

#### ✅ 基礎コース修了後（4時間）
- [ ] marimo ノートブックの基本操作と Python リアクティブプログラミング
- [ ] BigQuery データセット俯瞰ノートブックの起動と操作
- [ ] 環境構築（Python 3.12、uv、Docker）
- [ ] Git による版管理と基本的なワークフロー
- [ ] 簡単なデータ読み込みと表示

#### ✅ 標準コース修了後（8時間）
上記に加えて：
- [ ] BigQuery からのデータ取得と基本的な前処理
- [ ] DuckDB・S3・Google Sheets コネクタの使用
- [ ] marimo による インタラクティブ UI の構築
- [ ] Polars / Pandas による データ加工
- [ ] 単体分析ノートブックの構築と実行
- [ ] EDA ツール（YData Profiling / AutoViz）の活用

#### ✅ 上級コース修了後（12時間+）
上記に加えて：
- [ ] 並列分析フレームワークの理解と運用
- [ ] 複数 Marimo ノートブックの同時実行管理
- [ ] 高解像度画像生成（300 DPI）と レポート自動化
- [ ] マスターコーディネーターによる分析パイプライン構築
- [ ] Serena MCP との連携による AI アシスト開発
- [ ] テストの設計・実装（Unit / Integration）

---

## 🗺️ 学習パス選択

### 推奨ルート

#### **🟢 ビジネス分析者向け（4時間）**

**目標**: 既存ノートブックを理解し、ビジネス質問に答えるデータ取得

```
モジュール 0: 環境構築と基本操作    (30分)
      ↓
モジュール 1: BigQuery 俯瞰ノート    (45分)
      ↓
モジュール 2: データ読み込みの基礎   (1.5時間)
      ↓
モジュール 3: 簡単な集計分析        (45分)
```

#### **🟡 データエンジニア向け（8時間）**

**目標**: 独自のデータ分析ノートブックを構築・運用

```
モジュール 0: 環境構築と基本操作    (30分)
      ↓
モジュール 1: BigQuery 俯瞰ノート    (45分)
      ↓
モジュール 2-4: データ接続と前処理   (3時間)
      ↓
モジュール 5: EDA とビジュアライゼーション (1.5時間)
      ↓
モジュール 6-7: 単体ノートブック構築 (2時間)
```

#### **🔴 AI エンジニア向け（12時間+）**

**目標**: 並列分析フレームワークを構築・カスタマイズ

```
モジュール 0-7: 全モジュール基礎    (8時間)
      ↓
モジュール 8: 並列分析フレームワーク (2時間)
      ↓
モジュール 9: マスターコーディネーター (1.5時間)
      ↓
モジュール 10: Serena MCP 連携    (45分)
      ↓
モジュール 11: 本番運用・CI/CD      (1時間)
```

---

## 📚 モジュール構成（全12モジュール）

### **モジュール 0: 環境構築と基本操作**

**⏱️ 学習時間**: 30 分
**難易度**: ⭐ 初級
**前提知識**: Terminal 操作の基本

#### 学習内容
1. **プロジェクト概要の理解**
   - AI Data Lab とは
   - 主要技術スタック
   - ディレクトリ構造

2. **環境構築**
   - Python 3.12 インストール確認
   - `uv` パッケージマネージャーのインストール
   - 依存関係のインストール: `uv sync --all-extras`
   - Playwright ブラウザのセットアップ

3. **初回起動テスト**
   ```bash
   uv run marimo run notebooks/bigquery_overview.py --port 4173
   ```
   ブラウザで `http://localhost:4173` が表示されることを確認

4. **リポジトリ構造の理解**
   - `notebooks/`: Marimo ノートブック一覧
   - `src/ai_data_lab/`: コネクター、EDA ツール、ポート管理
   - `tests/`: Unit / Integration テスト
   - `data/` / `reports/`: 出力ディレクトリ

#### ハンズオン
- [ ] ローカルリポジトリのクローン
- [ ] 依存関係のインストール
- [ ] BigQuery 俯瞰ノートブックの起動
- [ ] Terminal での基本的な Marimo 操作

#### チェックリスト
- [ ] `uv --version` で 0.7+ が表示される
- [ ] `uv run marimo --version` で起動可能
- [ ] ブラウザでポート 4173 が正常に起動

---

### **モジュール 1: BigQuery データセット俯瞰**

**⏱️ 学習時間**: 45 分
**難易度**: ⭐ 初級
**前提知識**: モジュール 0 修了

#### 学習内容
1. **GCP 認証の理解**
   - Application Default Credentials (ADC) の概念
   - `gcloud auth application-default login` の実行と確認
   - ローカル認証ファイルの場所（`~/.config/gcloud`）

2. **BigQuery 接続の基礎**
   - BigQuery プロジェクト ID の確認
   - データセット一覧の表示
   - テーブル情報（行数、サイズ）の確認

3. **Marimo インタラクティブ UI**
   - ドロップダウンメニューによるデータセット選択
   - テーブル一覧のリアルティムリフレッシュ
   - スキーマ定義の表示（フィールド名、型、モード、説明）
   - 行数グラフの表示

4. **ノートブックの読み方**
   - セル（Cell）の構成
   - UI コンポーネント（ボタン、ドロップダウン等）
   - 出力結果の確認

#### ハンズオン
```bash
# BigQuery 俯瞰ノートブックの起動
uv run marimo run notebooks/bigquery_overview.py --port 4173
```

操作手順：
1. デフォルト GCP プロジェクト ID を入力
2. 「データセット一覧を表示」ボタンをクリック
3. 好みのデータセットを選択
4. テーブル一覧を表示
5. 特定テーブルを選択してスキーマを確認

#### チェックリスト
- [ ] ADC が正常に設定されている（`gcloud auth list` で確認）
- [ ] BigQuery プロジェクト ID が正しく表示される
- [ ] 複数のデータセットが表示される
- [ ] テーブルのスキーマ定義が表示される

---

### **モジュール 2: データ接続と読み込みの基礎**

**⏱️ 学習時間**: 1.5 時間
**難易度**: ⭐⭐ 初級～中級
**前提知識**: モジュール 0, 1 修了

#### 学習内容
1. **コネクター系の理解**
   - `src/ai_data_lab/connectors/` の構成
   - 各コネクターの責務（BigQuery、S3、Sheets、DuckDB、Filesystem）

2. **BigQuery コネクターの使用**
   ```python
   from src.ai_data_lab.connectors.bigquery import BigQueryConnector

   bq = BigQueryConnector(project_id="your-project-id")
   df = bq.read_query(
       query="SELECT * FROM dataset.table LIMIT 100"
   )
   ```

3. **Polars による効率的なデータ処理**
   - Pandas との違い
   - Lazy 評価による最適化
   - メモリ効率

4. **基本的なデータ操作**
   - 行・列の選択
   - フィルタリング
   - グループ化・集計
   - 結合（JOIN）

5. **データサンプリングと品質チェック**
   - `df.head()` / `df.tail()`
   - `df.info()` による型確認
   - 欠損値（NULL）のチェック

#### ハンズオン
**実習 1: BigQuery からのデータ読み込み**
```python
# 新しいノートブックを作成
# notebooks/02_sample_analysis.py

import marimo as mo
from src.ai_data_lab.connectors.bigquery import BigQueryConnector
import polars as pl

# コネクター初期化
bq = BigQueryConnector(project_id="yoake-dev-analysis")

# データ読み込み（最初は 100 行のみ）
df = bq.read_query("""
    SELECT
        post_id,
        text,
        created_at,
        author_name
    FROM `yoake-dev-analysis.dataset_name.table_name`
    LIMIT 100
""")

# 統計情報の表示
mo.md(f"**データセット概要**: {len(df)} 行")
df.head(10)
```

**実習 2: データ品質チェック**
```python
# 欠損値の確認
null_counts = df.select(pl.all().null_count())
mo.md(f"## 欠損値統計\n{null_counts}")

# 型の確認
mo.md(f"## スキーマ\n{df.schema}")
```

#### チェックリスト
- [ ] BigQuery コネクターが正常に機能する
- [ ] データフレームが正常に表示される
- [ ] 基本的な集計操作が可能
- [ ] 欠損値が識別できる

---

### **モジュール 3: インタラクティブ UI と簡単な集計分析**

**⏱️ 学習時間**: 45 分
**難易度**: ⭐⭐ 初級～中級
**前提知識**: モジュール 0, 1, 2 修了

#### 学習内容
1. **Marimo UI コンポーネント**
   - Slider（スライダー）
   - Dropdown（ドロップダウン）
   - Button（ボタン）
   - Checkbox / Radio（チェックボックス・ラジオボタン）

2. **リアクティブプログラミング**
   - `@mo.reactive` デコレーター
   - 依存関係の自動追跡
   - UI の変更に対する自動再計算

3. **インタラクティブなデータ分析**
   ```python
   import marimo as mo

   # UI コンポーネントの作成
   group_selector = mo.ui.dropdown(
       options=["Group A", "Group B", "Group C"],
       label="グループを選択"
   )

   @mo.reactive
   def filtered_data():
       return df.filter(pl.col("group") == group_selector.value)

   mo.md(f"## 選択されたグループ: {group_selector.value}")
   filtered_data()
   ```

4. **簡単なグラフ生成**
   - Matplotlib / Seaborn の基本
   - Plotly による インタラクティブグラフ
   - グラフの保存

#### ハンズオン
**実習 1: グループ別集計ダッシュボード**
```python
# ノートブック: 03_simple_dashboard.py

group_selector = mo.ui.dropdown(
    options=df.select("group").unique().to_series().to_list(),
    label="分析対象グループ"
)

@mo.reactive
def group_data():
    return df.filter(pl.col("group") == group_selector.value)

@mo.reactive
def summary():
    gd = group_data()
    return {
        "投稿数": len(gd),
        "平均文字数": gd["text_length"].mean(),
        "最大文字数": gd["text_length"].max(),
    }

mo.md(f"# {group_selector.value} の分析")
mo.md(f"## 統計情報\n- 投稿数: {summary()['投稿数']}")
gd = group_data()
gd.head(20)
```

**実習 2: 簡単なグラフ表示**
```python
import matplotlib.pyplot as plt

@mo.reactive
def plot_histogram():
    gd = group_data()
    plt.figure(figsize=(10, 6))
    plt.hist(gd["text_length"], bins=30)
    plt.xlabel("文字数")
    plt.ylabel("投稿数")
    plt.title(f"{group_selector.value} の文字数分布")
    return plt.gcf()

plot_histogram()
```

#### チェックリスト
- [ ] Dropdown UI が正常に動作
- [ ] UI 変更に対して データが自動更新
- [ ] グラフが正常に表示される
- [ ] Marimo ノートブックの再実行が必要ない（リアクティブ）

---

### **モジュール 4: 複数データソースの統合**

**⏱️ 学習時間**: 1 時間
**難易度**: ⭐⭐ 中級
**前提知識**: モジュール 0-3 修了

#### 学習内容
1. **複数コネクターの使用**
   - BigQuery、S3、Google Sheets、DuckDB の使い分け
   - 各コネクターの特性・パフォーマンス

2. **データの結合（JOIN）**
   ```python
   # BigQuery からテーブル A を読み込み
   df_a = bq.read_query("SELECT * FROM dataset.table_a")

   # S3 から CSV を読み込み
   df_b = bq.read_parquet("s3://bucket/file.parquet")

   # 結合
   result = df_a.join(df_b, on="id", how="inner")
   ```

3. **Google Sheets との連携**
   - Service Account 認証
   - Gspread ライブラリの使用
   - シートからのデータ読み込み

4. **DuckDB による効率的なクエリ**
   - In-memory SQL エンジン
   - Polars DataFrames との統合
   - パフォーマンスの最適化

#### ハンズオン
**実習 1: BigQuery と Google Sheets の統合**
```python
from src.ai_data_lab.connectors.sheets import SheetsConnector
from src.ai_data_lab.connectors.bigquery import BigQueryConnector

# BigQuery からマスターデータを読み込み
bq = BigQueryConnector()
master = bq.read_query("SELECT * FROM dataset.master_table")

# Google Sheets から補足データを読み込み
sheets = SheetsConnector()
supplement = sheets.read_sheet("spreadsheet_id", "Sheet1")

# 結合
combined = master.join(supplement, on="id", how="left")
```

#### チェックリスト
- [ ] 複数のコネクターが動作する
- [ ] Google Sheets の認証が完了
- [ ] データ結合が正常に実行される
- [ ] 結合結果のデータ品質を確認

---

### **モジュール 5: EDA（探索的データ分析）とビジュアライゼーション**

**⏱️ 学習時間**: 1.5 時間
**難易度**: ⭐⭐ 中級
**前提知識**: モジュール 0-4 修了

#### 学習内容
1. **EDA ツールの理解**
   - YData Profiling: 包括的なプロファイルレポート
   - AutoViz: 自動可視化ツール
   - Squarify: ツリーマップの生成

2. **YData Profiling の使用**
   ```python
   from ydata_profiling import ProfileReport

   profile = ProfileReport(df, title="Data Profile Report")
   profile.to_file("report.html")
   ```

3. **AutoViz による自動可視化**
   ```python
   from autoviz.AutoViz_Class import AutoViz_Class

   av = AutoViz_Class()
   av.AutoViz(filename=None, dfte=df, max_rows_analyzed=5000)
   ```

4. **Matplotlib / Seaborn による カスタム可視化**
   - 棒グラフ、折れ線グラフ、ヒストグラム
   - ヒートマップ、レーダーチャート
   - 高解像度出力（DPI 設定）

5. **Plotly による インタラクティブグラフ**
   - 3D グラフ
   - ダッシュボード風レイアウト

#### ハンズオン
**実習 1: YData Profiling レポート生成**
```python
from ydata_profiling import ProfileReport
import marimo as mo

@mo.reactive
def eda_report():
    profile = ProfileReport(group_data(), title="EDA Report")
    html = profile.to_html()
    return mo.Html(html)

eda_report()
```

**実習 2: 複数グラフの一括生成**
```python
import matplotlib.pyplot as plt
import seaborn as sns

fig, axes = plt.subplots(2, 2, figsize=(15, 12))

# グラフ 1: 投稿数分布
axes[0, 0].hist(df["text_length"], bins=50)
axes[0, 0].set_title("文字数分布")

# グラフ 2: 時系列
axes[0, 1].plot(df["created_at"], df["likes"])
axes[0, 1].set_title("時系列いいね数")

# グラフ 3: グループ別比較
df.groupby("group").size().plot(kind="bar", ax=axes[1, 0])
axes[1, 0].set_title("グループ別投稿数")

# グラフ 4: ヒートマップ
correlation = df[["likes", "retweets", "text_length"]].corr()
sns.heatmap(correlation, ax=axes[1, 1])
axes[1, 1].set_title("相関係数")

plt.tight_layout()
plt.savefig("reports/eda_overview.png", dpi=300, bbox_inches="tight")
```

#### チェックリスト
- [ ] YData Profiling がインストール・動作している
- [ ] EDA レポートが HTML で正常に生成される
- [ ] 複数グラフが 1 つのファイルに保存される
- [ ] 高解像度（300 DPI）で出力されている

---

### **モジュール 6: 単体分析ノートブックの構築**

**⏱️ 学習時間**: 1 時間
**難易度**: ⭐⭐ 中級
**前提知識**: モジュール 0-5 修了

#### 学習内容
1. **Marimo ノートブックのテンプレート設計**
   - ヘッダー（タイトル、説明、作成日）
   - 環境構築セクション
   - データ読み込みセクション
   - 分析セクション
   - 結果セクション

2. **再現性の確保**
   - シード値の固定
   - 環境変数の明示
   - パラメーターの外部化

3. **Marimo の ベストプラクティス**
   - セルの順序と依存関係
   - UI コンポーネントの レイアウト
   - パフォーマンス最適化（キャッシング）

4. **ノートブックのドキュメンテーション**
   - Mardown による説明
   - 各セクションのコメント
   - 出力の自動化

#### ハンズオン
**実習: グループ分析ノートブックテンプレート作成**

```python
# ファイル: notebooks/06_group_analysis_template.py
import marimo as mo
import polars as pl
from src.ai_data_lab.connectors.bigquery import BigQueryConnector

# ===== ヘッダー =====
mo.md("""
# グループ分析ノートブック
このノートブックは、特定のアイドルグループの投稿データを包括的に分析します。
- **作成日**: 2026-02-02
- **対象**: カスタマイズ可能
- **更新頻度**: 毎日 9:00
""")

# ===== パラメーター設定 =====
group_name = mo.ui.text_input(
    label="対象グループ名",
    value="FRUITS",
)

sample_size = mo.ui.slider(
    label="サンプルサイズ",
    start=100,
    stop=10000,
    step=100,
    value=1000,
)

mo.md(f"## 設定\n- グループ: {group_name.value}\n- サンプルサイズ: {sample_size.value}")

# ===== データ読み込み =====
@mo.reactive
def load_data():
    bq = BigQueryConnector()
    query = f"""
        SELECT *
        FROM `yoake-dev-analysis.dataset.table`
        WHERE group_name = '{group_name.value}'
        LIMIT {sample_size.value}
    """
    return bq.read_query(query)

mo.md("## データ読み込み")
data = load_data()
mo.md(f"読み込み行数: {len(data)}")

# ===== 基本統計 =====
mo.md("## 基本統計")
@mo.reactive
def stats():
    return {
        "総投稿数": len(data),
        "平均文字数": data["text_length"].mean(),
        "平均いいね数": data["likes"].mean(),
    }

stats()

# ===== ビジュアライゼーション =====
mo.md("## ビジュアライゼーション")
import matplotlib.pyplot as plt
fig, ax = plt.subplots()
ax.hist(data["text_length"], bins=50)
ax.set_title(f"{group_name.value} の文字数分布")
fig
```

#### チェックリスト
- [ ] ノートブックが再現性を確保している（同じ入力で同じ出力）
- [ ] パラメーターが外部化されている
- [ ] ドキュメンテーションが充実している
- [ ] 実行時間が許容範囲内（< 5 分）

---

### **モジュール 7: テスト駆動開発と品質管理**

**⏱️ 学習時間**: 1.5 時間
**難易度**: ⭐⭐⭐ 中級～上級
**前提知識**: モジュール 0-6 修了

#### 学習内容
1. **テスト戦略**
   - Unit テスト: 個別関数のテスト
   - Integration テスト: 複数コンポーネント間のテスト
   - EDA テスト: 可視化・分析結果のテスト

2. **pytest の基本**
   ```python
   # tests/unit/test_connectors.py
   import pytest
   from src.ai_data_lab.connectors.bigquery import BigQueryConnector

   def test_bigquery_connection():
       bq = BigQueryConnector(project_id="test-project")
       assert bq.project_id == "test-project"

   def test_query_returns_dataframe():
       bq = BigQueryConnector()
       result = bq.read_query("SELECT 1 as id")
       assert result is not None
       assert len(result) > 0
   ```

3. **Mock / Fixture の使用**
   ```python
   import pytest
   from unittest.mock import Mock, patch

   @pytest.fixture
   def mock_bigquery(monkeypatch):
       mock = Mock()
       mock.read_query.return_value = pl.DataFrame({
           "id": [1, 2, 3],
           "value": [10, 20, 30]
       })
       monkeypatch.setattr("src.ai_data_lab.connectors.bigquery.BigQueryConnector", mock)
       return mock

   def test_analysis_with_mock(mock_bigquery):
       result = analyze()
       assert len(result) == 3
   ```

4. **テストの実行と カバレッジ**
   ```bash
   # Unit テストのみ実行
   uv run pytest tests/unit/

   # Integration テストを含める
   uv run pytest -m integration

   # EDA テストを含める
   RUN_EDA_TESTS=1 uv run pytest -m eda

   # カバレッジレポート
   uv run pytest --cov=src/ai_data_lab
   ```

#### ハンズオン
**実習 1: 簡単なユニットテスト作成**
```python
# tests/unit/test_marimo_launcher.py
import pytest
from src.ai_data_lab.eda.marimo_launcher import MarimoLauncher

def test_launcher_initialization():
    launcher = MarimoLauncher()
    assert launcher is not None

def test_launch_notebook():
    launcher = MarimoLauncher()
    # 実際の起動テストは Integration テストで
    # ここでは初期化のみ
    assert launcher.is_initialized()
```

**実習 2: Integration テスト作成**
```python
# tests/integration/test_bigquery_integration.py
import pytest
import os

@pytest.mark.integration
@pytest.mark.skipif(
    not os.getenv("BIGQUERY_PROJECT_ID"),
    reason="BigQuery not configured"
)
def test_real_bigquery_query():
    from src.ai_data_lab.connectors.bigquery import BigQueryConnector
    bq = BigQueryConnector()
    result = bq.read_query("SELECT 1 as test_col")
    assert len(result) == 1
    assert "test_col" in result.columns
```

#### チェックリスト
- [ ] Unit テストが 5 個以上作成されている
- [ ] `pytest` で全テストが実行できる
- [ ] Integration テストが実行できる（適切なスキップ条件付き）
- [ ] テストカバレッジが 70% 以上

---

### **モジュール 8: 並列分析フレームワークの理解**

**⏱️ 学習時間**: 2 時間
**難易度**: ⭐⭐⭐ 上級
**前提知識**: モジュール 0-7 修了

#### 学習内容
1. **並列分析の概念**
   - 5 Phase 分析パイプライン
   - Phase 間の依存関係
   - 並列実行可能な Phase の特定

2. **分析 5 Phase の理解**
   | Phase | 説明 | 依存関係 | 並列可能 |
   |-------|------|--------|--------|
   | Phase 1 | データ収集と前処理 | なし | - |
   | Phase 2 | 基礎統計分析 | Phase 1 | Phase 3 と並列 |
   | Phase 3 | テキストマイニング | Phase 1 | Phase 2 と並列 |
   | Phase 4 | 比較分析 | Phase 2, 3 | - |
   | Phase 5 | レポート統合 | Phase 4 | - |

3. **マルチプロセス・マルチスレッドの運用**
   - Python `subprocess` による ノートブック起動
   - ポート管理（自動割当・固定割当）
   - プロセス監視・ヘルスチェック

4. **SessionRegistry による セッション管理**
   ```python
   from src.ai_data_lab.eda.session_registry import SessionRegistry

   registry = SessionRegistry()
   # セッション情報を保存
   registry.add_session(
       notebook="phase1_data_collection.py",
       port=41000,
       pid=12345
   )
   # セッション一覧を取得
   sessions = registry.get_all_sessions()
   ```

5. **ポート自動割当**
   ```python
   from src.ai_data_lab.ports import PortAllocator

   allocator = PortAllocator(port_range="41000-41999")
   port = allocator.find_available_port()
   ```

#### ハンズオン
**実習 1: Phase 1 - データ収集の実行**
```bash
uv run marimo edit notebooks/phase1_data_collection.py --port 41000
```

操作手順：
1. ブラウザで `http://localhost:41000` を開く
2. グループキーワードを確認（FRUITS, ZIPPER など）
3. サンプルサイズを確認（デフォルト: 1000）
4. 「データ収集開始」ボタンをクリック
5. 進捗表示を確認
6. 完了するまで待機（3-5 分）

**実習 2: SessionRegistry の確認**
```python
from src.ai_data_lab.eda.session_registry import SessionRegistry
import json

registry = SessionRegistry()
sessions = registry.get_all_sessions()

mo.md("## アクティブセッション")
for session in sessions:
    mo.md(f"- {session['notebook']}: {session['url']} (PID: {session['pid']})")
```

**実習 3: Phase 2 & 3 の並列起動**
```python
from src.ai_data_lab.eda.marimo_launcher import MarimoLauncher
import subprocess
import time

launcher = MarimoLauncher()

# Phase 2 を開始
launcher.launch_notebook("phase2_basic_statistics.py", port=41001)
time.sleep(2)

# Phase 3 を開始（Phase 2 と並列）
launcher.launch_notebook("phase3_text_mining.py", port=41002)

# 両方の完了を待機
launcher.wait_for_completion(timeout=600)
```

#### チェックリスト
- [ ] Phase 1 が正常に完了する
- [ ] SessionRegistry が セッション情報を記録する
- [ ] ポート自動割当が機能する
- [ ] Phase 2 と Phase 3 が並列実行される
- [ ] `reports/comprehensive_analysis/data/` にサンプルデータが保存される

---

### **モジュール 9: マスターコーディネーターと包括的分析**

**⏱️ 学習時間**: 1.5 時間
**難易度**: ⭐⭐⭐ 上級
**前提知識**: モジュール 0-8 修了

#### 学習内容
1. **マスターコーディネーターの役割**
   - 全 5 Phase の管理・調整
   - ボタン操作による 段階的実行
   - リアルタイムステータス表示

2. **ノートブック：`05_comprehensive_analysis_coordinator.py`**
   - UI コンポーネント設計
   - 各 Phase の subprocess 管理
   - エラーハンドリング

3. **レポート生成パイプライン**
   - Markdown レポート自動生成
   - HTML レポート自動生成（画像埋め込み）
   - 300 DPI 高解像度画像の生成

4. **出力ファイル構成**
   ```
   reports/comprehensive_analysis/
   ├── final_comprehensive_report.md      # Markdown レポート
   ├── final_comprehensive_report.html    # HTML レポート
   ├── data/
   │   ├── group_data_sample.parquet
   │   └── individual_data_sample.parquet
   └── visualizations/
       ├── phase2/
       │   ├── 01_group_post_counts.png (300 DPI)
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

#### ハンズオン
**実習 1: マスターコーディネーター起動**
```bash
uv run marimo edit notebooks/05_comprehensive_analysis_coordinator.py --port 40000
```

操作手順：
1. ブラウザで `http://localhost:40000` を開く
2. 「🚀 Phase 1 起動: データ収集」をクリック
3. 進捗表示で完了を確認
4. 「⚡ Phase 2 & 3 並列起動」をクリック
5. 進捗バーで両 Phase の実行を確認
6. 全 Phase 完了を待機

**実習 2: 生成されたレポートの確認**
```bash
# HTML レポートをブラウザで開く
open reports/comprehensive_analysis/final_comprehensive_report.html

# または、Markdown を確認
cat reports/comprehensive_analysis/final_comprehensive_report.md
```

**実習 3: 生成画像の確認**
```python
from PIL import Image
import matplotlib.pyplot as plt

# 生成されたグラフを確認
img = Image.open("reports/comprehensive_analysis/visualizations/phase2/01_group_post_counts.png")
plt.figure(figsize=(10, 8))
plt.imshow(img)
plt.axis("off")
plt.tight_layout()
```

#### チェックリスト
- [ ] マスターコーディネーターが正常に起動する
- [ ] 全 5 Phase が順序通りに実行される
- [ ] Phase 2 と 3 が並列実行される
- [ ] 8 枚の高解像度画像が生成される
- [ ] HTML レポートが正常に表示される
- [ ] レポートに全画像が埋め込まれている

---

### **モジュール 10: Serena MCP との連携**

**⏱️ 学習時間**: 45 分
**難易度**: ⭐⭐⭐ 上級
**前提知識**: モジュール 0-9 修了

#### 学習内容
1. **Serena とは**
   - LSP（Language Server Protocol）ベースのシンボリック操作ツール
   - Cursor / Claude Code との連携
   - コード検索・編集の統一インターフェース

2. **Serena セットアップ**
   ```bash
   # プロジェクト初期化（既に実施済み）
   uvx --from git+https://github.com/oraios/serena \
     serena project create \
     --name "AI Data Lab" \
     --language python \
     --index

   # ヘルスチェック
   uvx --from git+https://github.com/oraios/serena \
     serena project health-check
   ```

3. **MCP サーバーの起動**
   ```bash
   uvx --from git+https://github.com/oraios/serena \
     serena start-mcp-server \
     --context ide-assistant \
     --project /Users/kou1904/aieda_agent \
     --mode planning \
     --mode editing \
     --mode no-onboarding
   ```

4. **Cursor との連携**
   - `~/.cursor/mcp.json` での設定
   - Cursor チャット内での Serena コマンド実行
   - コード検索・編集フロー

#### ハンズオン
**実習 1: Serena ヘルスチェック**
```bash
# プロジェクトの言語サーバーがオンライン状態を確認
uvx --from git+https://github.com/oraios/serena serena project health-check

# 出力例：
# - Python language server: ✓ Online
# - Index: ✓ Updated
# - Cache: ✓ Valid
```

**実習 2: Serena MCP サーバー起動と Dashboard 確認**
```bash
uvx --from git+https://github.com/oraios/serena \
  serena start-mcp-server \
  --context ide-assistant \
  --project /Users/kou1904/aieda_agent \
  --mode planning \
  --mode editing
```
ブラウザで `http://localhost:24282/dashboard/` を開く

**実習 3: Cursor チャット内での検索**
Cursor の Chat ツールで以下のようなプロンプトを入力：
```
@serena: BigQueryConnector の read_query メソッドの実装を表示してください
```

#### チェックリスト
- [ ] `.serena/project.yml` が存在する
- [ ] `serena project health-check` が成功
- [ ] MCP サーバーが起動できる
- [ ] Cursor の chat で Serena コマンドが実行できる

---

### **モジュール 11: 本番運用と CI/CD**

**⏱️ 学習時間**: 1 時間
**難易度**: ⭐⭐⭐ 上級
**前提知識**: モジュール 0-10 修了

#### 学習内容
1. **Docker による コンテナ化**
   - `Dockerfile` の構成
   - `docker-compose.yml` でのマウント設定
   - ローカル認証情報の安全なマウント

2. **本番環境での実行**
   ```bash
   docker compose up --build
   ```

3. **CI/CD パイプラインの基本**
   - GitHub Actions / GitLab CI
   - Unit テスト の自動実行
   - Integration テストの定期実行
   - 定期的な EDA レポート生成

4. **スケジューリング**
   - cron による定期実行
   - GitHub Actions Workflow による スケジュール実行
   - エラーアラート設定

5. **ロギングと監視**
   - Python `logging` モジュール
   - レポート生成後のメール通知
   - エラーの自動検出と アラート

#### ハンズオン
**実習 1: Docker で 本番環境をシミュレート**
```bash
# ローカルで Docker を構築・実行
docker compose up --build

# ブラウザでアクセス
open http://localhost:2718
```

**実習 2: CI/CD 用の Shell スクリプト作成**
```bash
# ファイル: scripts/run_daily_analysis.sh
#!/bin/bash
set -e

echo "Starting daily comprehensive analysis..."
uv run marimo edit notebooks/05_comprehensive_analysis_coordinator.py --port 40000 &
COORDINATOR_PID=$!

# 待機
sleep 10

# Phase 1 を実行
echo "Phase 1: Data collection..."
curl -X POST http://localhost:40000/api/phase1

# 待機
sleep 60

# Phase 2 & 3 を実行
echo "Phase 2 & 3: Parallel analysis..."
curl -X POST http://localhost:40000/api/phase2_phase3

# 待機
sleep 120

# レポート確認
if [ -f "reports/comprehensive_analysis/final_comprehensive_report.html" ]; then
    echo "Analysis completed successfully!"
    # メール送信など
else
    echo "Analysis failed!"
    exit 1
fi

kill $COORDINATOR_PID
```

**実習 3: テスト実行スクリプト**
```bash
# ファイル: scripts/run_tests.sh
#!/bin/bash
set -e

echo "Running Unit Tests..."
uv run pytest tests/unit/ --cov=src/ai_data_lab --cov-report=html

echo "Running Integration Tests..."
uv run pytest -m integration --cov=src/ai_data_lab --cov-report=term-summary

echo "Tests completed!"
```

#### チェックリスト
- [ ] `docker compose up --build` で正常に起動
- [ ] Docker コンテナ内で ノートブックが実行可能
- [ ] 定期実行スクリプトが正常に動作
- [ ] テスト自動実行スクリプトが動作
- [ ] エラーハンドリングが機能

---

## 📊 モジュール別 学習時間と難易度

| モジュール | タイトル | 時間 | 難易度 | 必須度 |
|----------|---------|-----|------|------|
| 0 | 環境構築と基本操作 | 30分 | ⭐ | 必須 |
| 1 | BigQuery データセット俯瞰 | 45分 | ⭐ | 必須 |
| 2 | データ接続と読み込みの基礎 | 1.5h | ⭐⭐ | 必須 |
| 3 | インタラクティブ UI と簡単な集計 | 45分 | ⭐⭐ | 推奨 |
| 4 | 複数データソースの統合 | 1h | ⭐⭐ | 推奨 |
| 5 | EDA とビジュアライゼーション | 1.5h | ⭐⭐ | 推奨 |
| 6 | 単体分析ノートブックの構築 | 1h | ⭐⭐ | 推奨 |
| 7 | テスト駆動開発と品質管理 | 1.5h | ⭐⭐⭐ | オプション |
| 8 | 並列分析フレームワークの理解 | 2h | ⭐⭐⭐ | オプション |
| 9 | マスターコーディネーター | 1.5h | ⭐⭐⭐ | オプション |
| 10 | Serena MCP との連携 | 45分 | ⭐⭐⭐ | オプション |
| 11 | 本番運用と CI/CD | 1h | ⭐⭐⭐ | オプション |

**合計時間**:
- 基礎コース（0-3）: 4 時間
- 標準コース（0-7）: 8 時間
- 上級コース（0-11）: 12 時間 30 分

---

## 🎯 学習パス別 推奨モジュール選択

### 🟢 ビジネス分析者向け（4時間）

```
推奨対象: ビジネス質問に答えるデータ抽出が必要な方

【必須】
✓ Module 0: 環境構築と基本操作          (30分)
✓ Module 1: BigQuery データセット俯瞰   (45分)
✓ Module 2: データ接続と読み込みの基礎 (1.5h)
✓ Module 3: インタラクティブ UI        (45分)

【学習成果】
- BigQuery からデータを抽出
- 簡単な集計分析
- 基本的なグラフ表示
```

### 🟡 データエンジニア向け（8時間）

```
推奨対象: 独自の分析ノートブック開発・運用が必要な方

【必須】
✓ Module 0-2: 基礎（2.5h）
✓ Module 3-6: 中級（4.5h）

【推奨】
△ Module 7: テスト駆動開発（1.5h） - 品質重視なら実施

【学習成果】
- 複数データソースからの統合データ取得
- Marimo による インタラクティブ分析ノートブック構築
- EDA 自動化とビジュアライゼーション
- 単体ノートブックの独立構築・運用
```

### 🔴 AI エンジニア向け（12.5時間）

```
推奨対象: 並列分析パイプライン構築・運用、チーム全体の仕組み構築が必要な方

【全モジュール実施】
✓ Module 0-11: 全て実施（12.5h）

【学習成果】
- マルチプロセス/マルチスレッド分析フレームワーク理解
- 5 Phase 並列分析パイプライン構築
- マスターコーディネーターによるオーケストレーション
- 300 DPI 高解像度レポート自動生成
- 本番環境への Docker デプロイ
- CI/CD パイプラインの構築
- Serena MCP による AI アシスト開発
```

---

## ✅ プログレストラッキング

### 学習進捗記録シート

自分の学習状況を記録してください：

```markdown
## 私の学習進捗

### 基礎コース（4時間）
- [ ] Module 0: 環境構築と基本操作 ---- 完了日: ____
- [ ] Module 1: BigQuery 俯瞰 --------- 完了日: ____
- [ ] Module 2: データ接続基礎 ------- 完了日: ____
- [ ] Module 3: インタラクティブ UI --- 完了日: ____

### 標準コース（追加 4時間）
- [ ] Module 4: 複数データソース統合 - 完了日: ____
- [ ] Module 5: EDA とビジュアライゼーション - 完了日: ____
- [ ] Module 6: 単体ノートブック構築 - 完了日: ____
- [ ] Module 7: テスト駆動開発 ------ 完了日: ____

### 上級コース（追加 4.5時間）
- [ ] Module 8: 並列分析フレームワーク - 完了日: ____
- [ ] Module 9: マスターコーディネーター - 完了日: ____
- [ ] Module 10: Serena MCP 連携 ----- 完了日: ____
- [ ] Module 11: 本番運用と CI/CD ---- 完了日: ____

### 実績
- **完了モジュール数**: _____ / 12
- **累計学習時間**: _____ 時間
- **次のステップ**: _______
```

### 達成度評価

各モジュール修了時に、以下を確認してください：

#### ✅ 基本的な達成基準
- [ ] 全てのハンズオンが実行可能
- [ ] チェックリスト項目が全て達成
- [ ] ノートブック / スクリプトが 1 回以上正常に実行

#### 🎓 発展的な達成基準
- [ ] 提供されたテンプレートを カスタマイズ
- [ ] 独自のデータを用いた分析を実施
- [ ] 何か新しい機能を追加・改善

---

## 🏅 修了証要件（オプション）

AI Data Lab 完全習得を証明するために、以下の要件を達成してください：

### 基礎修了証（4時間コース）
✅ 要件:
1. Module 0-3 の全てを修了
2. ビジネス質問 2 個に対し BigQuery からデータ抽出・集計
3. 簡単なグラフ 1 枚を生成

### 標準修了証（8時間コース）
✅ 要件:
1. Module 0-7 の全てを修了
2. 複数データソース（BigQuery + 1 個以上）を統合したノートブック構築
3. EDA レポートと ビジュアライゼーション（3 枚以上のグラフ）を生成
4. Unit テスト 3 個以上を作成・実行

### 上級修了証（12時間コース）
✅ 要件:
1. Module 0-11 の全てを修了
2. 独自の分析テーマで 5 Phase 並列分析を実行
3. マスターコーディネーターで 8 枚以上の高解像度画像を含むレポート生成
4. Integration テスト 5 個以上を作成・実行
5. Docker で 本番環境シミュレーション

---

## 📚 追加リソース

### 公式ドキュメント
- [README.md](../README.md): プロジェクト概要
- [QUICK_START.md](../docs/QUICK_START.md): 5 分で開始
- [COMPREHENSIVE_ANALYSIS_GUIDE.md](../docs/COMPREHENSIVE_ANALYSIS_GUIDE.md): 詳細実行ガイド
- [IMPLEMENTATION_SUMMARY.md](../docs/IMPLEMENTATION_SUMMARY.md): 実装サマリー

### Marimo 公式ドキュメント
- [Marimo Documentation](https://docs.marimo.io/)
- [Marimo Tutorial](https://docs.marimo.io/tutorials/)

### BigQuery ドキュメント
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [BigQuery Connector](https://cloud.google.com/bigquery/docs/pandas-connector)

### Python データ分析
- [Polars Documentation](https://docs.pola.rs/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Matplotlib](https://matplotlib.org/stable/contents.html)
- [Seaborn](https://seaborn.pydata.org/)

### Serena MCP
- [Serena GitHub](https://github.com/oraios/serena)
- [MCP Specification](https://spec.modelcontextprotocol.io/)

---

## 🤔 よくある質問（FAQ）

### Q1: 初心者ですが、どから始めたら良いですか？
**A**: 🟢 ビジネス分析者向け（4時間）を推奨します。Module 0-3 を順序通りに進めてください。

### Q2: 既にデータ分析の経験がある場合は？
**A**: 🟡 データエンジニア向け（8時間）を推奨します。Module 2 から開始でも大丈夫です。

### Q3: テストは本当に必要ですか？
**A**: 本番運用する場合は必須です。簡単な実験なら Module 7 はスキップ可能です。

### Q4: 修了証はどうやって取得できますか？
**A**: 要件ページを参照し、成果物を提出してください。（オプション）

### Q5: 各モジュールの所要時間は？
**A**: 表は目安です。以下の要因で変動します：
- 予備知識
- コンピュータ環境
- 実験の深さ

### Q6: 同時に複数モジュール学習できますか？
**A**: 推奨しません。依存関係があるため、順序通り学習してください。

### Q7: わからないことが出てきたら？
**A**: 以下の対応を推奨：
1. 公式ドキュメント確認
2. GitHub Issues で類似質問を検索
3. コミュニティフォーラムで質問
4. AI（Claude / ChatGPT）に相談

---

## 🚀 次のステップ

### レベル別の推奨アクション

#### 🟢 基礎コース修了後
1. BigQuery に新しいテーブルを追加
2. 異なるデータセットで集計分析を実施
3. グラフをカスタマイズ

#### 🟡 標準コース修了後
1. チームメンバーと ノートブックを共有
2. 定期的な分析レポートの自動生成
3. 新しい EDA ツールの試行

#### 🔴 上級コース修了後
1. 本番環境への Docker デプロイ
2. GitHub Actions による自動テスト・分析実行
3. チーム全体の分析基盤の構築
4. カスタム分析テンプレートの開発

---

## 📝 学習時の注意事項

### 重要ポイント
1. **再現性**: 各ノートブックは何度実行しても同じ結果になることを確認
2. **ドキュメンテーション**: コードだけでなく、何をしているか説明も残す
3. **テスト**: 本番運用する場合は必ずテストを書く
4. **バージョン管理**: Git で進捗を記録・管理
5. **セキュリティ**: 認証情報（`.env`）を Git に含めない

### トラブルシューティング

| 問題 | 対処法 |
|------|------|
| ポートが使用中 | `lsof -i :PORT_NUMBER` で確認し `kill -9 PID` |
| BigQuery 認証エラー | `gcloud auth application-default login` を再実行 |
| メモリ不足 | サンプルサイズを減らす、ブラウザを再起動 |
| Marimo が起動しない | `uv sync --all-extras` で依存関係を再インストール |

---

## 📞 サポートと フィードバック

このカリキュラムをより良くするために、以下の方法でフィードバックをお寄せください：

- GitHub Issues: 不具合報告・機能リクエスト
- Discussions: 質問・提案・経験共有
- Email: プロジェクト管理者まで

---

**作成日**: 2026-02-02
**最終更新**: 2026-02-02
**バージョン**: 1.0

このカリキュラムは AI Data Lab コミュニティにより継続的に改善されています。

# AI Data Lab

AI と協調しながら BigQuery / S3 / Google Sheets / DuckDB を横断し、marimo で再現性の高いデータ分析を行うための uv + Docker ベースのリポジトリです。EDA 自動化ツール（YData Profiling / AutoViz）や Squarify などの可視化ユーティリティも選択的に利用できます。

## スタック

| レイヤ | 採用技術 | 補足 |
| --- | --- | --- |
| IDE/Notebook | [marimo](https://marimo.io/) | リアクティブ Python Notebook（.py でGit管理） |
| ランタイム | uv + Python 3.12 | `pyproject.toml` で依存とロック管理 |
| データ接続 | BigQuery (ADC), S3 (AWS CLI profile), Google Sheets (Service Account), DuckDB, Filesystem | src/ai_data_lab/connectors/ 以下に実装 |
| EDA | YData Profiling, AutoViz, Squarify, Colab Charts 風ユーティリティ | `eda` エクストラで有効化 |
| コンテナ | Dockerfile + docker-compose | gcloud/AWS/Secretsをボリュームマウント |

## ディレクトリ構成（抜粋）

```
.
├── notebooks/              # marimo notebooks（index.py がポータル予定）
├── src/ai_data_lab/
│   ├── connectors/         # BigQuery/S3/Sheets/DuckDB/Filesystem 等
│   └── eda/                # YData Profiling / AutoViz ランナー
├── tests/
│   ├── unit/
│   └── integration/
├── data/                   # DuckDBや一時ファイル（Git追跡なし）
├── reports/                # EDAレポート（Git追跡なし）
├── secrets/                # サービスアカウントJSON等（Git追跡なし）
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
├── uv.lock
└── .env.example
```

## 前提ツール

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) 0.7+
- Docker / Docker Compose
- gcloud CLI（`gcloud auth application-default login` 実行済み）
- AWS CLI（`~/.aws` に default プロファイル）

## Marimo ノートブック

### BigQuery データセット俯瞰

`notebooks/bigquery_overview.py` は、GCP プロジェクト内の全データセット・テーブル・スキーマを俯瞰するためのインタラクティブ UI を提供します。

#### 使い方

```bash
# Application Default Credentials (ADC) でログイン（初回のみ）
gcloud auth application-default login

# ノートブックを起動（ポート 4173 を使用）
uv run marimo run notebooks/bigquery_overview.py --port 4173
```

ブラウザで `http://localhost:4173` を開くと、以下の操作ができます：

1. **プロジェクト ID 入力**（デフォルト: `yoake-dev-analysis`）
2. **データセット一覧表示**
3. **データセット選択**
4. **テーブル一覧表示**（行数・サイズ含む）
5. **テーブル選択**（`dev_yoake_posts` があればデフォルト選択）
6. **スキーマ定義表示**（フィールド名、型、モード、説明）
7. **行数グラフ表示**

#### ポート管理（マルチエージェント運用）

複数の Marimo ノートブックを同時に起動する場合は、**ポート番号を明示的に指定** してください：

```bash
# ノートブック 1: BigQuery 俯瞰
uv run marimo run notebooks/bigquery_overview.py --port 4173

# ノートブック 2: ポータル（別ターミナル）
uv run marimo run notebooks/index.py --port 4174

# ノートブック 3: 分析ノート（別ターミナル）
uv run marimo run notebooks/analysis.py --port 4175
```

**推奨ポート割り当て：**

| ノートブック | 用途 | ポート |
| --- | --- | --- |
| `bigquery_overview.py` | BigQuery 俯瞰 | 4173 |
| `index.py` | ポータル | 4174 |
| `analysis.py` | データ分析 | 4175 |
| その他 | カスタム用途 | 4176～ |

> マルチエージェント環境では、各エージェントが異なるポート番号を使用するように調整してください。

### 自動ポート割当と SessionRegistry

- `src/ai_data_lab/ports.py` の `PortAllocator` が `MARIMO_PORT_RANGE`（例: `41000-41999`）内で空きポートをスキャンし、自動で割り当てます。
- `MARIMO_PORT` もしくは `MARIMO_PORT_FIXED` を設定すると、自動割当をスキップして固定ポートを使用します（空いていない場合は起動前にエラー）。
- 起動した notebook/ポート/PID は `src/ai_data_lab/eda/session_registry.py` の `SessionRegistry` によって `.marimo/sessions.json` に保存されます。Cursor の別エージェントや別 worktree からこのファイルを参照すれば、既存セッションへのアタッチや URL 共有が簡単です。

#### direnv での推奨設定

```
# .envrc （各 worktree ごとに設定）
export MARIMO_PORT_RANGE="41000-41999"

layout python

if [ -f .marimo/sessions.json ]; then
  echo "Active marimo sessions:"
  jq -r '.sessions[] | "- " + .notebook + " -> " + .url' .marimo/sessions.json
fi
```

- worktree ごとに `MARIMO_PORT_RANGE` を変えておけば、Cursor のマルチエージェントでも衝突しづらくなります。
- 起動後のポートを direnv に「渡す」ことはできないため、`.marimo/sessions.json` を共通の真実として使い、必要に応じて `jq` などで利用者に通知する運用を推奨しています。


## セットアップ（ローカル実行）

```bash
cp .env.example .env
uv sync --all-extras            # 依存インストール（dev/eda含む）
uv run marimo run notebooks/index.py --host 0.0.0.0 --port ${MARIMO_PORT:-2718}
```

> ※ Google Sheets 用のサービスアカウント JSON は `secrets/google-sheets-sa.json` に配置し、`.env` でパスを指定します。BigQuery は ADC（`~/.config/gcloud`）をマウントし、S3 は `~/.aws` の資格情報を用います。

## Docker / Compose

```bash
docker compose up --build
```

`docker-compose.yml` では以下をマウントします。

- `${HOME}/.config/gcloud` → `/root/.config/gcloud`（BigQuery ADC）
- `${HOME}/.aws` → `/root/.aws`（S3 認証）
- `./secrets` → `/secrets`（Google Sheets SA等）
- `./data` → `/app/data`
- `./reports` → `/app/reports`

## 環境変数

`.env.example` を参照し、必要な値を設定してください。

主なキー:

- `MARIMO_PORT`（デフォルト 2718）
- `DUCKDB_PATH=/app/data/duckdb/database.duckdb`
- `GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON=/secrets/google-sheets-sa.json`
- `AWS_PROFILE=default`
- `AWS_REGION=ap-northeast-1`
- `BIGQUERY_PROJECT_ID`（任意、ADCの既定を上書き）
- `EDA_ENABLED=0/1`, `EDA_TOOL=ydata|autoviz`, `EDA_MAX_ROWS`, `EDA_OUTPUT_DIR`

## テスト

```bash
uv run pytest                     # unitのみ
RUN_EDA_TESTS=1 uv run pytest -m eda
uv run pytest -m integration       # 外部接続テスト（要資格情報）
```

## Serena MCP 連携

Serena は LSP ベースのシンボリック操作ツールを提供する MCP サーバーで、Claude Code や Cursor などのクライアントにコード検索・編集の追加手段を与えられます[[1]](https://github.com/oraios/serena)。このリポジトリでも Serena を標準で使えるよう、以下の手順で初期設定を済ませてあります。

### 1. Serena CLI の取得

`uvx` で最新版を直接実行できます。常に最新版を使いたい場合はインストール不要です。

```bash
uvx --from git+https://github.com/oraios/serena serena --help
```

### 2. プロジェクト初期化とインデックス

ルート直下で以下を実行済みです。別の環境でも初期化したい場合は同じコマンドを再実行してください。

```bash
uvx --from git+https://github.com/oraios/serena \
  serena project create \
  --name "AI Data Lab" \
  --language python \
  --index
```

`.serena/project.yml` と LSP キャッシュが生成され、Serena からコード構造を即参照できます。

### 3. ヘルスチェック

言語サーバーや主要ツールの動作確認には

```bash
uvx --from git+https://github.com/oraios/serena serena project health-check
```

を利用します。結果は `.serena/logs/health-checks/` に保存されるため、不具合時のログ参照も容易です。

### 4. Cursor への登録

Cursor（MCP 対応版）は `~/.cursor/mcp.json` で MCP サーバーを一元管理します。以下のエントリを追加すると、チャット側からいつでも Serena を呼び出せます（本リポジトリでは既に追加済みです）。

```jsonc
"serena": {
  "command": "uvx",
  "args": [
    "--from", "git+https://github.com/oraios/serena",
    "serena", "start-mcp-server",
    "--context", "ide-assistant",
    "--project", "/Users/kou1904/aieda_agent",
    "--mode", "planning",
    "--mode", "editing",
    "--mode", "no-onboarding"
  ]
}
```

- `--context ide-assistant`: Cursor 側の組み込み操作と重複しない最小限のツールセット構成。
- `--project /Users/kou1904/aieda_agent`: 対象ディレクトリを固定し、会話開始直後からツールが使えるようにする。
- `--mode planning/editing/no-onboarding`: 設計→実装フローを強化しつつ、毎回のオンボーディングをスキップ。

### 5. 単体起動（必要に応じて）

Cursor 以外の MCP クライアントや検証用途で手動起動したい場合は次のコマンドを直接実行してください。

```bash
uvx --from git+https://github.com/oraios/serena \
  serena start-mcp-server \
  --context ide-assistant \
  --project /Users/kou1904/aieda_agent \
  --mode planning \
  --mode editing \
  --mode no-onboarding
```

起動時にブラウザで `http://localhost:24282/dashboard/` が開き、各ツール呼び出しや LSP の状態をリアルタイムで確認できます。

## コードスタイル / ルール

- `.cursor/rules/` に分析ルールを分割管理（communication, development, data_analysis, notebook, visualization, analysis_modes）。
- `.cursor/rules_background/` に各ルールの背景説明とベストプラクティスを記載。
- 自動EDAツール（YData Profiling, AutoViz）を積極的に活用してデータの全体像を把握。
- NotebookはMarimoを使用し、`edit_notebook` ツール経由で編集。仮想環境（uv）を使用。
- データ補完・加工時には根拠をNotebookに記述し、トレーサビリティを保つ。

## 包括的分析フレームワーク

アイドル・グループ・ファンダムの比較分析など、大規模な分析タスクを**並列実行**できるフレームワークを提供しています。

### 特徴

- **自動ポート割り当て**: 各エージェントが独立したポートを自動取得（41000-41999）
- **並列実行**: Phase 2とPhase 3を同時実行し、分析時間を大幅短縮
- **高解像度画像生成**: 全ての可視化を300 DPIで出力（エビデンス提示モード）
- **統合レポート**: Markdown + HTMLで画像を多用したレポート自動生成

### 実行方法

```bash
# マスターコーディネーターを起動
uv run marimo edit notebooks/05_comprehensive_analysis_coordinator.py --port 40000
```

詳細は [包括的分析実行ガイド](docs/COMPREHENSIVE_ANALYSIS_GUIDE.md) を参照してください。

### 分析フロー

```
Phase 1: データ収集 → Phase 2 & 3: 統計/テキスト分析（並列）→ Phase 4: 比較分析 → Phase 5: レポート統合
```

各Phaseは独立したMarimoノートブックとして実装され、依存関係に基づいて自動的に実行順序が管理されます。

## 今後のロードマップ

1. ✅ 包括的分析フレームワークの実装（並列実行・自動ポート割り当て）
2. `config.py`（Pydantic Settings）実装とテスト
3. DuckDB / Filesystem コネクタ実装
4. Marimo ポータル（ソース選択 + プレビュー + 保存）
5. EDAタブ（YData Profiling / AutoViz）の統合 - Marimoで実装
6. S3 / Sheets / BigQuery コネクタ
7. CI（unitテスト）と integration / eda トリガ
8. リアルタイム分析ダッシュボード

ご意見・要望があれば Issue / PR / DM でお知らせください。


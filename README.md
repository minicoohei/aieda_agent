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

- `.cursor.rules` に分析ルールを定義（目的明記、データ品質、バイアス防止、Squarify/Colab Chartsスタイルの可視化方針など）。
- Notebookは `edit_notebook` ツール経由で編集し、仮想環境（uv）を使用。
- データ補完・加工時には根拠をNotebookに記述し、トレーサビリティを保つ。

## 今後のロードマップ

1. `config.py`（Pydantic Settings）実装とテスト
2. DuckDB / Filesystem コネクタ実装
3. marimo ポータル（ソース選択 + プレビュー + 保存）
4. EDAタブ（YData Profiling / AutoViz / Squarify）
5. S3 / Sheets / BigQuery コネクタ
6. CI（unitテスト）と integration / eda トリガ

ご意見・要望があれば Issue / PR / DM でお知らせください。


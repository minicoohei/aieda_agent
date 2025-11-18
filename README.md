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


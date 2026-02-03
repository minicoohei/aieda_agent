import marimo

__generated_with = "0.17.8"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(
        """
        # ダウンロード×インテント×行動ダッシュボード

        直近30日の企業ダウンロードTOPとインテントスコアを紐づけ、
        リスト追加/CSVダウンロードの流れを把握するためのダッシュボードです。
        """
    )
    return


@app.cell
def _():
    import os
    import sys
    import tomllib
    from datetime import date, timedelta
    from pathlib import Path

    import pandas as pd
    from dotenv import load_dotenv

    snowflake = None
    default_backend = None
    serialization = None
    snowflake_error = None
    try:
        import snowflake.connector
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization
        import snowflake as snowflake
    except Exception as exc:
        snowflake_error = exc

    bq_error = None
    bigquery = None
    service_account = None
    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account
    except Exception as exc:
        bq_error = exc

    possible_env_paths = [
        Path(__file__).parent.parent / ".env",
        Path.cwd() / ".env",
        Path.cwd().parent / ".env",
        Path("/Users/kou1904/githubactions_fordata/work/aieda_agent/.env"),
    ]
    for env_path in possible_env_paths:
        if env_path.exists():
            load_dotenv(env_path)
            break

    root_dir = Path("/Users/kou1904/githubactions_fordata/work/aieda_agent")
    if str(root_dir / "src") not in sys.path:
        sys.path.insert(0, str(root_dir / "src"))

    return (
        Path,
        bq_error,
        bigquery,
        date,
        default_backend,
        os,
        pd,
        serialization,
        service_account,
        snowflake,
        snowflake_error,
        timedelta,
        tomllib,
    )


@app.cell
def _(bq_error, mo, snowflake_error):
    if snowflake_error:
        mo.md(f"**Snowflakeモジュール読み込みエラー**: `{snowflake_error}`")
    if bq_error:
        mo.md(f"**BigQueryモジュール読み込みエラー**: `{bq_error}`")
    return


@app.cell
def _(Path, bq_error, bigquery, os, service_account):
    BQ_PROJECT_ID = "gree-dionysus-infobox"
    BQ_DATASET_ID = "production_infobox"

    def get_bq_client():
        if bq_error:
            raise RuntimeError(f"BigQuery connector not available: {bq_error}")
        key_path = (
            os.getenv("BIGQUERY_KEY_PATH")
            or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            or "~/.gcp/gree-dionysus-infobox.json"
        )
        key_path = Path(os.path.expanduser(key_path))
        if key_path.exists():
            credentials = service_account.Credentials.from_service_account_file(
                key_path
            )
            return bigquery.Client(project=BQ_PROJECT_ID, credentials=credentials)
        return bigquery.Client(project=BQ_PROJECT_ID)

    return BQ_DATASET_ID, BQ_PROJECT_ID, get_bq_client


@app.cell
def _(Path, default_backend, os, serialization, snowflake, snowflake_error, tomllib):
    def get_snowflake_connection():
        if snowflake is None:
            raise RuntimeError(f"Snowflake connector not available: {snowflake_error}")

        config_path = Path.home() / ".snowflake" / "connections.toml"
        config = {}
        if config_path.exists():
            with open(config_path, "rb") as f:
                config = tomllib.load(f).get("default", {})

        account = os.getenv("SNOWFLAKE_ACCOUNT") or config.get("account")
        user = os.getenv("SNOWFLAKE_USER") or config.get("user")
        password = os.getenv("SNOWFLAKE_PASSWORD") or config.get("password")
        private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH") or config.get(
            "private_key_path"
        )
        authenticator = os.getenv("SNOWFLAKE_AUTHENTICATOR") or config.get(
            "authenticator"
        )
        role = os.getenv("SNOWFLAKE_ROLE") or config.get("role")
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE") or config.get("warehouse")
        database = os.getenv("SNOWFLAKE_DATABASE") or config.get("database")
        schema = os.getenv("SNOWFLAKE_SCHEMA") or config.get("schema")

        if not account or not user:
            raise ValueError("Snowflake接続情報が不足しています")

        connect_params = {"account": account, "user": user}

        if private_key_path:
            key_path = Path(private_key_path).expanduser()
            with open(key_path, "rb") as key_file:
                private_key = serialization.load_pem_private_key(
                    key_file.read(), password=None, backend=default_backend()
                )
            connect_params["private_key"] = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        elif password:
            connect_params["password"] = password

        if authenticator and not private_key_path:
            connect_params["authenticator"] = authenticator
        if role:
            connect_params["role"] = role
        if warehouse:
            connect_params["warehouse"] = warehouse
        if database:
            connect_params["database"] = database
        if schema:
            connect_params["schema"] = schema

        return snowflake.connector.connect(**connect_params)

    return (get_snowflake_connection,)


@app.cell
def _(mo):
    mo.md("## パラメータ")
    return


@app.cell
def _(date, mo, timedelta):
    default_end = date.today()
    default_start = default_end - timedelta(days=30)
    start_date = mo.ui.date(value=default_start, label="開始日")
    end_date = mo.ui.date(value=default_end, label="終了日")
    start_date
    end_date
    return end_date, start_date


@app.cell
def _(mo):
    top_n = mo.ui.number(label="TOP件数", value=50, min=10, max=200, step=10)
    top_n
    return (top_n,)


@app.cell
def _(mo):
    mo.md("## 1. BigQuery: インテントスコア候補テーブル探索")
    return


@app.cell
def _(BQ_DATASET_ID, get_bq_client, mo, pd):
    df_bq_tables = pd.DataFrame()
    df_bq_intent_cols = pd.DataFrame()
    try:
        client = get_bq_client()
        tables_sql = f"""
        SELECT table_name, row_count, size_bytes
        FROM `{BQ_DATASET_ID}.__TABLES__`
        ORDER BY size_bytes DESC
        LIMIT 200
        """
        df_bq_tables = client.query(tables_sql).to_dataframe()

        cols_sql = f"""
        SELECT table_name, column_name, data_type
        FROM `{BQ_DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
        WHERE REGEXP_CONTAINS(LOWER(column_name), r'(intent|score)')
        ORDER BY table_name, ordinal_position
        """
        df_bq_intent_cols = client.query(cols_sql).to_dataframe()
    except Exception as e:
        mo.md(f"**BigQuery探索エラー**: `{e}`")
    return df_bq_intent_cols, df_bq_tables


@app.cell
def _(df_bq_tables, mo):
    mo.md("### BigQuery テーブル一覧（上位200）")
    if df_bq_tables is not None and len(df_bq_tables) > 0:
        mo.ui.table(df_bq_tables, pagination=True)
    else:
        mo.md("*データがありません*")
    return


@app.cell
def _(df_bq_intent_cols, mo):
    mo.md("### intent/score を含むカラム一覧")
    if df_bq_intent_cols is not None and len(df_bq_intent_cols) > 0:
        mo.ui.table(df_bq_intent_cols, pagination=True)
    else:
        mo.md("*データがありません*")
    return


@app.cell
def _(mo):
    mo.md("## 2. Snowflake: 直近30日DL企業TOP")
    return


@app.cell
def _(end_date, get_snowflake_connection, mo, pd, start_date, top_n):
    df_download_top = pd.DataFrame()
    if start_date.value and end_date.value:
        start_ts = start_date.value.strftime("%Y-%m-%d")
        end_ts = end_date.value.strftime("%Y-%m-%d")
        sql = f"""
        SELECT
          bc.ID AS company_id,
          bc.SHOGO AS company_name,
          COUNT(*) AS download_count
        FROM ETL_S3_TRANSALES_DB.TRANSALES_DAILY_SCHEMA.CSVDOWNLOADLOG dl
        JOIN ETL_S3_TRANSALES_DB.TRANSALES_DAILY_SCHEMA._BEEGLECOMPANYTOCSVDOWNLOADLOG bcdl
          ON bcdl.A = dl.ID
        JOIN ETL_S3_TRANSALES_DB.TRANSALES_DAILY_SCHEMA.BEEGLECOMPANY bc
          ON bcdl.B = bc.ID
        WHERE dl.CREATEDAT >= '{start_ts}'
          AND dl.CREATEDAT < DATEADD('day', 1, '{end_ts}')
        GROUP BY company_id, company_name
        ORDER BY download_count DESC
        LIMIT {int(top_n.value or 50)}
        """
        try:
            conn = get_snowflake_connection()
            cur = conn.cursor()
            cur.execute(sql)
            df_download_top = cur.fetch_pandas_all()
        except Exception as e:
            mo.md(f"**DL集計エラー**: `{e}`")
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass
    return (df_download_top,)


@app.cell
def _(df_download_top, mo):
    mo.md("### 企業別ダウンロードTOP")
    if df_download_top is not None and len(df_download_top) > 0:
        mo.ui.table(df_download_top, pagination=True)
    else:
        mo.md("*データがありません*")
    return


@app.cell
def _(mo):
    mo.md("## 3. リスト追加・CSV行動の概要")
    return


@app.cell
def _(end_date, get_snowflake_connection, mo, pd, start_date):
    df_list_add = pd.DataFrame()
    if start_date.value and end_date.value:
        start_ts = start_date.value.strftime("%Y-%m-%d")
        end_ts = end_date.value.strftime("%Y-%m-%d")
        sql = f"""
        SELECT DATE_TRUNC('day', bcl.CREATEDAT) AS day,
               COUNT(*) AS add_count
        FROM ETL_S3_TRANSALES_DB.TRANSALES_DAILY_SCHEMA._BEEGLECOMPANYTOCOMPANYLIST bcl
        WHERE bcl.CREATEDAT >= '{start_ts}'
          AND bcl.CREATEDAT < DATEADD('day', 1, '{end_ts}')
        GROUP BY day
        ORDER BY day
        """
        try:
            conn = get_snowflake_connection()
            cur = conn.cursor()
            cur.execute(sql)
            df_list_add = cur.fetch_pandas_all()
        except Exception as e:
            mo.md(f"**リスト追加集計エラー**: `{e}`")
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass
    return (df_list_add,)


@app.cell
def _(df_list_add, mo):
    mo.md("### リスト追加の時系列")
    if df_list_add is not None and len(df_list_add) > 0:
        mo.ui.table(df_list_add, pagination=True)
    else:
        mo.md("*データがありません*")
    return


@app.cell
def _(mo):
    mo.md("## 4. インテントスコア結合（候補探索後に使用）")
    return


@app.cell
def _(df_download_top, mo):
    mo.md(
        """
        **インテントスコアの結合には以下が必要です。**

        - BigQueryのインテントテーブル名
        - Snowflake側の企業ID（BEEGLECOMPANY.ID）と
          BigQuery側の企業ID/ORGIDの対応キー
        """
    )
    if df_download_top is not None and len(df_download_top) > 0:
        mo.ui.table(df_download_top.head(20), pagination=False)
    return


@app.cell
def _(mo):
    mo.md("## 5. セマンティック確認用の質問")
    return


@app.cell
def _(mo):
    mo.md(
        """
        - インテントスコアの正本テーブル名はどれですか？
        - インテントスコアのキーは `company_id` / `org_id` / `beegle_company_id` のどれですか？
        - インテントスコアの更新頻度（日次/週次/リアルタイム）は？
        - スコアの種類（1st/2nd、カテゴリ別）はどのカラムに入っていますか？
        - CSVダウンロードの `USERID` はどのユーザーID体系ですか？
        """
    )
    return


if __name__ == "__main__":
    app.run()

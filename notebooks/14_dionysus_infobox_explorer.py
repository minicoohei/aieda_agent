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
        # Dionysus Intent Explorer

        Snowflake上のIntentデータを探索するノートブックです。
        `ETL_S3_TRANSALES_DB.TRANSALES_DAILY_SCHEMA` を対象に
        Intent関連テーブルの確認と簡易サンプル抽出を行います。
        """
    )
    return


@app.cell
def _():
    import os
    import sys
    import tomllib
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
        default_backend,
        os,
        pd,
        serialization,
        snowflake,
        snowflake_error,
        tomllib,
    )


@app.cell
def _(mo, snowflake_error):
    if snowflake_error:
        mo.md(f"**Snowflakeモジュール読み込みエラー**: `{snowflake_error}`")
    return


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
def _():
    DATABASE_NAME = "ETL_S3_TRANSALES_DB"
    SCHEMA_NAME = "TRANSALES_DAILY_SCHEMA"
    return DATABASE_NAME, SCHEMA_NAME


@app.cell
def _(mo):
    table_filter = mo.ui.text(label="テーブル名フィルタ（任意）", value="INTENT")
    sample_limit = mo.ui.number(label="サンプル行数", value=100, min=10, max=1000, step=10)
    table_filter
    sample_limit
    return sample_limit, table_filter


@app.cell
def _(DATABASE_NAME, SCHEMA_NAME, get_snowflake_connection, mo, pd, table_filter):
    df_tables = pd.DataFrame()
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        cur.execute(f"SHOW TABLES IN {DATABASE_NAME}.{SCHEMA_NAME}")
        df_tables = cur.fetch_pandas_all()
    except Exception as e:
        mo.md(f"**テーブル一覧取得エラー**: `{e}`")
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

    if len(df_tables) > 0 and "name" in df_tables.columns and table_filter.value:
        df_tables = df_tables[
            df_tables["name"].str.contains(table_filter.value, case=False, na=False)
        ]
    return (df_tables,)


@app.cell
def _(df_tables, mo):
    _output = mo.md("*テーブルがありません*")
    if len(df_tables) > 0:
        _output = mo.ui.table(df_tables, pagination=True)
    _output
    return


@app.cell
def _(df_tables, mo):
    table_options = (
        df_tables["name"].dropna().sort_values().unique().tolist()
        if len(df_tables) > 0 and "name" in df_tables.columns
        else []
    )
    table_selector = mo.ui.dropdown(
        options=table_options,
        value=table_options[0] if table_options else None,
        label="Intentテーブル選択",
    )
    table_selector
    return (table_selector,)


@app.cell
def _(DATABASE_NAME, SCHEMA_NAME, get_snowflake_connection, mo, pd, sample_limit, table_selector):
    df_sample = pd.DataFrame()
    if table_selector.value:
        try:
            conn = get_snowflake_connection()
            cur = conn.cursor()
            limit_value = int(sample_limit.value or 100)
            sql = (
                f"SELECT * FROM {DATABASE_NAME}.{SCHEMA_NAME}.{table_selector.value} "
                f"LIMIT {limit_value}"
            )
            cur.execute(sql)
            df_sample = cur.fetch_pandas_all()
        except Exception as e:
            mo.md(f"**サンプル取得エラー**: `{e}`")
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass
    return (df_sample,)


@app.cell
def _(df_sample, mo):
    _output = mo.md("*サンプルがありません*")
    if len(df_sample) > 0:
        _output = mo.ui.table(df_sample, pagination=True)
    _output
    return


if __name__ == "__main__":
    app.run()

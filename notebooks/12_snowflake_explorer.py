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
        # Snowflake Explorer

        Snowflakeの接続確認、DB/Schema/Table一覧、任意SQLの実行を行う
        **汎用エクスプローラ** です。

        - 接続情報は `.env` または `~/.snowflake/connections.toml` を利用
        - キーペア認証/パスワード認証のどちらにも対応
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
def _(get_snowflake_connection, mo, pd):
    df_context = pd.DataFrame()
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT CURRENT_USER() AS USER, CURRENT_ROLE() AS ROLE, "
            "CURRENT_WAREHOUSE() AS WAREHOUSE, CURRENT_DATABASE() AS DATABASE, "
            "CURRENT_SCHEMA() AS SCHEMA"
        )
        df_context = cur.fetch_pandas_all()
    except Exception as e:
        mo.md(f"**接続確認エラー**: `{e}`")
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
    return (df_context,)


@app.cell
def _(df_context, mo):
    _output = mo.md("*接続情報がありません*")
    if len(df_context) > 0:
        _output = mo.ui.table(df_context, pagination=False)
    _output
    return


@app.cell
def _(get_snowflake_connection, mo, pd):
    df_databases = pd.DataFrame()
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        cur.execute("SHOW DATABASES")
        df_databases = cur.fetch_pandas_all()
    except Exception as e:
        mo.md(f"**DB一覧取得エラー**: `{e}`")
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
    return (df_databases,)


@app.cell
def _(df_databases, mo):
    _output = mo.md("*データベースがありません*")
    if len(df_databases) > 0:
        _output = mo.ui.table(df_databases, pagination=True)
    _output
    return


@app.cell
def _(df_databases, mo):
    db_options = (
        df_databases["name"].dropna().sort_values().unique().tolist()
        if len(df_databases) > 0 and "name" in df_databases.columns
        else []
    )
    db_selector = mo.ui.dropdown(
        options=db_options,
        value=db_options[0] if db_options else None,
        label="Database",
    )
    db_selector
    return (db_selector,)


@app.cell
def _(db_selector, get_snowflake_connection, mo, pd):
    df_schemas = pd.DataFrame()
    if db_selector.value:
        try:
            conn = get_snowflake_connection()
            cur = conn.cursor()
            cur.execute(f"SHOW SCHEMAS IN DATABASE {db_selector.value}")
            df_schemas = cur.fetch_pandas_all()
        except Exception as e:
            mo.md(f"**Schema一覧取得エラー**: `{e}`")
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass
    return (df_schemas,)


@app.cell
def _(df_schemas, mo):
    _output = mo.md("*スキーマがありません*")
    if len(df_schemas) > 0:
        _output = mo.ui.table(df_schemas, pagination=True)
    _output
    return


@app.cell
def _(df_schemas, mo):
    schema_options = (
        df_schemas["name"].dropna().sort_values().unique().tolist()
        if len(df_schemas) > 0 and "name" in df_schemas.columns
        else []
    )
    schema_selector = mo.ui.dropdown(
        options=schema_options,
        value=schema_options[0] if schema_options else None,
        label="Schema",
    )
    schema_selector
    return (schema_selector,)


@app.cell
def _(db_selector, get_snowflake_connection, mo, pd, schema_selector):
    df_tables = pd.DataFrame()
    if db_selector.value and schema_selector.value:
        try:
            conn = get_snowflake_connection()
            cur = conn.cursor()
            cur.execute(
                f"SHOW TABLES IN {db_selector.value}.{schema_selector.value}"
            )
            df_tables = cur.fetch_pandas_all()
        except Exception as e:
            mo.md(f"**テーブル一覧取得エラー**: `{e}`")
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass
    return (df_tables,)


@app.cell
def _(df_tables, mo):
    _output = mo.md("*テーブルがありません*")
    if len(df_tables) > 0:
        _output = mo.ui.table(df_tables, pagination=True)
    _output
    return


@app.cell
def _(mo):
    mo.md("## 任意SQLの実行")
    return


@app.cell
def _(mo):
    default_sql = """SELECT CURRENT_USER() AS USER, CURRENT_ROLE() AS ROLE;"""
    sql_editor = mo.ui.code_editor(value=default_sql, language="sql", min_height=200)
    sql_editor
    return (sql_editor,)


@app.cell
def _(mo):
    run_button = mo.ui.run_button(label="クエリ実行")
    run_button
    return (run_button,)


@app.cell
def _(get_snowflake_connection, mo, pd, run_button, sql_editor):
    df_sql = None
    if run_button.value and sql_editor.value:
        try:
            conn = get_snowflake_connection()
            cur = conn.cursor()
            cur.execute(sql_editor.value)
            df_sql = cur.fetch_pandas_all()
            mo.md(f"**結果**: {len(df_sql)} 行")
        except Exception as e:
            mo.md(f"**クエリエラー**:\n```\n{e}\n```")
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass
    else:
        mo.md("*SQLを入力して「クエリ実行」をクリック*")
    return (df_sql,)


@app.cell
def _(df_sql, mo):
    _output = mo.md("*データがありません*")
    if df_sql is not None and len(df_sql) > 0:
        _output = mo.ui.table(df_sql, pagination=True)
    _output
    return


if __name__ == "__main__":
    app.run()

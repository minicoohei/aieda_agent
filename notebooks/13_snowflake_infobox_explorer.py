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
        # Snowflake Infobox Explorer

        `ETL_S3_TRANSALES_DB.TRANSALES_DAILY_SCHEMA` を中心に、
        **Org/Companyの対応** やテーブル概要を確認するノートブックです。
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
def _(mo):
    mo.md("## 1. スキーマ内テーブル一覧")
    return


@app.cell
def _(get_snowflake_connection, mo, pd):
    df_tables = pd.DataFrame()
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        cur.execute(
            "SHOW TABLES IN ETL_S3_TRANSALES_DB.TRANSALES_DAILY_SCHEMA"
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
    mo.md("## 2. Org / Company 対応")
    return


@app.cell
def _(mo):
    org_id_filter = mo.ui.text(label="org_id（任意）", value="")
    company_filter = mo.ui.text(label="会社名キーワード（任意）", value="")
    org_id_filter
    company_filter
    return company_filter, org_id_filter


@app.cell
def _(company_filter, get_snowflake_connection, mo, org_id_filter, pd):
    df_org_company = pd.DataFrame()
    conditions = ["u.ORGID IS NOT NULL"]
    if org_id_filter.value:
        conditions.append(f"u.ORGID = '{org_id_filter.value}'")
    if company_filter.value:
        conditions.append(f"bc.SHOGO ILIKE '%{company_filter.value}%'")

    where_clause = " AND ".join(conditions)

    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
                u.ORGID,
                u.NAME AS ORG_NAME,
                u.COMPANYID,
                bc.SHOGO AS COMPANY_NAME,
                bc.SHOGOFULL AS COMPANY_FULL_NAME
            FROM ETL_S3_TRANSALES_DB.TRANSALES_DAILY_SCHEMA.USERORGANIZATION u
            LEFT JOIN ETL_S3_TRANSALES_DB.TRANSALES_DAILY_SCHEMA.BEEGLECOMPANY bc
              ON u.COMPANYID = bc.ID
            WHERE {where_clause}
            ORDER BY u.ORGID
            LIMIT 500
            """
        )
        df_org_company = cur.fetch_pandas_all()
    except Exception as e:
        mo.md(f"**Org/Company取得エラー**: `{e}`")
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
    return (df_org_company,)


@app.cell
def _(df_org_company, mo):
    _output = mo.md("*データがありません*")
    if len(df_org_company) > 0:
        _output = mo.ui.table(df_org_company, pagination=True)
    _output
    return


@app.cell
def _(mo):
    mo.md("## 3. USERORGRELATION サンプル")
    return


@app.cell
def _(get_snowflake_connection, mo, pd):
    df_rel = pd.DataFrame()
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT *
            FROM ETL_S3_TRANSALES_DB.TRANSALES_DAILY_SCHEMA.USERORGRELATION
            LIMIT 200
            """
        )
        df_rel = cur.fetch_pandas_all()
    except Exception as e:
        mo.md(f"**USERORGRELATION取得エラー**: `{e}`")
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
    return (df_rel,)


@app.cell
def _(df_rel, mo):
    _output = mo.md("*データがありません*")
    if len(df_rel) > 0:
        _output = mo.ui.table(df_rel, pagination=True)
    _output
    return


@app.cell
def _(mo):
    mo.md("## 4. 任意SQLの実行")
    return


@app.cell
def _(mo):
    default_sql = """SELECT *\nFROM ETL_S3_TRANSALES_DB.TRANSALES_DAILY_SCHEMA.USERORGANIZATION\nLIMIT 100;"""
    sql_editor = mo.ui.code_editor(value=default_sql, language="sql", min_height=200)
    sql_editor
    return (sql_editor,)


@app.cell
def _(mo):
    run_button = mo.ui.run_button(label="クエリ実行")
    run_button
    return (run_button,)


@app.cell
def _(get_snowflake_connection, mo, run_button, sql_editor):
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

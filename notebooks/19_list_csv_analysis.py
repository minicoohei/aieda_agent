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
        # リスト登録・追加・CSVエクスポート分析

        Snowflakeの **リスト登録/追加** と **CSVエクスポート** を深掘りするノートブックです。
        主要テーブルのカラムを確認し、分析用のクエリを実行します。
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
    schema = "ETL_S3_TRANSALES_DB.TRANSALES_DAILY_SCHEMA"

    def run_query(sql: str) -> pd.DataFrame:
        df = pd.DataFrame()
        try:
            conn = get_snowflake_connection()
            cur = conn.cursor()
            cur.execute(sql)
            df = cur.fetch_pandas_all()
        except Exception as exc:
            mo.md(f"**クエリエラー**:\n```\n{exc}\n```")
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass
        return df

    def get_table_columns(table_name: str) -> pd.DataFrame:
        sql = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM ETL_S3_TRANSALES_DB.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'TRANSALES_DAILY_SCHEMA'
          AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
        return run_query(sql)

    def pick_default(columns: list[str], candidates: list[str]) -> str | None:
        for cand in candidates:
            if cand in columns:
                return cand
        return columns[0] if columns else None

    return get_table_columns, pick_default, run_query, schema


@app.cell
def _(mo):
    mo.md("## 1. カラム確認")
    return


@app.cell
def _(get_table_columns):
    list_tables = [
        "COMPANYLIST",
        "_BEEGLECOMPANYTOCOMPANYLIST",
        "PEOPLELIST",
        "_KEYMANTOPEOPLELIST",
        "CSVDOWNLOADLOG",
        "_BEEGLECOMPANYTOCSVDOWNLOADLOG",
        "CSVDOWNLOADQUOTAUPDATELOG",
        "BEEGLECOMPANY",
    ]
    column_map = {name: get_table_columns(name) for name in list_tables}
    return column_map, list_tables


@app.cell
def _(column_map, list_tables, mo):
    for name in list_tables:
        df_cols = column_map.get(name)
        mo.md(f"### {name}")
        if df_cols is not None and len(df_cols) > 0:
            mo.ui.table(df_cols, pagination=False)
        else:
            mo.md("*カラム情報がありません*")
    return


@app.cell
def _(column_map, mo, pick_default):
    companylist_columns = column_map["COMPANYLIST"]["COLUMN_NAME"].tolist()
    bcl_columns = column_map["_BEEGLECOMPANYTOCOMPANYLIST"]["COLUMN_NAME"].tolist()
    beegle_columns = column_map["BEEGLECOMPANY"]["COLUMN_NAME"].tolist()
    csv_columns = column_map["CSVDOWNLOADLOG"]["COLUMN_NAME"].tolist()
    bcdl_columns = column_map["_BEEGLECOMPANYTOCSVDOWNLOADLOG"]["COLUMN_NAME"].tolist()

    companylist_id_col = mo.ui.dropdown(
        options=companylist_columns,
        value=pick_default(companylist_columns, ["ID", "COMPANYLISTID", "LISTID"]),
        label="COMPANYLIST: ID列",
    )
    companylist_creator_col = mo.ui.dropdown(
        options=companylist_columns,
        value=pick_default(
            companylist_columns, ["USERID", "CREATEDBY", "CREATEDBYID", "CREATEDUSERID"]
        ),
        label="COMPANYLIST: 作成者列",
    )
    companylist_created_col = mo.ui.dropdown(
        options=companylist_columns,
        value=pick_default(companylist_columns, ["CREATEDAT", "CREATED_AT"]),
        label="COMPANYLIST: 作成日時列",
    )

    list_link_company_col = mo.ui.dropdown(
        options=bcl_columns,
        value=pick_default(
            bcl_columns, ["B", "A", "BEEGLECOMPANYID", "COMPANYID"]
        ),
        label="_BEEGLECOMPANYTOCOMPANYLIST: 企業ID列",
    )
    list_link_list_col = mo.ui.dropdown(
        options=bcl_columns,
        value=pick_default(bcl_columns, ["A", "B", "COMPANYLISTID"]),
        label="_BEEGLECOMPANYTOCOMPANYLIST: リストID列",
    )
    list_link_created_col = mo.ui.dropdown(
        options=bcl_columns,
        value=pick_default(bcl_columns, ["CREATEDAT", "CREATED_AT", "CREATED"]),
        label="_BEEGLECOMPANYTOCOMPANYLIST: 追加日時列",
    )

    company_id_col = mo.ui.dropdown(
        options=beegle_columns,
        value=pick_default(beegle_columns, ["ID", "COMPANYID"]),
        label="BEEGLECOMPANY: ID列",
    )
    company_name_col = mo.ui.dropdown(
        options=beegle_columns,
        value=pick_default(beegle_columns, ["SHOGO", "SHOGOFULL", "NAME"]),
        label="BEEGLECOMPANY: 表示名列",
    )

    csv_log_id_col = mo.ui.dropdown(
        options=csv_columns,
        value=pick_default(csv_columns, ["ID", "CSVDOWNLOADLOGID"]),
        label="CSVDOWNLOADLOG: ID列",
    )
    csv_log_user_col = mo.ui.dropdown(
        options=csv_columns,
        value=pick_default(csv_columns, ["USERID", "CREATEDBY", "CREATEDBYID"]),
        label="CSVDOWNLOADLOG: ユーザー列",
    )
    csv_log_created_col = mo.ui.dropdown(
        options=csv_columns,
        value=pick_default(csv_columns, ["CREATEDAT", "CREATED_AT"]),
        label="CSVDOWNLOADLOG: 作成日時列",
    )
    csv_link_company_col = mo.ui.dropdown(
        options=bcdl_columns,
        value=pick_default(
            bcdl_columns, ["B", "A", "BEEGLECOMPANYID", "COMPANYID"]
        ),
        label="_BEEGLECOMPANYTOCSVDOWNLOADLOG: 企業ID列",
    )
    csv_link_log_col = mo.ui.dropdown(
        options=bcdl_columns,
        value=pick_default(bcdl_columns, ["A", "B", "CSVDOWNLOADLOGID"]),
        label="_BEEGLECOMPANYTOCSVDOWNLOADLOG: ログID列",
    )

    return (
        company_id_col,
        company_name_col,
        companylist_created_col,
        companylist_creator_col,
        companylist_id_col,
        csv_link_company_col,
        csv_link_log_col,
        csv_log_created_col,
        csv_log_id_col,
        csv_log_user_col,
        list_link_company_col,
        list_link_created_col,
        list_link_list_col,
    )


@app.cell
def _(mo):
    mo.md("## 2. リスト登録分析")
    return


@app.cell
def _(mo):
    list_run_button = mo.ui.run_button(label="リスト登録分析を実行")
    list_run_button
    return (list_run_button,)


@app.cell
def _(
    companylist_created_col,
    companylist_creator_col,
    companylist_id_col,
    list_run_button,
    mo,
    run_query,
    schema,
):
    df_list_creators = None
    df_list_timeseries = None
    df_list_size = None

    if list_run_button.value:
        if companylist_creator_col.value:
            creator_sql = f"""
            SELECT {companylist_creator_col.value} AS creator_id,
                   COUNT(*) AS list_count
            FROM {schema}.COMPANYLIST
            WHERE {companylist_creator_col.value} IS NOT NULL
            GROUP BY creator_id
            ORDER BY list_count DESC
            LIMIT 100
            """
            df_list_creators = run_query(creator_sql)

        if companylist_created_col.value:
            list_timeseries_sql = f"""
            SELECT DATE_TRUNC('month', {companylist_created_col.value}) AS month,
                   COUNT(*) AS list_count
            FROM {schema}.COMPANYLIST
            WHERE {companylist_created_col.value} IS NOT NULL
            GROUP BY month
            ORDER BY month
            """
            df_list_timeseries = run_query(list_timeseries_sql)

        if companylist_id_col.value:
            size_sql = f"""
            SELECT {companylist_id_col.value} AS list_id,
                   COUNT(*) AS company_count
            FROM {schema}._BEEGLECOMPANYTOCOMPANYLIST
            GROUP BY list_id
            ORDER BY company_count DESC
            """
            df_list_size = run_query(size_sql)

    if df_list_creators is None and df_list_timeseries is None and df_list_size is None:
        mo.md("*列の選択後に実行してください*")
    return df_list_creators, df_list_size, df_list_timeseries


@app.cell
def _(df_list_creators, mo):
    mo.md("### 作成者ランキング")
    if df_list_creators is not None and len(df_list_creators) > 0:
        mo.ui.table(df_list_creators, pagination=True)
    else:
        mo.md("*データがありません*")
    return


@app.cell
def _(df_list_timeseries, mo):
    mo.md("### リスト作成の時系列")
    if df_list_timeseries is not None and len(df_list_timeseries) > 0:
        mo.ui.table(df_list_timeseries, pagination=True)
    else:
        mo.md("*データがありません*")
    return


@app.cell
def _(df_list_size, mo):
    mo.md("### リストの企業数分布")
    if df_list_size is not None and len(df_list_size) > 0:
        mo.ui.table(df_list_size, pagination=True)
    else:
        mo.md("*データがありません*")
    return


@app.cell
def _(mo):
    mo.md("## 3. リスト追加分析（深堀り）")
    return


@app.cell
def _(mo):
    list_add_button = mo.ui.run_button(label="リスト追加分析を実行")
    list_add_button
    return (list_add_button,)


@app.cell
def _(
    company_id_col,
    company_name_col,
    companylist_creator_col,
    companylist_id_col,
    list_add_button,
    list_link_company_col,
    list_link_created_col,
    list_link_list_col,
    mo,
    run_query,
    schema,
):
    df_company_list_freq = None
    df_add_timeseries = None
    df_creator_add = None
    df_multi_list_ratio = None

    if list_add_button.value:
        if list_link_company_col.value and company_id_col.value and company_name_col.value:
            company_freq_sql = f"""
            SELECT bc.{company_name_col.value} AS company_name,
                   COUNT(DISTINCT bcl.{list_link_list_col.value}) AS list_count
            FROM {schema}._BEEGLECOMPANYTOCOMPANYLIST bcl
            JOIN {schema}.BEEGLECOMPANY bc
              ON bcl.{list_link_company_col.value} = bc.{company_id_col.value}
            GROUP BY company_name
            ORDER BY list_count DESC
            LIMIT 100
            """
            df_company_list_freq = run_query(company_freq_sql)

        if list_link_created_col.value:
            add_time_sql = f"""
            SELECT DATE_TRUNC('month', {list_link_created_col.value}) AS month,
                   COUNT(*) AS add_count
            FROM {schema}._BEEGLECOMPANYTOCOMPANYLIST
            WHERE {list_link_created_col.value} IS NOT NULL
            GROUP BY month
            ORDER BY month
            """
            df_add_timeseries = run_query(add_time_sql)

        if companylist_creator_col.value and companylist_id_col.value:
            creator_add_sql = f"""
            SELECT cl.{companylist_creator_col.value} AS creator_id,
                   COUNT(*) AS add_count
            FROM {schema}._BEEGLECOMPANYTOCOMPANYLIST bcl
            JOIN {schema}.COMPANYLIST cl
              ON bcl.{list_link_list_col.value} = cl.{companylist_id_col.value}
            WHERE cl.{companylist_creator_col.value} IS NOT NULL
            GROUP BY creator_id
            ORDER BY add_count DESC
            LIMIT 100
            """
            df_creator_add = run_query(creator_add_sql)

        if list_link_company_col.value and list_link_list_col.value:
            multi_list_sql = f"""
            WITH company_counts AS (
              SELECT {list_link_company_col.value} AS company_id,
                     COUNT(DISTINCT {list_link_list_col.value}) AS list_count
              FROM {schema}._BEEGLECOMPANYTOCOMPANYLIST
              GROUP BY company_id
            )
            SELECT
              COUNT(*) AS company_total,
              SUM(IFF(list_count > 1, 1, 0)) AS multi_list_companies
            FROM company_counts
            """
            df_multi_list_ratio = run_query(multi_list_sql)

    if (
        df_company_list_freq is None
        and df_add_timeseries is None
        and df_creator_add is None
        and df_multi_list_ratio is None
    ):
        mo.md("*列の選択後に実行してください*")
    return (
        df_add_timeseries,
        df_company_list_freq,
        df_creator_add,
        df_multi_list_ratio,
    )


@app.cell
def _(df_company_list_freq, mo):
    mo.md("### 企業のリスト追加頻度")
    if df_company_list_freq is not None and len(df_company_list_freq) > 0:
        mo.ui.table(df_company_list_freq, pagination=True)
    else:
        mo.md("*データがありません*")
    return


@app.cell
def _(df_add_timeseries, mo):
    mo.md("### リスト追加の時系列")
    if df_add_timeseries is not None and len(df_add_timeseries) > 0:
        mo.ui.table(df_add_timeseries, pagination=True)
    else:
        mo.md("*データがありません*")
    return


@app.cell
def _(df_creator_add, mo):
    mo.md("### リスト追加のユーザー別パターン")
    if df_creator_add is not None and len(df_creator_add) > 0:
        mo.ui.table(df_creator_add, pagination=True)
    else:
        mo.md("*データがありません*")
    return


@app.cell
def _(df_multi_list_ratio, mo):
    mo.md("### 同一企業が複数リストに入っている割合")
    if df_multi_list_ratio is not None and len(df_multi_list_ratio) > 0:
        mo.ui.table(df_multi_list_ratio, pagination=False)
    else:
        mo.md("*データがありません*")
    return


@app.cell
def _(mo):
    mo.md("## 4. CSVエクスポート分析（深堀り）")
    return


@app.cell
def _(mo):
    csv_run_button = mo.ui.run_button(label="CSVエクスポート分析を実行")
    csv_run_button
    return (csv_run_button,)


@app.cell
def _(
    company_id_col,
    company_name_col,
    csv_link_company_col,
    csv_link_log_col,
    csv_log_created_col,
    csv_log_id_col,
    csv_log_user_col,
    csv_run_button,
    mo,
    run_query,
    schema,
):
    df_csv_users = None
    df_csv_timeseries = None
    df_csv_companies = None
    df_csv_hours = None
    df_csv_quota = None

    if csv_run_button.value:
        if csv_log_user_col.value:
            user_sql = f"""
            SELECT {csv_log_user_col.value} AS user_id,
                   COUNT(*) AS download_count
            FROM {schema}.CSVDOWNLOADLOG
            WHERE {csv_log_user_col.value} IS NOT NULL
            GROUP BY user_id
            ORDER BY download_count DESC
            LIMIT 100
            """
            df_csv_users = run_query(user_sql)

        if csv_log_created_col.value:
            csv_timeseries_sql = f"""
            SELECT DATE_TRUNC('day', {csv_log_created_col.value}) AS day,
                   COUNT(*) AS download_count
            FROM {schema}.CSVDOWNLOADLOG
            WHERE {csv_log_created_col.value} IS NOT NULL
            GROUP BY day
            ORDER BY day
            """
            df_csv_timeseries = run_query(csv_timeseries_sql)

            hour_sql = f"""
            SELECT DATE_PART('hour', {csv_log_created_col.value}) AS hour,
                   COUNT(*) AS download_count
            FROM {schema}.CSVDOWNLOADLOG
            WHERE {csv_log_created_col.value} IS NOT NULL
            GROUP BY hour
            ORDER BY hour
            """
            df_csv_hours = run_query(hour_sql)

        if (
            csv_link_company_col.value
            and csv_link_log_col.value
            and csv_log_id_col.value
            and company_id_col.value
            and company_name_col.value
        ):
            company_sql = f"""
            SELECT bc.{company_name_col.value} AS company_name,
                   COUNT(*) AS download_count
            FROM {schema}._BEEGLECOMPANYTOCSVDOWNLOADLOG bcdl
            JOIN {schema}.CSVDOWNLOADLOG dl
              ON bcdl.{csv_link_log_col.value} = dl.{csv_log_id_col.value}
            JOIN {schema}.BEEGLECOMPANY bc
              ON bcdl.{csv_link_company_col.value} = bc.{company_id_col.value}
            GROUP BY company_name
            ORDER BY download_count DESC
            LIMIT 100
            """
            df_csv_companies = run_query(company_sql)

        df_csv_quota = run_query(
            f"""
            SELECT *
            FROM {schema}.CSVDOWNLOADQUOTAUPDATELOG
            ORDER BY UPDATEDAT DESC
            LIMIT 200
            """
        )

    if (
        df_csv_users is None
        and df_csv_timeseries is None
        and df_csv_companies is None
        and df_csv_hours is None
    ):
        mo.md("*列の選択後に実行してください*")
    return (
        df_csv_companies,
        df_csv_hours,
        df_csv_quota,
        df_csv_timeseries,
        df_csv_users,
    )


@app.cell
def _(df_csv_users, mo):
    mo.md("### ダウンロードユーザーランキング")
    if df_csv_users is not None and len(df_csv_users) > 0:
        mo.ui.table(df_csv_users, pagination=True)
    else:
        mo.md("*データがありません*")
    return


@app.cell
def _(df_csv_timeseries, mo):
    mo.md("### ダウンロード頻度の時系列")
    if df_csv_timeseries is not None and len(df_csv_timeseries) > 0:
        mo.ui.table(df_csv_timeseries, pagination=True)
    else:
        mo.md("*データがありません*")
    return


@app.cell
def _(df_csv_companies, mo):
    mo.md("### ダウンロードされる企業TOP")
    if df_csv_companies is not None and len(df_csv_companies) > 0:
        mo.ui.table(df_csv_companies, pagination=True)
    else:
        mo.md("*データがありません*")
    return


@app.cell
def _(df_csv_hours, mo):
    mo.md("### ダウンロードの時間帯分布")
    if df_csv_hours is not None and len(df_csv_hours) > 0:
        mo.ui.table(df_csv_hours, pagination=False)
    else:
        mo.md("*データがありません*")
    return


@app.cell
def _(df_csv_quota, mo):
    mo.md("### クォータ更新ログ")
    if df_csv_quota is not None and len(df_csv_quota) > 0:
        mo.ui.table(df_csv_quota, pagination=True)
    else:
        mo.md("*データがありません*")
    return


@app.cell
def _(mo):
    mo.md("## 5. クロス分析")
    return


@app.cell
def _(mo):
    cross_button = mo.ui.run_button(label="クロス分析を実行")
    cross_button
    return (cross_button,)


@app.cell
def _(
    companylist_creator_col,
    cross_button,
    csv_link_company_col,
    csv_log_user_col,
    list_link_company_col,
    mo,
    run_query,
    schema,
):
    df_company_overlap = None
    df_user_overlap = None

    if cross_button.value:
        if list_link_company_col.value and csv_link_company_col.value:
            company_overlap_sql = f"""
            WITH list_companies AS (
              SELECT DISTINCT {list_link_company_col.value} AS company_id
              FROM {schema}._BEEGLECOMPANYTOCOMPANYLIST
            ),
            download_companies AS (
              SELECT DISTINCT {csv_link_company_col.value} AS company_id
              FROM {schema}._BEEGLECOMPANYTOCSVDOWNLOADLOG
            )
            SELECT
              (SELECT COUNT(*) FROM list_companies) AS list_company_count,
              (SELECT COUNT(*) FROM download_companies) AS download_company_count,
              (SELECT COUNT(*)
               FROM list_companies lc
               JOIN download_companies dc ON lc.company_id = dc.company_id) AS overlap_count
            """
            df_company_overlap = run_query(company_overlap_sql)

        if companylist_creator_col.value and csv_log_user_col.value:
            user_overlap_sql = f"""
            WITH list_users AS (
              SELECT DISTINCT {companylist_creator_col.value} AS user_id
              FROM {schema}.COMPANYLIST
              WHERE {companylist_creator_col.value} IS NOT NULL
            ),
            download_users AS (
              SELECT DISTINCT {csv_log_user_col.value} AS user_id
              FROM {schema}.CSVDOWNLOADLOG
              WHERE {csv_log_user_col.value} IS NOT NULL
            )
            SELECT
              (SELECT COUNT(*) FROM list_users) AS list_user_count,
              (SELECT COUNT(*) FROM download_users) AS download_user_count,
              (SELECT COUNT(*)
               FROM list_users lu
               JOIN download_users du ON lu.user_id = du.user_id) AS overlap_count
            """
            df_user_overlap = run_query(user_overlap_sql)

    if df_company_overlap is None and df_user_overlap is None:
        mo.md("*列の選択後に実行してください*")
    return df_company_overlap, df_user_overlap


@app.cell
def _(df_company_overlap, mo):
    mo.md("### 企業の重複率")
    if df_company_overlap is not None and len(df_company_overlap) > 0:
        mo.ui.table(df_company_overlap, pagination=False)
    else:
        mo.md("*データがありません*")
    return


@app.cell
def _(df_user_overlap, mo):
    mo.md("### アクティブユーザー重複")
    if df_user_overlap is not None and len(df_user_overlap) > 0:
        mo.ui.table(df_user_overlap, pagination=False)
    else:
        mo.md("*データがありません*")
    return


@app.cell
def _(mo):
    mo.md("## 6. ユーザー単位のリスト→CSV紐づけ")
    return


@app.cell
def _(mo):
    user_link_button = mo.ui.run_button(label="ユーザー単位の紐づけを実行")
    user_link_button
    return (user_link_button,)


@app.cell
def _(
    companylist_creator_col,
    companylist_id_col,
    csv_link_company_col,
    csv_link_log_col,
    csv_log_id_col,
    csv_log_user_col,
    list_link_company_col,
    list_link_list_col,
    mo,
    run_query,
    schema,
    user_link_button,
):
    df_user_link = None

    if user_link_button.value:
        if (
            companylist_creator_col.value
            and companylist_id_col.value
            and list_link_company_col.value
            and list_link_list_col.value
            and csv_log_user_col.value
            and csv_log_id_col.value
            and csv_link_company_col.value
            and csv_link_log_col.value
        ):
            user_link_sql = f"""
            WITH list_user_companies AS (
              SELECT
                cl.{companylist_creator_col.value} AS user_id,
                bcl.{list_link_company_col.value} AS company_id
              FROM {schema}.COMPANYLIST cl
              JOIN {schema}._BEEGLECOMPANYTOCOMPANYLIST bcl
                ON bcl.{list_link_list_col.value} = cl.{companylist_id_col.value}
              WHERE cl.{companylist_creator_col.value} IS NOT NULL
                AND bcl.{list_link_company_col.value} IS NOT NULL
              GROUP BY user_id, company_id
            ),
            csv_user_companies AS (
              SELECT
                dl.{csv_log_user_col.value} AS user_id,
                bcdl.{csv_link_company_col.value} AS company_id
              FROM {schema}.CSVDOWNLOADLOG dl
              JOIN {schema}._BEEGLECOMPANYTOCSVDOWNLOADLOG bcdl
                ON bcdl.{csv_link_log_col.value} = dl.{csv_log_id_col.value}
              WHERE dl.{csv_log_user_col.value} IS NOT NULL
                AND bcdl.{csv_link_company_col.value} IS NOT NULL
              GROUP BY user_id, company_id
            ),
            list_counts AS (
              SELECT user_id, COUNT(*) AS list_companies
              FROM list_user_companies
              GROUP BY user_id
            ),
            csv_counts AS (
              SELECT user_id, COUNT(*) AS csv_companies
              FROM csv_user_companies
              GROUP BY user_id
            ),
            overlap_counts AS (
              SELECT
                l.user_id AS user_id,
                COUNT(*) AS overlap_companies
              FROM list_user_companies l
              JOIN csv_user_companies c
                ON l.user_id = c.user_id
               AND l.company_id = c.company_id
              GROUP BY l.user_id
            )
            SELECT
              COALESCE(l.user_id, c.user_id) AS user_id,
              COALESCE(l.list_companies, 0) AS list_companies,
              COALESCE(c.csv_companies, 0) AS csv_companies,
              COALESCE(o.overlap_companies, 0) AS overlap_companies,
              IFF(COALESCE(l.list_companies, 0) = 0,
                  0,
                  COALESCE(o.overlap_companies, 0) / COALESCE(l.list_companies, 1)
              ) AS overlap_ratio
            FROM list_counts l
            FULL OUTER JOIN csv_counts c
              ON l.user_id = c.user_id
            LEFT JOIN overlap_counts o
              ON COALESCE(l.user_id, c.user_id) = o.user_id
            ORDER BY overlap_ratio DESC, overlap_companies DESC
            LIMIT 200
            """
            df_user_link = run_query(user_link_sql)
        else:
            mo.md("*列の選択後に実行してください*")

    return (df_user_link,)


@app.cell
def _(df_user_link, mo):
    mo.md("### ユーザー別のリスト→CSV重複率")
    if df_user_link is not None and len(df_user_link) > 0:
        mo.ui.table(df_user_link, pagination=True)
    else:
        mo.md("*データがありません*")
    return


if __name__ == "__main__":
    app.run()

import marimo

__generated_with = "0.17.8"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md("""
    # GA4 org_id × Snowflake 組織/企業名マッピング

    **目的**: GA4の`org_id`とSnowflake側の組織・企業名を紐づけて一覧化し、
    利用実績の差分を探索的に可視化します。

    ## データソース
    - GA4: `analytics_400693944`
    - Snowflake: `ETL_S3_TRANSALES_DB.TRANSALES_DAILY_SCHEMA`

    ## 期待するキー
    - `USERORGANIZATION.ORGID` ⇔ `GA4 user_properties.org_id`
    """)
    return


@app.cell
def _():
    import os
    import tomllib
    from pathlib import Path

    import pandas as pd
    import plotly.express as px
    import snowflake.connector
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization
    from google.cloud import bigquery
    from google.oauth2 import service_account
    return (
        Path,
        bigquery,
        default_backend,
        os,
        pd,
        px,
        serialization,
        service_account,
        snowflake,
        tomllib,
    )


@app.cell
def _(bigquery, os, pd, service_account):
    # BigQuery接続（Storage Read API不使用）
    key_path = os.path.expanduser("~/.gcp/gree-dionysus-infobox.json")
    project_id = "gree-dionysus-infobox"
    dataset_id = "analytics_400693944"

    credentials_auth = service_account.Credentials.from_service_account_file(key_path)
    client_bq = bigquery.Client(project=project_id, credentials=credentials_auth)

    def query_bq_to_df(sql: str) -> pd.DataFrame:
        job = client_bq.query(sql)
        results = job.result()
        rows = [dict(row) for row in results]
        return pd.DataFrame(rows)
    return dataset_id, query_bq_to_df


@app.cell
def _(Path, default_backend, os, serialization, snowflake, tomllib):
    # Snowflake接続（connections.toml優先）
    def get_snowflake_connection():
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
    mo.md("""
    ## 1. GA4 org_id（全量）
    """)
    return


@app.cell
def _(dataset_id, mo, query_bq_to_df):
    # GA4 org_id 全量取得
    ga_org_sql = f"""
    SELECT DISTINCT up.value.string_value AS org_id
    FROM `{dataset_id}.events_*`, UNNEST(user_properties) AS up
    WHERE up.key = 'org_id'
      AND up.value.string_value IS NOT NULL
    """
    df_ga_org = query_bq_to_df(ga_org_sql)
    mo.md(f"**GA4 org_id 件数（全量）**: {len(df_ga_org):,}")
    return (df_ga_org,)


@app.cell
def _(df_ga_org, mo):
    _output = mo.md("*データがありません*")
    if len(df_ga_org) > 0:
        _output = mo.ui.table(df_ga_org, pagination=True)
    _output
    return


@app.cell
def _(mo):
    mo.md("""
    ## 1-2. GA4 主要集計（全量）
    """)
    return


@app.cell
def _(dataset_id, mo, query_bq_to_df):
    ga_metrics_sql = f"""
    WITH base AS (
        SELECT
            user_pseudo_id,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
            (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'org_id') AS org_id,
            event_name,
            event_date
        FROM `{dataset_id}.events_*`
        WHERE NOT STARTS_WITH(_TABLE_SUFFIX, 'intraday_')
    )
    SELECT
        org_id,
        COUNT(DISTINCT user_pseudo_id) AS users,
        COUNT(DISTINCT CONCAT(user_pseudo_id, '-', CAST(ga_session_id AS STRING))) AS sessions,
        COUNTIF(event_name = 'page_view') AS page_views,
        COUNTIF(event_name = 'first_visit') AS new_users,
        MIN(event_date) AS first_date,
        MAX(event_date) AS last_date
    FROM base
    WHERE org_id IS NOT NULL
    GROUP BY org_id
    ORDER BY users DESC
    """
    df_ga_metrics = query_bq_to_df(ga_metrics_sql)
    mo.md(f"**GA4 主要集計件数（org_id単位）**: {len(df_ga_metrics):,}")
    return (df_ga_metrics,)


@app.cell
def _(df_ga_metrics, mo):
    _output = mo.md("*データがありません*")
    if len(df_ga_metrics) > 0:
        _output = mo.ui.table(df_ga_metrics, pagination=True)
    _output
    return


@app.cell
def _(mo):
    mo.md("""
    ## 2. Snowflake 側のORG/COMPANY探索
    """)
    return


@app.cell
def _(get_snowflake_connection, mo, pd):
    def fetch_snowflake_org_company():
        df_org = pd.DataFrame()

        sf_conn = get_snowflake_connection()
        sf_cur = sf_conn.cursor()
        try:
            # USERORGANIZATION + BEEGLECOMPANY を結合して必要列のみ取得
            sf_cur.execute(
                """
                SELECT
                    u.ORGID,
                    u.NAME AS ORG_NAME,
                    u.COMPANYID,
                    bc.SHOGO AS COMPANY_NAME
                FROM ETL_S3_TRANSALES_DB.TRANSALES_DAILY_SCHEMA.USERORGANIZATION u
                LEFT JOIN ETL_S3_TRANSALES_DB.TRANSALES_DAILY_SCHEMA.BEEGLECOMPANY bc
                  ON u.COMPANYID = bc.ID
                WHERE u.ORGID IS NOT NULL
                """
            )
            df_org = sf_cur.fetch_pandas_all()
        finally:
            sf_cur.close()
            sf_conn.close()

        return df_org

    try:
        df_sf_org = fetch_snowflake_org_company()
    except Exception as e:
        df_sf_org = pd.DataFrame()
        mo.md(f"**Snowflake接続エラー**: {e}")

    if len(df_sf_org) > 0:
        mo.md(f"**USERORGANIZATION 件数**: {len(df_sf_org):,}")
    return (df_sf_org,)


@app.cell
def _(df_sf_org, mo):
    _output = mo.md("*データがありません*")
    if len(df_sf_org) > 0:
        _output = mo.ui.table(df_sf_org, pagination=True)
    _output
    return


@app.cell
def _(mo):
    mo.md("""
    ## 3. GA4 org_id × Snowflake 結合
    """)
    return


@app.cell
def _(df_ga_metrics, df_ga_org, df_sf_org, mo, pd):
    df_mapping = pd.DataFrame()
    match_rate = None

    if len(df_ga_org) > 0 and len(df_sf_org) > 0:
        df_mapping = df_ga_org.merge(
            df_sf_org, left_on="org_id", right_on="ORGID", how="left"
        )
        if len(df_ga_metrics) > 0:
            df_mapping = df_mapping.merge(
                df_ga_metrics,
                on="org_id",
                how="left",
            )

        if "users" in df_mapping.columns and "sessions" in df_mapping.columns:
            df_mapping["sessions_per_user"] = (
                df_mapping["sessions"] / df_mapping["users"]
            )
        if "page_views" in df_mapping.columns and "users" in df_mapping.columns:
            df_mapping["page_views_per_user"] = (
                df_mapping["page_views"] / df_mapping["users"]
            )
        if "page_views" in df_mapping.columns and "sessions" in df_mapping.columns:
            df_mapping["page_views_per_session"] = (
                df_mapping["page_views"] / df_mapping["sessions"]
            )

        total = len(df_mapping)
        matched = df_mapping["ORG_NAME"].notna().sum()
        match_rate = matched / total * 100 if total else None
        mo.md(f"**マッチ率**: {matched}/{total} = {match_rate:.2f}%")
    return (df_mapping,)


@app.cell
def _(df_mapping, mo):
    _output = mo.md("*データがありません*")
    if len(df_mapping) > 0:
        _output = mo.ui.table(df_mapping, pagination=True)
    _output
    return


@app.cell
def _(mo):
    mo.md("""
    ## 4. 会社別来訪状況・利用頻度
    """)
    return


@app.cell
def _(df_mapping, mo):
    _output = mo.md("*データがありません*")
    if len(df_mapping) > 0:
        cols = [
            "org_id",
            "ORG_NAME",
            "COMPANY_NAME",
            "users",
            "sessions",
            "page_views",
            "new_users",
            "sessions_per_user",
            "page_views_per_user",
            "page_views_per_session",
            "first_date",
            "last_date",
        ]
        display_cols = [c for c in cols if c in df_mapping.columns]
        _output = mo.ui.table(df_mapping[display_cols], pagination=True)
    _output
    return


@app.cell
def _(mo):
    mo.md("""
    ## 5. ページ閲覧（全体）
    """)
    return


@app.cell
def _(dataset_id, query_bq_to_df):
    ga_pages_sql = f"""
    SELECT
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title') AS page_title,
        COUNT(*) AS page_views,
        COUNT(DISTINCT user_pseudo_id) AS users,
        COUNT(DISTINCT CONCAT(user_pseudo_id, '-', CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))) AS sessions
    FROM `{dataset_id}.events_*`
    WHERE NOT STARTS_WITH(_TABLE_SUFFIX, 'intraday_')
      AND event_name = 'page_view'
    GROUP BY page_location, page_title
    ORDER BY page_views DESC
    LIMIT 100
    """
    df_ga_pages = query_bq_to_df(ga_pages_sql)
    return (df_ga_pages,)


@app.cell
def _(df_ga_pages, mo):
    _output = mo.md("*データがありません*")
    if len(df_ga_pages) > 0:
        _output = mo.ui.table(df_ga_pages, pagination=True)
    _output
    return


@app.cell
def _(mo):
    mo.md("""
    ## 6. ページ閲覧（会社別）
    """)
    return


@app.cell
def _(df_mapping, mo):
    org_list = []
    if len(df_mapping) > 0:
        org_list = (
            df_mapping[["org_id", "ORG_NAME"]]
            .dropna()
            .drop_duplicates()
            .sort_values("org_id")
            .apply(lambda r: f"{r['org_id']} | {r['ORG_NAME']}", axis=1)
            .tolist()
        )
    org_selector = mo.ui.dropdown(
        options=org_list,
        value=org_list[0] if org_list else None,
        label="org_id を選択（会社別ページ閲覧）",
    )
    org_selector
    return (org_selector,)


@app.cell
def _(dataset_id, mo, org_selector, query_bq_to_df):
    df_ga_org_pages = None
    if org_selector.value:
        org_id = org_selector.value.split(" | ")[0]
        ga_org_pages_sql = f"""
        SELECT
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title') AS page_title,
            COUNT(*) AS page_views,
            COUNT(DISTINCT user_pseudo_id) AS users,
            COUNT(DISTINCT CONCAT(user_pseudo_id, '-', CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING))) AS sessions
        FROM `{dataset_id}.events_*`
        WHERE NOT STARTS_WITH(_TABLE_SUFFIX, 'intraday_')
          AND event_name = 'page_view'
          AND (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'org_id') = '{org_id}'
        GROUP BY page_location, page_title
        ORDER BY page_views DESC
        LIMIT 100
        """
        df_ga_org_pages = query_bq_to_df(ga_org_pages_sql)
        mo.md(f"**org_id**: `{org_id}`")
    return (df_ga_org_pages,)


@app.cell
def _(df_ga_org_pages, mo):
    _output = mo.md("*データがありません*")
    if df_ga_org_pages is not None and len(df_ga_org_pages) > 0:
        _output = mo.ui.table(df_ga_org_pages, pagination=True)
    _output
    return


@app.cell
def _(mo):
    mo.md("""
    ## 7. 探索図解（企業群の差）
    """)
    return


@app.cell
def _(df_ga_pages, mo):
    df_top_pages = (
        df_ga_pages.dropna(subset=["page_location"]).head(20).copy()
        if len(df_ga_pages) > 0
        else df_ga_pages
    )
    if len(df_top_pages) > 0:
        _output = mo.ui.table(df_top_pages, pagination=True)
    else:
        _output = mo.md("*上位ページがありません*")
    _output
    return


app._unparsable_cell(
    r"""
    if len(df_top_pages) == 0:
        return (None,)

    def escape_bq_string(value: str) -> str:
        return value.replace(\"'\", \"''\")

    page_structs = \",\n        \".join(
        [
            f\"STRUCT('{escape_bq_string(p)}' AS page_location)\"
            for p in df_top_pages[\"page_location\"].tolist()
        ]
    )

    ga_org_page_sql = f\"\"\"
    WITH top_pages AS (
        SELECT * FROM UNNEST([
        {page_structs}
        ])
    ),
    base AS (
        SELECT
            user_pseudo_id,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
            (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'org_id') AS org_id,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location
        FROM `{dataset_id}.events_*`
        WHERE NOT STARTS_WITH(_TABLE_SUFFIX, 'intraday_')
          AND event_name = 'page_view'
    )
    SELECT
        b.org_id,
        b.page_location,
        COUNT(*) AS page_views,
        COUNT(DISTINCT b.user_pseudo_id) AS users,
        COUNT(DISTINCT CONCAT(b.user_pseudo_id, '-', CAST(b.ga_session_id AS STRING))) AS sessions
    FROM base b
    JOIN top_pages t
      ON b.page_location = t.page_location
    WHERE b.org_id IS NOT NULL
    GROUP BY b.org_id, b.page_location
    \"\"\"
    df_org_page_counts = query_bq_to_df(ga_org_page_sql)
    """,
    name="_"
)


@app.cell
def _(df_mapping, df_org_page_counts, pd):
    df_features = df_mapping.copy() if len(df_mapping) > 0 else pd.DataFrame()

    if df_org_page_counts is not None and len(df_org_page_counts) > 0:
        pivot_counts = df_org_page_counts.pivot_table(
            index="org_id",
            columns="page_location",
            values="page_views",
            fill_value=0,
        )
        df_counts = pivot_counts.add_prefix("cnt_")
        df_dummies = (pivot_counts > 0).astype(int).add_prefix("has_")

        df_features = df_features.merge(
            df_counts, left_on="org_id", right_index=True, how="left"
        ).merge(df_dummies, left_on="org_id", right_index=True, how="left")

        for col in df_features.columns:
            if col.startswith(("cnt_", "has_")):
                df_features[col] = df_features[col].fillna(0)
    return (df_features,)


@app.cell
def _(mo):
    good_percentile = mo.ui.slider(
        start=50, stop=95, step=5, value=80, label="優秀企業群の上位%（利用度）"
    )
    good_percentile
    return (good_percentile,)


@app.cell
def _(df_features, good_percentile, mo, pd):
    df_scored = pd.DataFrame()
    if len(df_features) > 0 and "page_views_per_user" in df_features.columns:
        threshold = df_features["page_views_per_user"].quantile(
            good_percentile.value / 100
        )
        df_scored = df_features.copy()
        df_scored["good_company"] = df_scored["page_views_per_user"] >= threshold
        mo.md(f"**優秀企業群しきい値**: {threshold:.2f}（page_views_per_user）")
    else:
        mo.md("*利用度指標がありません*")
    return (df_scored,)


@app.cell
def _(df_scored, mo, px):
    if len(df_scored) > 0:
        fig_scatter = px.scatter(
            df_scored,
            x="users",
            y="page_views",
            color="good_company",
            hover_data=["org_id", "ORG_NAME", "COMPANY_NAME"],
            title="ユーザー数 × ページビュー（優秀企業群の色分け）",
        )
        fig_scatter
    else:
        mo.md("*散布図を表示できません*")
    return


@app.cell
def _(df_scored, mo, px):
    if len(df_scored) > 0 and "page_views_per_user" in df_scored.columns:
        fig_box = px.box(
            df_scored,
            x="good_company",
            y="page_views_per_user",
            title="優秀企業群 vs その他（page_views_per_user）",
        )
        fig_box
    else:
        mo.md("*箱ひげ図を表示できません*")
    return


@app.cell
def _(df_scored, mo, px):
    if len(df_scored) > 0:
        dummy_cols = [c for c in df_scored.columns if c.startswith("has_")]
        if dummy_cols:
            heat_df = df_scored.sort_values(
                "page_views_per_user", ascending=False
            ).head(50)
            fig_heat = px.imshow(
                heat_df[dummy_cols],
                aspect="auto",
                title="上位50社×上位ページ（閲覧有無）",
            )
            fig_heat
        else:
            mo.md("*ダミー変数がありません*")
    else:
        mo.md("*ヒートマップを表示できません*")
    return


@app.cell
def _(mo):
    mo.md("""
    ## 8. カスタムSQL（Snowflake）
    """)
    return


@app.cell
def _(mo):
    default_sql = """-- 例: org_idと会社名の対応
    SELECT ORGID, NAME
    FROM ETL_S3_TRANSALES_DB.TRANSALES_DAILY_SCHEMA.USERORGANIZATION
    WHERE ORGID IS NOT NULL
    LIMIT 100"""

    sql_editor = mo.ui.code_editor(value=default_sql, language="sql", min_height=200)
    sql_editor
    return (sql_editor,)


@app.cell
def _(mo):
    query_button = mo.ui.run_button(label="クエリ実行")
    query_button
    return (query_button,)


@app.cell
def _(get_snowflake_connection, mo, query_button, sql_editor):
    def run_snowflake_sql(sql_text: str):
        sf_conn = get_snowflake_connection()
        sf_cur = sf_conn.cursor()
        try:
            sf_cur.execute(sql_text)
            return sf_cur.fetch_pandas_all()
        finally:
            sf_cur.close()
            sf_conn.close()

    df_sql = None
    if query_button.value and sql_editor.value:
        try:
            df_sql = run_snowflake_sql(sql_editor.value)
            mo.md(f"### 結果: {len(df_sql)} 行")
        except Exception as e:
            mo.md(f"### クエリエラー\n```\n{e}\n```")
    else:
        mo.md("*クエリを入力して「クエリ実行」をクリック*")
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

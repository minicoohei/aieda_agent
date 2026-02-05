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
        # Churnアクション分析

        解約/継続企業のデータ期間を揃えた上で、チャーンに効くアクション（量・種類）を特定します。

        ## 分析設計
        - **比較期間**: 解約企業の解約日までに合わせ、継続企業も同期間で打ち切り
        - **基準日（t=0）**: 契約開始日（後から更新可能なパラメータ）
        - **ID接続**: COMPNO → COMPANYID → ORGID → GA org_id
        """
    )
    return


@app.cell
def _():
    import os
    import re
    import sys
    import tomllib
    from datetime import date, timedelta
    from pathlib import Path

    import numpy as np
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
        np,
        os,
        pd,
        re,
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


# =============================================================================
# パラメータ設定（後から更新可能）
# =============================================================================
@app.cell
def _(mo):
    mo.md("## パラメータ設定")
    return


@app.cell
def _(mo):
    mo.md(
        """
        **契約開始日の設定方法**:
        - 現在はTSVに契約開始日がないため、GAの `first_date` を代替使用
        - 後から契約開始日データが入手できた場合は、以下のパラメータを変更してください
        """
    )
    return


@app.cell
def _(mo):
    # 契約開始日のソース設定（後から変更可能）
    contract_start_source = mo.ui.dropdown(
        options=["ga_first_date", "tsv_column", "external_csv"],
        value="ga_first_date",
        label="契約開始日のソース",
    )
    contract_start_source
    return (contract_start_source,)


@app.cell
def _(mo):
    # 契約開始日の列名（tsv_columnまたはexternal_csvを選択した場合に使用）
    contract_start_column = mo.ui.text(
        value="contract_start_date",
        label="契約開始日の列名（TSV/CSVに存在する場合）",
    )
    contract_start_column
    return (contract_start_column,)


@app.cell
def _(mo):
    # 外部CSVのパス（external_csvを選択した場合に使用）
    external_contract_csv = mo.ui.text(
        value="",
        label="外部契約開始日CSVのパス（空欄でスキップ）",
    )
    external_contract_csv
    return (external_contract_csv,)


@app.cell
def _(mo):
    # GAイベント最小件数フィルタ
    min_ga_events = mo.ui.number(
        value=10,
        start=1,
        stop=100,
        label="GAイベント最小件数（これ未満は除外）",
    )
    min_ga_events
    return (min_ga_events,)


# =============================================================================
# 接続設定
# =============================================================================
@app.cell
def _(Path, bq_error, bigquery, os, service_account):
    BQ_PROJECT_ID = "gree-dionysus-infobox"
    GA_DATASET_ID = "analytics_400693944"

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

    return BQ_PROJECT_ID, GA_DATASET_ID, get_bq_client


@app.cell
def _(Path, default_backend, os, serialization, snowflake, snowflake_error, tomllib):
    SF_SCHEMA = "ETL_S3_TRANSALES_DB.TRANSALES_DAILY_SCHEMA"

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

    return SF_SCHEMA, get_snowflake_connection


# =============================================================================
# 1. Churn TSV読み込み
# =============================================================================
@app.cell
def _(mo):
    mo.md("## 1. Churn TSV読み込み")
    return


@app.cell
def _(Path, pd):
    tsv_path = Path("/Users/kou1904/githubactions_fordata/work/aieda_agent/docs/assets/infobox_data.tsv")
    df_churn_raw = pd.read_csv(tsv_path, sep="\t")

    # エラー行を除外
    df_churn = df_churn_raw[
        ~df_churn_raw["アクティブ率 infobox利用状況"].astype(str).str.contains("エラー", na=False)
    ].copy()

    # status列をバイナリ化
    df_churn["is_churned"] = (df_churn["status"] == "解約済み").astype(int)

    # COMPNO列を文字列に統一
    df_churn["COMPNO"] = df_churn["COMPNO"].astype(str)

    return df_churn, df_churn_raw, tsv_path


@app.cell
def _(df_churn, df_churn_raw, mo):
    mo.md(
        f"""
        **読み込み結果**:
        - 全行数: {len(df_churn_raw):,}
        - エラー除外後: {len(df_churn):,}
        - 解約済み: {df_churn['is_churned'].sum():,}
        - 契約中: {(~df_churn['is_churned'].astype(bool)).sum():,}
        """
    )
    return


@app.cell
def _(df_churn, mo):
    mo.ui.table(df_churn.head(20), pagination=True)
    return


# =============================================================================
# 2. Snowflake ID接続確認（COMPNO → ORGID）
# =============================================================================
@app.cell
def _(mo):
    mo.md("## 2. Snowflake ID接続確認")
    return


@app.cell
def _(SF_SCHEMA, get_snowflake_connection, mo, pd):
    # BEEGLECOMPANYのスキーマを確認してCOMPNOに対応する列を探す
    df_bc_schema = pd.DataFrame()
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        cur.execute(f"DESCRIBE TABLE {SF_SCHEMA}.BEEGLECOMPANY")
        df_bc_schema = cur.fetch_pandas_all()
        cur.close()
        conn.close()
        mo.md("### BEEGLECOMPANY スキーマ")
    except Exception as e:
        mo.md(f"**Snowflake接続エラー**: `{e}`")
    return (df_bc_schema,)


@app.cell
def _(df_bc_schema, mo):
    if len(df_bc_schema) > 0:
        mo.ui.table(df_bc_schema, pagination=True)
    else:
        mo.md("*スキーマ取得失敗*")
    return


@app.cell
def _(SF_SCHEMA, df_churn, get_snowflake_connection, mo, pd):
    # COMPNOに対応しそうな列を探索（HOUJINNO, COMPNO, CORPORATENUMBER等）
    df_id_mapping = pd.DataFrame()
    compno_column_candidates = ["HOUJINNO", "COMPNO", "CORPORATENUMBER", "CORPORATE_NUMBER"]

    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()

        # まず、COMPNOに対応しそうな列があるか確認
        check_sql = f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'TRANSALES_DAILY_SCHEMA'
          AND TABLE_NAME = 'BEEGLECOMPANY'
          AND UPPER(COLUMN_NAME) IN ('HOUJINNO', 'COMPNO', 'CORPORATENUMBER', 'CORPORATE_NUMBER')
        """
        cur.execute(check_sql)
        df_compno_cols = cur.fetch_pandas_all()

        if len(df_compno_cols) > 0:
            compno_col = df_compno_cols.iloc[0, 0]
            mo.md(f"**COMPNO対応列発見**: `{compno_col}`")

            # サンプルで結合確認
            sample_compnos = df_churn["COMPNO"].head(10).tolist()
            compno_list = ",".join([f"'{c}'" for c in sample_compnos])

            mapping_sql = f"""
            SELECT
                bc.{compno_col} AS COMPNO,
                bc.ID AS COMPANYID,
                bc.SHOGO AS COMPANY_NAME,
                u.ORGID,
                u.NAME AS ORG_NAME
            FROM {SF_SCHEMA}.BEEGLECOMPANY bc
            LEFT JOIN {SF_SCHEMA}.USERORGANIZATION u
              ON bc.ID = u.COMPANYID
            WHERE bc.{compno_col} IN ({compno_list})
            """
            cur.execute(mapping_sql)
            df_id_mapping = cur.fetch_pandas_all()
        else:
            mo.md("**COMPNO対応列が見つかりません** → CompanyNameでの結合を試みます")

        cur.close()
        conn.close()
    except Exception as e:
        mo.md(f"**ID接続確認エラー**: `{e}`")

    return compno_column_candidates, df_id_mapping


@app.cell
def _(df_id_mapping, mo):
    if len(df_id_mapping) > 0:
        mo.md(f"**サンプル結合結果**: {len(df_id_mapping)} 件")
        mo.ui.table(df_id_mapping, pagination=True)
    else:
        mo.md("*サンプル結合結果なし*")
    return


# =============================================================================
# 3. GA4データ取得（org_id単位 + ページ群別）
# =============================================================================
@app.cell
def _(mo):
    mo.md("## 3. GA4データ取得")
    return


@app.cell
def _(GA_DATASET_ID, get_bq_client, mo, pd):
    # GA4 org_id単位の基本指標
    df_ga_metrics = pd.DataFrame()
    try:
        client = get_bq_client()
        ga_metrics_sql = f"""
        WITH base AS (
            SELECT
                user_pseudo_id,
                (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
                (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'org_id') AS org_id,
                event_name,
                event_date
            FROM `{GA_DATASET_ID}.events_*`
            WHERE NOT STARTS_WITH(_TABLE_SUFFIX, 'intraday_')
        )
        SELECT
            org_id,
            COUNT(DISTINCT user_pseudo_id) AS users,
            COUNT(DISTINCT CONCAT(user_pseudo_id, '-', CAST(ga_session_id AS STRING))) AS sessions,
            COUNTIF(event_name = 'page_view') AS page_views,
            COUNTIF(event_name = 'first_visit') AS new_users,
            MIN(event_date) AS first_date,
            MAX(event_date) AS last_date,
            COUNT(*) AS total_events
        FROM base
        WHERE org_id IS NOT NULL
        GROUP BY org_id
        ORDER BY users DESC
        """
        df_ga_metrics = client.query(ga_metrics_sql).to_dataframe()
        mo.md(f"**GA4 org_id単位指標**: {len(df_ga_metrics):,} 件")
    except Exception as e:
        mo.md(f"**GA4取得エラー**: `{e}`")
    return (df_ga_metrics,)


@app.cell
def _(df_ga_metrics, mo):
    if len(df_ga_metrics) > 0:
        mo.ui.table(df_ga_metrics.head(20), pagination=True)
    return


# =============================================================================
# 4. ページ群別閲覧量（GA4）
# =============================================================================
@app.cell
def _(mo):
    mo.md("## 4. ページ群別閲覧量")
    return


@app.cell
def _(mo):
    mo.md(
        """
        **ページ群の定義**:
        | ページ群 | パス正規表現 | 意味 |
        | --- | --- | --- |
        | list | `^/list` | リスト一覧/詳細 |
        | company | `^/company` | 企業詳細 |
        | download | `^/download` または `csv` を含む | CSVダウンロード関連 |
        | search | `^/search` | 検索機能 |
        | settings | `^/settings` | 設定画面 |
        | other | 上記以外 | その他 |
        """
    )
    return


@app.cell
def _(GA_DATASET_ID, get_bq_client, mo, pd):
    # ページ群別閲覧量（org_id単位）
    df_page_groups = pd.DataFrame()
    try:
        client = get_bq_client()
        page_groups_sql = f"""
        WITH base AS (
            SELECT
                (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'org_id') AS org_id,
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location
            FROM `{GA_DATASET_ID}.events_*`
            WHERE NOT STARTS_WITH(_TABLE_SUFFIX, 'intraday_')
              AND event_name = 'page_view'
        ),
        classified AS (
            SELECT
                org_id,
                page_location,
                CASE
                    WHEN REGEXP_CONTAINS(page_location, r'/list') THEN 'list'
                    WHEN REGEXP_CONTAINS(page_location, r'/company') THEN 'company'
                    WHEN REGEXP_CONTAINS(page_location, r'/download|csv') THEN 'download'
                    WHEN REGEXP_CONTAINS(page_location, r'/search') THEN 'search'
                    WHEN REGEXP_CONTAINS(page_location, r'/settings') THEN 'settings'
                    ELSE 'other'
                END AS page_group
            FROM base
            WHERE org_id IS NOT NULL
        )
        SELECT
            org_id,
            page_group,
            COUNT(*) AS page_views
        FROM classified
        GROUP BY org_id, page_group
        """
        df_page_groups = client.query(page_groups_sql).to_dataframe()
        mo.md(f"**ページ群別閲覧量**: {len(df_page_groups):,} 件")
    except Exception as e:
        mo.md(f"**ページ群取得エラー**: `{e}`")
    return (df_page_groups,)


@app.cell
def _(df_page_groups, mo, pd):
    # ページ群をピボットして org_id 単位の特徴量に変換
    df_page_pivot = pd.DataFrame()
    if len(df_page_groups) > 0:
        df_page_pivot = df_page_groups.pivot_table(
            index="org_id",
            columns="page_group",
            values="page_views",
            fill_value=0,
        ).reset_index()
        df_page_pivot.columns = ["org_id"] + [f"pv_{c}" for c in df_page_pivot.columns[1:]]
        mo.md(f"**ページ群ピボット**: {len(df_page_pivot):,} 件")
    return (df_page_pivot,)


@app.cell
def _(df_page_pivot, mo):
    if len(df_page_pivot) > 0:
        mo.ui.table(df_page_pivot.head(20), pagination=True)
    return


# =============================================================================
# 5. Snowflake org_id → COMPNO マッピング
# =============================================================================
@app.cell
def _(mo):
    mo.md("## 5. Snowflake org_id → COMPNO マッピング")
    return


@app.cell
def _(SF_SCHEMA, get_snowflake_connection, mo, pd):
    # USERORGANIZATION + BEEGLECOMPANY を結合して ORGID → COMPNO のマッピングを作成
    df_sf_mapping = pd.DataFrame()
    compno_col_name = None

    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()

        # COMPNO対応列を再確認
        check_sql = f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'TRANSALES_DAILY_SCHEMA'
          AND TABLE_NAME = 'BEEGLECOMPANY'
          AND UPPER(COLUMN_NAME) IN ('HOUJINNO', 'COMPNO', 'CORPORATENUMBER', 'CORPORATE_NUMBER')
        """
        cur.execute(check_sql)
        df_cols = cur.fetch_pandas_all()

        if len(df_cols) > 0:
            compno_col_name = df_cols.iloc[0, 0]

            mapping_sql = f"""
            SELECT
                u.ORGID,
                u.NAME AS ORG_NAME,
                u.COMPANYID,
                bc.{compno_col_name} AS COMPNO,
                bc.SHOGO AS COMPANY_NAME
            FROM {SF_SCHEMA}.USERORGANIZATION u
            LEFT JOIN {SF_SCHEMA}.BEEGLECOMPANY bc
              ON u.COMPANYID = bc.ID
            WHERE u.ORGID IS NOT NULL
            """
            cur.execute(mapping_sql)
            df_sf_mapping = cur.fetch_pandas_all()
            mo.md(f"**ORGID → COMPNO マッピング**: {len(df_sf_mapping):,} 件")
        else:
            mo.md("**COMPNO対応列が見つかりません**")

        cur.close()
        conn.close()
    except Exception as e:
        mo.md(f"**マッピング取得エラー**: `{e}`")

    return compno_col_name, df_sf_mapping


@app.cell
def _(df_sf_mapping, mo):
    if len(df_sf_mapping) > 0:
        mo.ui.table(df_sf_mapping.head(20), pagination=True)
    return


# =============================================================================
# 6. データ統合（Churn + GA + Snowflake）
# =============================================================================
@app.cell
def _(mo):
    mo.md("## 6. データ統合")
    return


@app.cell
def _(
    contract_start_source,
    df_churn,
    df_ga_metrics,
    df_page_pivot,
    df_sf_mapping,
    min_ga_events,
    mo,
    np,
    pd,
):
    df_merged = pd.DataFrame()

    if len(df_sf_mapping) > 0 and len(df_ga_metrics) > 0:
        # 1. Snowflakeマッピングを使ってCOMPNO → ORGID
        df_sf_mapping["COMPNO"] = df_sf_mapping["COMPNO"].astype(str)

        # 2. ChurnデータにORGIDを付与
        df_merged = df_churn.merge(
            df_sf_mapping[["COMPNO", "ORGID", "ORG_NAME", "COMPANY_NAME"]],
            on="COMPNO",
            how="left",
        )

        # 3. GA指標を結合
        df_merged = df_merged.merge(
            df_ga_metrics,
            left_on="ORGID",
            right_on="org_id",
            how="left",
        )

        # 4. ページ群指標を結合
        if len(df_page_pivot) > 0:
            df_merged = df_merged.merge(
                df_page_pivot,
                on="org_id",
                how="left",
            )

        # 5. 派生指標を計算
        df_merged["sessions_per_user"] = df_merged["sessions"] / df_merged["users"].replace(0, np.nan)
        df_merged["page_views_per_user"] = df_merged["page_views"] / df_merged["users"].replace(0, np.nan)
        df_merged["page_views_per_session"] = df_merged["page_views"] / df_merged["sessions"].replace(0, np.nan)

        # 6. 契約開始日の設定
        if contract_start_source.value == "ga_first_date":
            df_merged["contract_start_date"] = pd.to_datetime(df_merged["first_date"], format="%Y%m%d", errors="coerce")

        # 7. GAイベント最小件数フィルタ
        df_merged = df_merged[
            (df_merged["total_events"] >= min_ga_events.value) | (df_merged["total_events"].isna())
        ]

        mo.md(
            f"""
            **統合結果**:
            - 統合後行数: {len(df_merged):,}
            - ORGID結合成功: {df_merged['ORGID'].notna().sum():,}
            - GA結合成功: {df_merged['org_id'].notna().sum():,}
            """
        )
    else:
        mo.md("*必要なデータが揃っていません*")

    return (df_merged,)


@app.cell
def _(df_merged, mo):
    if len(df_merged) > 0:
        mo.ui.table(df_merged.head(20), pagination=True)
    return


# =============================================================================
# 7. 期間整合（Apple-to-Apple）
# =============================================================================
@app.cell
def _(mo):
    mo.md("## 7. 期間整合（Apple-to-Apple）")
    return


@app.cell
def _(df_merged, mo, np, pd):
    df_aligned = pd.DataFrame()

    if len(df_merged) > 0 and "first_date" in df_merged.columns and "last_date" in df_merged.columns:
        df_aligned = df_merged.copy()

        # 観測期間を計算
        df_aligned["first_date_dt"] = pd.to_datetime(df_aligned["first_date"], format="%Y%m%d", errors="coerce")
        df_aligned["last_date_dt"] = pd.to_datetime(df_aligned["last_date"], format="%Y%m%d", errors="coerce")
        df_aligned["observation_days"] = (df_aligned["last_date_dt"] - df_aligned["first_date_dt"]).dt.days

        # 解約企業の観測期間の中央値を計算
        churn_obs_days = df_aligned[df_aligned["is_churned"] == 1]["observation_days"].dropna()
        if len(churn_obs_days) > 0:
            cutoff_days = int(churn_obs_days.median())
            mo.md(f"**解約企業の観測期間中央値**: {cutoff_days} 日")

            # 継続企業も同じ期間で打ち切り（指標は調整が必要な場合がある）
            # ここでは期間情報のみ付与し、実際の指標調整は分析段階で行う
            df_aligned["cutoff_days"] = cutoff_days
            df_aligned["is_within_cutoff"] = df_aligned["observation_days"] <= cutoff_days
        else:
            df_aligned["cutoff_days"] = np.nan
            df_aligned["is_within_cutoff"] = True
            mo.md("*解約企業の観測期間データがありません*")

        mo.md(
            f"""
            **期間整合結果**:
            - 打ち切り期間内企業: {df_aligned['is_within_cutoff'].sum():,}
            - 打ち切り期間外企業: {(~df_aligned['is_within_cutoff']).sum():,}
            """
        )
    else:
        mo.md("*期間情報がありません*")

    return (df_aligned,)


# =============================================================================
# 8. EDA（Churn vs 継続の差分）
# =============================================================================
@app.cell
def _(mo):
    mo.md("## 8. EDA（Churn vs 継続の差分）")
    return


@app.cell
def _(df_aligned, mo, pd):
    # 主要指標のChurn vs 継続比較
    df_comparison = pd.DataFrame()

    if len(df_aligned) > 0:
        metrics = [
            "users", "sessions", "page_views", "new_users",
            "sessions_per_user", "page_views_per_user", "page_views_per_session",
            "observation_days",
        ]
        # ページ群指標を追加
        pv_cols = [c for c in df_aligned.columns if c.startswith("pv_")]
        metrics.extend(pv_cols)

        comparison_data = []
        for metric in metrics:
            if metric in df_aligned.columns:
                churn_vals = df_aligned[df_aligned["is_churned"] == 1][metric].dropna()
                active_vals = df_aligned[df_aligned["is_churned"] == 0][metric].dropna()

                comparison_data.append({
                    "指標": metric,
                    "解約_中央値": churn_vals.median() if len(churn_vals) > 0 else None,
                    "解約_平均": churn_vals.mean() if len(churn_vals) > 0 else None,
                    "継続_中央値": active_vals.median() if len(active_vals) > 0 else None,
                    "継続_平均": active_vals.mean() if len(active_vals) > 0 else None,
                    "差分_中央値": (active_vals.median() - churn_vals.median()) if len(churn_vals) > 0 and len(active_vals) > 0 else None,
                })

        df_comparison = pd.DataFrame(comparison_data)
        mo.md("### Churn vs 継続 指標比較")

    return (df_comparison,)


@app.cell
def _(df_comparison, mo):
    if len(df_comparison) > 0:
        mo.ui.table(df_comparison, pagination=False)
    return


# =============================================================================
# 9. 機械学習（ロジスティック回帰 + 決定木）
# =============================================================================
@app.cell
def _(mo):
    mo.md("## 9. 機械学習（閾値抽出 + 寄与度推定）")
    return


@app.cell
def _(mo):
    run_ml_button = mo.ui.run_button(label="機械学習を実行")
    run_ml_button
    return (run_ml_button,)


@app.cell
def _(df_aligned, mo, np, pd, run_ml_button):
    df_importance = pd.DataFrame()
    df_tree_rules = pd.DataFrame()
    model_logit = None
    model_tree = None

    if run_ml_button.value and len(df_aligned) > 0:
        try:
            from sklearn.linear_model import LogisticRegression
            from sklearn.tree import DecisionTreeClassifier, export_text
            from sklearn.preprocessing import StandardScaler
            from sklearn.model_selection import train_test_split

            # 特徴量選択
            feature_cols = [
                "users", "sessions", "page_views", "new_users",
                "sessions_per_user", "page_views_per_user", "page_views_per_session",
            ]
            # ページ群指標を追加
            pv_cols = [c for c in df_aligned.columns if c.startswith("pv_")]
            feature_cols.extend(pv_cols)

            # 欠損値を除外
            df_ml = df_aligned[["is_churned"] + feature_cols].dropna()

            if len(df_ml) > 10:
                X = df_ml[feature_cols]
                y = df_ml["is_churned"]

                # 標準化
                scaler = StandardScaler()
                X_scaled = scaler.fit_transform(X)

                # ロジスティック回帰（寄与度推定）
                model_logit = LogisticRegression(max_iter=1000, random_state=42)
                model_logit.fit(X_scaled, y)

                importance_data = []
                for col, coef in zip(feature_cols, model_logit.coef_[0]):
                    importance_data.append({
                        "特徴量": col,
                        "係数": coef,
                        "効果方向": "解約促進" if coef > 0 else "継続促進",
                        "重要度": abs(coef),
                    })
                df_importance = pd.DataFrame(importance_data).sort_values("重要度", ascending=False)

                # 決定木（閾値抽出）
                model_tree = DecisionTreeClassifier(max_depth=4, min_samples_leaf=5, random_state=42)
                model_tree.fit(X, y)

                tree_rules = export_text(model_tree, feature_names=feature_cols)
                df_tree_rules = pd.DataFrame({"決定木ルール": [tree_rules]})

                mo.md(f"**学習完了**: サンプル数 {len(df_ml)}, 特徴量数 {len(feature_cols)}")
            else:
                mo.md("*サンプル数が不足しています（10件以上必要）*")

        except ImportError as e:
            mo.md(f"**scikit-learn未インストール**: `{e}`")
        except Exception as e:
            mo.md(f"**ML実行エラー**: `{e}`")
    else:
        mo.md("*「機械学習を実行」ボタンをクリックしてください*")

    return df_importance, df_tree_rules, model_logit, model_tree


@app.cell
def _(df_importance, mo):
    mo.md("### ロジスティック回帰 寄与度")
    if len(df_importance) > 0:
        mo.ui.table(df_importance, pagination=False)
    return


@app.cell
def _(df_tree_rules, mo):
    mo.md("### 決定木 閾値ルール")
    if len(df_tree_rules) > 0:
        mo.md(f"```\n{df_tree_rules.iloc[0, 0]}\n```")
    return


# =============================================================================
# 10. サマリー
# =============================================================================
@app.cell
def _(mo):
    mo.md("## 10. サマリー")
    return


@app.cell
def _(df_comparison, df_importance, mo):
    mo.md(
        """
        ### 分析結果サマリー

        **主要な発見**:
        - EDA比較表とロジスティック回帰の寄与度を確認してください
        - 決定木の閾値ルールから「チャーン分岐点」を抽出できます

        **次のステップ**:
        1. 契約開始日データが入手できたら、パラメータを更新して再実行
        2. 閾値の妥当性をビジネス観点で検証
        3. 追加の特徴量（Snowflake行動ログ等）を検討
        """
    )
    return


if __name__ == "__main__":
    app.run()

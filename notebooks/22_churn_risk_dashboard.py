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
    # Churn Risk Dashboard with Intent

    チャーンリスク、GA利用状況、インテント（競合含む）を統合したダッシュボードです。
    LLMで各会社の危険度を判定します。

    ## データソース
    - **チャーン情報**: `infobox_data.tsv`
    - **GA利用ログ**: BigQuery `analytics_400693944`
    - **インテント**: BigQuery `gree-dionysus-infobox.production_infobox`
    - **組織マスタ**: Snowflake `USERORGANIZATION`, `BEEGLECOMPANY`

    ## セマンティック定義
    - 参照: `docs/semantic_questions_status.md`
    - ER図: `docs/snowflake_elt_entity.puml`
    """)
    return


@app.cell
def _():
    import json
    import os
    import re
    import sys
    import tomllib
    from datetime import date, timedelta
    from pathlib import Path

    import numpy as np
    import pandas as pd
    from dotenv import load_dotenv

    # Snowflake
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

    # BigQuery
    bq_error = None
    bigquery = None
    service_account = None
    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account
    except Exception as exc:
        bq_error = exc

    # Gemini
    genai = None
    genai_types = None
    genai_error = None
    try:
        from google import genai
        from google.genai import types as genai_types
    except Exception as exc:
        genai_error = exc

    # .env読み込み
    possible_env_paths = [
        Path("/Users/kou1904/githubactions_fordata/work/aieda_agent/.env"),
        Path(__file__).parent.parent / ".env",
        Path.cwd() / ".env",
        Path.cwd().parent / ".env",
    ]
    loaded_env_path = None
    for env_path in possible_env_paths:
        if env_path.exists():
            load_dotenv(env_path, override=True)
            loaded_env_path = str(env_path)
            break

    # APIキー読み込み確認
    gemini_api_key_loaded = os.getenv("GEMINI_API_KEY")
    gemini_key_status = f"設定済み ({gemini_api_key_loaded[:8]}...)" if gemini_api_key_loaded else "未設定"

    root_dir = Path("/Users/kou1904/githubactions_fordata/work/aieda_agent")
    if str(root_dir / "src") not in sys.path:
        sys.path.insert(0, str(root_dir / "src"))
    return (
        Path,
        bigquery,
        bq_error,
        default_backend,
        genai,
        genai_error,
        genai_types,
        gemini_key_status,
        json,
        loaded_env_path,
        np,
        os,
        pd,
        re,
        serialization,
        service_account,
        snowflake,
        snowflake_error,
        tomllib,
    )


@app.cell
def _(bq_error, genai_error, gemini_key_status, loaded_env_path, mo, snowflake_error):
    errors = []
    if snowflake_error:
        errors.append(f"Snowflake: `{snowflake_error}`")
    if bq_error:
        errors.append(f"BigQuery: `{bq_error}`")
    if genai_error:
        errors.append(f"Gemini: `{genai_error}`")
    
    status_lines = [
        f"- **Gemini API Key**: {gemini_key_status}",
        f"- **.env読み込み**: {loaded_env_path or '未読み込み'}",
    ]
    
    output = "## 🔧 初期化ステータス\n\n"
    if errors:
        output += "**モジュール読み込みエラー**:\n" + "\n".join([f"- {e}" for e in errors]) + "\n\n"
    output += "**設定状況**:\n" + "\n".join(status_lines)
    
    mo.md(output)
    return


@app.cell
def _(re):
    def normalize_company_name(name: str | None) -> str | None:
        if name is None:
            return None
        value = str(name)
        if not value.strip():
            return None
        value = re.sub(r"\s+", "", value)
        value = re.sub(
            r"(株式会社|有限会社|合同会社|一般社団法人|一般財団法人|公益社団法人|公益財団法人)",
            "",
            value,
        )
        value = re.sub(r"[（）()]", "", value)
        value = value.replace("様", "")
        return value or None

    def extract_company_name_from_deal(deal_name: str | None) -> str | None:
        if deal_name is None:
            return None
        raw = str(deal_name)
        if not raw.strip():
            return None
        patterns = [
            r"(?P<name>.+?)様向け",
            r"(?P<name>.+?)向け",
            r"(?P<name>.+?)様",
        ]
        for pattern in patterns:
            match = re.search(pattern, raw)
            if match:
                return match.group("name")
        return raw

    def extract_json_block(text: str) -> str:
        if not text:
            return ""
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return ""
        return text[start : end + 1]
    return (
        extract_company_name_from_deal,
        extract_json_block,
        normalize_company_name,
    )


@app.cell
def _(Path, bigquery, os, pd, service_account):
    # BigQuery接続（gree-dionysus-infobox）
    BQ_PROJECT_ID = "gree-dionysus-infobox"
    BQ_DATASET_INTENT = "production_infobox"
    GA_DATASET_ID = "analytics_400693944"

    def get_bq_client():
        key_path = os.path.expanduser("~/.gcp/gree-dionysus-infobox.json")
        if Path(key_path).exists():
            credentials = service_account.Credentials.from_service_account_file(key_path)
            return bigquery.Client(project=BQ_PROJECT_ID, credentials=credentials)
        return bigquery.Client(project=BQ_PROJECT_ID)

    def query_bq(sql):
        """BigQueryクエリ実行"""
        client = get_bq_client()
        job = client.query(sql)
        results = job.result()
        rows = [dict(row) for row in results]
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    return GA_DATASET_ID, query_bq


@app.cell
def _(
    Path,
    default_backend,
    os,
    serialization,
    snowflake,
    snowflake_error,
    tomllib,
):
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
        private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH") or config.get("private_key_path")
        authenticator = os.getenv("SNOWFLAKE_AUTHENTICATOR") or config.get("authenticator")
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

    def query_sf(sql):
        """Snowflakeクエリ実行"""
        conn = get_snowflake_connection()
        cur = conn.cursor()
        try:
            cur.execute(sql)
            return cur.fetch_pandas_all()
        finally:
            cur.close()
            conn.close()
    return SF_SCHEMA, query_sf


@app.cell
def _(mo):
    mo.md("""
    ## 1. チャーンデータ読み込み
    """)
    return


@app.cell
def _(Path, normalize_company_name, pd):
    tsv_path = Path("/Users/kou1904/githubactions_fordata/work/aieda_agent/docs/assets/infobox_data.tsv")
    df_churn_raw = pd.read_csv(tsv_path, sep="\t")

    # カラム名を正規化
    df_churn_raw.columns = [c.strip() for c in df_churn_raw.columns]

    # エラー行を除外（アクティブ率カラムを探す）
    active_rate_col = [c for c in df_churn_raw.columns if "アクティブ率" in c]
    if active_rate_col:
        df_churn = df_churn_raw[
            ~df_churn_raw[active_rate_col[0]].astype(str).str.contains("エラー", na=False)
        ].copy()
    else:
        # エラー行がある列を探す
        df_churn = df_churn_raw[
            ~df_churn_raw.apply(lambda row: row.astype(str).str.contains("エラー").any(), axis=1)
        ].copy()

    # status列をバイナリ化（大文字小文字対応）
    status_col = "STATUS" if "STATUS" in df_churn.columns else "status"
    df_churn["is_churned"] = (df_churn[status_col] == "解約済み").astype(int)
    df_churn["COMPNO"] = df_churn["COMPNO"].astype(str)

    # 利用しやすいカラム名にリネーム
    rename_map = {
        "STATUS": "status",
        "COMPANY_NAME": "CompanyName",
        "ACCOUT_COUNT": "account_count",
    }
    # アクティブ率カラム
    if active_rate_col:
        rename_map[active_rate_col[0]] = "active_rate"
    df_churn.rename(columns={k: v for k, v in rename_map.items() if k in df_churn.columns}, inplace=True)
    if "CompanyName" in df_churn.columns:
        df_churn["company_name_norm"] = df_churn["CompanyName"].apply(normalize_company_name)
    else:
        df_churn["company_name_norm"] = None
    return df_churn, df_churn_raw


@app.cell
def _(df_churn, df_churn_raw, mo):
    mo.md(f"""
    **チャーンデータ**:
    - 全行数: {len(df_churn_raw):,}
    - エラー除外後: {len(df_churn):,}
    - 解約済み: {df_churn['is_churned'].sum():,}
    - 契約中: {(~df_churn['is_churned'].astype(bool)).sum():,}
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 1-b. 解約理由データ（CS）
    """)
    return


@app.cell
def _(Path, extract_company_name_from_deal, normalize_company_name, pd):
    churn_reason_path = Path(
        "/Users/kou1904/githubactions_fordata/work/aieda_agent/docs/assets/【CS】解約顧客一覧_25年1月〜 - シート1.csv"
    )
    df_churn_reason_raw = pd.DataFrame()
    df_churn_reason_latest = pd.DataFrame()

    if churn_reason_path.exists():
        df_churn_reason_raw = pd.read_csv(churn_reason_path)
        df_churn_reason = df_churn_reason_raw.copy()

        if "商談名" in df_churn_reason.columns:
            df_churn_reason["deal_company_name"] = df_churn_reason["商談名"].apply(
                extract_company_name_from_deal
            )
        else:
            df_churn_reason["deal_company_name"] = None

        df_churn_reason["company_name_norm"] = df_churn_reason["deal_company_name"].apply(
            normalize_company_name
        )

        loss_date_col = "現契約終了日" if "現契約終了日" in df_churn_reason.columns else None
        if loss_date_col is None and "完了予定日" in df_churn_reason.columns:
            loss_date_col = "完了予定日"

        if loss_date_col:
            df_churn_reason["loss_end_date"] = pd.to_datetime(
                df_churn_reason[loss_date_col], errors="coerce"
            )
        else:
            df_churn_reason["loss_end_date"] = pd.NaT

        df_churn_reason_sorted = df_churn_reason.sort_values("loss_end_date")
        df_churn_reason_latest = (
            df_churn_reason_sorted.dropna(subset=["company_name_norm"])
            .groupby("company_name_norm")
            .tail(1)
            .copy()
        )

        churn_reason_rename_map = {
            "商談名": "deal_name",
            "失注種別": "loss_type",
            "失注理由": "loss_reason",
            "受注/失注理由詳細": "loss_detail",
            "商談 所有者": "deal_owner",
            "現契約終了日": "current_contract_end_date",
            "完了予定日": "planned_close_date",
        }
        needed_cols = ["company_name_norm", "deal_company_name"] + [
            col for col in churn_reason_rename_map if col in df_churn_reason_latest.columns
        ]
        df_churn_reason_latest = df_churn_reason_latest[needed_cols].rename(
            columns=churn_reason_rename_map
        )
    return df_churn_reason_latest, df_churn_reason_raw


@app.cell
def _(df_churn_reason_latest, df_churn_reason_raw, mo):
    mo.md(f"""
    **解約理由データ**:
    - 全行数: {len(df_churn_reason_raw):,}
    - 抽出会社数: {len(df_churn_reason_latest):,}
    """)
    return


@app.cell
def _(df_churn_reason_latest, mo):
    mo.ui.table(df_churn_reason_latest.head(20), pagination=True)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 2. カテゴリ探索（営業支援/CRM/SFA関連）
    """)
    return


@app.cell
def _(mo, query_bq):
    # カテゴリ一覧取得
    category_query = """
    SELECT DISTINCT 
        original_category_name as category,
        COUNT(*) as count
    FROM `gree-dionysus-infobox.production_infobox.company_category_daily_v3`
    WHERE view_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY 1
    ORDER BY count DESC
    LIMIT 100
    """
    df_categories = query_bq(category_query)
    mo.md(f"**カテゴリ数（直近30日）**: {len(df_categories):,}")
    return (df_categories,)


@app.cell
def _(df_categories, mo):
    mo.ui.table(df_categories, pagination=True)
    return


@app.cell
def _(mo):
    # 営業支援関連カテゴリのフィルタ
    sales_keywords = ["営業", "CRM", "SFA", "セールス", "リード", "顧客管理", "商談", "MA", "マーケティング"]
    category_filter = mo.ui.text(
        label="カテゴリ検索キーワード（カンマ区切り）",
        value=",".join(sales_keywords),
    )
    category_filter
    return (category_filter,)


@app.cell
def _(category_filter, df_categories, mo, pd):
    # カテゴリフィルタリング
    keywords = [k.strip() for k in category_filter.value.split(",") if k.strip()]
    df_sales_categories = pd.DataFrame()

    if len(df_categories) > 0 and keywords:
        pattern = "|".join(keywords)
        df_sales_categories = df_categories[
            df_categories["category"].str.contains(pattern, case=False, na=False)
        ].copy()
        mo.md(f"**営業支援関連カテゴリ**: {len(df_sales_categories):,} 件")
    return (df_sales_categories,)


@app.cell
def _(df_sales_categories, mo):
    mo.ui.table(df_sales_categories, pagination=True)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 3. 競合インテント分析（営業支援カテゴリ）
    """)
    return


@app.cell
def _(df_sales_categories, mo, pd, query_bq):
    # 営業支援カテゴリのインテント変動を取得
    df_competitor_intent = pd.DataFrame()

    if len(df_sales_categories) > 0:
        categories_list = df_sales_categories["category"].tolist()[:10]  # 上位10カテゴリ
        categories_sql = ",".join([f"'{c}'" for c in categories_list])

        competitor_intent_query = f"""
        SELECT 
            c.original_category_name as category,
            s.intent_level,
            CASE s.intent_level 
                WHEN 1 THEN 'Low' 
                WHEN 2 THEN 'Middle' 
                WHEN 3 THEN 'High' 
            END as level_name,
            COUNT(DISTINCT s.corporate_id) as company_count
        FROM `gree-dionysus-infobox.production_infobox.company_category_daily_v3` c
        JOIN `gree-dionysus-infobox.production_infobox.first_party_score_company_latest` s
          ON CAST(c.corporate_id AS INT64) = s.corporate_id
        WHERE c.view_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
          AND c.original_category_name IN ({categories_sql})
          AND s.intent_level IN (2, 3)
        GROUP BY 1, 2, 3
        ORDER BY 1, 2
        """
        df_competitor_intent = query_bq(competitor_intent_query)
    mo.md(f"**競合インテント（営業支援カテゴリ）**: {len(df_competitor_intent):,} 件")
    return (df_competitor_intent,)


@app.cell
def _(df_competitor_intent, mo):
    mo.ui.table(df_competitor_intent, pagination=True)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 4. ID紐づけ（COMPNO → ORGID → corporate_id）
    """)
    return


@app.cell
def _(SF_SCHEMA, mo, pd, query_sf):
    # Snowflake: BEEGLECOMPANY + USERORGANIZATION でORGIDを取得
    # BeegleCompany.ID → USERORGANIZATION.COMPANYID → USERORGANIZATION.ORGID（GAのorg_idと一致）
    df_id_mapping = pd.DataFrame()

    try:
        mapping_query = f"""
        SELECT
            bc.COMPNO,
            bc.ID AS COMPANYID,
            u.ORGID,
            u.NAME AS ORG_NAME,
            bc.SHOGO AS BQ_COMPANY_NAME,
            bc.KANA AS BQ_COMPANY_KANA,
            bc.CEO,
            bc.SETURITU AS ESTABLISHMENT,
            bc.ZIP,
            bc.ADD AS ADDRESS,
            bc.PREFID,
            bc.CITYID,
            bc.TEL AS PHONE,
            bc.HPURL AS WEBSITE_URL,
            bc.MAIL,
            bc.GYOSHUSHOID AS INDUSTRY_ID,
            bc.EMPID AS EMPLOYEE_ID,
            bc.EMPCOUNT AS EMPLOYEE_COUNT,
            bc.REVENUEID AS REVENUE_ID,
            bc.SHIHONID AS CAPITAL_ID,
            bc.ISCLOSED,
            bc.CREATEDAT AS BQ_CREATED_AT,
            u.CREATEDAT AS ORG_CREATED_AT
        FROM {SF_SCHEMA}.BEEGLECOMPANY bc
        JOIN {SF_SCHEMA}.USERORGANIZATION u
            ON bc.ID = u.COMPANYID
        WHERE bc.COMPNO IS NOT NULL
        """
        df_id_mapping = query_sf(mapping_query)
        df_id_mapping["COMPNO"] = df_id_mapping["COMPNO"].astype(str)
        mo.md(f"**BeegleCompany + USERORGANIZATION マッピング件数**: {len(df_id_mapping):,}")
    except Exception as e:
        mo.md(f"**Snowflake接続エラー**: `{e}`")
    return (df_id_mapping,)


@app.cell
def _(df_id_mapping, mo):
    mo.ui.table(df_id_mapping.head(20), pagination=True)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 5. GA利用データ（ページ群別）
    """)
    return


@app.cell
def _(GA_DATASET_ID, mo, query_bq):
    # GA4 org_id単位の基本指標 + ページ群別
    ga_query = f"""
    WITH base AS (
        SELECT
            user_pseudo_id,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
            (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'org_id') AS org_id,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
            event_name,
            event_date
        FROM `{GA_DATASET_ID}.events_*`
        WHERE NOT STARTS_WITH(_TABLE_SUFFIX, 'intraday_')
          AND _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))
    ),
    classified AS (
        SELECT
            org_id,
            user_pseudo_id,
            ga_session_id,
            event_name,
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
        COUNT(DISTINCT user_pseudo_id) AS users,
        COUNT(DISTINCT CONCAT(user_pseudo_id, '-', CAST(ga_session_id AS STRING))) AS sessions,
        COUNTIF(event_name = 'page_view') AS page_views,
        COUNTIF(page_group = 'list' AND event_name = 'page_view') AS pv_list,
        COUNTIF(page_group = 'company' AND event_name = 'page_view') AS pv_company,
        COUNTIF(page_group = 'download' AND event_name = 'page_view') AS pv_download,
        COUNTIF(page_group = 'search' AND event_name = 'page_view') AS pv_search,
        COUNTIF(page_group = 'settings' AND event_name = 'page_view') AS pv_settings
    FROM classified
    GROUP BY org_id
    """
    df_ga = query_bq(ga_query)
    mo.md(f"**GA利用データ（org_id単位）**: {len(df_ga):,} 件")
    return (df_ga,)


@app.cell
def _(df_ga, mo):
    mo.ui.table(df_ga.head(20), pagination=True)
    return


@app.cell
def _(mo):
    mo.md("""
    ### other内訳（page_location 上位）
    """)
    return


@app.cell
def _(GA_DATASET_ID, mo, query_bq):
    other_overall_query = f"""
    WITH base AS (
        SELECT
            (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'org_id') AS org_id,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
            event_name
        FROM `{GA_DATASET_ID}.events_*`
        WHERE NOT STARTS_WITH(_TABLE_SUFFIX, 'intraday_')
          AND _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))
    ),
    classified AS (
        SELECT
            org_id,
            page_location,
            event_name,
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
          AND page_location IS NOT NULL
    )
    SELECT
        page_location,
        COUNTIF(event_name = 'page_view') AS page_views
    FROM classified
    WHERE page_group = 'other'
    GROUP BY page_location
    ORDER BY page_views DESC
    LIMIT 50
    """

    other_by_org_query = f"""
    WITH base AS (
        SELECT
            (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'org_id') AS org_id,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
            event_name
        FROM `{GA_DATASET_ID}.events_*`
        WHERE NOT STARTS_WITH(_TABLE_SUFFIX, 'intraday_')
          AND _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))
    ),
    classified AS (
        SELECT
            org_id,
            page_location,
            event_name,
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
          AND page_location IS NOT NULL
    )
    SELECT
        org_id,
        page_location,
        COUNTIF(event_name = 'page_view') AS page_views
    FROM classified
    WHERE page_group = 'other'
    GROUP BY org_id, page_location
    ORDER BY page_views DESC
    LIMIT 200
    """

    df_other_pages = query_bq(other_overall_query)
    df_other_pages_by_org = query_bq(other_by_org_query)
    mo.md(
        f"**other内訳**: 全体 {len(df_other_pages):,} 件 / org_id別 {len(df_other_pages_by_org):,} 件"
    )
    return df_other_pages, df_other_pages_by_org


@app.cell
def _(df_other_pages, df_other_pages_by_org, mo):
    mo.md("#### 全体上位")
    mo.ui.table(df_other_pages, pagination=True)
    mo.md("#### org_id別 上位")
    mo.ui.table(df_other_pages_by_org, pagination=True)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 6. List分析（CompanyList / PeopleList）
    """)
    return


@app.cell
def _(SF_SCHEMA, df_id_mapping, mo, pd, query_sf):
    debug_outputs = []  # デバッグ出力を収集
    df_list_summary = pd.DataFrame()
    df_companylist_detail = pd.DataFrame()
    df_peoplelist_detail = pd.DataFrame()
    list_orgid_truncated = False

    list_orgids = (
        df_id_mapping["ORGID"].dropna().astype(str).unique().tolist()
        if len(df_id_mapping) > 0 and "ORGID" in df_id_mapping.columns
        else []
    )
    
    # デバッグ: list_orgids の状態を常に表示
    debug_outputs.append(mo.md(f"**List分析デバッグ**: df_id_mapping={len(df_id_mapping)}行, list_orgids={len(list_orgids)}件"))
    if list_orgids:
        debug_outputs.append(mo.md(f"**ORGIDサンプル（df_id_mapping）**: {list_orgids[:3]}"))
    
    if list_orgids:
        list_orgid_limit = 500
        if len(list_orgids) > list_orgid_limit:
            list_orgid_truncated = True
        list_orgids_sql = ",".join([f"'{orgid}'" for orgid in list_orgids[:list_orgid_limit]])

        try:
            # デバッグ1: 各テーブルの全体像
            debug_counts_query = f"""
            SELECT
                (SELECT COUNT(*) FROM {SF_SCHEMA}.USERORGANIZATION) AS total_userorg,
                (SELECT COUNT(*) FROM {SF_SCHEMA}.USERORGRELATION) AS total_userorgrel,
                (SELECT COUNT(*) FROM {SF_SCHEMA}.COMPANYLIST) AS total_companylist,
                (SELECT COUNT(*) FROM {SF_SCHEMA}.PEOPLELIST) AS total_peoplelist
            """
            df_debug_counts = query_sf(debug_counts_query)
            if len(df_debug_counts) > 0:
                dc = df_debug_counts.iloc[0].to_dict()
                debug_outputs.append(mo.md(
                    "**テーブル全体件数**: "
                    + f"USERORGANIZATION={dc.get('TOTAL_USERORG', dc.get('total_userorg'))}, "
                    + f"USERORGRELATION={dc.get('TOTAL_USERORGREL', dc.get('total_userorgrel'))}, "
                    + f"COMPANYLIST={dc.get('TOTAL_COMPANYLIST', dc.get('total_companylist'))}, "
                    + f"PEOPLELIST={dc.get('TOTAL_PEOPLELIST', dc.get('total_peoplelist'))}"
                ))
            
            # デバッグ2: ORGIDサンプル比較（各テーブルから独立して取得）
            userorg_sample_query = f"""
            SELECT ORGID
            FROM {SF_SCHEMA}.USERORGANIZATION
            LIMIT 3
            """
            df_userorg_sample = query_sf(userorg_sample_query)
            
            userorgrel_sample_query = f"""
            SELECT ORGANIZATIONID
            FROM {SF_SCHEMA}.USERORGRELATION
            LIMIT 3
            """
            df_userorgrel_sample = query_sf(userorgrel_sample_query)
            
            debug_outputs.append(mo.md(
                "**USERORGANIZATION.ORGID サンプル**:\n"
                + "\n".join([f"- `{row.get('ORGID', row.get('orgid'))}`" 
                           for _, row in df_userorg_sample.iterrows()])
            ))
            debug_outputs.append(mo.md(
                "**USERORGRELATION.ORGANIZATIONID サンプル**:\n"
                + "\n".join([f"- `{row.get('ORGANIZATIONID', row.get('organizationid'))}`" 
                           for _, row in df_userorgrel_sample.iterrows()])
            ))
            
            # デバッグ3: JOIN結果確認（CAST無しで直接比較）
            join_test_query = f"""
            SELECT
                COUNT(DISTINCT u.ORGID) AS userorg_matched,
                COUNT(DISTINCT ur.ORGANIZATIONID) AS userorgrel_matched
            FROM {SF_SCHEMA}.USERORGANIZATION u
            JOIN {SF_SCHEMA}.USERORGRELATION ur
                ON u.ORGID = ur.ORGANIZATIONID
            """
            df_join_test = query_sf(join_test_query)
            if len(df_join_test) > 0:
                jt = df_join_test.iloc[0].to_dict()
                debug_outputs.append(mo.md(
                    f"**JOIN直接比較（CAST無し）**: userorg_matched={jt.get('USERORG_MATCHED', jt.get('userorg_matched'))}, "
                    + f"userorgrel_matched={jt.get('USERORGREL_MATCHED', jt.get('userorgrel_matched'))}"
                ))
            
            # デバッグ4: フィルタ後JOIN結果
            list_debug_query = f"""
            SELECT
                COUNT(DISTINCT CAST(u.ORGID AS STRING)) AS userorg_count,
                COUNT(DISTINCT CAST(ur.ORGANIZATIONID AS STRING)) AS userorgrel_org_count,
                COUNT(DISTINCT cl.ID) AS companylist_count,
                COUNT(DISTINCT pl.ID) AS peoplelist_count
            FROM {SF_SCHEMA}.USERORGANIZATION u
            LEFT JOIN {SF_SCHEMA}.USERORGRELATION ur
                ON CAST(u.ORGID AS STRING) = CAST(ur.ORGANIZATIONID AS STRING)
            LEFT JOIN {SF_SCHEMA}.COMPANYLIST cl
                ON cl.USERORGRELATIONID = ur.ID
            LEFT JOIN {SF_SCHEMA}.PEOPLELIST pl
                ON pl.USERORGRELATIONID = ur.ID
            WHERE CAST(u.ORGID AS STRING) IN ({list_orgids_sql})
            """
            df_list_debug = query_sf(list_debug_query)
            if len(df_list_debug) > 0:
                debug_row = df_list_debug.iloc[0].to_dict()
                debug_outputs.append(mo.md(
                    "**フィルタ後JOIN結果**: "
                    + f"userorg={debug_row.get('USERORG_COUNT', debug_row.get('userorg_count'))}, "
                    + f"userorgrel={debug_row.get('USERORGREL_ORG_COUNT', debug_row.get('userorgrel_org_count'))}, "
                    + f"companylist={debug_row.get('COMPANYLIST_COUNT', debug_row.get('companylist_count'))}, "
                    + f"peoplelist={debug_row.get('PEOPLELIST_COUNT', debug_row.get('peoplelist_count'))}"
                ))

            # Step 1: org_rel だけ取得してデバッグ
            debug_outputs.append(mo.md("🔄 list_summary_query実行中..."))
            
            org_rel_query = f"""
            SELECT CAST(u.ORGID AS STRING) AS ORGID, ur.ID AS USERORGRELATIONID
            FROM {SF_SCHEMA}.USERORGANIZATION u
            JOIN {SF_SCHEMA}.USERORGRELATION ur
                ON u.ORGID = ur.ORGANIZATIONID
            WHERE u.ORGID IN ({list_orgids_sql})
            """
            df_org_rel = query_sf(org_rel_query)
            debug_outputs.append(mo.md(f"✅ org_rel取得: {len(df_org_rel)}行"))
            
            if len(df_org_rel) == 0:
                debug_outputs.append(mo.md("⚠️ org_relが0行のためスキップ"))
            else:
                # USERORGRELATIONIDのリスト作成
                userorgrel_ids = df_org_rel["USERORGRELATIONID"].dropna().unique().tolist()
                debug_outputs.append(mo.md(f"✅ USERORGRELATIONID: {len(userorgrel_ids)}件"))
                
                if userorgrel_ids:
                    userorgrel_ids_sql = ",".join([f"'{uid}'" for uid in userorgrel_ids[:500]])
                    
                    # CompanyList集計
                    companylist_query = f"""
                    SELECT 
                        USERORGRELATIONID,
                        COUNT(*) AS list_count
                    FROM {SF_SCHEMA}.COMPANYLIST
                    WHERE USERORGRELATIONID IN ({userorgrel_ids_sql})
                    GROUP BY USERORGRELATIONID
                    """
                    df_companylist_agg = query_sf(companylist_query)
                    debug_outputs.append(mo.md(f"✅ CompanyList集計: {len(df_companylist_agg)}行, 合計={df_companylist_agg['LIST_COUNT'].sum() if len(df_companylist_agg) > 0 and 'LIST_COUNT' in df_companylist_agg.columns else 0}"))
                    
                    # PeopleList集計
                    peoplelist_query = f"""
                    SELECT 
                        USERORGRELATIONID,
                        COUNT(*) AS list_count
                    FROM {SF_SCHEMA}.PEOPLELIST
                    WHERE USERORGRELATIONID IN ({userorgrel_ids_sql})
                    GROUP BY USERORGRELATIONID
                    """
                    df_peoplelist_agg = query_sf(peoplelist_query)
                    debug_outputs.append(mo.md(f"✅ PeopleList集計: {len(df_peoplelist_agg)}行, 合計={df_peoplelist_agg['LIST_COUNT'].sum() if len(df_peoplelist_agg) > 0 and 'LIST_COUNT' in df_peoplelist_agg.columns else 0}"))
                    
                    # org_rel と集計データをマージ
                    df_list_summary = df_org_rel[["ORGID", "USERORGRELATIONID"]].drop_duplicates()
                    
                    if len(df_companylist_agg) > 0:
                        col_name = "LIST_COUNT" if "LIST_COUNT" in df_companylist_agg.columns else "list_count"
                        df_companylist_agg = df_companylist_agg.rename(columns={col_name: "companylist_count"})
                        uorid_col = "USERORGRELATIONID" if "USERORGRELATIONID" in df_companylist_agg.columns else "userorgrelationid"
                        df_companylist_agg = df_companylist_agg.rename(columns={uorid_col: "USERORGRELATIONID"})
                        df_list_summary = df_list_summary.merge(
                            df_companylist_agg[["USERORGRELATIONID", "companylist_count"]],
                            on="USERORGRELATIONID", how="left"
                        )
                    else:
                        df_list_summary["companylist_count"] = 0
                    
                    if len(df_peoplelist_agg) > 0:
                        col_name = "LIST_COUNT" if "LIST_COUNT" in df_peoplelist_agg.columns else "list_count"
                        df_peoplelist_agg = df_peoplelist_agg.rename(columns={col_name: "peoplelist_count"})
                        uorid_col = "USERORGRELATIONID" if "USERORGRELATIONID" in df_peoplelist_agg.columns else "userorgrelationid"
                        df_peoplelist_agg = df_peoplelist_agg.rename(columns={uorid_col: "USERORGRELATIONID"})
                        df_list_summary = df_list_summary.merge(
                            df_peoplelist_agg[["USERORGRELATIONID", "peoplelist_count"]],
                            on="USERORGRELATIONID", how="left"
                        )
                    else:
                        df_list_summary["peoplelist_count"] = 0
                    
                    df_list_summary["companylist_count"] = df_list_summary["companylist_count"].fillna(0).astype(int)
                    df_list_summary["peoplelist_count"] = df_list_summary["peoplelist_count"].fillna(0).astype(int)
                    
                    # ORGIDでグループ化して集計
                    df_list_summary = df_list_summary.groupby("ORGID").agg({
                        "companylist_count": "sum",
                        "peoplelist_count": "sum"
                    }).reset_index()
                    
                    debug_outputs.append(mo.md(f"✅ df_list_summary構築完了: {len(df_list_summary)}行"))

            # CompanyList詳細（業種・地域カラム追加）
            companylist_detail_query = f"""
            SELECT
                u.ORGID,
                cl.ID AS LIST_ID,
                cl.CREATEDAT AS LIST_CREATED_AT,
                bc.ID AS COMPANY_ID,
                bc.SHOGO AS COMPANY_NAME,
                bc.GYOSHUSHOID AS INDUSTRY_ID,
                bc.PREFID,
                bc.EMPCOUNT AS EMPLOYEE_COUNT
            FROM {SF_SCHEMA}.USERORGANIZATION u
            JOIN {SF_SCHEMA}.USERORGRELATION ur ON u.ORGID = ur.ORGANIZATIONID
            JOIN {SF_SCHEMA}.COMPANYLIST cl ON cl.USERORGRELATIONID = ur.ID
            JOIN {SF_SCHEMA}._BEEGLECOMPANYTOCOMPANYLIST rel ON rel.B = cl.ID
            JOIN {SF_SCHEMA}.BEEGLECOMPANY bc ON rel.A = bc.ID
            WHERE u.ORGID IN ({list_orgids_sql})
            """
            df_companylist_detail = query_sf(companylist_detail_query)
            debug_outputs.append(mo.md(f"✅ CompanyList詳細: {len(df_companylist_detail)}行"))

            # PeopleList詳細（企業IDも追加）
            peoplelist_detail_query = f"""
            SELECT
                u.ORGID,
                pl.ID AS LIST_ID,
                pl.CREATEDAT AS LIST_CREATED_AT,
                km.ID AS KEYMAN_ID,
                km.NAME AS KEYMAN_NAME
            FROM {SF_SCHEMA}.USERORGANIZATION u
            JOIN {SF_SCHEMA}.USERORGRELATION ur ON u.ORGID = ur.ORGANIZATIONID
            JOIN {SF_SCHEMA}.PEOPLELIST pl ON pl.USERORGRELATIONID = ur.ID
            JOIN {SF_SCHEMA}._KEYMANTOPEOPLELIST rel ON rel.B = pl.ID
            JOIN {SF_SCHEMA}.KEYMAN km ON rel.A = km.ID
            WHERE u.ORGID IN ({list_orgids_sql})
            """
            df_peoplelist_detail = query_sf(peoplelist_detail_query)
            debug_outputs.append(mo.md(f"✅ PeopleList詳細: {len(df_peoplelist_detail)}行"))

            if len(df_companylist_detail) > 0:
                company_counts = (
                    df_companylist_detail.groupby("ORGID")["COMPANY_ID"].nunique().reset_index()
                )
                company_counts.rename(
                    columns={"COMPANY_ID": "companylist_company_count"}, inplace=True
                )
                company_top_series = (
                    df_companylist_detail.groupby("ORGID")["COMPANY_NAME"]
                    .apply(lambda series: " / ".join(sorted(series.dropna().unique())[:5]))
                    .reset_index()
                    .rename(columns={"COMPANY_NAME": "companylist_top_companies"})
                )
                df_list_summary = df_list_summary.merge(company_counts, on="ORGID", how="left")
                df_list_summary = df_list_summary.merge(
                    company_top_series, on="ORGID", how="left"
                )

            if len(df_peoplelist_detail) > 0:
                people_counts = (
                    df_peoplelist_detail.groupby("ORGID")["KEYMAN_ID"].nunique().reset_index()
                )
                people_counts.rename(
                    columns={"KEYMAN_ID": "peoplelist_keyman_count"}, inplace=True
                )
                people_top_series = (
                    df_peoplelist_detail.groupby("ORGID")["KEYMAN_NAME"]
                    .apply(lambda series: " / ".join(sorted(series.dropna().astype(str).unique())[:5]))
                    .reset_index()
                    .rename(columns={"KEYMAN_NAME": "peoplelist_top_keymen"})
                )
                df_list_summary = df_list_summary.merge(people_counts, on="ORGID", how="left")
                df_list_summary = df_list_summary.merge(
                    people_top_series, on="ORGID", how="left"
                )

            for col_name in [
                "companylist_company_count",
                "peoplelist_keyman_count",
                "companylist_top_companies",
                "peoplelist_top_keymen",
            ]:
                if col_name not in df_list_summary.columns:
                    df_list_summary[col_name] = None

        except Exception as exc:
            import traceback
            tb = traceback.format_exc()
            debug_outputs.append(mo.md(f"**List分析クエリエラー**: `{type(exc).__name__}: {exc}`\n\n```\n{tb}\n```"))
            df_list_summary = pd.DataFrame()
            df_companylist_detail = pd.DataFrame()
            df_peoplelist_detail = pd.DataFrame()

    if list_orgid_truncated:
        debug_outputs.append(mo.md("*ORGIDが多いため先頭500件のみでList分析しています*"))
    
    # デバッグ出力をまとめて表示
    debug_outputs.append(mo.md(f"**最終結果**: df_list_summary={len(df_list_summary)}行"))
    mo.vstack(debug_outputs)
    return df_companylist_detail, df_list_summary, df_peoplelist_detail


@app.cell
def _(df_list_summary, mo):
    mo.ui.table(df_list_summary.head(20), pagination=True)
    return


@app.cell
def _(df_id_mapping, df_list_summary, mo):
    # デバッグ情報
    debug_lines = ["**List Dropdown デバッグ情報**:"]
    
    debug_lines.append(f"- df_id_mapping: {len(df_id_mapping)} 行")
    debug_lines.append(f"- df_list_summary: {len(df_list_summary)} 行")
    
    if len(df_id_mapping) > 0 and "ORGID" in df_id_mapping.columns:
        id_map_orgids = df_id_mapping["ORGID"].dropna()
        debug_lines.append(f"- df_id_mapping ORGID型: {id_map_orgids.dtype}")
        debug_lines.append(f"- df_id_mapping ORGIDサンプル: {id_map_orgids.head(3).tolist()}")
    
    if len(df_list_summary) > 0 and "ORGID" in df_list_summary.columns:
        list_sum_orgids = df_list_summary["ORGID"].dropna()
        debug_lines.append(f"- df_list_summary ORGID型: {list_sum_orgids.dtype}")
        debug_lines.append(f"- df_list_summary ORGIDサンプル: {list_sum_orgids.head(3).tolist()}")
        
        # リストがある行のカウント
        has_companylist = (df_list_summary["companylist_count"].fillna(0) > 0).sum()
        has_peoplelist = (df_list_summary["peoplelist_count"].fillna(0) > 0).sum()
        debug_lines.append(f"- companylist_count > 0: {has_companylist} 行")
        debug_lines.append(f"- peoplelist_count > 0: {has_peoplelist} 行")
        
        # companylist_count, peoplelist_count の分布
        if "companylist_count" in df_list_summary.columns:
            debug_lines.append(f"- companylist_count 最大: {df_list_summary['companylist_count'].max()}")
        if "peoplelist_count" in df_list_summary.columns:
            debug_lines.append(f"- peoplelist_count 最大: {df_list_summary['peoplelist_count'].max()}")
    
    mo.md("\n".join(debug_lines))
    
    list_options = {}
    if len(df_id_mapping) > 0 and len(df_list_summary) > 0:
        list_name_cols = [c for c in ["BQ_COMPANY_NAME", "ORG_NAME"] if c in df_id_mapping.columns]
        df_list_option_base = df_id_mapping.dropna(subset=["ORGID"]).copy()
        if list_name_cols:
            df_list_option_base["company_label"] = df_list_option_base[list_name_cols[0]].astype(str)
        else:
            df_list_option_base["company_label"] = df_list_option_base["ORGID"].astype(str)

        orgids_with_lists = df_list_summary[
            (df_list_summary["companylist_count"].fillna(0) > 0)
            | (df_list_summary["peoplelist_count"].fillna(0) > 0)
        ]["ORGID"].astype(str).unique().tolist()
        
        debug_lines2 = [f"- orgids_with_lists: {len(orgids_with_lists)} 件"]
        if orgids_with_lists:
            debug_lines2.append(f"- orgids_with_listsサンプル: {orgids_with_lists[:3]}")
        mo.md("\n".join(debug_lines2))

        for _, list_row in df_list_option_base.drop_duplicates(subset=["ORGID"]).iterrows():
            if str(list_row["ORGID"]) in orgids_with_lists:
                list_label = f"{list_row['company_label']} ({list_row['ORGID']})"
                list_options[list_label] = list_row["ORGID"]

    list_org_options = list_options
    list_default_value = next(iter(list_org_options.keys()), None) if list_org_options else None
    list_org_selector = mo.ui.dropdown(
        options=list_org_options,
        label="List分析: 企業を選択（リスト1件以上）",
        value=list_default_value,
    )
    list_org_selector
    return list_org_options, list_org_selector


@app.cell
def _(
    df_companylist_detail,
    df_list_summary,
    df_peoplelist_detail,
    list_org_options,
    list_org_selector,
    pd,
):
    df_list_summary_selected = pd.DataFrame()
    df_companylist_detail_selected = pd.DataFrame()
    df_peoplelist_detail_selected = pd.DataFrame()

    selected_orgid = (
        list_org_options.get(list_org_selector.value) if list_org_selector.value else None
    )

    if selected_orgid:
        df_list_summary_selected = df_list_summary[
            df_list_summary["ORGID"] == selected_orgid
        ].copy()
        df_companylist_detail_selected = df_companylist_detail[
            df_companylist_detail["ORGID"] == selected_orgid
        ].copy()
        df_peoplelist_detail_selected = df_peoplelist_detail[
            df_peoplelist_detail["ORGID"] == selected_orgid
        ].copy()
    return (
        df_companylist_detail_selected,
        df_list_summary_selected,
        df_peoplelist_detail_selected,
    )


@app.cell
def _(
    df_companylist_detail_selected,
    df_list_summary_selected,
    df_peoplelist_detail_selected,
    list_org_selector,
    mo,
):
    _list_outputs = []
    
    if list_org_selector.value:
        _list_outputs.append(mo.md(f"### List分析: {list_org_selector.value}"))
        
        # スピナー付きで分析実行
        with mo.status.spinner(title="List分析中...") as _status:
            _status.update("CompanyList分析中...")
            
            # CompanyList分析
            if len(df_companylist_detail_selected) > 0:
                list_count = df_companylist_detail_selected["LIST_ID"].nunique()
                company_count = df_companylist_detail_selected["COMPANY_ID"].nunique()
                
                list_type_label = "(複数リスト運用)" if list_count > 1 else "(単一リスト)"
                _list_outputs.append(mo.md(f"""
**CompanyList サマリー**
- リスト数: **{list_count}件** {list_type_label}
- 登録企業数: **{company_count}社**
"""))
                
                # 業種分布
                if "INDUSTRY_ID" in df_companylist_detail_selected.columns:
                    industry_data = df_companylist_detail_selected["INDUSTRY_ID"].dropna()
                    if len(industry_data) > 0:
                        _list_outputs.append(mo.md("**業種分布（上位5件）**"))
                        industry_top = df_companylist_detail_selected.groupby("INDUSTRY_ID").size().nlargest(5).reset_index(name="件数")
                        _list_outputs.append(mo.ui.table(industry_top, pagination=False))
                
                # 地域分布
                if "PREFID" in df_companylist_detail_selected.columns:
                    region_data = df_companylist_detail_selected["PREFID"].dropna()
                    if len(region_data) > 0:
                        _list_outputs.append(mo.md("**地域分布（上位5件）**"))
                        region_top = df_companylist_detail_selected.groupby("PREFID").size().nlargest(5).reset_index(name="件数")
                        _list_outputs.append(mo.ui.table(region_top, pagination=False))
                
                # 従業員規模分布
                if "EMPLOYEE_COUNT" in df_companylist_detail_selected.columns:
                    emp_data = df_companylist_detail_selected["EMPLOYEE_COUNT"].dropna()
                    if len(emp_data) > 0:
                        avg_emp = emp_data.mean()
                        max_emp = emp_data.max()
                        _list_outputs.append(mo.md(f"**企業規模**: 平均従業員数 {avg_emp:.0f}人, 最大 {max_emp:.0f}人"))
            
            _status.update("PeopleList分析中...")
            
            # PeopleList分析
            if len(df_peoplelist_detail_selected) > 0:
                plist_count = df_peoplelist_detail_selected["LIST_ID"].nunique()
                keyman_count = df_peoplelist_detail_selected["KEYMAN_ID"].nunique()
                
                plist_type_label = "(複数リスト運用)" if plist_count > 1 else "(単一リスト)"
                _list_outputs.append(mo.md(f"""
**PeopleList サマリー**
- リスト数: **{plist_count}件** {plist_type_label}
- 登録人物数: **{keyman_count}人**
"""))
                
        
        # 詳細テーブル表示
        if len(df_companylist_detail_selected) > 0:
            _list_outputs.append(mo.md(f"**CompanyList詳細** ({len(df_companylist_detail_selected)}件)"))
            _list_outputs.append(mo.ui.table(df_companylist_detail_selected.head(50), pagination=True))
        
        if len(df_peoplelist_detail_selected) > 0:
            _list_outputs.append(mo.md(f"**PeopleList詳細** ({len(df_peoplelist_detail_selected)}件)"))
            _list_outputs.append(mo.ui.table(df_peoplelist_detail_selected.head(50), pagination=True))
        
        if len(df_companylist_detail_selected) == 0 and len(df_peoplelist_detail_selected) == 0:
            _list_outputs.append(mo.md("*この企業にはリストがありません*"))
    else:
        _list_outputs.append(mo.md("*企業を選択してください*"))
    
    mo.vstack(_list_outputs)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 7. データ統合（チャーン + GA + インテント）
    """)
    return


@app.cell
def _(
    df_churn,
    df_churn_reason_latest,
    df_ga,
    df_id_mapping,
    df_list_summary,
    mo,
    np,
    pd,
    query_bq,
):
    df_merged = pd.DataFrame()

    if len(df_id_mapping) > 0 and len(df_ga) > 0:
        # 1. チャーン（TSV）+ BeegleCompany + USERORGANIZATION（COMPNO で紐づけ）
        bq_cols = [
            "COMPNO", "COMPANYID", "ORGID", "ORG_NAME", "BQ_COMPANY_NAME", "BQ_COMPANY_KANA",
            "CEO", "ESTABLISHMENT", "ADDRESS", "PHONE", "WEBSITE_URL", "MAIL",
            "INDUSTRY_ID", "EMPLOYEE_ID", "EMPLOYEE_COUNT", "REVENUE_ID", "CAPITAL_ID",
            "ISCLOSED", "BQ_CREATED_AT", "ORG_CREATED_AT"
        ]
        bq_cols_exist = [c for c in bq_cols if c in df_id_mapping.columns]
        df_merged = df_churn.merge(
            df_id_mapping[bq_cols_exist],
            on="COMPNO",
            how="left",
        )

        # 2. 解約理由（CSV）を結合
        if len(df_churn_reason_latest) > 0 and "company_name_norm" in df_merged.columns:
            df_merged = df_merged.merge(
                df_churn_reason_latest,
                on="company_name_norm",
                how="left",
                suffixes=("", "_reason"),
            )

        # 3. GA指標を結合
        df_merged = df_merged.merge(
            df_ga,
            left_on="ORGID",
            right_on="org_id",
            how="left",
        )

        # 4. List分析（Snowflake）
        if len(df_list_summary) > 0:
            df_merged = df_merged.merge(
                df_list_summary,
                on="ORGID",
                how="left",
                suffixes=("", "_list"),
            )

        # 5. 派生指標
        df_merged["sessions_per_user"] = df_merged["sessions"] / df_merged["users"].replace(0, np.nan)
        df_merged["page_views_per_user"] = df_merged["page_views"] / df_merged["users"].replace(0, np.nan)

        # 6. ページ利用率
        total_pv = df_merged["page_views"].replace(0, np.nan)
        df_merged["list_rate"] = df_merged["pv_list"] / total_pv * 100
        df_merged["company_rate"] = df_merged["pv_company"] / total_pv * 100
        df_merged["download_rate"] = df_merged["pv_download"] / total_pv * 100
        df_merged["search_rate"] = df_merged["pv_search"] / total_pv * 100

        # 7. インテントスコア取得（BigQuery first_party_score）
        # ORGIDをcorporate_idとして使用（要確認）
        orgids = df_merged["ORGID"].dropna().unique().tolist()
        if orgids:
            orgids_sql = ",".join([f"'{o}'" for o in orgids[:500]])  # 上限500
            intent_query = f"""
            SELECT 
                CAST(first_party_corporate_id AS STRING) AS org_id,
                AVG(CASE WHEN intent_level = 3 THEN 1 ELSE 0 END) * 100 AS high_intent_rate,
                AVG(CASE WHEN intent_level = 2 THEN 1 ELSE 0 END) * 100 AS middle_intent_rate,
                COUNT(DISTINCT corporate_id) AS intent_company_count,
                MAX(change_date) AS latest_intent_change_date,
                MAX(CASE WHEN intent_level = 3 THEN 1 ELSE 0 END) AS intent_activation_flag,
                MAX(
                    CASE
                        WHEN change_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                        THEN 1
                        ELSE 0
                    END
                ) AS intent_recent_change_flag
            FROM `gree-dionysus-infobox.production_infobox.first_party_score_company_latest`
            WHERE CAST(first_party_corporate_id AS STRING) IN ({orgids_sql})
            GROUP BY 1
            """
            try:
                df_intent = query_bq(intent_query)
                if len(df_intent) > 0:
                    df_merged = df_merged.merge(
                        df_intent,
                        left_on="ORGID",
                        right_on="org_id",
                        how="left",
                        suffixes=("", "_intent"),
                    )
            except Exception:
                pass

            sfa_intent_query = f"""
            SELECT 
                CAST(s.first_party_corporate_id AS STRING) AS org_id,
                MAX(
                    CASE
                        WHEN REGEXP_CONTAINS(c.original_category_name, r'(SFA|営業支援|CRM|セールス)') THEN 1
                        ELSE 0
                    END
                ) AS sfa_intent_flag,
                COUNT(DISTINCT CASE
                    WHEN REGEXP_CONTAINS(c.original_category_name, r'(SFA|営業支援|CRM|セールス)')
                    THEN s.corporate_id
                    ELSE NULL
                END) AS sfa_intent_company_count
            FROM `gree-dionysus-infobox.production_infobox.company_category_daily_v3` c
            JOIN `gree-dionysus-infobox.production_infobox.first_party_score_company_latest` s
              ON CAST(c.corporate_id AS INT64) = s.corporate_id
            WHERE c.view_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
              AND s.change_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
              AND s.intent_level IN (2, 3)
              AND CAST(s.first_party_corporate_id AS STRING) IN ({orgids_sql})
            GROUP BY 1
            """
            try:
                df_sfa_intent = query_bq(sfa_intent_query)
                if len(df_sfa_intent) > 0:
                    df_merged = df_merged.merge(
                        df_sfa_intent,
                        left_on="ORGID",
                        right_on="org_id",
                        how="left",
                        suffixes=("", "_sfa"),
                    )
            except Exception:
                pass

        if "sfa_intent_flag" in df_merged.columns:
            df_merged["sfa_intent_flag"] = (
                df_merged["sfa_intent_flag"].fillna(0).astype(int)
            )
        else:
            df_merged["sfa_intent_flag"] = 0

        if "sfa_intent_company_count" in df_merged.columns:
            df_merged["sfa_intent_company_count"] = (
                df_merged["sfa_intent_company_count"].fillna(0).astype(int)
            )
        else:
            df_merged["sfa_intent_company_count"] = 0

        if "intent_activation_flag" in df_merged.columns:
            df_merged["intent_activation_flag"] = (
                df_merged["intent_activation_flag"].fillna(0).astype(int)
            )
        else:
            df_merged["intent_activation_flag"] = 0

        if "intent_recent_change_flag" in df_merged.columns:
            df_merged["intent_recent_change_flag"] = (
                df_merged["intent_recent_change_flag"].fillna(0).astype(int)
            )
        else:
            df_merged["intent_recent_change_flag"] = 0

        high_series = (
            df_merged["high_intent_rate"]
            if "high_intent_rate" in df_merged.columns
            else pd.Series(0, index=df_merged.index)
        )
        middle_series = (
            df_merged["middle_intent_rate"]
            if "middle_intent_rate" in df_merged.columns
            else pd.Series(0, index=df_merged.index)
        )
        sfa_series = (
            df_merged["sfa_intent_flag"]
            if "sfa_intent_flag" in df_merged.columns
            else pd.Series(0, index=df_merged.index)
        )
        df_merged["intent_interest_flag"] = (
            (high_series.fillna(0) > 0)
            | (middle_series.fillna(0) > 0)
            | (sfa_series.fillna(0) > 0)
        ).astype(int)

        df_merged["intent_recent_change_activation_flag"] = (
            (df_merged["intent_recent_change_flag"] > 0)
            & (df_merged["intent_activation_flag"] > 0)
        ).astype(int)

        mo.md(
            f"""
            **統合結果**:
            - 統合後行数: {len(df_merged):,}
            - ORGID結合成功: {df_merged['ORGID'].notna().sum():,}
            - GA結合成功: {df_merged['sessions'].notna().sum():,}
            """
        )
    else:
        mo.md("*データ統合に必要なデータが揃っていません*")
    return (df_merged,)


@app.cell
def _(df_merged, mo):
    # 会社名カラムを探す
    company_col = next((c for c in ["BQ_COMPANY_NAME", "CompanyName", "COMPANY_NAME"] if c in df_merged.columns), None) if len(df_merged) > 0 else None
    display_cols = [
        company_col, "INDUSTRY_ID", "EMPLOYEE_COUNT", "ADDRESS",
        "status", "is_churned", "ORGID",
        "sessions", "page_views", "users",
        "list_rate", "download_rate", "search_rate",
        "sfa_intent_flag", "sfa_intent_company_count", "intent_interest_flag",
        "intent_activation_flag",
        "intent_recent_change_flag",
        "intent_recent_change_activation_flag",
        "latest_intent_change_date",
        "companylist_count", "peoplelist_count",
        "companylist_company_count", "peoplelist_keyman_count",
        "companylist_top_companies", "peoplelist_top_keymen",
        "loss_type", "loss_reason", "loss_detail",
    ]
    cols_exist = [c for c in display_cols if c and c in df_merged.columns] if len(df_merged) > 0 else []
    df_display = df_merged[cols_exist].head(30) if cols_exist else df_merged.head(30)
    mo.ui.table(df_display, pagination=True)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 8. ロジスティック回帰（チャーン寄与度）
    """)
    return


@app.cell
def _(df_merged, mo, pd):
    df_importance = pd.DataFrame()

    if len(df_merged) > 0:
        try:
            from sklearn.linear_model import LogisticRegression
            from sklearn.preprocessing import StandardScaler

            # 特徴量選択
            feature_cols = [
                "sessions", "page_views", "users",
                "sessions_per_user", "page_views_per_user",
                "pv_list", "pv_company", "pv_download", "pv_search",
                "list_rate", "download_rate", "search_rate",
            ]
            # インテント指標があれば追加
            if "high_intent_rate" in df_merged.columns:
                feature_cols.extend(["high_intent_rate", "middle_intent_rate", "intent_company_count"])

            feature_cols = [c for c in feature_cols if c in df_merged.columns]

            # 欠損値を除外
            df_ml = df_merged[["is_churned"] + feature_cols].dropna()

            if len(df_ml) > 10:
                X = df_ml[feature_cols]
                y = df_ml["is_churned"]

                # 標準化
                scaler = StandardScaler()
                X_scaled = scaler.fit_transform(X)

                # ロジスティック回帰
                model_logit = LogisticRegression(max_iter=1000, random_state=42)
                model_logit.fit(X_scaled, y)

                importance_data = []
                for feat_col, coef in zip(feature_cols, model_logit.coef_[0]):
                    importance_data.append({
                        "特徴量": feat_col,
                        "係数": round(coef, 4),
                        "効果方向": "解約促進" if coef > 0 else "継続促進",
                        "重要度": round(abs(coef), 4),
                    })
                df_importance = pd.DataFrame(importance_data).sort_values("重要度", ascending=False)

                mo.md(f"**学習完了**: サンプル数 {len(df_ml)}, 特徴量数 {len(feature_cols)}")
            else:
                mo.md("*サンプル数が不足しています（10件以上必要）*")

        except ImportError as e:
            mo.md(f"**scikit-learn未インストール**: `{e}`")
        except Exception as e:
            mo.md(f"**ML実行エラー**: `{e}`")
    else:
        mo.md("*データ統合が完了するまでお待ちください*")
    return (df_importance,)


@app.cell
def _(df_importance, mo):
    mo.md("### ロジスティック回帰 寄与度")
    mo.ui.table(df_importance, pagination=False)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 9. LLM危険度判定（Gemini）
    """)
    return


@app.cell
def _(df_merged, mo):
    llm_company_options = {}
    if len(df_merged) > 0:
        llm_name_cols = [c for c in ["BQ_COMPANY_NAME", "ORG_NAME", "CompanyName"] if c in df_merged.columns]
        df_llm_company_base = df_merged.dropna(subset=["ORGID"]).copy()
        if llm_name_cols:
            df_llm_company_base["company_label"] = df_llm_company_base[llm_name_cols[0]].astype(str)
        else:
            df_llm_company_base["company_label"] = df_llm_company_base["ORGID"].astype(str)

        for _, llm_company_row in df_llm_company_base.drop_duplicates(subset=["ORGID"]).iterrows():
            llm_label = f"{llm_company_row['company_label']} ({llm_company_row['ORGID']})"
            llm_company_options[llm_label] = llm_company_row["ORGID"]

    llm_company_default_value = next(iter(llm_company_options.keys()), None) if llm_company_options else None
    company_selector = mo.ui.dropdown(
        options=llm_company_options,
        label="会社を選択",
        value=llm_company_default_value,
    )
    company_selector
    return company_selector, llm_company_options


@app.cell
def _(company_selector, df_merged, pd):
    df_company_detail = pd.DataFrame()

    # company_selector.value は既にORGID（dropdown の dict では .value が値を直接返す）
    selected_company_orgid = company_selector.value

    if len(df_merged) > 0 and selected_company_orgid:
        df_company_detail = df_merged[
            df_merged["ORGID"] == selected_company_orgid
        ].copy()
    return (df_company_detail,)


@app.cell
def _(company_selector, df_company_detail, mo):
    _company_outputs = []
    
    if company_selector.value and len(df_company_detail) > 0:
        _company_outputs.append(mo.md(f"**選択企業**: {company_selector.value} ({len(df_company_detail)} 件)"))
        _company_outputs.append(mo.ui.table(df_company_detail.head(10), pagination=True))
    else:
        _company_outputs.append(mo.md("*企業を選択してください*"))
    
    return mo.vstack(_company_outputs)


@app.cell
def _(mo):
    run_llm_button = mo.ui.run_button(label="LLM危険度判定を実行")
    run_llm_button
    return (run_llm_button,)


@app.cell
def _(
    GA_DATASET_ID,
    SF_SCHEMA,
    company_selector,
    df_company_detail,
    extract_json_block,
    genai,
    genai_error,
    genai_types,
    json,
    mo,
    os,
    query_bq,
    query_sf,
    run_llm_button,
):
    _llm_outputs = []
    
    # ドロップダウンで選択された企業を使用
    if len(df_company_detail) > 0:
        sample_company = df_company_detail.iloc[0:1].copy()
        row = sample_company.iloc[0]
        
        # 複数カラムからNaNでない最初の値を取得（フォールバックチェーン）
        import pandas as _pd_check
        company_name = (
            row.get("CompanyName") 
            if _pd_check.notna(row.get("CompanyName")) 
            else row.get("BQ_COMPANY_NAME") 
            if _pd_check.notna(row.get("BQ_COMPANY_NAME"))
            else row.get("ORG_NAME")
            if _pd_check.notna(row.get("ORG_NAME"))
            else "N/A"
        )
        orgid = row.get("ORGID") if _pd_check.notna(row.get("ORGID")) else "N/A"
        
        _llm_outputs.append(mo.md(f"### 選択企業: {company_name}"))
        _llm_outputs.append(mo.md(f"ORGID: `{orgid}`"))
        
        # デバッグ: ボタンとgenaiの状態
        _llm_outputs.append(mo.md(f"*Debug: button={run_llm_button.value}, genai={genai is not None}, genai_error={genai_error}*"))
        
        if run_llm_button.value and genai is not None:
            # スピナー付きでLLM処理実行
            with mo.status.spinner(title="分析データ取得 + LLM処理中...") as _status:
                try:
                    # === 1. GA 6ヶ月推移を取得 ===
                    _status.update("GA利用推移を取得中（6ヶ月）...")
                    ga_trend_query = f"""
                    WITH base AS (
                        SELECT
                            FORMAT_DATE('%Y-%m', PARSE_DATE('%Y%m%d', event_date)) AS month,
                            (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'org_id') AS org_id,
                            user_pseudo_id,
                            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
                            event_name,
                            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location
                        FROM `{GA_DATASET_ID}.events_*`
                        WHERE _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH))
                          AND (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'org_id') = '{orgid}'
                    )
                    SELECT
                        month,
                        COUNT(DISTINCT user_pseudo_id) AS users,
                        COUNT(DISTINCT CONCAT(user_pseudo_id, '-', CAST(session_id AS STRING))) AS sessions,
                        COUNTIF(event_name = 'page_view') AS page_views,
                        COUNTIF(REGEXP_CONTAINS(page_location, r'/list') AND event_name = 'page_view') AS pv_list,
                        COUNTIF(REGEXP_CONTAINS(page_location, r'/company') AND event_name = 'page_view') AS pv_company,
                        COUNTIF(REGEXP_CONTAINS(page_location, r'/download|csv') AND event_name = 'page_view') AS pv_download,
                        COUNTIF(REGEXP_CONTAINS(page_location, r'/search') AND event_name = 'page_view') AS pv_search
                    FROM base
                    GROUP BY month
                    ORDER BY month
                    """
                    try:
                        df_ga_trend = query_bq(ga_trend_query)
                        ga_trend_json = df_ga_trend.to_json(orient="records", force_ascii=False) if len(df_ga_trend) > 0 else "[]"
                        _llm_outputs.append(mo.md(f"✅ GA推移取得: {len(df_ga_trend)}行"))
                    except Exception as ga_err:
                        df_ga_trend = None
                        ga_trend_json = "[]"
                        _llm_outputs.append(mo.md(f"⚠️ GA推移取得エラー: `{ga_err}`"))
                    
                    # === 2. Snowflake アクティビティ推移を取得 ===
                    _status.update("Snowflakeアクティビティ推移を取得中...")
                    
                    # CompanyList作成推移
                    list_trend_query = f"""
                    SELECT
                        DATE_TRUNC('month', cl.CREATEDAT) AS MONTH,
                        COUNT(DISTINCT cl.ID) AS COMPANYLIST_CREATED
                    FROM {SF_SCHEMA}.USERORGANIZATION u
                    JOIN {SF_SCHEMA}.USERORGRELATION ur ON u.ORGID = ur.ORGANIZATIONID
                    JOIN {SF_SCHEMA}.COMPANYLIST cl ON cl.USERORGRELATIONID = ur.ID
                    WHERE u.ORGID = '{orgid}'
                      AND cl.CREATEDAT >= DATEADD('month', -6, CURRENT_DATE())
                    GROUP BY MONTH
                    ORDER BY MONTH
                    """
                    try:
                        df_list_trend = query_sf(list_trend_query)
                        list_trend_json = df_list_trend.to_json(orient="records", force_ascii=False, date_format="iso") if len(df_list_trend) > 0 else "[]"
                        _llm_outputs.append(mo.md(f"✅ CompanyList推移: {len(df_list_trend)}行"))
                    except Exception as cl_err:
                        df_list_trend = None
                        list_trend_json = "[]"
                        _llm_outputs.append(mo.md(f"⚠️ CompanyList推移エラー: `{cl_err}`"))
                    
                    # PeopleList作成推移（キーマン登録数）
                    peoplelist_trend_query = f"""
                    SELECT
                        DATE_TRUNC('month', pl.CREATEDAT) AS MONTH,
                        COUNT(DISTINCT pl.ID) AS PEOPLELIST_CREATED,
                        COUNT(DISTINCT k.ID) AS KEYMAN_REGISTERED
                    FROM {SF_SCHEMA}.USERORGANIZATION u
                    JOIN {SF_SCHEMA}.USERORGRELATION ur ON u.ORGID = ur.ORGANIZATIONID
                    JOIN {SF_SCHEMA}.PEOPLELIST pl ON pl.USERORGRELATIONID = ur.ID
                    LEFT JOIN {SF_SCHEMA}._KEYMANTOPEOPLELIST rel ON rel.B = pl.ID
                    LEFT JOIN {SF_SCHEMA}.KEYMAN k ON rel.A = k.ID
                    WHERE u.ORGID = '{orgid}'
                      AND pl.CREATEDAT >= DATEADD('month', -6, CURRENT_DATE())
                    GROUP BY MONTH
                    ORDER BY MONTH
                    """
                    try:
                        df_peoplelist_trend = query_sf(peoplelist_trend_query)
                        peoplelist_trend_json = df_peoplelist_trend.to_json(orient="records", force_ascii=False, date_format="iso") if len(df_peoplelist_trend) > 0 else "[]"
                        _llm_outputs.append(mo.md(f"✅ PeopleList推移: {len(df_peoplelist_trend)}行"))
                    except Exception as pl_err:
                        df_peoplelist_trend = None
                        peoplelist_trend_json = "[]"
                        _llm_outputs.append(mo.md(f"⚠️ PeopleList推移エラー: `{pl_err}`"))
                    
                    # Memo作成推移
                    memo_trend_query = f"""
                    SELECT
                        DATE_TRUNC('month', m.CREATEDAT) AS MONTH,
                        COUNT(*) AS MEMO_CREATED
                    FROM {SF_SCHEMA}.USERORGANIZATION u
                    JOIN {SF_SCHEMA}.USERORGRELATION ur ON u.ORGID = ur.ORGANIZATIONID
                    JOIN {SF_SCHEMA}.MEMO m ON m.USERORGRELATIONID = ur.ID
                    WHERE u.ORGID = '{orgid}'
                      AND m.CREATEDAT >= DATEADD('month', -6, CURRENT_DATE())
                    GROUP BY MONTH
                    ORDER BY MONTH
                    """
                    try:
                        df_memo_trend = query_sf(memo_trend_query)
                        memo_trend_json = df_memo_trend.to_json(orient="records", force_ascii=False, date_format="iso") if len(df_memo_trend) > 0 else "[]"
                        _llm_outputs.append(mo.md(f"✅ メモ推移: {len(df_memo_trend)}行"))
                    except Exception as memo_err:
                        df_memo_trend = None
                        memo_trend_json = "[]"
                        _llm_outputs.append(mo.md(f"⚠️ メモ推移エラー: `{memo_err}`"))
                    
                    # === 3. 推移データを表示 ===
                    _llm_outputs.append(mo.md("### GA利用推移（6ヶ月）"))
                    if df_ga_trend is not None and len(df_ga_trend) > 0:
                        _llm_outputs.append(mo.ui.table(df_ga_trend, pagination=False))
                    else:
                        _llm_outputs.append(mo.md("*GAデータなし*"))
                    
                    _llm_outputs.append(mo.md("### Snowflakeアクティビティ推移"))
                    _llm_outputs.append(mo.md("**CompanyList作成推移**"))
                    if df_list_trend is not None and len(df_list_trend) > 0:
                        _llm_outputs.append(mo.ui.table(df_list_trend, pagination=False))
                    else:
                        _llm_outputs.append(mo.md("*CompanyList作成なし*"))
                    
                    _llm_outputs.append(mo.md("**PeopleList作成推移（キーマン登録含む）**"))
                    if df_peoplelist_trend is not None and len(df_peoplelist_trend) > 0:
                        _llm_outputs.append(mo.ui.table(df_peoplelist_trend, pagination=False))
                    else:
                        _llm_outputs.append(mo.md("*PeopleList作成なし*"))
                    
                    _llm_outputs.append(mo.md("**メモ作成推移**"))
                    if df_memo_trend is not None and len(df_memo_trend) > 0:
                        _llm_outputs.append(mo.ui.table(df_memo_trend, pagination=False))
                    else:
                        _llm_outputs.append(mo.md("*メモ作成なし*"))
                    
                    # === 4. LLM処理 ===
                    _status.update("環境変数読み込み中...")
                    
                    # .envから直接読み込み
                    import dotenv as _dotenv_mod
                    import pathlib as _pathlib_mod
                    _llm_env_path = _pathlib_mod.Path("/Users/kou1904/githubactions_fordata/work/aieda_agent/.env")
                    if _llm_env_path.exists():
                        _dotenv_mod.load_dotenv(_llm_env_path, override=True)
                    
                    # Gemini 3 Pro Preview
                    api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
                    if not api_key:
                        raise ValueError("GEMINI_API_KEY または GOOGLE_API_KEY が設定されていません")
                    
                    _status.update("Geminiクライアント初期化中...")
                    client = genai.Client(api_key=api_key)

                    # 会社データを整形
                    company_data = sample_company.iloc[0].to_dict()
                    company_json = json.dumps({k: str(v) for k, v in company_data.items() if v is not None}, ensure_ascii=False, indent=2)

                    prompt = f"""
    あなたは営業支援SaaS「InfoBox」のカスタマーサクセス担当です。
    以下の会社データと利用推移を分析し、チャーン（解約）リスクを判定してください。

    ## 会社基本データ
    ```json
    {company_json}
    ```

    ## GA利用推移（6ヶ月）
    - users: ログインユーザー数
    - sessions: セッション数
    - page_views: PV数
    - pv_list: リストページアクセス
    - pv_company: 企業詳細アクセス
    - pv_download: ダウンロードアクセス
    - pv_search: 検索アクセス
    ```json
    {ga_trend_json}
    ```

    ## Snowflakeアクティビティ推移（6ヶ月）
    ### CompanyList作成数（企業リスト）
    ```json
    {list_trend_json}
    ```
    ### PeopleList作成数（キーマンリスト）
    - PEOPLELIST_CREATED: 作成されたPeopleList数
    - KEYMAN_REGISTERED: 登録されたキーマン数
    ```json
    {peoplelist_trend_json}
    ```
    ### メモ作成数
    ```json
    {memo_trend_json}
    ```

    ## 判定基準
    - **High（70-100点）**: 解約リスクが高い
      - 利用率が減少傾向
      - CompanyList/PeopleList/メモ作成が停滞
      - 競合検討の兆候
    - **Medium（40-69点）**: 注意が必要
      - 利用頻度に波がある
      - 一部機能のみ活用（例: CompanyListは使うがPeopleListは未使用）
    - **Low（0-39点）**: 安定
      - 継続的な利用
      - 複数機能を活用（CompanyList + PeopleList + メモ）
      - アクティビティが維持/増加

    ## 出力形式（JSON）
    ```json
    {{
      "risk_level": "High" | "Medium" | "Low",
      "risk_score": 0-100,
      "reason": "判定理由（日本語、3文以内）",
      "key_signals": ["シグナル1", "シグナル2"],
      "recommended_actions": ["アクション1", "アクション2"],
      "trend_analysis": "推移データからの分析（日本語、2文以内）"
    }}
    ```
    - JSONのみ返す（余計な説明やコードフェンス不要）
    """

                    _status.update("Gemini API呼び出し中（10-30秒かかります）...")
                    
                    response = client.models.generate_content(
                        model="gemini-3-pro-preview",
                        contents=prompt,
                        config=genai_types.GenerateContentConfig(
                            temperature=0.1,
                            max_output_tokens=2000,
                        ),
                    )

                    _status.update("応答解析中...")
                    llm_result = response.text
                    llm_json = extract_json_block(llm_result) or llm_result
                    
                except Exception as e:
                    _llm_outputs.append(mo.md(f"❌ **LLM実行エラー**: `{e}`"))
                    llm_json = None
            
            # 処理完了後の結果表示
            if llm_json:
                _llm_outputs.append(mo.md("✅ **LLM処理完了**"))
                _llm_outputs.append(mo.md("### LLM判定結果"))
                _llm_outputs.append(mo.md(f"```json\n{llm_json}\n```"))

                # JSONをパースしてリスクレベルを可視化
                try:
                    llm_parsed = json.loads(llm_json)
                    risk_level = llm_parsed.get("risk_level", "Unknown")
                    risk_score = llm_parsed.get("risk_score", 0)
                    reason = llm_parsed.get("reason", "")
                    key_signals = llm_parsed.get("key_signals", [])
                    actions = llm_parsed.get("recommended_actions", [])
                    trend_analysis = llm_parsed.get("trend_analysis", "")
                    
                    color = {"High": "red", "Medium": "orange", "Low": "green"}.get(risk_level, "gray")
                    
                    _llm_outputs.append(mo.md(f"""
### リスク判定サマリー

| 項目 | 値 |
|------|-----|
| **リスクレベル** | <span style="color:{color};font-weight:bold">{risk_level}</span> |
| **リスクスコア** | {risk_score}/100 |
| **判定理由** | {reason} |
| **推移分析** | {trend_analysis} |
| **主要シグナル** | {', '.join(key_signals) if key_signals else '-'} |
| **推奨アクション** | {', '.join(actions) if actions else '-'} |
"""))
                except Exception:
                    pass
                    
        elif genai_error:
            _llm_outputs.append(mo.md(f"**Gemini未設定**: `{genai_error}`"))
        else:
            _llm_outputs.append(mo.md("*「LLM危険度判定を実行」ボタンをクリックしてください*"))
    else:
        _llm_outputs.append(mo.md("*上のドロップダウンから企業を選択してください*"))
    
    mo.vstack(_llm_outputs)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 10. ダッシュボード全体サマリー
    """)
    return


@app.cell
def _(df_competitor_intent, df_importance, df_merged, mo):
    summary_parts = []

    if len(df_merged) > 0:
        churned = df_merged["is_churned"].sum()
        active = len(df_merged) - churned
        summary_parts.append(f"- **企業数**: 解約 {churned}, 契約中 {active}")

        if "sessions" in df_merged.columns:
            avg_sessions = df_merged[df_merged["is_churned"] == 0]["sessions"].mean()
            avg_sessions_churned = df_merged[df_merged["is_churned"] == 1]["sessions"].mean()
            summary_parts.append(f"- **平均セッション**: 契約中 {avg_sessions:.1f}, 解約 {avg_sessions_churned:.1f}")

    if len(df_importance) > 0:
        top_factors = df_importance.head(3)["特徴量"].tolist()
        summary_parts.append(f"- **チャーン寄与度TOP3**: {', '.join(top_factors)}")

    if len(df_competitor_intent) > 0:
        high_intent = df_competitor_intent[df_competitor_intent["level_name"] == "High"]["company_count"].sum()
        summary_parts.append(f"- **競合カテゴリ High インテント**: {high_intent:,} 社")

    if summary_parts:
        mo.md("### サマリー\n" + "\n".join(summary_parts))
    else:
        mo.md("*データを読み込んでください*")
    summary_parts
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

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
    # 実際のURL構造:
    #   /companies (一覧=検索), /companies/[id] (企業詳細)
    #   /company-lists, /people-lists, /leads-lists (リスト系)
    #   /analysis (トレンド/1stParty/CRM連携)
    #   /people (人物)
    #   /settings
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
                WHEN REGEXP_CONTAINS(page_location, r'/company-lists|/people-lists|/leads-lists') THEN 'list'
                WHEN REGEXP_CONTAINS(page_location, r'/companies/[a-z0-9]') THEN 'company_detail'
                WHEN REGEXP_CONTAINS(page_location, r'/analysis') THEN 'analysis'
                WHEN REGEXP_CONTAINS(page_location, r'/people') THEN 'people'
                WHEN REGEXP_CONTAINS(page_location, r'/settings') THEN 'settings'
                WHEN REGEXP_CONTAINS(page_location, r'/sign-in|/sign-up|/org-selection') THEN 'auth'
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
        COUNTIF(page_group = 'company_detail' AND event_name = 'page_view') AS pv_company_detail,
        COUNTIF(page_group = 'list' AND event_name = 'page_view') AS pv_list,
        COUNTIF(page_group = 'analysis' AND event_name = 'page_view') AS pv_analysis,
        COUNTIF(page_group = 'people' AND event_name = 'page_view') AS pv_people,
        COUNTIF(page_group = 'settings' AND event_name = 'page_view') AS pv_settings
    FROM classified
    GROUP BY org_id
    """
    df_ga = query_bq(ga_query)
    mo.md(f"**GA利用データ（org_id単位）**: {len(df_ga):,} 件")
    return (df_ga,)


@app.cell
def _(df_ga, mo, pd):
    import altair as _alt

    _ga_summary_outputs = []

    if len(df_ga) > 0:
        _total_orgs = len(df_ga)
        _pv_cols = {
            "pv_company_detail": "企業詳細",
            "pv_list": "リスト系",
            "pv_analysis": "分析",
            "pv_people": "人物",
            "pv_settings": "設定",
        }

        # 各ページにアクセスした企業数と割合
        _summary_rows = []
        for _col, _label in _pv_cols.items():
            if _col in df_ga.columns:
                _org_count = int((df_ga[_col] > 0).sum())
                _pv_total = int(df_ga[_col].sum())
                _rate = round(_org_count / _total_orgs * 100, 1) if _total_orgs > 0 else 0
                _summary_rows.append({
                    "ページ種別": _label,
                    "利用企業数": _org_count,
                    "利用率(%)": _rate,
                    "総PV": _pv_total,
                    "企業あたり平均PV": round(_pv_total / _org_count, 1) if _org_count > 0 else 0,
                })

        _df_summary = pd.DataFrame(_summary_rows)
        _ga_summary_outputs.append(mo.md(f"### 機能別利用状況（分母: GA登録企業 **{_total_orgs}** 社）"))

        # 棒グラフ: 利用企業数 | 利用率
        _chart_count = (
            _alt.Chart(_df_summary)
            .mark_bar()
            .encode(
                x=_alt.X("利用企業数:Q", title="利用企業数"),
                y=_alt.Y("ページ種別:N", title="", sort=list(_pv_cols.values())),
                color=_alt.Color("ページ種別:N", legend=None),
                tooltip=["ページ種別", "利用企業数", "利用率(%)", "総PV"],
            )
            .properties(title="利用企業数", width=350, height=220)
        )
        _chart_rate = (
            _alt.Chart(_df_summary)
            .mark_bar()
            .encode(
                x=_alt.X("利用率(%):Q", title="利用率 (%)", scale=_alt.Scale(domain=[0, 100])),
                y=_alt.Y("ページ種別:N", title="", sort=list(_pv_cols.values())),
                color=_alt.Color("ページ種別:N", legend=None),
                tooltip=["ページ種別", "利用企業数", "利用率(%)", "総PV"],
            )
            .properties(title="利用率 (%)", width=350, height=220)
        )
        _ga_summary_outputs.append(_chart_count | _chart_rate)
        _ga_summary_outputs.append(mo.ui.table(_df_summary, pagination=False))

        # 元データ（先頭20件）
        _ga_summary_outputs.append(mo.md("### org_id別 詳細データ（先頭20件）"))
        _ga_summary_outputs.append(mo.ui.table(df_ga.head(20), pagination=True))
    else:
        _ga_summary_outputs.append(mo.md("*GAデータなし*"))

    mo.vstack(_ga_summary_outputs)
    return


@app.cell
def _(mo):
    mo.md("""
    ### other内訳（page_location 上位）
    """)
    return


@app.cell
def _(GA_DATASET_ID, mo, query_bq):
    # other内訳: 正しいURL分類に基づく
    _classify_case = """
            CASE
                WHEN REGEXP_CONTAINS(page_location, r'/company-lists|/people-lists|/leads-lists') THEN 'list'
                WHEN REGEXP_CONTAINS(page_location, r'/companies/[a-z0-9]') THEN 'company_detail'
                WHEN REGEXP_CONTAINS(page_location, r'/companies') THEN 'search'
                WHEN REGEXP_CONTAINS(page_location, r'/analysis') THEN 'analysis'
                WHEN REGEXP_CONTAINS(page_location, r'/people') THEN 'people'
                WHEN REGEXP_CONTAINS(page_location, r'/settings') THEN 'settings'
                WHEN REGEXP_CONTAINS(page_location, r'/sign-in|/sign-up|/org-selection') THEN 'auth'
                ELSE 'other'
            END
    """

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
            {_classify_case} AS page_group
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
            {_classify_case} AS page_group
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
    ## 5-b. GA推移（全体チャート）

    全組織のGA利用推移（6ヶ月）をPV・UUで可視化します。
    """)
    return


@app.cell
def _(GA_DATASET_ID, query_bq):
    _ga_overall_query = f"""
    WITH base AS (
        SELECT
            FORMAT_DATE('%Y-%m', PARSE_DATE('%Y%m%d', event_date)) AS month,
            user_pseudo_id,
            event_name
        FROM `{GA_DATASET_ID}.events_*`
        WHERE NOT STARTS_WITH(_TABLE_SUFFIX, 'intraday_')
          AND _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH))
    )
    SELECT
        month,
        COUNT(DISTINCT user_pseudo_id) AS uu,
        COUNTIF(event_name = 'page_view') AS pv
    FROM base
    GROUP BY month
    ORDER BY month
    """
    df_ga_overall_trend = query_bq(_ga_overall_query)
    return (df_ga_overall_trend,)


@app.cell
def _(df_ga_overall_trend, mo):
    import altair as _alt

    _ga_chart_outputs = []
    if len(df_ga_overall_trend) > 0:
        _ga_chart_outputs.append(mo.md(f"**全体GA推移**: {len(df_ga_overall_trend)} ヶ月分"))

        _chart_pv = (
            _alt.Chart(df_ga_overall_trend)
            .mark_line(point=True)
            .encode(
                x=_alt.X("month:N", title="月"),
                y=_alt.Y("pv:Q", title="PV", scale=_alt.Scale(zero=False)),
                tooltip=["month", "pv"],
            )
            .properties(title="PV推移（全体）", width=400, height=280)
        )

        _chart_uu = (
            _alt.Chart(df_ga_overall_trend)
            .mark_line(point=True, color="orange")
            .encode(
                x=_alt.X("month:N", title="月"),
                y=_alt.Y("uu:Q", title="UU", scale=_alt.Scale(zero=False)),
                tooltip=["month", "uu"],
            )
            .properties(title="UU推移（全体）", width=400, height=280)
        )

        _ga_chart_outputs.append(_chart_pv | _chart_uu)
        _ga_chart_outputs.append(mo.ui.table(df_ga_overall_trend, pagination=False))
    else:
        _ga_chart_outputs.append(mo.md("*GAデータなし*"))

    mo.vstack(_ga_chart_outputs)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 5-c. リスト追加推移（全体チャート）

    CompanyList・PeopleListの月次追加推移（6ヶ月）をUser数・回数で可視化します。
    """)
    return


@app.cell
def _(SF_SCHEMA, query_sf):
    _cl_trend_query = f"""
    SELECT
        DATE_TRUNC('month', cl.CREATEDAT) AS MONTH,
        COUNT(DISTINCT cl.ID) AS LIST_COUNT,
        COUNT(DISTINCT cl.USERORGRELATIONID) AS USER_COUNT
    FROM {SF_SCHEMA}.COMPANYLIST cl
    WHERE cl.CREATEDAT >= DATEADD('month', -6, CURRENT_DATE())
    GROUP BY MONTH
    ORDER BY MONTH
    """
    df_companylist_trend_all = query_sf(_cl_trend_query)

    _pl_trend_query = f"""
    SELECT
        DATE_TRUNC('month', pl.CREATEDAT) AS MONTH,
        COUNT(DISTINCT pl.ID) AS LIST_COUNT,
        COUNT(DISTINCT pl.USERORGRELATIONID) AS USER_COUNT
    FROM {SF_SCHEMA}.PEOPLELIST pl
    WHERE pl.CREATEDAT >= DATEADD('month', -6, CURRENT_DATE())
    GROUP BY MONTH
    ORDER BY MONTH
    """
    df_peoplelist_trend_all = query_sf(_pl_trend_query)
    return df_companylist_trend_all, df_peoplelist_trend_all


@app.cell
def _(df_companylist_trend_all, df_peoplelist_trend_all, mo):
    import altair as _alt

    _list_chart_outputs = []

    # --- CompanyList ---
    _list_chart_outputs.append(mo.md("### CompanyList追加推移（全体）"))
    if len(df_companylist_trend_all) > 0:
        _df_cl = df_companylist_trend_all.copy()
        _df_cl.columns = [c.upper() for c in _df_cl.columns]
        _df_cl["MONTH"] = _df_cl["MONTH"].astype(str).str[:7]

        _chart_cl_count = (
            _alt.Chart(_df_cl)
            .mark_bar(color="steelblue", opacity=0.7)
            .encode(
                x=_alt.X("MONTH:N", title="月"),
                y=_alt.Y("LIST_COUNT:Q", title="リスト作成数"),
                tooltip=["MONTH", "LIST_COUNT", "USER_COUNT"],
            )
            .properties(title="CompanyList作成数", width=400, height=280)
        )
        _chart_cl_user = (
            _alt.Chart(_df_cl)
            .mark_line(point=True, color="red")
            .encode(
                x=_alt.X("MONTH:N", title="月"),
                y=_alt.Y("USER_COUNT:Q", title="ユーザー数", scale=_alt.Scale(zero=False)),
                tooltip=["MONTH", "LIST_COUNT", "USER_COUNT"],
            )
            .properties(title="CompanyList作成ユーザー数", width=400, height=280)
        )
        _list_chart_outputs.append(_chart_cl_count | _chart_cl_user)
        _list_chart_outputs.append(mo.ui.table(_df_cl, pagination=False))
    else:
        _list_chart_outputs.append(mo.md("*CompanyListデータなし*"))

    # --- PeopleList ---
    _list_chart_outputs.append(mo.md("### PeopleList追加推移（全体）"))
    if len(df_peoplelist_trend_all) > 0:
        _df_pl = df_peoplelist_trend_all.copy()
        _df_pl.columns = [c.upper() for c in _df_pl.columns]
        _df_pl["MONTH"] = _df_pl["MONTH"].astype(str).str[:7]

        _chart_pl_count = (
            _alt.Chart(_df_pl)
            .mark_bar(color="teal", opacity=0.7)
            .encode(
                x=_alt.X("MONTH:N", title="月"),
                y=_alt.Y("LIST_COUNT:Q", title="リスト作成数"),
                tooltip=["MONTH", "LIST_COUNT", "USER_COUNT"],
            )
            .properties(title="PeopleList作成数", width=400, height=280)
        )
        _chart_pl_user = (
            _alt.Chart(_df_pl)
            .mark_line(point=True, color="purple")
            .encode(
                x=_alt.X("MONTH:N", title="月"),
                y=_alt.Y("USER_COUNT:Q", title="ユーザー数", scale=_alt.Scale(zero=False)),
                tooltip=["MONTH", "LIST_COUNT", "USER_COUNT"],
            )
            .properties(title="PeopleList作成ユーザー数", width=400, height=280)
        )
        _list_chart_outputs.append(_chart_pl_count | _chart_pl_user)
        _list_chart_outputs.append(mo.ui.table(_df_pl, pagination=False))
    else:
        _list_chart_outputs.append(mo.md("*PeopleListデータなし*"))

    mo.vstack(_list_chart_outputs)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 5-b2. ページ別アクセス率推移

    各機能ページの「アクセスUU / 総ログインUU」比率を月次で可視化します。
    """)
    return


@app.cell
def _(GA_DATASET_ID, query_bq):
    # --- ユーザー単位: 月別 × カテゴリ別 UU数 + 率 ---
    # 実際のURL構造:
    #   /companies(?:[?#]|$) → 企業検索(一覧)
    #   /companies/[a-z0-9]  → 企業詳細
    #   /company-lists|/people-lists|/leads-lists → リスト系
    #   -lists/import        → インポート
    #   /analysis/trends     → トレンド分析
    #   /analysis/intent-settings → 1stPartyスコア設定
    #   /analysis/crm-integration → CRM連携
    #   /people              → 人物
    _user_query = f"""
    WITH base AS (
        SELECT
            FORMAT_DATE('%Y-%m', PARSE_DATE('%Y%m%d', event_date)) AS month,
            user_pseudo_id,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location
        FROM `{GA_DATASET_ID}.events_*`
        WHERE NOT STARTS_WITH(_TABLE_SUFFIX, 'intraday_')
          AND _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH))
          AND event_name = 'page_view'
    ),
    monthly_total AS (
        SELECT month, COUNT(DISTINCT user_pseudo_id) AS total_uu
        FROM base GROUP BY month
    ),
    monthly_page AS (
        SELECT month,
            COUNT(DISTINCT CASE WHEN REGEXP_CONTAINS(page_location, r'/companies(?:[?#]|$)') THEN user_pseudo_id END) AS uu_search,
            COUNT(DISTINCT CASE WHEN REGEXP_CONTAINS(page_location, r'/companies/[a-z0-9]') THEN user_pseudo_id END) AS uu_company_detail,
            COUNT(DISTINCT CASE WHEN REGEXP_CONTAINS(page_location, r'/company-lists|/people-lists|/leads-lists') THEN user_pseudo_id END) AS uu_lists,
            COUNT(DISTINCT CASE WHEN REGEXP_CONTAINS(page_location, r'-lists/import') THEN user_pseudo_id END) AS uu_import,
            COUNT(DISTINCT CASE WHEN REGEXP_CONTAINS(page_location, r'/analysis/trends') THEN user_pseudo_id END) AS uu_trends,
            COUNT(DISTINCT CASE WHEN REGEXP_CONTAINS(page_location, r'/analysis/intent-settings') THEN user_pseudo_id END) AS uu_intent,
            COUNT(DISTINCT CASE WHEN REGEXP_CONTAINS(page_location, r'/analysis/crm-integration') THEN user_pseudo_id END) AS uu_crm,
            COUNT(DISTINCT CASE WHEN REGEXP_CONTAINS(page_location, r'/people') THEN user_pseudo_id END) AS uu_people
        FROM base GROUP BY month
    )
    SELECT p.month, t.total_uu,
           p.uu_search,          ROUND(SAFE_DIVIDE(p.uu_search, t.total_uu) * 100, 1)          AS rate_search,
           p.uu_company_detail,  ROUND(SAFE_DIVIDE(p.uu_company_detail, t.total_uu) * 100, 1)  AS rate_company_detail,
           p.uu_lists,           ROUND(SAFE_DIVIDE(p.uu_lists, t.total_uu) * 100, 1)           AS rate_lists,
           p.uu_import,          ROUND(SAFE_DIVIDE(p.uu_import, t.total_uu) * 100, 1)          AS rate_import,
           p.uu_trends,          ROUND(SAFE_DIVIDE(p.uu_trends, t.total_uu) * 100, 1)          AS rate_trends,
           p.uu_intent,          ROUND(SAFE_DIVIDE(p.uu_intent, t.total_uu) * 100, 1)          AS rate_intent,
           p.uu_crm,             ROUND(SAFE_DIVIDE(p.uu_crm, t.total_uu) * 100, 1)             AS rate_crm,
           p.uu_people,          ROUND(SAFE_DIVIDE(p.uu_people, t.total_uu) * 100, 1)          AS rate_people
    FROM monthly_page p JOIN monthly_total t ON p.month = t.month
    ORDER BY p.month
    """
    df_ga_page_rate_user = query_bq(_user_query)
    return (df_ga_page_rate_user,)


@app.cell
def _(GA_DATASET_ID, query_bq):
    # --- 企業(org_id)単位: 月×org_id ごとの各カテゴリ利用有無 (0/1) ---
    _org_raw_query = f"""
    SELECT
        month,
        org_id,
        MAX(CASE WHEN REGEXP_CONTAINS(page_location, r'/companies(?:[?#]|$)') THEN 1 ELSE 0 END) AS has_search,
        MAX(CASE WHEN REGEXP_CONTAINS(page_location, r'/companies/[a-z0-9]') THEN 1 ELSE 0 END) AS has_company_detail,
        MAX(CASE WHEN REGEXP_CONTAINS(page_location, r'/company-lists|/people-lists|/leads-lists') THEN 1 ELSE 0 END) AS has_lists,
        MAX(CASE WHEN REGEXP_CONTAINS(page_location, r'-lists/import') THEN 1 ELSE 0 END) AS has_import,
        MAX(CASE WHEN REGEXP_CONTAINS(page_location, r'/analysis/trends') THEN 1 ELSE 0 END) AS has_trends,
        MAX(CASE WHEN REGEXP_CONTAINS(page_location, r'/analysis/intent-settings') THEN 1 ELSE 0 END) AS has_intent,
        MAX(CASE WHEN REGEXP_CONTAINS(page_location, r'/analysis/crm-integration') THEN 1 ELSE 0 END) AS has_crm,
        MAX(CASE WHEN REGEXP_CONTAINS(page_location, r'/people') THEN 1 ELSE 0 END) AS has_people
    FROM (
        SELECT
            FORMAT_DATE('%Y-%m', PARSE_DATE('%Y%m%d', event_date)) AS month,
            (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'org_id') AS org_id,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location
        FROM `{GA_DATASET_ID}.events_*`
        WHERE NOT STARTS_WITH(_TABLE_SUFFIX, 'intraday_')
          AND _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH))
          AND event_name = 'page_view'
    )
    WHERE org_id IS NOT NULL
    GROUP BY month, org_id
    """
    df_ga_page_org_raw = query_bq(_org_raw_query)
    return (df_ga_page_org_raw,)


@app.cell
def _(df_churn, df_ga_page_org_raw, df_id_mapping, pd):
    # --- 企業単位の月次集約 + 解約/契約中の紐づけ ---
    df_ga_page_rate_org = pd.DataFrame()
    df_ga_page_rate_churn = pd.DataFrame()

    _has_cols = [c for c in df_ga_page_org_raw.columns if c.startswith("has_")]

    if len(df_ga_page_org_raw) > 0 and _has_cols:
        # 全体集約: 月別に各カテゴリの企業数と総アクティブ企業数
        _total_org = df_ga_page_org_raw.groupby("month")["org_id"].nunique().reset_index(name="total_orgs")
        _page_org = df_ga_page_org_raw.groupby("month")[_has_cols].sum().astype(int).reset_index()
        df_ga_page_rate_org = _page_org.merge(_total_org, on="month")
        for _hc in _has_cols:
            _rc = _hc.replace("has_", "rate_")
            _oc = _hc.replace("has_", "org_")
            df_ga_page_rate_org[_oc] = df_ga_page_rate_org[_hc]
            df_ga_page_rate_org[_rc] = (df_ga_page_rate_org[_hc] / df_ga_page_rate_org["total_orgs"] * 100).round(1)

        # 解約/契約中の紐づけ
        _org_churn = df_ga_page_org_raw.copy()
        if len(df_id_mapping) > 0 and "ORGID" in df_id_mapping.columns:
            _map = df_id_mapping[["ORGID", "COMPNO"]].drop_duplicates(subset=["ORGID"])
            _org_churn = _org_churn.merge(_map, left_on="org_id", right_on="ORGID", how="left")
        if len(df_churn) > 0 and "COMPNO" in _org_churn.columns and "COMPNO" in df_churn.columns:
            _churn_map = df_churn[["COMPNO", "status"]].drop_duplicates(subset=["COMPNO"])
            _org_churn = _org_churn.merge(_churn_map, on="COMPNO", how="left")
        if "status" not in _org_churn.columns:
            _org_churn["status"] = "不明"
        _org_churn["status"] = _org_churn["status"].fillna("不明")

        # 解約/契約中別の月次集約
        _churn_total = _org_churn.groupby(["month", "status"])["org_id"].nunique().reset_index(name="total_orgs")
        _churn_page = _org_churn.groupby(["month", "status"])[_has_cols].sum().astype(int).reset_index()
        df_ga_page_rate_churn = _churn_page.merge(_churn_total, on=["month", "status"])
        for _hc in _has_cols:
            _rc = _hc.replace("has_", "rate_")
            _oc = _hc.replace("has_", "org_")
            df_ga_page_rate_churn[_oc] = df_ga_page_rate_churn[_hc]
            df_ga_page_rate_churn[_rc] = (df_ga_page_rate_churn[_hc] / df_ga_page_rate_churn["total_orgs"] * 100).round(1)

    return df_ga_page_rate_churn, df_ga_page_rate_org


@app.cell
def _(df_ga_page_rate_churn, df_ga_page_rate_org, df_ga_page_rate_user, mo, pd):
    import altair as _alt

    _outputs = []

    # ========== カテゴリ定義 ==========
    _main_labels = {
        "search": "企業検索(一覧)",
        "company_detail": "企業詳細",
        "lists": "リスト系",
        "people": "人物",
    }
    _sub_labels = {
        "trends": "トレンド分析",
        "intent": "1stPartyスコア設定",
        "crm": "CRM連携",
        "import": "リストインポート",
    }
    _all_labels = {**_main_labels, **_sub_labels}

    # ========== ヘルパー: melt してチャートを作る ==========
    def _make_line_chart(df, cols_map, id_col, value_col, title, y_title, width=480, height=280):
        _available = [c for c in cols_map if c in df.columns]
        if not _available:
            return None
        _df = df[[id_col] + _available].copy()
        _melted = _df.melt(id_vars=id_col, var_name="page_type", value_name=value_col)
        _melted["page_type"] = _melted["page_type"].map(cols_map)
        return (
            _alt.Chart(_melted)
            .mark_line(point=True)
            .encode(
                x=_alt.X(f"{id_col}:N", title="月"),
                y=_alt.Y(f"{value_col}:Q", title=y_title, scale=_alt.Scale(zero=True)),
                color=_alt.Color("page_type:N", title="ページ種別"),
                tooltip=[id_col, "page_type", value_col],
            )
            .properties(title=title, width=width, height=height)
        )

    # ========== 1. ユーザー単位 ==========
    _outputs.append(mo.md("### ユーザー(UU)単位"))
    if len(df_ga_page_rate_user) > 0:
        # UU数 + 率の列マッピング
        _uu_main = {f"uu_{k}": v for k, v in _main_labels.items()}
        _rate_main = {f"rate_{k}": v for k, v in _main_labels.items()}
        _uu_sub = {f"uu_{k}": v for k, v in _sub_labels.items()}
        _rate_sub = {f"rate_{k}": v for k, v in _sub_labels.items()}

        # メイン: 数 | 率
        _c_uu_main = _make_line_chart(df_ga_page_rate_user, _uu_main, "month", "uu", "主要機能 UU数推移", "UU数")
        _c_rate_main = _make_line_chart(df_ga_page_rate_user, _rate_main, "month", "rate", "主要機能 UU率推移 (%)", "アクセス率 (%)")
        if _c_uu_main and _c_rate_main:
            _outputs.append(_c_uu_main | _c_rate_main)

        # サブ: 数 | 率
        _c_uu_sub = _make_line_chart(df_ga_page_rate_user, _uu_sub, "month", "uu", "分析・設定系 UU数推移", "UU数")
        _c_rate_sub = _make_line_chart(df_ga_page_rate_user, _rate_sub, "month", "rate", "分析・設定系 UU率推移 (%)", "アクセス率 (%)")
        if _c_uu_sub and _c_rate_sub:
            _outputs.append(_c_uu_sub | _c_rate_sub)

        _outputs.append(mo.ui.table(df_ga_page_rate_user, pagination=False))
    else:
        _outputs.append(mo.md("*ユーザー単位 GAデータなし*"))

    # ========== 2. 企業(org_id)単位 ==========
    _outputs.append(mo.md("### 企業(org_id)単位"))
    if len(df_ga_page_rate_org) > 0:
        _org_main = {f"org_{k}": v for k, v in _main_labels.items()}
        _org_rate_main = {f"rate_{k}": v for k, v in _main_labels.items()}
        _org_sub = {f"org_{k}": v for k, v in _sub_labels.items()}
        _org_rate_sub = {f"rate_{k}": v for k, v in _sub_labels.items()}

        _c_org_main = _make_line_chart(df_ga_page_rate_org, _org_main, "month", "count", "主要機能 企業数推移", "企業数")
        _c_org_rate = _make_line_chart(df_ga_page_rate_org, _org_rate_main, "month", "rate", "主要機能 企業率推移 (%)", "アクセス企業率 (%)")
        if _c_org_main and _c_org_rate:
            _outputs.append(_c_org_main | _c_org_rate)

        _c_org_sub = _make_line_chart(df_ga_page_rate_org, _org_sub, "month", "count", "分析・設定系 企業数推移", "企業数")
        _c_org_rate_sub = _make_line_chart(df_ga_page_rate_org, _org_rate_sub, "month", "rate", "分析・設定系 企業率推移 (%)", "アクセス企業率 (%)")
        if _c_org_sub and _c_org_rate_sub:
            _outputs.append(_c_org_sub | _c_org_rate_sub)

        _outputs.append(mo.ui.table(df_ga_page_rate_org, pagination=False))
    else:
        _outputs.append(mo.md("*企業単位 GAデータなし*"))

    # ========== 3. 解約 vs 契約中 比較 ==========
    _outputs.append(mo.md("### 解約企業 vs 契約中企業"))
    if len(df_ga_page_rate_churn) > 0:
        # 解約済み・契約中のみに絞る（「不明」は除外）
        _df_churn_viz = df_ga_page_rate_churn[
            df_ga_page_rate_churn["status"].isin(["解約済み", "契約中"])
        ].copy()

        if len(_df_churn_viz) > 0:
            _outputs.append(mo.md(f"解約済み: {_df_churn_viz[_df_churn_viz['status']=='解約済み']['total_orgs'].max() if len(_df_churn_viz[_df_churn_viz['status']=='解約済み']) > 0 else 0} 社, "
                                  f"契約中: {_df_churn_viz[_df_churn_viz['status']=='契約中']['total_orgs'].max() if len(_df_churn_viz[_df_churn_viz['status']=='契約中']) > 0 else 0} 社"))

            # 主要4カテゴリのみ: 率比較
            _rate_churn_cols = [f"rate_{k}" for k in _main_labels]
            _rate_churn_available = [c for c in _rate_churn_cols if c in _df_churn_viz.columns]
            if _rate_churn_available:
                _df_cr = _df_churn_viz[["month", "status"] + _rate_churn_available].copy()
                _label_map = {f"rate_{k}": v for k, v in _main_labels.items()}
                _df_cr_melted = _df_cr.melt(id_vars=["month", "status"], var_name="page_type", value_name="rate")
                _df_cr_melted["page_type"] = _df_cr_melted["page_type"].map(_label_map)

                _chart_churn_rate = (
                    _alt.Chart(_df_cr_melted)
                    .mark_line(point=True)
                    .encode(
                        x=_alt.X("month:N", title="月"),
                        y=_alt.Y("rate:Q", title="アクセス率 (%)", scale=_alt.Scale(zero=True)),
                        color=_alt.Color("page_type:N", title="ページ種別"),
                        strokeDash=_alt.StrokeDash("status:N", title="契約状態"),
                        tooltip=["month", "status", "page_type", "rate"],
                    )
                    .properties(title="解約 vs 契約中: アクセス率比較", width=480, height=300)
                )

                # 主要4カテゴリ: 数比較
                _org_churn_cols = [f"org_{k}" for k in _main_labels]
                _org_churn_available = [c for c in _org_churn_cols if c in _df_churn_viz.columns]
                _df_cc = _df_churn_viz[["month", "status"] + _org_churn_available].copy()
                _count_label_map = {f"org_{k}": v for k, v in _main_labels.items()}
                _df_cc_melted = _df_cc.melt(id_vars=["month", "status"], var_name="page_type", value_name="count")
                _df_cc_melted["page_type"] = _df_cc_melted["page_type"].map(_count_label_map)

                _chart_churn_count = (
                    _alt.Chart(_df_cc_melted)
                    .mark_line(point=True)
                    .encode(
                        x=_alt.X("month:N", title="月"),
                        y=_alt.Y("count:Q", title="企業数"),
                        color=_alt.Color("page_type:N", title="ページ種別"),
                        strokeDash=_alt.StrokeDash("status:N", title="契約状態"),
                        tooltip=["month", "status", "page_type", "count"],
                    )
                    .properties(title="解約 vs 契約中: 企業数比較", width=480, height=300)
                )
                _outputs.append(_chart_churn_rate | _chart_churn_count)

            _outputs.append(mo.ui.table(_df_churn_viz, pagination=False))
        else:
            _outputs.append(mo.md("*解約/契約中の紐づけ企業が不足*"))
    else:
        _outputs.append(mo.md("*企業×解約データなし*"))

    mo.vstack(_outputs)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 5-d. CSVダウンロード推移（全体）
    """)
    return


@app.cell
def _(SF_SCHEMA, query_sf):
    _csv_dl_query = f"""
    SELECT
        DATE_TRUNC('month', dl.CREATEDAT) AS MONTH,
        COUNT(DISTINCT dl.ID) AS DOWNLOAD_COUNT,
        COUNT(DISTINCT dl.USERORGRELATIONID) AS USER_COUNT
    FROM {SF_SCHEMA}.CSVDOWNLOADLOG dl
    WHERE dl.CREATEDAT >= DATEADD('month', -6, CURRENT_DATE())
    GROUP BY MONTH
    ORDER BY MONTH
    """
    df_csv_trend_all = query_sf(_csv_dl_query)
    return (df_csv_trend_all,)


@app.cell
def _(df_csv_trend_all, mo):
    import altair as _alt

    _csv_outputs = []
    _csv_outputs.append(mo.md("### CSVダウンロード推移（全体）"))
    if len(df_csv_trend_all) > 0:
        _df_csv = df_csv_trend_all.copy()
        _df_csv.columns = [c.upper() for c in _df_csv.columns]
        _df_csv["MONTH"] = _df_csv["MONTH"].astype(str).str[:7]

        _chart_csv_count = (
            _alt.Chart(_df_csv)
            .mark_bar(color="darkorange", opacity=0.7)
            .encode(
                x=_alt.X("MONTH:N", title="月"),
                y=_alt.Y("DOWNLOAD_COUNT:Q", title="DL数"),
                tooltip=["MONTH", "DOWNLOAD_COUNT", "USER_COUNT"],
            )
            .properties(title="CSVダウンロード数", width=400, height=280)
        )
        _chart_csv_user = (
            _alt.Chart(_df_csv)
            .mark_line(point=True, color="crimson")
            .encode(
                x=_alt.X("MONTH:N", title="月"),
                y=_alt.Y("USER_COUNT:Q", title="ユーザー数", scale=_alt.Scale(zero=False)),
                tooltip=["MONTH", "DOWNLOAD_COUNT", "USER_COUNT"],
            )
            .properties(title="CSVダウンロードユーザー数", width=400, height=280)
        )
        _csv_outputs.append(_chart_csv_count | _chart_csv_user)
        _csv_outputs.append(mo.ui.table(_df_csv, pagination=False))
    else:
        _csv_outputs.append(mo.md("*CSVダウンロードデータなし*"))

    mo.vstack(_csv_outputs)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 5-e. Memo活動推移（全体）
    """)
    return


@app.cell
def _(SF_SCHEMA, query_sf):
    _memo_trend_query = f"""
    SELECT
        DATE_TRUNC('month', m.CREATEDAT) AS MONTH,
        COUNT(*) AS MEMO_COUNT,
        COUNT(DISTINCT m.USERORGRELATIONID) AS USER_COUNT
    FROM {SF_SCHEMA}.MEMO m
    WHERE m.CREATEDAT >= DATEADD('month', -6, CURRENT_DATE())
    GROUP BY MONTH
    ORDER BY MONTH
    """
    df_memo_trend_all = query_sf(_memo_trend_query)

    _memo_priority_query = f"""
    SELECT
        COALESCE(PRIORITY, 'なし') AS PRIORITY,
        COUNT(*) AS COUNT
    FROM {SF_SCHEMA}.MEMO
    WHERE CREATEDAT >= DATEADD('month', -6, CURRENT_DATE())
    GROUP BY PRIORITY
    ORDER BY COUNT DESC
    """
    df_memo_priority = query_sf(_memo_priority_query)
    return df_memo_priority, df_memo_trend_all


@app.cell
def _(df_memo_priority, df_memo_trend_all, mo):
    import altair as _alt

    _memo_outputs = []
    _memo_outputs.append(mo.md("### Memo活動推移（全体）"))
    if len(df_memo_trend_all) > 0:
        _df_memo = df_memo_trend_all.copy()
        _df_memo.columns = [c.upper() for c in _df_memo.columns]
        _df_memo["MONTH"] = _df_memo["MONTH"].astype(str).str[:7]

        _chart_memo_count = (
            _alt.Chart(_df_memo)
            .mark_bar(color="mediumpurple", opacity=0.7)
            .encode(
                x=_alt.X("MONTH:N", title="月"),
                y=_alt.Y("MEMO_COUNT:Q", title="Memo件数"),
                tooltip=["MONTH", "MEMO_COUNT", "USER_COUNT"],
            )
            .properties(title="Memo作成数", width=400, height=280)
        )
        _chart_memo_user = (
            _alt.Chart(_df_memo)
            .mark_line(point=True, color="darkviolet")
            .encode(
                x=_alt.X("MONTH:N", title="月"),
                y=_alt.Y("USER_COUNT:Q", title="ユーザー数", scale=_alt.Scale(zero=False)),
                tooltip=["MONTH", "MEMO_COUNT", "USER_COUNT"],
            )
            .properties(title="Memo作成ユーザー数", width=400, height=280)
        )
        _memo_outputs.append(_chart_memo_count | _chart_memo_user)
        _memo_outputs.append(mo.ui.table(_df_memo, pagination=False))
    else:
        _memo_outputs.append(mo.md("*Memoデータなし*"))

    if len(df_memo_priority) > 0:
        _memo_outputs.append(mo.md("**Priority分布**"))
        _df_pri = df_memo_priority.copy()
        _df_pri.columns = [c.upper() for c in _df_pri.columns]
        _memo_outputs.append(mo.ui.table(_df_pri, pagination=False))

    mo.vstack(_memo_outputs)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 5-f. バリューファネル分析

    登録 → 企業検索 → 企業詳細 → リスト → メモ → CRM活用(Negotiation+Lead+LeadImport) の6ステップファネル。
    分母: GAでセッションが1件以上ある企業/アカウントのみ。解約 vs 契約中の比較あり。
    """)
    return


@app.cell
def _(GA_DATASET_ID, query_bq):
    # GA: 企業(org_id)単位 ステップ1-3
    # 実際のURLパターン: /companies(一覧=検索), /companies/[id](企業詳細)
    _funnel_org_query = f"""
    SELECT
        org_id,
        1 AS step_login,
        MAX(CASE WHEN REGEXP_CONTAINS(page_location, r'/companies') THEN 1 ELSE 0 END) AS step_search,
        MAX(CASE WHEN REGEXP_CONTAINS(page_location, r'/companies/[a-z0-9]') THEN 1 ELSE 0 END) AS step_company
    FROM (
        SELECT
            (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'org_id') AS org_id,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location
        FROM `{GA_DATASET_ID}.events_*`
        WHERE _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY))
          AND event_name = 'page_view'
    )
    WHERE org_id IS NOT NULL
    GROUP BY org_id
    """
    df_funnel_ga = query_bq(_funnel_org_query)

    # GA: アカウント(user)単位 ステップ1-3
    _funnel_user_query = f"""
    SELECT
        user_pseudo_id,
        org_id,
        1 AS step_login,
        MAX(CASE WHEN REGEXP_CONTAINS(page_location, r'/companies') THEN 1 ELSE 0 END) AS step_search,
        MAX(CASE WHEN REGEXP_CONTAINS(page_location, r'/companies/[a-z0-9]') THEN 1 ELSE 0 END) AS step_company
    FROM (
        SELECT
            user_pseudo_id,
            (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'org_id') AS org_id,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location
        FROM `{GA_DATASET_ID}.events_*`
        WHERE _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY))
          AND event_name = 'page_view'
    )
    WHERE org_id IS NOT NULL
    GROUP BY user_pseudo_id, org_id
    """
    df_funnel_ga_user = query_bq(_funnel_user_query)
    return df_funnel_ga, df_funnel_ga_user


@app.cell
def _(SF_SCHEMA, df_funnel_ga, pd, query_sf):
    # Snowflake: ステップ4(リスト) + 5(Memo) + 6(CRM: Negotiation/Lead/LeadImport) + アカウント数
    df_funnel_sf = pd.DataFrame()
    df_account_count_per_org = pd.DataFrame()

    _funnel_orgids = df_funnel_ga["org_id"].dropna().unique().tolist() if len(df_funnel_ga) > 0 else []

    if _funnel_orgids:
        _orgids_sql = ",".join([f"'{o}'" for o in _funnel_orgids[:1000]])

        _funnel_sf_query = f"""
        WITH base_uorid AS (
            SELECT CAST(u.ORGID AS STRING) AS ORGID, ur.ID AS UORID
            FROM {SF_SCHEMA}.USERORGANIZATION u
            JOIN {SF_SCHEMA}.USERORGRELATION ur ON u.ORGID = ur.ORGANIZATIONID
            WHERE CAST(u.ORGID AS STRING) IN ({_orgids_sql})
        ),
        list_check AS (
            SELECT DISTINCT b.ORGID
            FROM base_uorid b
            WHERE EXISTS (SELECT 1 FROM {SF_SCHEMA}.COMPANYLIST cl WHERE cl.USERORGRELATIONID = b.UORID)
               OR EXISTS (SELECT 1 FROM {SF_SCHEMA}.PEOPLELIST pl WHERE pl.USERORGRELATIONID = b.UORID)
        ),
        memo_check AS (
            SELECT DISTINCT b.ORGID
            FROM base_uorid b
            WHERE EXISTS (SELECT 1 FROM {SF_SCHEMA}.MEMO m WHERE m.USERORGRELATIONID = b.UORID)
        ),
        crm_check AS (
            SELECT DISTINCT d.ORGID
            FROM (SELECT DISTINCT ORGID FROM base_uorid) d
            WHERE EXISTS (SELECT 1 FROM {SF_SCHEMA}.NEGOTIATION n WHERE n.ORGANIZATIONID = d.ORGID)
               OR EXISTS (SELECT 1 FROM {SF_SCHEMA}.LEAD l WHERE l.ORGANIZATIONID = d.ORGID)
               OR EXISTS (
                   SELECT 1 FROM base_uorid b
                   JOIN {SF_SCHEMA}.LEADIMPORTEVENT lie ON lie.USERORGRELATIONID = b.UORID
                   WHERE b.ORGID = d.ORGID
               )
        )
        SELECT
            d.ORGID,
            CASE WHEN lc.ORGID IS NOT NULL THEN 1 ELSE 0 END AS STEP_LIST,
            CASE WHEN mc.ORGID IS NOT NULL THEN 1 ELSE 0 END AS STEP_MEMO,
            CASE WHEN cc.ORGID IS NOT NULL THEN 1 ELSE 0 END AS STEP_CRM
        FROM (SELECT DISTINCT ORGID FROM base_uorid) d
        LEFT JOIN list_check lc ON d.ORGID = lc.ORGID
        LEFT JOIN memo_check mc ON d.ORGID = mc.ORGID
        LEFT JOIN crm_check cc ON d.ORGID = cc.ORGID
        """
        try:
            df_funnel_sf = query_sf(_funnel_sf_query)
        except Exception:
            df_funnel_sf = pd.DataFrame()

        _account_count_query = f"""
        SELECT
            CAST(ORGANIZATIONID AS STRING) AS ORGID,
            COUNT(DISTINCT ID) AS ACCOUNT_COUNT
        FROM {SF_SCHEMA}.USERORGRELATION
        WHERE CAST(ORGANIZATIONID AS STRING) IN ({_orgids_sql})
        GROUP BY ORGANIZATIONID
        """
        try:
            df_account_count_per_org = query_sf(_account_count_query)
        except Exception:
            df_account_count_per_org = pd.DataFrame()

    return df_account_count_per_org, df_funnel_sf


@app.cell
def _(df_account_count_per_org, df_churn, df_funnel_ga, df_funnel_sf, df_id_mapping, pd):
    # ファネルデータ統合 + セグメント属性付与
    df_funnel_org = pd.DataFrame()

    if len(df_funnel_ga) > 0:
        df_funnel_org = df_funnel_ga.copy()

        # SF steps join
        if len(df_funnel_sf) > 0:
            _sf = df_funnel_sf.copy()
            _sf.columns = [c.upper() for c in _sf.columns]
            df_funnel_org = df_funnel_org.merge(_sf, left_on="org_id", right_on="ORGID", how="left")

        for _col in ["STEP_LIST", "STEP_MEMO", "STEP_CRM"]:
            _lc = _col.lower()
            _src = _col if _col in df_funnel_org.columns else _lc
            if _src in df_funnel_org.columns:
                df_funnel_org[_lc] = df_funnel_org[_src].fillna(0).astype(int)
            else:
                df_funnel_org[_lc] = 0

        # 企業属性 join
        if len(df_id_mapping) > 0:
            _attr_cols = [c for c in ["ORGID", "EMPLOYEE_COUNT", "INDUSTRY_ID", "PREFID", "COMPNO"] if c in df_id_mapping.columns]
            _df_attr = df_id_mapping[_attr_cols].drop_duplicates(subset=["ORGID"])
            df_funnel_org = df_funnel_org.merge(_df_attr, left_on="org_id", right_on="ORGID", how="left", suffixes=("", "_attr"))

        # チャーンステータス join
        if len(df_churn) > 0 and "COMPNO" in df_funnel_org.columns:
            _churn_cols = [c for c in ["COMPNO", "status"] if c in df_churn.columns]
            if _churn_cols:
                df_funnel_org = df_funnel_org.merge(
                    df_churn[_churn_cols].drop_duplicates(subset=["COMPNO"]),
                    on="COMPNO", how="left",
                )

        # アカウント数 join
        if len(df_account_count_per_org) > 0:
            _acc = df_account_count_per_org.copy()
            _acc.columns = [c.upper() for c in _acc.columns]
            df_funnel_org = df_funnel_org.merge(
                _acc[["ORGID", "ACCOUNT_COUNT"]], left_on="org_id", right_on="ORGID",
                how="left", suffixes=("", "_acc"),
            )

        # --- セグメント列追加 ---
        def _emp_bucket(val):
            if pd.isna(val) or val <= 0: return "不明"
            if val <= 10: return "1-10人"
            if val <= 50: return "11-50人"
            if val <= 200: return "51-200人"
            if val <= 1000: return "201-1000人"
            return "1001人+"

        def _region(val):
            try: v = int(val)
            except (TypeError, ValueError): return "不明"
            if v in (11, 12, 13, 14): return "首都圏"
            if v in (26, 27, 28): return "関西"
            if v in (22, 23): return "中部"
            return "その他地方"

        def _acc_bucket(val):
            if pd.isna(val) or val <= 0: return "不明"
            if val == 1: return "1"
            if val <= 5: return "2-5"
            if val <= 10: return "6-10"
            return "11+"

        df_funnel_org["emp_bucket"] = df_funnel_org["EMPLOYEE_COUNT"].apply(_emp_bucket) if "EMPLOYEE_COUNT" in df_funnel_org.columns else "不明"
        df_funnel_org["region"] = df_funnel_org["PREFID"].apply(_region) if "PREFID" in df_funnel_org.columns else "不明"
        df_funnel_org["account_bucket"] = df_funnel_org["ACCOUNT_COUNT"].apply(_acc_bucket) if "ACCOUNT_COUNT" in df_funnel_org.columns else "不明"

        if "INDUSTRY_ID" in df_funnel_org.columns:
            _top_ind = df_funnel_org["INDUSTRY_ID"].value_counts().head(10).index.tolist()
            df_funnel_org["industry_group"] = df_funnel_org["INDUSTRY_ID"].apply(lambda x: str(x) if x in _top_ind else "その他")
        else:
            df_funnel_org["industry_group"] = "不明"

        if "status" not in df_funnel_org.columns:
            df_funnel_org["status"] = "不明"
        df_funnel_org["status"] = df_funnel_org["status"].fillna("不明")

    return (df_funnel_org,)


@app.cell
def _(df_funnel_ga_user, df_funnel_org, mo, pd):
    import altair as _alt

    _funnel_outputs = []

    # ステップ名の定義（6ステップ）
    _step_keys = [
        "1. 登録", "2. 企業検索", "3. 企業詳細",
        "4. リスト", "5. メモ", "6. CRM活用",
    ]

    def _get_step_col(df, name):
        return name if name in df.columns else name.upper()

    def _sum_step(df, col_name):
        _c = _get_step_col(df, col_name)
        return int(df[_c].sum()) if _c in df.columns else 0

    if len(df_funnel_org) > 0:
        # --- 企業単位ファネル（全体） ---
        _total_org = len(df_funnel_org)
        _steps_org = {
            "1. 登録": int(df_funnel_org["step_login"].sum()),
            "2. 企業検索": int(df_funnel_org["step_search"].sum()),
            "3. 企業詳細": int(df_funnel_org["step_company"].sum()),
            "4. リスト": _sum_step(df_funnel_org, "step_list"),
            "5. メモ": _sum_step(df_funnel_org, "step_memo"),
            "6. CRM活用": _sum_step(df_funnel_org, "step_crm"),
        }
        _df_funnel_chart = pd.DataFrame([
            {"step": k, "count": v, "rate": round(v / _total_org * 100, 1)}
            for k, v in _steps_org.items()
        ])

        _funnel_outputs.append(mo.md(f"### 企業単位ファネル（分母: {_total_org:,} 社）"))
        _chart_funnel_org = (
            _alt.Chart(_df_funnel_chart)
            .mark_bar()
            .encode(
                x=_alt.X("rate:Q", title="通過率 (%)", scale=_alt.Scale(domain=[0, 100])),
                y=_alt.Y("step:N", title="ステップ", sort=_step_keys),
                color=_alt.Color("step:N", legend=None),
                tooltip=["step", "count", "rate"],
            )
            .properties(title="バリューファネル（企業単位・全体）", width=600, height=280)
        )
        _funnel_outputs.append(_chart_funnel_org)
        _funnel_outputs.append(mo.ui.table(_df_funnel_chart, pagination=False))

        # --- 解約 vs 契約中 比較ファネル ---
        if "status" in df_funnel_org.columns:
            _churn_compare_data = []
            for _status_val in ["解約済み", "契約中"]:
                _grp = df_funnel_org[df_funnel_org["status"] == _status_val]
                _n = len(_grp)
                if _n == 0:
                    continue
                _grp_steps = {
                    "1. 登録": int(_grp["step_login"].sum()),
                    "2. 企業検索": int(_grp["step_search"].sum()),
                    "3. 企業詳細": int(_grp["step_company"].sum()),
                    "4. リスト": _sum_step(_grp, "step_list"),
                    "5. メモ": _sum_step(_grp, "step_memo"),
                    "6. CRM活用": _sum_step(_grp, "step_crm"),
                }
                for _sk, _sv in _grp_steps.items():
                    _churn_compare_data.append({
                        "status": _status_val,
                        "step": _sk,
                        "count": _sv,
                        "total": _n,
                        "rate": round(_sv / _n * 100, 1),
                    })

            if _churn_compare_data:
                _df_cc = pd.DataFrame(_churn_compare_data)
                _funnel_outputs.append(mo.md("### 解約 vs 契約中 ファネル比較"))

                _chart_cc = (
                    _alt.Chart(_df_cc)
                    .mark_bar()
                    .encode(
                        x=_alt.X("rate:Q", title="通過率 (%)", scale=_alt.Scale(domain=[0, 100])),
                        y=_alt.Y("step:N", title="ステップ", sort=_step_keys),
                        color=_alt.Color("status:N", title="ステータス",
                            scale=_alt.Scale(domain=["契約中", "解約済み"], range=["#4c78a8", "#e45756"])),
                        tooltip=["status", "step", "count", "total", "rate"],
                        xOffset="status:N",
                    )
                    .properties(title="解約 vs 契約中 ファネル比較", width=700, height=300)
                )
                _funnel_outputs.append(_chart_cc)

                # ピボットテーブル
                _df_cc_pivot = _df_cc.pivot_table(
                    index="status", columns="step", values="rate", aggfunc="first"
                )
                _ordered = [s for s in _step_keys if s in _df_cc_pivot.columns]
                _df_cc_pivot = _df_cc_pivot[_ordered].reset_index()
                _funnel_outputs.append(mo.md("**通過率比較テーブル (%)**"))
                _funnel_outputs.append(mo.ui.table(_df_cc_pivot, pagination=False))

                # 件数テーブル
                _df_cc_count = _df_cc.pivot_table(
                    index="status", columns="step", values="count", aggfunc="first"
                )
                _df_cc_count = _df_cc_count[_ordered].reset_index()
                _df_cc_count.insert(1, "企業数", _df_cc.groupby("status")["total"].first().values)
                _funnel_outputs.append(mo.md("**件数テーブル**"))
                _funnel_outputs.append(mo.ui.table(_df_cc_count, pagination=False))

        # --- アカウント単位ファネル (GA Step 1-3) ---
        if len(df_funnel_ga_user) > 0:
            _total_user = len(df_funnel_ga_user)
            _user_keys = ["1. 登録", "2. 企業検索", "3. 企業詳細"]
            _steps_user = {
                "1. 登録": int(df_funnel_ga_user["step_login"].sum()),
                "2. 企業検索": int(df_funnel_ga_user["step_search"].sum()),
                "3. 企業詳細": int(df_funnel_ga_user["step_company"].sum()),
            }
            _df_funnel_user = pd.DataFrame([
                {"step": k, "count": v, "rate": round(v / _total_user * 100, 1)}
                for k, v in _steps_user.items()
            ])
            _funnel_outputs.append(mo.md(f"### アカウント単位ファネル（分母: {_total_user:,} ユーザー、GA Step 1-3）"))
            _chart_funnel_user = (
                _alt.Chart(_df_funnel_user)
                .mark_bar(color="coral")
                .encode(
                    x=_alt.X("rate:Q", title="通過率 (%)", scale=_alt.Scale(domain=[0, 100])),
                    y=_alt.Y("step:N", title="ステップ", sort=_user_keys),
                    tooltip=["step", "count", "rate"],
                )
                .properties(title="バリューファネル（アカウント単位）", width=600, height=180)
            )
            _funnel_outputs.append(_chart_funnel_user)
            _funnel_outputs.append(mo.ui.table(_df_funnel_user, pagination=False))
    else:
        _funnel_outputs.append(mo.md("*ファネルデータなし*"))

    mo.vstack(_funnel_outputs)
    return


@app.cell
def _(mo):
    funnel_segment_selector = mo.ui.dropdown(
        options={
            "従業員規模別": "emp_bucket",
            "業種別": "industry_group",
            "アカウント人数別": "account_bucket",
            "地域別": "region",
            "契約ステータス別": "status",
        },
        label="セグメント切り替え",
        value="emp_bucket",
    )
    funnel_segment_selector
    return (funnel_segment_selector,)


@app.cell
def _(df_funnel_org, funnel_segment_selector, mo, pd):
    import altair as _alt

    _seg_outputs = []

    if len(df_funnel_org) > 0 and funnel_segment_selector.value:
        _seg_col = funnel_segment_selector.value
        _seg_label = {
            "emp_bucket": "従業員規模",
            "industry_group": "業種",
            "account_bucket": "アカウント人数",
            "region": "地域",
            "status": "契約ステータス",
        }.get(_seg_col, _seg_col)

        _seg_outputs.append(mo.md(f"### セグメント別ファネル: {_seg_label}"))

        _step_list_col = "step_list" if "step_list" in df_funnel_org.columns else "STEP_LIST"
        _step_memo_col = "step_memo" if "step_memo" in df_funnel_org.columns else "STEP_MEMO"
        _step_crm_col = "step_crm" if "step_crm" in df_funnel_org.columns else "STEP_CRM"
        _step_cols = ["step_login", "step_search", "step_company", _step_list_col, _step_memo_col, _step_crm_col]
        _step_cols = [c for c in _step_cols if c in df_funnel_org.columns]
        _step_labels = {
            "step_login": "1.登録",
            "step_search": "2.企業検索",
            "step_company": "3.企業詳細",
            _step_list_col: "4.リスト",
            _step_memo_col: "5.メモ",
            _step_crm_col: "6.CRM活用",
        }

        _seg_data = []
        for _seg_val, _grp in df_funnel_org.groupby(_seg_col):
            _n = len(_grp)
            if _n < 3:
                continue
            for _sc in _step_cols:
                _cnt = int(_grp[_sc].sum())
                _seg_data.append({
                    "segment": str(_seg_val),
                    "step": _step_labels.get(_sc, _sc),
                    "count": _cnt,
                    "total": _n,
                    "rate": round(_cnt / _n * 100, 1),
                })

        if _seg_data:
            _df_seg = pd.DataFrame(_seg_data)

            _chart_seg = (
                _alt.Chart(_df_seg)
                .mark_bar()
                .encode(
                    x=_alt.X("rate:Q", title="通過率 (%)", scale=_alt.Scale(domain=[0, 100])),
                    y=_alt.Y("step:N", title="ステップ", sort=[v for v in _step_labels.values()]),
                    color=_alt.Color("segment:N", title=_seg_label),
                    tooltip=["segment", "step", "count", "total", "rate"],
                    xOffset="segment:N",
                )
                .properties(title=f"セグメント別ファネル: {_seg_label}", width=700, height=350)
            )
            _seg_outputs.append(_chart_seg)

            # ピボットテーブル
            _df_pivot = _df_seg.pivot_table(
                index="segment", columns="step", values="rate", aggfunc="first"
            )
            _ordered_steps = [v for v in _step_labels.values() if v in _df_pivot.columns]
            _df_pivot = _df_pivot[_ordered_steps].reset_index()
            _seg_outputs.append(mo.md("**通過率テーブル (%)**"))
            _seg_outputs.append(mo.ui.table(_df_pivot, pagination=False))

            # セグメント別企業数
            _df_n = df_funnel_org.groupby(_seg_col).size().reset_index(name="企業数")
            _df_n.columns = [_seg_label, "企業数"]
            _seg_outputs.append(mo.md("**セグメント別企業数**"))
            _seg_outputs.append(mo.ui.table(_df_n, pagination=False))
        else:
            _seg_outputs.append(mo.md("*十分なデータがありません*"))
    else:
        _seg_outputs.append(mo.md("*セグメントを選択してください*"))

    mo.vstack(_seg_outputs)
    return


@app.cell
def _(df_list_summary, mo):
    # List集計結果（軽量・1行サマリー表示のみ、詳細分析はセクション10で）
    _cl = (df_list_summary['companylist_count'] > 0).sum() if len(df_list_summary) > 0 else 0
    _pl = (df_list_summary['peoplelist_count'] > 0).sum() if len(df_list_summary) > 0 else 0
    mo.md(f"**List保有状況**: CompanyList保有 {_cl}社 / PeopleList保有 {_pl}社（詳細はセクション10参照）")
    return


@app.cell
def _(SF_SCHEMA, df_id_mapping, mo, pd, query_sf):
    # List集計（軽量版）: ORGID単位のCompanyList/PeopleList件数のみ
    df_list_summary = pd.DataFrame()

    _list_orgids = (
        df_id_mapping["ORGID"].dropna().astype(str).unique().tolist()
        if len(df_id_mapping) > 0 and "ORGID" in df_id_mapping.columns
        else []
    )

    if _list_orgids:
        _orgids_sql = ",".join([f"'{o}'" for o in _list_orgids[:500]])
        try:
            _q = f"""
            SELECT
                u.ORGID,
                COUNT(DISTINCT cl.ID) AS companylist_count,
                COUNT(DISTINCT pl.ID) AS peoplelist_count
            FROM {SF_SCHEMA}.USERORGANIZATION u
            JOIN {SF_SCHEMA}.USERORGRELATION ur ON u.ORGID = ur.ORGANIZATIONID
            LEFT JOIN {SF_SCHEMA}.COMPANYLIST cl ON cl.USERORGRELATIONID = ur.ID
            LEFT JOIN {SF_SCHEMA}.PEOPLELIST pl ON pl.USERORGRELATIONID = ur.ID
            WHERE u.ORGID IN ({_orgids_sql})
            GROUP BY u.ORGID
            """
            df_list_summary = query_sf(_q)
            # カラム名を統一
            _rename = {}
            for _c in df_list_summary.columns:
                if _c.upper() == "COMPANYLIST_COUNT":
                    _rename[_c] = "companylist_count"
                elif _c.upper() == "PEOPLELIST_COUNT":
                    _rename[_c] = "peoplelist_count"
                elif _c.upper() == "ORGID":
                    _rename[_c] = "ORGID"
            df_list_summary = df_list_summary.rename(columns=_rename)
            df_list_summary["companylist_count"] = df_list_summary["companylist_count"].fillna(0).astype(int)
            df_list_summary["peoplelist_count"] = df_list_summary["peoplelist_count"].fillna(0).astype(int)
        except Exception as _e:
            mo.md(f"List集計エラー: {_e}")
            df_list_summary = pd.DataFrame()

    mo.md(f"**List集計**: {len(df_list_summary)}企業 (CompanyList保有: {(df_list_summary['companylist_count'] > 0).sum() if len(df_list_summary) > 0 else 0}, PeopleList保有: {(df_list_summary['peoplelist_count'] > 0).sum() if len(df_list_summary) > 0 else 0})")
    return (df_list_summary,)




@app.cell
def _(mo):
    mo.md("""
    ## 6. データ統合（チャーン + GA + インテント）
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
        # pv_search は除外（InfoBoxドメインに該当ページなし）

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
        "list_rate", "download_rate",
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
    ## 7. ロジスティック回帰（チャーン寄与度）
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
                "pv_list", "pv_company", "pv_download",
                "list_rate", "download_rate",
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
    ## 8. LLM危険度判定（Gemini）
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
                        COUNTIF(REGEXP_CONTAINS(page_location, r'/companies/[a-z0-9]') AND event_name = 'page_view') AS pv_company_detail,
                        COUNTIF(REGEXP_CONTAINS(page_location, r'/company-lists|/people-lists|/leads-lists') AND event_name = 'page_view') AS pv_list,
                        COUNTIF(REGEXP_CONTAINS(page_location, r'/analysis') AND event_name = 'page_view') AS pv_analysis,
                        COUNTIF(REGEXP_CONTAINS(page_location, r'/people') AND event_name = 'page_view') AS pv_people
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
    ## 9. ダッシュボード全体サマリー
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
def _(mo):
    mo.md("""
    ## 10. List分析（CompanyList / PeopleList）
    企業を選択すると、その企業のリスト詳細をオンデマンドで取得・表示します。
    """)
    return


@app.cell
def _(df_id_mapping, df_list_summary, mo):
    _list_options = {}
    if len(df_id_mapping) > 0 and len(df_list_summary) > 0:
        _name_cols = [c for c in ["BQ_COMPANY_NAME", "ORG_NAME"] if c in df_id_mapping.columns]
        _base = df_id_mapping.dropna(subset=["ORGID"]).copy()
        if _name_cols:
            _base["_label"] = _base[_name_cols[0]].astype(str)
        else:
            _base["_label"] = _base["ORGID"].astype(str)

        _orgids_with_lists = set(
            df_list_summary[
                (df_list_summary["companylist_count"].fillna(0) > 0)
                | (df_list_summary["peoplelist_count"].fillna(0) > 0)
            ]["ORGID"].astype(str).tolist()
        )

        for _, _r in _base.drop_duplicates(subset=["ORGID"]).iterrows():
            if str(_r["ORGID"]) in _orgids_with_lists:
                _list_options[f"{_r['_label']} ({_r['ORGID']})"] = _r["ORGID"]

    _default = next(iter(_list_options.keys()), None) if _list_options else None
    list_org_selector = mo.ui.dropdown(
        options=_list_options,
        label="List分析: 企業を選択（リスト1件以上）",
        value=_default,
    )
    list_org_selector
    return (list_org_selector,)


@app.cell
def _(
    SF_SCHEMA,
    df_list_summary,
    list_org_selector,
    mo,
    pd,
    query_sf,
):
    _out = []
    _orgid = list_org_selector.value if list_org_selector.value else None

    if _orgid:
        # サマリー（事前集計済み）
        _sum = df_list_summary[df_list_summary["ORGID"] == _orgid] if len(df_list_summary) > 0 else pd.DataFrame()
        _cl_count = int(_sum.iloc[0]["companylist_count"]) if len(_sum) > 0 else 0
        _pl_count = int(_sum.iloc[0]["peoplelist_count"]) if len(_sum) > 0 else 0
        _out.append(mo.md(f"### {list_org_selector.value}\nCompanyList: **{_cl_count}件** / PeopleList: **{_pl_count}件**"))

        # オンデマンドで詳細取得
        with mo.status.spinner(title="List詳細取得中..."):
            _df_cl = pd.DataFrame()
            _df_pl = pd.DataFrame()
            try:
                if _cl_count > 0:
                    _df_cl = query_sf(f"""
                    SELECT bc.SHOGO AS COMPANY_NAME, bc.GYOSHUSHOID AS INDUSTRY_ID,
                           bc.PREFID, bc.EMPCOUNT AS EMPLOYEE_COUNT, cl.CREATEDAT
                    FROM {SF_SCHEMA}.USERORGANIZATION u
                    JOIN {SF_SCHEMA}.USERORGRELATION ur ON u.ORGID = ur.ORGANIZATIONID
                    JOIN {SF_SCHEMA}.COMPANYLIST cl ON cl.USERORGRELATIONID = ur.ID
                    JOIN {SF_SCHEMA}._BEEGLECOMPANYTOCOMPANYLIST rel ON rel.B = cl.ID
                    JOIN {SF_SCHEMA}.BEEGLECOMPANY bc ON rel.A = bc.ID
                    WHERE u.ORGID = '{_orgid}' LIMIT 500
                    """)
            except Exception:
                pass
            try:
                if _pl_count > 0:
                    _df_pl = query_sf(f"""
                    SELECT km.ID AS KEYMAN_ID, km.NAME AS KEYMAN_NAME, pl.CREATEDAT
                    FROM {SF_SCHEMA}.USERORGANIZATION u
                    JOIN {SF_SCHEMA}.USERORGRELATION ur ON u.ORGID = ur.ORGANIZATIONID
                    JOIN {SF_SCHEMA}.PEOPLELIST pl ON pl.USERORGRELATIONID = ur.ID
                    JOIN {SF_SCHEMA}._KEYMANTOPEOPLELIST rel ON rel.B = pl.ID
                    JOIN {SF_SCHEMA}.KEYMAN km ON rel.A = km.ID
                    WHERE u.ORGID = '{_orgid}' LIMIT 500
                    """)
            except Exception:
                pass

        # CompanyList
        if len(_df_cl) > 0:
            _out.append(mo.md(f"#### CompanyList詳細 ({len(_df_cl)}件)"))
            if "INDUSTRY_ID" in _df_cl.columns:
                _ind = _df_cl["INDUSTRY_ID"].dropna().value_counts().head(5).reset_index()
                _ind.columns = ["業種ID", "件数"]
                _out.append(mo.md("**業種分布（上位5件）**"))
                _out.append(mo.ui.table(_ind, pagination=False))
            if "PREFID" in _df_cl.columns:
                _reg = _df_cl["PREFID"].dropna().value_counts().head(5).reset_index()
                _reg.columns = ["都道府県ID", "件数"]
                _out.append(mo.md("**地域分布（上位5件）**"))
                _out.append(mo.ui.table(_reg, pagination=False))
            _out.append(mo.ui.table(_df_cl.head(50), pagination=True))

        # PeopleList
        if len(_df_pl) > 0:
            _out.append(mo.md(f"#### PeopleList詳細 ({len(_df_pl)}件)"))
            _out.append(mo.ui.table(_df_pl.head(50), pagination=True))

        if len(_df_cl) == 0 and len(_df_pl) == 0:
            _out.append(mo.md("*この企業にはリストがありません*"))
    else:
        _out.append(mo.md("*企業を選択してください*"))

    mo.vstack(_out)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

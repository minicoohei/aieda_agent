import marimo

__generated_with = "0.17.8"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import os
    import sys
    import tomllib
    from pathlib import Path

    import pandas as pd
    from dotenv import load_dotenv

    snowflake = None
    snowflake_error = None
    default_backend = None
    serialization = None
    try:
        import snowflake.connector
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization
        import snowflake as snowflake
    except Exception as exc:
        snowflake_error = exc

    root_dir = Path(__file__).parent.parent
    if str(root_dir / "src") not in sys.path:
        sys.path.insert(0, str(root_dir / "src"))

    from ai_data_lab.connectors.bigquery import BigQueryConnector

    return (
        BigQueryConnector,
        Path,
        default_backend,
        load_dotenv,
        mo,
        os,
        pd,
        root_dir,
        serialization,
        snowflake,
        snowflake_error,
        sys,
        tomllib,
    )


@app.cell
def _(mo):
    mo.md(
        """
        # ER図リレーション検証

        - `docs/snowflake_elt_entity_notion.mmd` のリレーションに対して **JOIN件数が0件にならないこと** を確認
        - FKの孤立、NULL、空白/大小文字/Unicode/制御文字のパターンを注釈として出力
        - Snowflake / BigQuery / クロスシステムで分けて検証
        """
    )
    return


@app.cell
def _(Path, load_dotenv, mo, root_dir):
    possible_env_paths = [
        root_dir / ".env",
        Path.cwd() / ".env",
        Path.cwd().parent / ".env",
    ]
    loaded_env_path = None
    for env_path in possible_env_paths:
        if env_path.exists():
            load_dotenv(env_path)
            loaded_env_path = str(env_path)
            break
    if loaded_env_path:
        mo.md(f"✅ `.env` 読み込み: `{loaded_env_path}`")
    else:
        mo.md("⚠️ `.env` が見つかりません（環境変数/`connections.toml` を使用）")
    return (loaded_env_path, possible_env_paths)


@app.cell
def _(Path, mo, os):
    gac_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if gac_path and not Path(gac_path).exists():
        del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        mo.md(
            "⚠️ GOOGLE_APPLICATION_CREDENTIALS が無効なため削除しました（ADCを使用）"
        )
    return (gac_path,)


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
            with open(config_path, "rb") as config_file:
                config = tomllib.load(config_file).get("default", {})

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
    snowflake_context_df = pd.DataFrame()
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT CURRENT_USER() AS USER, CURRENT_ROLE() AS ROLE, "
            "CURRENT_WAREHOUSE() AS WAREHOUSE, CURRENT_DATABASE() AS DATABASE, "
            "CURRENT_SCHEMA() AS SCHEMA"
        )
        snowflake_context_df = cur.fetch_pandas_all()
    except Exception as exc:
        mo.md(f"**接続確認エラー**: `{exc}`")
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
    return (snowflake_context_df,)


@app.cell
def _(mo, snowflake_context_df):
    context_output = mo.md("*接続情報がありません*")
    if len(snowflake_context_df) > 0:
        context_output = mo.ui.table(snowflake_context_df, pagination=False)
    context_output
    return


@app.cell
def _(mo, os):
    sf_db_ui = mo.ui.text(value="ETL_S3_TRANSALES_DB", label="Snowflake Database")
    sf_schema_ui = mo.ui.text(value="TRANSALES_DAILY_SCHEMA", label="Snowflake Schema")
    bq_project_ui = mo.ui.text(
        value=os.getenv("BIGQUERY_PROJECT_ID") or "", label="BigQuery Project ID"
    )
    bq_dataset_ui = mo.ui.text(value="production_infobox", label="BigQuery Dataset")
    sample_size_ui = mo.ui.number(
        value=200,
        label="クロスシステム検証サンプル数",
        start=10,
        stop=5000,
        step=10,
    )
    run_checks_ui = mo.ui.run_button(label="検証を実行")
    mo.vstack(
        [sf_db_ui, sf_schema_ui, bq_project_ui, bq_dataset_ui, sample_size_ui, run_checks_ui]
    )
    return (
        bq_dataset_ui,
        bq_project_ui,
        run_checks_ui,
        sample_size_ui,
        sf_db_ui,
        sf_schema_ui,
    )


@app.cell
def _(mo, os, run_checks_ui, sys):
    autorun_flag = ("--autorun" in sys.argv) or os.getenv("ER_VALIDATE_AUTORUN") == "1"
    run_checks_ready = bool(run_checks_ui.value) or autorun_flag
    if autorun_flag:
        mo.md("✅ autorunモードで検証を実行します。")
    if not run_checks_ready:
        mo.stop(True, mo.md("▶️ 「検証を実行」を押すと結果が表示されます。"))
    return (run_checks_ready,)


@app.cell
def _(bq_dataset_ui, bq_project_ui, mo, sample_size_ui, sf_db_ui, sf_schema_ui):
    sf_db_name = sf_db_ui.value.strip()
    sf_schema_name = sf_schema_ui.value.strip()
    bq_project_id = bq_project_ui.value.strip()
    bq_dataset_name = bq_dataset_ui.value.strip()
    sample_size = int(sample_size_ui.value) if sample_size_ui.value else 200

    if not sf_db_name or not sf_schema_name:
        mo.stop(True, mo.md("❌ SnowflakeのDB/Schemaを入力してください。"))
    if not bq_project_id or not bq_dataset_name:
        mo.stop(True, mo.md("❌ BigQueryのProject/Datasetを入力してください。"))
    return bq_dataset_name, bq_project_id, sample_size, sf_db_name, sf_schema_name


@app.cell
def _():
    sf_relations_user_org = [
        {
            "relation_id": 1,
            "parent_table": "BEEGLECOMPANY",
            "parent_key": "ID",
            "child_table": "USERORGANIZATION",
            "child_key": "COMPANYID",
            "relation_group": "user_org",
        },
        {
            "relation_id": 2,
            "parent_table": "USERORGANIZATION",
            "parent_key": "ORGID",
            "child_table": "USERORGRELATION",
            "child_key": "ORGANIZATIONID",
            "relation_group": "user_org",
        },
        {
            "relation_id": 3,
            "parent_table": "USER",
            "parent_key": "ID",
            "child_table": "USERORGRELATION",
            "child_key": "USERID",
            "relation_group": "user_org",
        },
    ]

    sf_relations_first_party = [
        {
            "relation_id": 4,
            "parent_table": "FIRSTPARTYGROUP",
            "parent_key": "ID",
            "child_table": "FIRSTPARTYVISITLOGS",
            "child_key": "FIRSTPARTYGROUPID",
            "relation_group": "first_party",
        },
        {
            "relation_id": 5,
            "parent_table": "BEEGLECOMPANY",
            "parent_key": "ID",
            "child_table": "FIRSTPARTYVISITLOGS",
            "child_key": "VISITORCOMPANYID",
            "relation_group": "first_party",
        },
        {
            "relation_id": 6,
            "parent_table": "FIRSTPARTYGROUP",
            "parent_key": "ID",
            "child_table": "FIRSTPARTYSCOREVISITOR",
            "child_key": "FIRSTPARTYGROUPID",
            "relation_group": "first_party",
        },
        {
            "relation_id": 7,
            "parent_table": "USERORGANIZATION",
            "parent_key": "ORGID",
            "child_table": "FIRSTPARTYSCOREVISITOR",
            "child_key": "COMPANYID",
            "relation_group": "first_party",
        },
    ]

    sf_relations_intent = [
        {
            "relation_id": 8,
            "parent_table": "FIRSTPARTYGROUP",
            "parent_key": "ID",
            "child_table": "FIRSTPARTYSCORECHANGE",
            "child_key": "FIRSTPARTYGROUPID",
            "relation_group": "intent_score",
        },
        {
            "relation_id": 9,
            "parent_table": "BEEGLECOMPANY",
            "parent_key": "ID",
            "child_table": "FIRSTPARTYSCORECHANGE",
            "child_key": "COMPANYID",
            "relation_group": "intent_score",
        },
        {
            "relation_id": 10,
            "parent_table": "FIRSTPARTYGROUP",
            "parent_key": "ID",
            "child_table": "FIRSTPARTYSCORECHANGEHISTORY",
            "child_key": "FIRSTPARTYGROUPID",
            "relation_group": "intent_score",
        },
        {
            "relation_id": 11,
            "parent_table": "BEEGLECOMPANY",
            "parent_key": "ID",
            "child_table": "FIRSTPARTYSCORECHANGEHISTORY",
            "child_key": "COMPANYID",
            "relation_group": "intent_score",
        },
        {
            "relation_id": 12,
            "parent_table": "BEEGLECOMPANY",
            "parent_key": "ID",
            "child_table": "INTENTSCORECHANGE",
            "child_key": "COMPANYID",
            "relation_group": "intent_score",
        },
        {
            "relation_id": 13,
            "parent_table": "BEEGLECOMPANY",
            "parent_key": "ID",
            "child_table": "INTENTSCORECHANGEHISTORY",
            "child_key": "COMPANYID",
            "relation_group": "intent_score",
        },
    ]

    sf_relations_list = [
        {
            "relation_id": 14,
            "parent_table": "USERORGRELATION",
            "parent_key": "ID",
            "child_table": "COMPANYLIST",
            "child_key": "USERORGRELATIONID",
            "relation_group": "list",
        },
        {
            "relation_id": 15,
            "parent_table": "BEEGLECOMPANY",
            "parent_key": "ID",
            "child_table": "_BEEGLECOMPANYTOCOMPANYLIST",
            "child_key": "A",
            "relation_group": "list",
        },
        {
            "relation_id": 16,
            "parent_table": "COMPANYLIST",
            "parent_key": "ID",
            "child_table": "_BEEGLECOMPANYTOCOMPANYLIST",
            "child_key": "B",
            "relation_group": "list",
        },
        {
            "relation_id": 17,
            "parent_table": "USERORGRELATION",
            "parent_key": "ID",
            "child_table": "PEOPLELIST",
            "child_key": "USERORGRELATIONID",
            "relation_group": "list",
        },
        {
            "relation_id": 18,
            "parent_table": "KEYMAN",
            "parent_key": "ID",
            "child_table": "_KEYMANTOPEOPLELIST",
            "child_key": "A",
            "relation_group": "list",
        },
        {
            "relation_id": 19,
            "parent_table": "PEOPLELIST",
            "parent_key": "ID",
            "child_table": "_KEYMANTOPEOPLELIST",
            "child_key": "B",
            "relation_group": "list",
        },
        {
            "relation_id": 20,
            "parent_table": "BEEGLECOMPANY",
            "parent_key": "ID",
            "child_table": "KEYMAN",
            "child_key": "BEEGLECOMPANYID",
            "relation_group": "list",
        },
    ]

    sf_relations_leadimport = [
        {
            "relation_id": 21,
            "parent_table": "USERORGRELATION",
            "parent_key": "ID",
            "child_table": "LEADIMPORTEVENT",
            "child_key": "CREATEDBYUSERORGRELATIONID",
            "relation_group": "leadimport",
        },
        {
            "relation_id": 22,
            "parent_table": "PEOPLELIST",
            "parent_key": "ID",
            "child_table": "LEADIMPORTEVENT",
            "child_key": "PEOPLELISTID",
            "relation_group": "leadimport",
        },
    ]

    sf_relations_crm = [
        {
            "relation_id": 23,
            "parent_table": "BEEGLECOMPANY",
            "parent_key": "ID",
            "child_table": "LEAD",
            "child_key": "COMPANYID",
            "relation_group": "crm",
        },
        {
            "relation_id": 24,
            "parent_table": "LEADIMPORTEVENT",
            "parent_key": "ID",
            "child_table": "LEAD",
            "child_key": "CREATEDBYLEADIMPORTEVENTID",
            "relation_group": "crm",
        },
        {
            "relation_id": 25,
            "parent_table": "LEADIMPORTEVENT",
            "parent_key": "ID",
            "child_table": "LEAD",
            "child_key": "UPDATEDBYLEADIMPORTEVENTID",
            "relation_group": "crm",
        },
        {
            "relation_id": 26,
            "parent_table": "LEAD",
            "parent_key": "ID",
            "child_table": "_LEADTOPEOPLELIST",
            "child_key": "A",
            "relation_group": "crm",
        },
        {
            "relation_id": 27,
            "parent_table": "PEOPLELIST",
            "parent_key": "ID",
            "child_table": "_LEADTOPEOPLELIST",
            "child_key": "B",
            "relation_group": "crm",
        },
        {
            "relation_id": 28,
            "parent_table": "BEEGLECOMPANY",
            "parent_key": "ID",
            "child_table": "NEGOTIATION",
            "child_key": "COMPANYID",
            "relation_group": "crm",
        },
    ]

    sf_relations_csv_memo = [
        {
            "relation_id": 29,
            "parent_table": "USERORGRELATION",
            "parent_key": "ID",
            "child_table": "CSVDOWNLOADLOG",
            "child_key": "USERORGRELATIONID",
            "relation_group": "csv",
        },
        {
            "relation_id": 30,
            "parent_table": "CSVDOWNLOADLOG",
            "parent_key": "ID",
            "child_table": "_BEEGLECOMPANYTOCSVDOWNLOADLOG",
            "child_key": "A",
            "relation_group": "csv",
        },
        {
            "relation_id": 31,
            "parent_table": "BEEGLECOMPANY",
            "parent_key": "ID",
            "child_table": "_BEEGLECOMPANYTOCSVDOWNLOADLOG",
            "child_key": "B",
            "relation_group": "csv",
        },
        {
            "relation_id": 32,
            "parent_table": "USERORGRELATION",
            "parent_key": "ID",
            "child_table": "CSVDOWNLOADQUOTAUPDATELOG",
            "child_key": "USERORGRELATIONID",
            "relation_group": "csv",
        },
        {
            "relation_id": 33,
            "parent_table": "USERORGRELATION",
            "parent_key": "ID",
            "child_table": "MEMO",
            "child_key": "USERORGRELATIONID",
            "relation_group": "memo",
        },
        {
            "relation_id": 34,
            "parent_table": "BEEGLECOMPANY",
            "parent_key": "ID",
            "child_table": "MEMO",
            "child_key": "COMPANYID",
            "relation_group": "memo",
        },
        {
            "relation_id": 35,
            "parent_table": "MEMOSTATUSSHO",
            "parent_key": "ID",
            "child_table": "MEMO",
            "child_key": "STATUSSHOID",
            "relation_group": "memo",
        },
        {
            "relation_id": 36,
            "parent_table": "MEMOSTATUSDAI",
            "parent_key": "ID",
            "child_table": "MEMOSTATUSSHO",
            "child_key": "MEMOSTATUSDAIID",
            "relation_group": "memo",
        },
    ]

    bq_relations = [
        {
            "relation_id": 37,
            "parent_table": "master_first_party_group",
            "parent_key": "group_id",
            "child_table": "first_party_score_company_all_history_by_group",
            "child_key": "group_id",
            "relation_group": "bq_first_party",
        },
        {
            "relation_id": 38,
            "parent_table": "master_first_party_group",
            "parent_key": "group_id",
            "child_table": "first_party_visitors_log_by_group",
            "child_key": "group_id",
            "relation_group": "bq_first_party",
        },
        {
            "relation_id": 39,
            "parent_table": "master_original_product_category",
            "parent_key": "original_category_id",
            "child_table": "score_company_category_change_v3_all_history",
            "child_key": "original_category_id",
            "relation_group": "bq_second_party",
        },
        {
            "relation_id": 40,
            "parent_table": "master_original_product_category",
            "parent_key": "original_category_id",
            "child_table": "company_category_daily_v3",
            "child_key": "original_category_id",
            "relation_group": "bq_second_party",
        },
    ]

    cross_relations = [
        {
            "relation_id": "41a",
            "parent_table": "BEEGLECOMPANY",
            "parent_key": "COMPNO",
            "child_table": "first_party_score_company_all_history",
            "child_key": "corporate_id",
            "relation_group": "cross_corporate_id",
        },
        {
            "relation_id": "41b",
            "parent_table": "BEEGLECOMPANY",
            "parent_key": "COMPNO",
            "child_table": "first_party_score_company_all_history_by_group",
            "child_key": "corporate_id",
            "relation_group": "cross_corporate_id",
        },
        {
            "relation_id": "41c",
            "parent_table": "BEEGLECOMPANY",
            "parent_key": "COMPNO",
            "child_table": "first_party_visitors_log",
            "child_key": "corporate_id",
            "relation_group": "cross_corporate_id",
        },
        {
            "relation_id": "41d",
            "parent_table": "BEEGLECOMPANY",
            "parent_key": "COMPNO",
            "child_table": "first_party_visitors_log_by_group",
            "child_key": "corporate_id",
            "relation_group": "cross_corporate_id",
        },
        {
            "relation_id": "41e",
            "parent_table": "BEEGLECOMPANY",
            "parent_key": "COMPNO",
            "child_table": "score_company_category_change_v3_all_history",
            "child_key": "corporate_id",
            "relation_group": "cross_corporate_id",
        },
        {
            "relation_id": "41f",
            "parent_table": "BEEGLECOMPANY",
            "parent_key": "COMPNO",
            "child_table": "company_category_daily_v3",
            "child_key": "corporate_id",
            "relation_group": "cross_corporate_id",
        },
        {
            "relation_id": 42,
            "parent_table": "FIRSTPARTYGROUP",
            "parent_key": "ID",
            "child_table": "master_first_party_group",
            "child_key": "group_id",
            "relation_group": "cross_group_id",
        },
    ]

    return (
        bq_relations,
        cross_relations,
        sf_relations_crm,
        sf_relations_csv_memo,
        sf_relations_first_party,
        sf_relations_intent,
        sf_relations_leadimport,
        sf_relations_list,
        sf_relations_user_org,
    )


@app.cell
def _():
    def build_relation_note(row_dict):
        notes = []
        if row_dict.get("error"):
            notes.append(f"error:{row_dict['error']}")
        if row_dict.get("join_count") == 0:
            notes.append("join_zero")
        if row_dict.get("orphan_fk_count", 0) and row_dict.get("orphan_fk_count", 0) > 0:
            notes.append("orphan_fk")
        if row_dict.get("null_fk_count", 0) and row_dict.get("null_fk_count", 0) > 0:
            notes.append("null_fk")
        if row_dict.get("trim_mismatch_count", 0) and row_dict.get(
            "trim_mismatch_count", 0
        ) > 0:
            notes.append("trim_mismatch")
        if row_dict.get("uppercase_mismatch_count", 0) and row_dict.get(
            "uppercase_mismatch_count", 0
        ) > 0:
            notes.append("uppercase_mismatch")
        if row_dict.get("lowercase_count", 0) and row_dict.get("lowercase_count", 0) > 0:
            notes.append("lowercase_present")
        if row_dict.get("non_ascii_count", 0) and row_dict.get("non_ascii_count", 0) > 0:
            notes.append("non_ascii_present")
        if row_dict.get("control_char_count", 0) and row_dict.get(
            "control_char_count", 0
        ) > 0:
            notes.append("control_char_present")
        if row_dict.get("note_extra"):
            notes.append(row_dict["note_extra"])
        return ", ".join(notes)

    return (build_relation_note,)


@app.cell
def _(build_relation_note, pd):
    def add_relation_notes(results_df):
        if results_df.empty:
            return results_df
        notes_series = results_df.apply(lambda row: build_relation_note(row.to_dict()), axis=1)
        results_df = results_df.copy()
        results_df["note"] = notes_series
        return results_df

    return (add_relation_notes,)


@app.cell
def _(get_snowflake_connection, pd):
    def run_sf_relations(relations, sf_db_name, sf_schema_name):
        results = []
        conn = get_snowflake_connection()
        try:
            for rel in relations:
                parent_full = f"{sf_db_name}.{sf_schema_name}.{rel['parent_table']}"
                child_full = f"{sf_db_name}.{sf_schema_name}.{rel['child_table']}"
                child_key = rel["child_key"]
                parent_key = rel["parent_key"]
                query = f"""
                SELECT
                    (SELECT COUNT(*) FROM {parent_full}) AS parent_count,
                    (SELECT COUNT(*) FROM {child_full}) AS child_count,
                    (
                        SELECT COUNT(*)
                        FROM {parent_full} p
                        JOIN {child_full} c
                          ON p.{parent_key} = c.{child_key}
                    ) AS join_count,
                    (
                        SELECT COUNT(*)
                        FROM {child_full} c
                        LEFT JOIN {parent_full} p
                          ON p.{parent_key} = c.{child_key}
                        WHERE c.{child_key} IS NOT NULL
                          AND p.{parent_key} IS NULL
                    ) AS orphan_fk_count,
                    (
                        SELECT COUNT(*)
                        FROM {child_full} c
                        WHERE c.{child_key} IS NULL
                    ) AS null_fk_count,
                    (
                        SELECT COUNT(*)
                        FROM {child_full} c
                        WHERE c.{child_key} IS NOT NULL
                          AND TRIM(TO_VARCHAR(c.{child_key})) != TO_VARCHAR(c.{child_key})
                    ) AS trim_mismatch_count,
                    (
                        SELECT COUNT(*)
                        FROM {child_full} c
                        WHERE c.{child_key} IS NOT NULL
                          AND UPPER(TO_VARCHAR(c.{child_key})) != TO_VARCHAR(c.{child_key})
                    ) AS uppercase_mismatch_count,
                    (
                        SELECT COUNT(*)
                        FROM {child_full} c
                        WHERE c.{child_key} IS NOT NULL
                          AND REGEXP_LIKE(TO_VARCHAR(c.{child_key}), '[a-z]')
                    ) AS lowercase_count,
                    (
                        SELECT COUNT(*)
                        FROM {child_full} c
                        WHERE c.{child_key} IS NOT NULL
                          AND REGEXP_LIKE(TO_VARCHAR(c.{child_key}), '[^\\x00-\\x7F]')
                    ) AS non_ascii_count,
                    (
                        SELECT COUNT(*)
                        FROM {child_full} c
                        WHERE c.{child_key} IS NOT NULL
                          AND REGEXP_LIKE(TO_VARCHAR(c.{child_key}), '[\\x00-\\x1F\\x7F]')
                    ) AS control_char_count
                """
                row_dict = {}
                try:
                    cur = conn.cursor()
                    cur.execute(query)
                    row_df = cur.fetch_pandas_all()
                    if len(row_df) > 0:
                        row_dict = row_df.iloc[0].to_dict()
                except Exception as exc:
                    row_dict = {"error": str(exc)}
                finally:
                    try:
                        cur.close()
                    except Exception:
                        pass
                row_dict.update(
                    {
                        "relation_id": rel["relation_id"],
                        "parent_table": rel["parent_table"],
                        "parent_key": rel["parent_key"],
                        "child_table": rel["child_table"],
                        "child_key": rel["child_key"],
                        "relation_group": rel["relation_group"],
                        "parent_table_full": parent_full,
                        "child_table_full": child_full,
                    }
                )
                results.append(row_dict)
        finally:
            try:
                conn.close()
            except Exception:
                pass
        return pd.DataFrame(results)

    return (run_sf_relations,)


@app.cell
def _(BigQueryConnector, pd):
    def run_bq_relations(relations, bq_project_id, bq_dataset_name):
        results = []
        connector = BigQueryConnector(project_id=bq_project_id)
        for rel in relations:
            parent_full = f"`{bq_project_id}.{bq_dataset_name}.{rel['parent_table']}`"
            child_full = f"`{bq_project_id}.{bq_dataset_name}.{rel['child_table']}`"
            parent_key = rel["parent_key"]
            child_key = rel["child_key"]
            query = f"""
            SELECT
              (SELECT COUNT(*) FROM {parent_full}) AS parent_count,
              (SELECT COUNT(*) FROM {child_full}) AS child_count,
              (
                SELECT COUNT(*)
                FROM {parent_full} p
                JOIN {child_full} c
                  ON p.{parent_key} = c.{child_key}
              ) AS join_count,
              (
                SELECT COUNT(*)
                FROM {child_full} c
                LEFT JOIN {parent_full} p
                  ON p.{parent_key} = c.{child_key}
                WHERE c.{child_key} IS NOT NULL
                  AND p.{parent_key} IS NULL
              ) AS orphan_fk_count,
              (
                SELECT COUNTIF({child_key} IS NULL)
                FROM {child_full}
              ) AS null_fk_count,
              (
                SELECT COUNTIF(
                  {child_key} IS NOT NULL
                  AND TRIM(CAST({child_key} AS STRING)) != CAST({child_key} AS STRING)
                )
                FROM {child_full}
              ) AS trim_mismatch_count,
              (
                SELECT COUNTIF(
                  {child_key} IS NOT NULL
                  AND UPPER(CAST({child_key} AS STRING)) != CAST({child_key} AS STRING)
                )
                FROM {child_full}
              ) AS uppercase_mismatch_count,
              (
                SELECT COUNTIF(
                  {child_key} IS NOT NULL
                  AND REGEXP_CONTAINS(CAST({child_key} AS STRING), r'[a-z]')
                )
                FROM {child_full}
              ) AS lowercase_count,
              (
                SELECT COUNTIF(
                  {child_key} IS NOT NULL
                  AND REGEXP_CONTAINS(CAST({child_key} AS STRING), r'[^\\x00-\\x7F]')
                )
                FROM {child_full}
              ) AS non_ascii_count,
              (
                SELECT COUNTIF(
                  {child_key} IS NOT NULL
                  AND REGEXP_CONTAINS(CAST({child_key} AS STRING), r'[\\x00-\\x1F\\x7F]')
                )
                FROM {child_full}
              ) AS control_char_count
            """
            row_dict = {}
            try:
                row_df = connector.query(query)
                if len(row_df) > 0:
                    row_dict = row_df.iloc[0].to_dict()
            except Exception as exc:
                row_dict = {"error": str(exc)}
            row_dict.update(
                {
                    "relation_id": rel["relation_id"],
                    "parent_table": rel["parent_table"],
                    "parent_key": rel["parent_key"],
                    "child_table": rel["child_table"],
                    "child_key": rel["child_key"],
                    "relation_group": rel["relation_group"],
                    "parent_table_full": parent_full,
                    "child_table_full": child_full,
                }
            )
            results.append(row_dict)
        return pd.DataFrame(results)

    return (run_bq_relations,)


@app.cell
def _(BigQueryConnector, get_snowflake_connection, pd):
    def format_in_list(values):
        cleaned = []
        for value in values:
            if value is None:
                continue
            value_str = str(value).replace("'", "''")
            cleaned.append(f"'{value_str}'")
        return ", ".join(cleaned)

    def run_cross_relations(relations, sf_db_name, sf_schema_name, bq_project_id, bq_dataset_name, sample_size):
        results = []
        connector = BigQueryConnector(project_id=bq_project_id)
        conn = get_snowflake_connection()
        try:
            for rel in relations:
                parent_full = f"{sf_db_name}.{sf_schema_name}.{rel['parent_table']}"
                child_full = f"`{bq_project_id}.{bq_dataset_name}.{rel['child_table']}`"
                parent_key = rel["parent_key"]
                child_key = rel["child_key"]

                row_dict = {}
                try:
                    cur = conn.cursor()
                    cur.execute(f"SELECT COUNT(*) AS cnt FROM {parent_full}")
                    parent_count = cur.fetch_pandas_all().iloc[0]["CNT"]
                except Exception as exc:
                    parent_count = None
                    row_dict["note_extra"] = f"parent_count_error:{exc}"
                finally:
                    try:
                        cur.close()
                    except Exception:
                        pass

                try:
                    child_count_df = connector.query(f"SELECT COUNT(*) AS cnt FROM {child_full}")
                    child_count = child_count_df.iloc[0]["cnt"]
                except Exception as exc:
                    child_count = None
                    extra_note = row_dict.get("note_extra", "")
                    row_dict["note_extra"] = f"{extra_note} child_count_error:{exc}".strip()

                sf_sample_keys = []
                try:
                    cur = conn.cursor()
                    cur.execute(
                        f"""
                        SELECT DISTINCT TO_VARCHAR({parent_key}) AS KEY_VALUE
                        FROM {parent_full}
                        WHERE {parent_key} IS NOT NULL
                        QUALIFY ROW_NUMBER() OVER (ORDER BY RANDOM()) <= {sample_size}
                        """
                    )
                    sf_sample_df = cur.fetch_pandas_all()
                    if "KEY_VALUE" in sf_sample_df.columns:
                        sf_sample_keys = sf_sample_df["KEY_VALUE"].dropna().tolist()
                except Exception as exc:
                    extra_note = row_dict.get("note_extra", "")
                    row_dict["note_extra"] = f"{extra_note} sf_sample_error:{exc}".strip()
                finally:
                    try:
                        cur.close()
                    except Exception:
                        pass

                sf_to_bq_match_count = 0
                if sf_sample_keys:
                    in_list = format_in_list(sf_sample_keys)
                    if in_list:
                        try:
                            match_df = connector.query(
                                f\"SELECT COUNT(*) AS match_count FROM {child_full} WHERE CAST({child_key} AS STRING) IN ({in_list})\"
                            )
                            sf_to_bq_match_count = match_df.iloc[0]["match_count"]
                        except Exception as exc:
                            extra_note = row_dict.get("note_extra", "")
                            row_dict["note_extra"] = f"{extra_note} sf_to_bq_error:{exc}".strip()

                bq_sample_keys = []
                try:
                    bq_sample_df = connector.query(
                        f"""
                        SELECT DISTINCT CAST({child_key} AS STRING) AS key_value
                        FROM {child_full}
                        WHERE {child_key} IS NOT NULL
                        ORDER BY RAND()
                        LIMIT {sample_size}
                        """
                    )
                    if "key_value" in bq_sample_df.columns:
                        bq_sample_keys = bq_sample_df["key_value"].dropna().tolist()
                except Exception as exc:
                    extra_note = row_dict.get("note_extra", "")
                    row_dict["note_extra"] = f"{extra_note} bq_sample_error:{exc}".strip()

                bq_to_sf_match_count = 0
                if bq_sample_keys:
                    in_list = format_in_list(bq_sample_keys)
                    if in_list:
                        try:
                            cur = conn.cursor()
                            cur.execute(
                                f\"SELECT COUNT(*) AS match_count FROM {parent_full} WHERE TO_VARCHAR({parent_key}) IN ({in_list})\"
                            )
                            bq_to_sf_match_count = cur.fetch_pandas_all().iloc[0]["MATCH_COUNT"]
                        except Exception as exc:
                            extra_note = row_dict.get("note_extra", "")
                            row_dict["note_extra"] = f"{extra_note} bq_to_sf_error:{exc}".strip()
                        finally:
                            try:
                                cur.close()
                            except Exception:
                                pass

                row_dict.update(
                    {
                        "relation_id": rel["relation_id"],
                        "parent_table": rel["parent_table"],
                        "parent_key": rel["parent_key"],
                        "child_table": rel["child_table"],
                        "child_key": rel["child_key"],
                        "relation_group": rel["relation_group"],
                        "parent_table_full": parent_full,
                        "child_table_full": child_full,
                        "parent_count": parent_count,
                        "child_count": child_count,
                        "join_count": sf_to_bq_match_count,
                        "orphan_fk_count": None,
                        "null_fk_count": None,
                        "trim_mismatch_count": None,
                        "uppercase_mismatch_count": None,
                        "lowercase_count": None,
                        "non_ascii_count": None,
                        "control_char_count": None,
                        "sample_size": sample_size,
                        "sample_sf_to_bq_match": sf_to_bq_match_count,
                        "sample_bq_to_sf_match": bq_to_sf_match_count,
                        "note_extra": row_dict.get(
                            "note_extra",
                            f"sampled:{sample_size} sf_to_bq:{sf_to_bq_match_count} bq_to_sf:{bq_to_sf_match_count}",
                        ),
                    }
                )
                results.append(row_dict)
        finally:
            try:
                conn.close()
            except Exception:
                pass
        return pd.DataFrame(results)

    return (run_cross_relations,)


@app.cell
def _(
    add_relation_notes,
    run_checks_ready,
    run_sf_relations,
    sf_db_name,
    sf_relations_user_org,
    sf_schema_name,
):
    sf_user_org_results_df = None
    if run_checks_ready:
        sf_user_org_results_raw_df = run_sf_relations(
            sf_relations_user_org, sf_db_name, sf_schema_name
        )
        sf_user_org_results_df = add_relation_notes(sf_user_org_results_raw_df)
    return (sf_user_org_results_df,)


@app.cell
def _(mo, sf_user_org_results_df):
    mo.md("## Snowflake: ユーザー・組織系")
    if sf_user_org_results_df is None:
        mo.md("未実行")
    else:
        mo.ui.table(sf_user_org_results_df, pagination=False)
    return


@app.cell
def _(
    add_relation_notes,
    run_checks_ready,
    run_sf_relations,
    sf_db_name,
    sf_relations_first_party,
    sf_schema_name,
):
    sf_first_party_results_df = None
    if run_checks_ready:
        sf_first_party_results_raw_df = run_sf_relations(
            sf_relations_first_party, sf_db_name, sf_schema_name
        )
        sf_first_party_results_df = add_relation_notes(sf_first_party_results_raw_df)
    return (sf_first_party_results_df,)


@app.cell
def _(mo, sf_first_party_results_df):
    mo.md("## Snowflake: 1stパーティ系")
    if sf_first_party_results_df is None:
        mo.md("未実行")
    else:
        mo.ui.table(sf_first_party_results_df, pagination=False)
    return


@app.cell
def _(
    add_relation_notes,
    run_checks_ready,
    run_sf_relations,
    sf_db_name,
    sf_relations_intent,
    sf_schema_name,
):
    sf_intent_results_df = None
    if run_checks_ready:
        sf_intent_results_raw_df = run_sf_relations(
            sf_relations_intent, sf_db_name, sf_schema_name
        )
        sf_intent_results_df = add_relation_notes(sf_intent_results_raw_df)
    return (sf_intent_results_df,)


@app.cell
def _(mo, sf_intent_results_df):
    mo.md("## Snowflake: インテントスコア系")
    if sf_intent_results_df is None:
        mo.md("未実行")
    else:
        mo.ui.table(sf_intent_results_df, pagination=False)
    return


@app.cell
def _(
    add_relation_notes,
    run_checks_ready,
    run_sf_relations,
    sf_db_name,
    sf_relations_list,
    sf_relations_leadimport,
    sf_relations_crm,
    sf_schema_name,
):
    sf_list_lead_results_df = None
    if run_checks_ready:
        sf_list_relations_all = (
            sf_relations_list + sf_relations_leadimport + sf_relations_crm
        )
        sf_list_lead_results_raw_df = run_sf_relations(
            sf_list_relations_all, sf_db_name, sf_schema_name
        )
        sf_list_lead_results_df = add_relation_notes(sf_list_lead_results_raw_df)
    return (sf_list_lead_results_df,)


@app.cell
def _(mo, sf_list_lead_results_df):
    mo.md("## Snowflake: リスト + LEADIMPORTEVENT + CRM")
    if sf_list_lead_results_df is None:
        mo.md("未実行")
    else:
        mo.ui.table(sf_list_lead_results_df, pagination=False)
    return


@app.cell
def _(
    add_relation_notes,
    run_checks_ready,
    run_sf_relations,
    sf_db_name,
    sf_relations_csv_memo,
    sf_schema_name,
):
    sf_csv_memo_results_df = None
    if run_checks_ready:
        sf_csv_memo_results_raw_df = run_sf_relations(
            sf_relations_csv_memo, sf_db_name, sf_schema_name
        )
        sf_csv_memo_results_df = add_relation_notes(sf_csv_memo_results_raw_df)
    return (sf_csv_memo_results_df,)


@app.cell
def _(mo, sf_csv_memo_results_df):
    mo.md("## Snowflake: CSVダウンロード + MEMO")
    if sf_csv_memo_results_df is None:
        mo.md("未実行")
    else:
        mo.ui.table(sf_csv_memo_results_df, pagination=False)
    return


@app.cell
def _(
    add_relation_notes,
    bq_dataset_name,
    bq_project_id,
    bq_relations,
    run_bq_relations,
    run_checks_ready,
):
    bq_results_df = None
    if run_checks_ready:
        bq_results_raw_df = run_bq_relations(
            bq_relations, bq_project_id, bq_dataset_name
        )
        bq_results_df = add_relation_notes(bq_results_raw_df)
    return (bq_results_df,)


@app.cell
def _(bq_results_df, mo):
    mo.md("## BigQuery: 1st/2ndパーティ系")
    if bq_results_df is None:
        mo.md("未実行")
    else:
        mo.ui.table(bq_results_df, pagination=False)
    return


@app.cell
def _(
    add_relation_notes,
    bq_dataset_name,
    bq_project_id,
    cross_relations,
    run_checks_ready,
    run_cross_relations,
    sample_size,
    sf_db_name,
    sf_schema_name,
):
    cross_results_df = None
    if run_checks_ready:
        cross_results_raw_df = run_cross_relations(
            cross_relations,
            sf_db_name,
            sf_schema_name,
            bq_project_id,
            bq_dataset_name,
            sample_size,
        )
        cross_results_df = add_relation_notes(cross_results_raw_df)
    return (cross_results_df,)


@app.cell
def _(cross_results_df, mo):
    mo.md("## クロスシステム: Snowflake ⇔ BigQuery")
    if cross_results_df is None:
        mo.md("未実行")
    else:
        mo.ui.table(cross_results_df, pagination=False)
    return


@app.cell
def _(
    bq_results_df,
    cross_results_df,
    pd,
    sf_csv_memo_results_df,
    sf_first_party_results_df,
    sf_intent_results_df,
    sf_list_lead_results_df,
    sf_user_org_results_df,
):
    result_frames = [
        sf_user_org_results_df,
        sf_first_party_results_df,
        sf_intent_results_df,
        sf_list_lead_results_df,
        sf_csv_memo_results_df,
        bq_results_df,
        cross_results_df,
    ]
    result_frames = [frame for frame in result_frames if frame is not None]
    if result_frames:
        all_results_df = pd.concat(result_frames, ignore_index=True, sort=False)
    else:
        all_results_df = pd.DataFrame()
    return (all_results_df,)


@app.cell
def _(all_results_df, mo):
    mo.md("## 全リレーション結果（一覧）")
    mo.ui.table(all_results_df, pagination=True)
    return


@app.cell
def _(all_results_df):
    zero_join_results_df = all_results_df[all_results_df["join_count"] == 0].copy()
    return (zero_join_results_df,)


@app.cell
def _(mo, zero_join_results_df):
    if zero_join_results_df.empty:
        mo.md("✅ JOIN件数が0のリレーションはありません。")
    else:
        mo.md(f"❌ JOIN件数が0のリレーション: {len(zero_join_results_df)}件")
        mo.ui.table(zero_join_results_df, pagination=True)
    return


@app.cell
def _(all_results_df, pd):
    issue_mask = (
        all_results_df["note"].fillna("") != ""
        if "note" in all_results_df.columns
        else pd.Series(False, index=all_results_df.index)
    )
    issues_results_df = all_results_df[issue_mask].copy()
    return (issues_results_df,)


@app.cell
def _(issues_results_df, mo):
    mo.md("## 注釈付きリレーション（要確認）")
    mo.ui.table(issues_results_df, pagination=True)
    return


if __name__ == "__main__":
    app.run()

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
        # MEMO 全量分析

        Snowflakeの `MEMO` テーブルを全量で確認し、**パース可能性**と
        **アクション記録の傾向**を把握するためのノートブックです。
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

    def escape_like(value: str) -> str:
        return value.replace("'", "''")

    return escape_like, run_query, schema


@app.cell
def _(mo):
    mo.md("## 1. MEMO 基本統計")
    return


@app.cell
def _(run_query, schema):
    memo_stats_sql = f"""
    SELECT
      COUNT(*) AS total,
      SUM(IFF(CONTENT IS NULL OR CONTENT = '', 1, 0)) AS null_count,
      SUM(IFF(LENGTH(COALESCE(CONTENT, '')) < 10, 1, 0)) AS short_count,
      SUM(IFF(LENGTH(COALESCE(CONTENT, '')) >= 10, 1, 0)) AS valid_count
    FROM {schema}.MEMO
    """
    df_memo_stats = run_query(memo_stats_sql)
    return df_memo_stats, memo_stats_sql


@app.cell
def _(df_memo_stats, mo):
    _output = mo.md("*データがありません*")
    if len(df_memo_stats) > 0:
        _output = mo.ui.table(df_memo_stats, pagination=False)
    _output
    return


@app.cell
def _(run_query, schema):
    length_dist_sql = f"""
    SELECT
      CASE
        WHEN LENGTH(COALESCE(CONTENT, '')) < 10 THEN '0-9'
        WHEN LENGTH(COALESCE(CONTENT, '')) < 50 THEN '10-49'
        WHEN LENGTH(COALESCE(CONTENT, '')) < 100 THEN '50-99'
        WHEN LENGTH(COALESCE(CONTENT, '')) < 500 THEN '100-499'
        ELSE '500+'
      END AS length_bucket,
      COUNT(*) AS count
    FROM {schema}.MEMO
    GROUP BY length_bucket
    ORDER BY length_bucket
    """
    df_length_dist = run_query(length_dist_sql)
    return df_length_dist, length_dist_sql


@app.cell
def _(df_length_dist, mo):
    _output = mo.md("*データがありません*")
    if len(df_length_dist) > 0:
        _output = mo.ui.table(df_length_dist, pagination=False)
    _output
    return


@app.cell
def _(run_query, schema):
    created_dist_sql = f"""
    SELECT
      DATE_TRUNC('month', CREATEDAT) AS month,
      COUNT(*) AS count
    FROM {schema}.MEMO
    WHERE CREATEDAT IS NOT NULL
    GROUP BY month
    ORDER BY month
    """
    df_created_dist = run_query(created_dist_sql)
    return created_dist_sql, df_created_dist


@app.cell
def _(df_created_dist, mo):
    _output = mo.md("*データがありません*")
    if len(df_created_dist) > 0:
        _output = mo.ui.table(df_created_dist, pagination=True)
    _output
    return


@app.cell
def _(run_query, schema):
    weekday_dist_sql = f"""
    SELECT
      TO_CHAR(CREATEDAT, 'DY') AS weekday,
      DAYOFWEEK(CREATEDAT) AS weekday_num,
      COUNT(*) AS count
    FROM {schema}.MEMO
    WHERE CREATEDAT IS NOT NULL
    GROUP BY weekday, weekday_num
    ORDER BY weekday_num
    """
    df_weekday_dist = run_query(weekday_dist_sql)
    return df_weekday_dist, weekday_dist_sql


@app.cell
def _(df_weekday_dist, mo):
    _output = mo.md("*データがありません*")
    if len(df_weekday_dist) > 0:
        _output = mo.ui.table(df_weekday_dist, pagination=False)
    _output
    return


@app.cell
def _(mo):
    mo.md("## 2. アクションキーワード出現率")
    return


@app.cell
def _(run_query, schema):
    keyword_sql = f"""
    SELECT
      SUM(IFF(CONTENT ILIKE '%電話%' OR CONTENT ILIKE '%架電%' OR CONTENT ILIKE '%TEL%', 1, 0)) AS phone,
      SUM(IFF(CONTENT ILIKE '%メール%' OR CONTENT ILIKE '%Mail%' OR CONTENT ILIKE '%mail%', 1, 0)) AS email,
      SUM(IFF(CONTENT ILIKE '%訪問%' OR CONTENT ILIKE '%来社%', 1, 0)) AS visit,
      SUM(IFF(CONTENT ILIKE '%商談%' OR CONTENT ILIKE '%MTG%' OR CONTENT ILIKE '%打合せ%', 1, 0)) AS meeting,
      SUM(IFF(CONTENT ILIKE '%資料%' OR CONTENT ILIKE '%提案書%', 1, 0)) AS document
    FROM {schema}.MEMO
    """
    df_keyword_stats = run_query(keyword_sql)
    return df_keyword_stats, keyword_sql


@app.cell
def _(df_keyword_stats, mo):
    _output = mo.md("*データがありません*")
    if len(df_keyword_stats) > 0:
        _output = mo.ui.table(df_keyword_stats, pagination=False)
    _output
    return


@app.cell
def _(mo):
    mo.md("## 3. サンプル確認")
    return


@app.cell
def _(mo):
    sample_size = mo.ui.number(label="サンプル件数", value=100, min=10, max=1000, step=10)
    min_length = mo.ui.number(label="最小文字数", value=10, min=0, max=500, step=10)
    keyword_filter = mo.ui.text(label="キーワード（任意）", value="")
    sample_button = mo.ui.run_button(label="サンプル取得")
    sample_size, min_length, keyword_filter, sample_button
    return keyword_filter, min_length, sample_button, sample_size


@app.cell
def _(
    escape_like,
    keyword_filter,
    min_length,
    run_query,
    sample_button,
    sample_size,
    schema,
):
    df_samples = None
    if sample_button.value:
        keyword_clause = ""
        if keyword_filter.value:
            keyword_value = escape_like(keyword_filter.value)
            keyword_clause = f"AND CONTENT ILIKE '%{keyword_value}%'"

        memo_sample_sql = f"""
        SELECT ID, CONTENT, CREATEDAT, COMPANYID, USERORGRELATIONID
        FROM {schema}.MEMO
        WHERE CONTENT IS NOT NULL
          AND LENGTH(CONTENT) >= {int(min_length.value)}
          {keyword_clause}
        SAMPLE ({int(sample_size.value)} ROWS)
        """
        df_samples = run_query(memo_sample_sql)
    return df_samples


@app.cell
def _(df_samples, mo):
    _output = mo.md("*サンプルがありません*")
    if df_samples is not None and len(df_samples) > 0:
        _output = mo.ui.table(df_samples, pagination=True)
    _output
    return


@app.cell
def _(mo):
    mo.md("## 4. パース可能性スコア")
    return


@app.cell
def _(run_query, schema):
    keyword_expression = (
        "CONTENT ILIKE '%電話%' OR CONTENT ILIKE '%架電%' OR CONTENT ILIKE '%TEL%' "
        "OR CONTENT ILIKE '%メール%' OR CONTENT ILIKE '%Mail%' OR CONTENT ILIKE '%mail%' "
        "OR CONTENT ILIKE '%訪問%' OR CONTENT ILIKE '%来社%' "
        "OR CONTENT ILIKE '%商談%' OR CONTENT ILIKE '%MTG%' OR CONTENT ILIKE '%打合せ%' "
        "OR CONTENT ILIKE '%資料%' OR CONTENT ILIKE '%提案書%'"
    )

    parse_score_sql = f"""
    SELECT
      COUNT(*) AS total,
      SUM(IFF(({keyword_expression}) AND LENGTH(COALESCE(CONTENT, '')) >= 50, 1, 0)) AS high_count,
      SUM(IFF(NOT ({keyword_expression}) AND LENGTH(COALESCE(CONTENT, '')) >= 50, 1, 0)) AS medium_count,
      SUM(IFF(LENGTH(COALESCE(CONTENT, '')) < 50, 1, 0)) AS low_count
    FROM {schema}.MEMO
    """
    df_parse_score = run_query(parse_score_sql)
    return df_parse_score, parse_score_sql


@app.cell
def _(df_parse_score, mo):
    _output = mo.md("*データがありません*")
    if len(df_parse_score) > 0:
        _output = mo.ui.table(df_parse_score, pagination=False)
    _output
    return


@app.cell
def _(mo):
    mo.md("## 5. LLMによるアクション分類（任意）")
    return


@app.cell
def _(mo):
    llm_enabled = mo.ui.checkbox(label="LLMでサンプル分類を実行", value=False)
    llm_sample_size = mo.ui.number(
        label="LLMサンプル件数", value=20, min=5, max=200, step=5
    )
    llm_button = mo.ui.run_button(label="LLM分類を実行")
    llm_enabled, llm_sample_size, llm_button
    return llm_button, llm_enabled, llm_sample_size


@app.cell
def _(llm_button, llm_enabled, llm_sample_size, mo, os, run_query, schema):
    df_llm = None
    api_key = (
        os.getenv("GOOGLE_API_KEY")
        or os.getenv("GEMINI_API_KEY")
        or os.getenv("GENAI_API_KEY")
    )
    if not llm_enabled.value:
        pass
    elif not api_key:
        mo.md("**APIキーが見つかりません**: `GOOGLE_API_KEY` などを設定してください。")
    elif llm_button.value:
        llm_sample_sql = f"""
        SELECT ID, CONTENT
        FROM {schema}.MEMO
        WHERE CONTENT IS NOT NULL AND LENGTH(CONTENT) >= 10
        SAMPLE ({int(llm_sample_size.value)} ROWS)
        """
        df_llm = run_query(llm_sample_sql)
        if df_llm is not None and len(df_llm) > 0:
            from google import genai
            from google.genai import types

            client = genai.Client(api_key=api_key)
            labels = []
            for content in df_llm["CONTENT"].tolist():
                prompt = f"""
次の営業メモを1つのラベルに分類してください。
ラベルは以下のいずれか1つのみ: 電話, メール, 訪問, 商談, 資料送付, その他

メモ:
{content}
"""
                response = client.models.generate_content(
                    model="gemini-3-flash-preview",
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        temperature=0.1,
                        max_output_tokens=30,
                    ),
                )
                raw = (response.text or "").strip()
                label = "その他"
                for candidate in ["電話", "メール", "訪問", "商談", "資料送付"]:
                    if candidate in raw:
                        label = candidate
                        break
                labels.append(label)

            df_llm = df_llm.assign(action_label=labels)
    return df_llm


@app.cell
def _(df_llm, mo):
    _output = mo.md("*LLM結果がありません*")
    if df_llm is None:
        _output = mo.md("*LLM分類を実行すると結果が表示されます*")
    elif len(df_llm) > 0:
        _output = mo.ui.table(df_llm, pagination=True)
    _output
    return


if __name__ == "__main__":
    app.run()

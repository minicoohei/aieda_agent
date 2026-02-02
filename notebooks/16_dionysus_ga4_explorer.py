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
        # Dionysus GA4 Explorer

        GA4の基本指標（users / sessions / page_views など）を確認し、
        org_idごとの利用状況を探索するノートブックです。
        """
    )
    return


@app.cell
def _():
    import os
    from datetime import date, timedelta
    from pathlib import Path

    import pandas as pd

    bq_error = None
    bigquery = None
    service_account = None
    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account
    except Exception as exc:
        bq_error = exc

    return Path, bq_error, bigquery, date, os, pd, service_account, timedelta


@app.cell
def _(bq_error, mo):
    if bq_error:
        mo.md(f"**BigQueryモジュール読み込みエラー**: `{bq_error}`")
    return


@app.cell
def _(Path, bigquery, os, service_account):
    PROJECT_ID = "gree-dionysus-infobox"
    DATASET_ID = "analytics_400693944"

    def get_bq_client():
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
            return bigquery.Client(project=PROJECT_ID, credentials=credentials)
        return bigquery.Client(project=PROJECT_ID)

    return DATASET_ID, PROJECT_ID, get_bq_client


@app.cell
def _(date, mo, timedelta):
    default_end = date.today()
    default_start = default_end - timedelta(days=30)
    start_date = mo.ui.date(value=default_start, label="開始日")
    end_date = mo.ui.date(value=default_end, label="終了日")
    start_date
    end_date
    return end_date, start_date


@app.cell
def _(mo):
    org_filter = mo.ui.text(label="org_id（任意）", value="")
    org_filter
    return (org_filter,)


@app.cell
def _(DATASET_ID, end_date, get_bq_client, mo, org_filter, pd, start_date):
    df_summary = pd.DataFrame()
    if start_date.value and end_date.value:
        start_suffix = start_date.value.strftime("%Y%m%d")
        end_suffix = end_date.value.strftime("%Y%m%d")
        org_condition = ""
        if org_filter.value:
            org_condition = (
                "AND (SELECT value.string_value FROM UNNEST(user_properties)"
                f" WHERE key = 'org_id') = '{org_filter.value}'"
            )
        sql = f"""
        WITH base AS (
            SELECT
                user_pseudo_id,
                (SELECT value.int_value FROM UNNEST(event_params)
                 WHERE key = 'ga_session_id') AS ga_session_id,
                (SELECT value.string_value FROM UNNEST(user_properties)
                 WHERE key = 'org_id') AS org_id,
                event_name
            FROM `{DATASET_ID}.events_*`
            WHERE _TABLE_SUFFIX BETWEEN '{start_suffix}' AND '{end_suffix}'
            {org_condition}
        )
        SELECT
            COUNT(DISTINCT user_pseudo_id) AS users,
            COUNT(DISTINCT CONCAT(user_pseudo_id, '-', CAST(ga_session_id AS STRING))) AS sessions,
            COUNTIF(event_name = 'page_view') AS page_views,
            COUNTIF(event_name = 'first_visit') AS new_users
        FROM base
        """
        try:
            client = get_bq_client()
            df_summary = client.query(sql).to_dataframe()
        except Exception as e:
            mo.md(f"**集計エラー**: `{e}`")
    return (df_summary,)


@app.cell
def _(df_summary, mo):
    _output = mo.md("*データがありません*")
    if len(df_summary) > 0:
        _output = mo.ui.table(df_summary, pagination=False)
    _output
    return


@app.cell
def _(DATASET_ID, end_date, get_bq_client, mo, org_filter, pd, start_date):
    df_events = pd.DataFrame()
    if start_date.value and end_date.value:
        start_suffix = start_date.value.strftime("%Y%m%d")
        end_suffix = end_date.value.strftime("%Y%m%d")
        org_condition = ""
        if org_filter.value:
            org_condition = (
                "AND (SELECT value.string_value FROM UNNEST(user_properties)"
                f" WHERE key = 'org_id') = '{org_filter.value}'"
            )
        sql = f"""
        SELECT
            event_name,
            COUNT(*) AS events,
            COUNT(DISTINCT user_pseudo_id) AS users
        FROM `{DATASET_ID}.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{start_suffix}' AND '{end_suffix}'
        {org_condition}
        GROUP BY event_name
        ORDER BY events DESC
        LIMIT 50
        """
        try:
            client = get_bq_client()
            df_events = client.query(sql).to_dataframe()
        except Exception as e:
            mo.md(f"**イベント集計エラー**: `{e}`")
    return (df_events,)


@app.cell
def _(df_events, mo):
    _output = mo.md("*データがありません*")
    if len(df_events) > 0:
        _output = mo.ui.table(df_events, pagination=True)
    _output
    return


@app.cell
def _(DATASET_ID, end_date, get_bq_client, mo, org_filter, pd, start_date):
    df_pages = pd.DataFrame()
    if start_date.value and end_date.value:
        start_suffix = start_date.value.strftime("%Y%m%d")
        end_suffix = end_date.value.strftime("%Y%m%d")
        org_condition = ""
        if org_filter.value:
            org_condition = (
                "AND (SELECT value.string_value FROM UNNEST(user_properties)"
                f" WHERE key = 'org_id') = '{org_filter.value}'"
            )
        sql = f"""
        SELECT
            (SELECT value.string_value
             FROM UNNEST(event_params)
             WHERE key = 'page_location') AS page_location,
            (SELECT value.string_value
             FROM UNNEST(event_params)
             WHERE key = 'page_title') AS page_title,
            COUNT(*) AS page_views,
            COUNT(DISTINCT user_pseudo_id) AS users
        FROM `{DATASET_ID}.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{start_suffix}' AND '{end_suffix}'
          AND event_name = 'page_view'
          {org_condition}
        GROUP BY page_location, page_title
        ORDER BY page_views DESC
        LIMIT 100
        """
        try:
            client = get_bq_client()
            df_pages = client.query(sql).to_dataframe()
        except Exception as e:
            mo.md(f"**ページ閲覧集計エラー**: `{e}`")
    return (df_pages,)


@app.cell
def _(df_pages, mo):
    _output = mo.md("*データがありません*")
    if len(df_pages) > 0:
        _output = mo.ui.table(df_pages, pagination=True)
    _output
    return


if __name__ == "__main__":
    app.run()

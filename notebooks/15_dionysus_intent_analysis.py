import marimo

__generated_with = "0.10.14"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md("""
    # GREE Dionysus InfoBox - インテント分析レポート
    
    **分析日**: 2026-01-28  
    **データソース**: gree-dionysus-infobox.production_infobox
    
    ---
    
    ## インテントレベルの仕組み
    
    ### スコア閾値（total_score による判定）
    | レベル | 名称 | total_score範囲 | 判定基準 |
    |--------|------|----------------|---------|
    | 1 | Low（低） | 0.4 ~ 11.6 | 閲覧行動が少ない |
    | 2 | Middle（中） | 5.2 ~ 40.0 | 一定の関心あり |
    | 3 | High（高） | 20.0 ~ 40.0 | 購買意向が高い |
    
    ### スコア計算式
    ```
    total_score = score_pv_2week + score_pv_2month + score_spike + score_frequency
    ```
    
    | 指標 | 説明 | Low平均 | Middle平均 | High平均 |
    |------|------|---------|-----------|---------|
    | `score_pv_2week` | 直近2週間のPVスコア | 0.44 | 5.34 | 6.20 |
    | `score_pv_2month` | 直近2ヶ月のPVスコア | 2.60 | 7.08 | 6.33 |
    | `score_spike` | **閲覧急増スコア** | 0.01 | 3.85 | **8.92** |
    | `score_frequency` | **閲覧頻度スコア** | 0.17 | 5.95 | **7.18** |
    
    ### レベル変化の条件
    - **Low → Middle**: 2週間で複数回の閲覧、定期的なアクセス
    - **Middle → High**: 閲覧の急増（spike）+ 高頻度の継続的アクセス（total_score 20以上）
    """)
    return


@app.cell
def _():
    import os
    import pandas as pd
    from google.cloud import bigquery
    from google.oauth2 import service_account
    import plotly.express as px
    return bigquery, os, pd, px, service_account


@app.cell
def _(bigquery, os, pd, service_account):
    # BigQuery接続
    key_path = os.path.expanduser("~/.gcp/gree-dionysus-infobox.json")
    project_id = "gree-dionysus-infobox"
    
    credentials_auth = service_account.Credentials.from_service_account_file(key_path)
    client_bq = bigquery.Client(project=project_id, credentials=credentials_auth)
    
    def query_to_df(sql):
        """Storage Read API権限なしでDataFrameを取得"""
        job = client_bq.query(sql)
        results = job.result()
        rows = [dict(row) for row in results]
        return pd.DataFrame(rows)
    
    return client_bq, credentials_auth, key_path, project_id, query_to_df


@app.cell
def _(mo):
    mo.md("## 1. 直近7日間のサマリー")
    return


@app.cell
def _(mo, query_to_df):
    # サマリー
    summary_query = """
    SELECT 
        CASE intent_level WHEN 2 THEN 'Middle（Level 2）' WHEN 3 THEN 'High（Level 3）' END as level,
        COUNT(DISTINCT corporate_id) as unique_companies
    FROM `gree-dionysus-infobox.production_infobox.first_party_score_company_latest`
    WHERE change_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
      AND intent_level IN (2, 3)
    GROUP BY 1
    ORDER BY 1 DESC
    """
    df_summary = query_to_df(summary_query)
    
    total_middle = df_summary[df_summary['level'] == 'Middle（Level 2）']['unique_companies'].values[0] if len(df_summary[df_summary['level'] == 'Middle（Level 2）']) > 0 else 0
    total_high = df_summary[df_summary['level'] == 'High（Level 3）']['unique_companies'].values[0] if len(df_summary[df_summary['level'] == 'High（Level 3）']) > 0 else 0
    
    mo.md(f"""
    ### インテントレベル別 企業数（直近7日間）
    
    | レベル | ユニーク企業数 |
    |--------|---------------|
    | **Middle（Level 2）** | {total_middle:,}社 |
    | **High（Level 3）** | {total_high:,}社 |
    | **合計** | {total_middle + total_high:,}社 |
    """)
    return df_summary, summary_query, total_high, total_middle


@app.cell
def _(mo):
    mo.md("## 2. カテゴリ別インテント分析")
    return


@app.cell
def _(px, query_to_df):
    # カテゴリ別
    category_intent_query = """
    SELECT 
        c.original_category_name as category,
        COUNTIF(s.intent_level = 2) as middle_count,
        COUNTIF(s.intent_level = 3) as high_count,
        COUNTIF(s.intent_level IN (2, 3)) as total_active
    FROM `gree-dionysus-infobox.production_infobox.company_category_daily_v3` c
    JOIN `gree-dionysus-infobox.production_infobox.first_party_score_company_latest` s
      ON CAST(c.corporate_id AS INT64) = s.corporate_id
    WHERE c.view_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
      AND s.change_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
      AND s.intent_level IN (2, 3)
    GROUP BY 1
    ORDER BY total_active DESC
    LIMIT 20
    """
    df_category_intent = query_to_df(category_intent_query)
    
    fig_category = px.bar(
        df_category_intent,
        x='total_active',
        y='category',
        orientation='h',
        title='カテゴリ別 インテント企業数 TOP20（直近7日間）',
        labels={'total_active': '企業数', 'category': 'カテゴリ'},
        color='total_active',
        color_continuous_scale='Reds'
    )
    fig_category.update_layout(yaxis={'categoryorder': 'total ascending'}, height=600)
    fig_category
    return category_intent_query, df_category_intent, fig_category


@app.cell
def _(df_category_intent, mo):
    mo.ui.table(df_category_intent, selection=None)
    return


@app.cell
def _(mo):
    mo.md("## 3. クライアント別インテント分析")
    return


@app.cell
def _(mo, query_to_df):
    # クライアント別
    client_intent_query = """
    WITH client_scores AS (
        SELECT 
            s.first_party_corporate_id,
            s.intent_level,
            COUNT(DISTINCT s.corporate_id) as company_count
        FROM `gree-dionysus-infobox.production_infobox.first_party_score_company_latest` s
        WHERE s.change_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
          AND s.intent_level IN (2, 3)
        GROUP BY 1, 2
    ),
    pivoted AS (
        SELECT 
            first_party_corporate_id,
            SUM(CASE WHEN intent_level = 2 THEN company_count ELSE 0 END) as middle_count,
            SUM(CASE WHEN intent_level = 3 THEN company_count ELSE 0 END) as high_count,
            SUM(company_count) as total_count
        FROM client_scores
        GROUP BY 1
    )
    SELECT * FROM pivoted ORDER BY total_count DESC LIMIT 20
    """
    df_client_intent = query_to_df(client_intent_query)
    
    mo.md("### クライアント別 インテント企業数 TOP20")
    return client_intent_query, df_client_intent


@app.cell
def _(df_client_intent, mo):
    mo.ui.table(df_client_intent, selection=None)
    return


@app.cell
def _(mo):
    mo.md("## 4. Level 3（High）企業リスト - 購買意向が高い企業")
    return


@app.cell
def _(query_to_df):
    # Level 3 企業リスト
    high_query = """
    SELECT 
        m.company_name as 企業名,
        m.industry_main_name as 業種,
        m.emp_size as 従業員規模,
        s.change_date as 更新日
    FROM `gree-dionysus-infobox.production_infobox.first_party_score_company_latest` s
    JOIN `gree-dionysus-infobox.production_infobox.master_company_industry` m
      ON CAST(s.corporate_id AS STRING) = m.company_id
    WHERE s.intent_level = 3
      AND s.change_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
      AND m.company_name IS NOT NULL
    ORDER BY s.change_date DESC, m.company_name
    LIMIT 100
    """
    df_high_companies = query_to_df(high_query)
    return df_high_companies, high_query


@app.cell
def _(df_high_companies, mo):
    mo.md(f"**High（Level 3）企業: {len(df_high_companies)}社**（直近7日間、先頭100社）")
    return


@app.cell
def _(df_high_companies, mo):
    mo.ui.table(df_high_companies, selection=None, pagination=True)
    return


@app.cell
def _(mo):
    mo.md("## 5. Level 2（Middle）企業リスト - 一定の関心がある企業")
    return


@app.cell
def _(query_to_df):
    # Level 2 企業リスト
    middle_query = """
    SELECT 
        m.company_name as 企業名,
        m.industry_main_name as 業種,
        m.emp_size as 従業員規模,
        s.change_date as 更新日
    FROM `gree-dionysus-infobox.production_infobox.first_party_score_company_latest` s
    JOIN `gree-dionysus-infobox.production_infobox.master_company_industry` m
      ON CAST(s.corporate_id AS STRING) = m.company_id
    WHERE s.intent_level = 2
      AND s.change_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
      AND m.company_name IS NOT NULL
    ORDER BY s.change_date DESC, m.company_name
    LIMIT 100
    """
    df_middle_companies = query_to_df(middle_query)
    return df_middle_companies, middle_query


@app.cell
def _(df_middle_companies, mo):
    mo.md(f"**Middle（Level 2）企業: {len(df_middle_companies)}社**（直近7日間、先頭100社）")
    return


@app.cell
def _(df_middle_companies, mo):
    mo.ui.table(df_middle_companies, selection=None, pagination=True)
    return


@app.cell
def _(mo):
    mo.md("## 6. スコア構成の詳細分析")
    return


@app.cell
def _(mo, query_to_df):
    # スコア構成
    score_composition_query = """
    SELECT 
        intent_level,
        CASE intent_level WHEN 1 THEN 'Low' WHEN 2 THEN 'Middle' WHEN 3 THEN 'High' END as level_name,
        COUNT(*) as count,
        ROUND(AVG(score_pv_2week), 2) as avg_pv_2week,
        ROUND(AVG(score_pv_2month), 2) as avg_pv_2month,
        ROUND(AVG(score_spike), 2) as avg_spike,
        ROUND(AVG(score_frequency), 2) as avg_frequency,
        ROUND(AVG(total_score), 2) as avg_total_score,
        ROUND(MIN(total_score), 2) as min_score,
        ROUND(MAX(total_score), 2) as max_score
    FROM `gree-dionysus-infobox.production_infobox.bi_company_category_daily_with_intent`
    WHERE intent_level IS NOT NULL
    GROUP BY 1, 2
    ORDER BY 1
    """
    df_score_composition = query_to_df(score_composition_query)
    
    mo.md("### インテントレベル別 スコア構成")
    return df_score_composition, score_composition_query


@app.cell
def _(df_score_composition, mo):
    mo.ui.table(df_score_composition, selection=None)
    return


@app.cell
def _(mo):
    mo.md("## 7. レベル変化パターン")
    return


@app.cell
def _(mo, query_to_df):
    # レベル変化
    level_change_query = """
    SELECT 
        CASE intent_level_before WHEN 1 THEN 'Low' WHEN 2 THEN 'Middle' WHEN 3 THEN 'High' END as from_level,
        CASE intent_level WHEN 1 THEN 'Low' WHEN 2 THEN 'Middle' WHEN 3 THEN 'High' END as to_level,
        COUNT(*) as count
    FROM `gree-dionysus-infobox.production_infobox.first_party_score_company_latest`
    WHERE intent_level_before IS NOT NULL
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    df_level_change = query_to_df(level_change_query)
    
    mo.md("""
    ### レベル変化の実績
    
    企業のインテントレベルがどのように遷移したかを示すデータ
    """)
    return df_level_change, level_change_query


@app.cell
def _(df_level_change, mo):
    mo.ui.table(df_level_change, selection=None)
    return


@app.cell
def _(mo):
    mo.md("## 8. 課題カテゴリ別の高インテント率")
    return


@app.cell
def _(px, query_to_df):
    # 課題カテゴリ別
    issue_intent_query = """
    SELECT 
        issue_main_name as 課題カテゴリ,
        COUNT(*) as total_count,
        COUNTIF(intent_level = 1) as low_count,
        COUNTIF(intent_level = 2) as middle_count,
        COUNTIF(intent_level = 3) as high_count,
        ROUND(COUNTIF(intent_level = 3) * 100.0 / COUNT(*), 1) as high_rate_pct
    FROM `gree-dionysus-infobox.production_infobox.bi_company_category_daily_with_intent`
    WHERE intent_level IS NOT NULL
      AND issue_main_name IS NOT NULL
    GROUP BY 1
    ORDER BY high_rate_pct DESC
    """
    df_issue_intent = query_to_df(issue_intent_query)
    
    fig_issue = px.bar(
        df_issue_intent,
        x='課題カテゴリ',
        y='high_rate_pct',
        title='課題カテゴリ別 高インテント率（%）',
        labels={'high_rate_pct': 'High率（%）'},
        color='high_rate_pct',
        color_continuous_scale='Oranges'
    )
    fig_issue.update_layout(xaxis_tickangle=45)
    fig_issue
    return df_issue_intent, fig_issue, issue_intent_query


@app.cell
def _(df_issue_intent, mo):
    mo.ui.table(df_issue_intent, selection=None)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    
    ## まとめ
    
    ### 傾向
    
    1. **Level 3（High）企業の特徴**
       - 大手製造業・IT企業が多い（キヤノン、スズキ、SCSK、TIS等）
       - 従業員1000人以上の大企業が中心
    
    2. **人気カテゴリ**
       - PDF編集が圧倒的（1,000社以上）
       - 業務効率化系（ノーコード、OCR、文書管理）が上位
       - CRM/SFA系も人気
    
    3. **High率が高いカテゴリ**
       - 働き方改革・生産性の向上（7.3%）
       - コスト削減（3.7%）
       - 売上拡大・マーケティング（3.6%）
    
    4. **レベル変化のポイント**
       - **閲覧の急増（spike）** と **高頻度アクセス（frequency）** がHigh到達に重要
       - total_score 20以上でHigh判定
    """)
    return


if __name__ == "__main__":
    app.run()

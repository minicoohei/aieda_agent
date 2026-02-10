# 会社マスタ系データマート設計

## 概要

企業単位のデータマートを設計するにあたり、各データ項目がどのレイヤー・テーブルに存在するかを整理し、作成可能なダミー変数の候補を一覧化する。

**目標マート粒度**: 企業（BeegleCompany / 法人番号）単位の1行

**結合キー**:
- Snowflake内: `BEEGLECOMPANY.ID`
- BigQuery ⇔ Snowflake: `corporate_id`（法人番号）= `BEEGLECOMPANY.COMPNO`

---

## 1. データ項目 x テーブル所在マップ

### 1.1 会社基本情報

| データ項目 | DKS | Snowflake (ETL_S3) | PlanetScale | BigQuery |
|---|---|---|---|---|
| 会社名（商号） | CompanyEntity | BEEGLECOMPANY.SHOGO / SHOGOFULL | BeegleCompany.shogo | - |
| 法人番号 | CompanyEntity.compno | BEEGLECOMPANY.COMPNO | BeegleCompany.compno | corporate_id |
| 本社所在地 | LocationEntity | int_beegle_company (pref, city, zip, add) | BeegleCompany (prefId, cityId, zip, add) + Pref + City | - |
| 設立年月日 | CompanyEntity.seturitu | int_beegle_company.seturitu | BeegleCompany.seturitu | - |
| 上場区分 | CompanyEntity.stock_market_id | cleansed_beegle_company_information.stock_market_label | StockMarket + BeegleCompany.stockMarketId | - |

### 1.2 業種・規模

| データ項目 | DKS | Snowflake (ETL_S3) | PlanetScale | BigQuery |
|---|---|---|---|---|
| 業種（大分類） | CompanyEntity.gyoshu_sho_id | cleansed_industry_type → int_beegle_company.gyoshushoid | GyoshuDai + GyoshuSho + BeegleCompany.gyoshuShoId | - |
| 業種（小分類） | 同上 | 同上 | GyoshuSho.code / name | - |
| 従業員数（レンジ） | CompanyEntity.emp_id | int_beegle_company.empid | Emp + BeegleCompany.empId | - |
| 従業員数（実数） | EmployeeTransitionRecord | int_beegle_company.empcount | BeegleCompany.empCount | - |
| 資本金（レンジ） | CompanyEntity.shihon_id | int_beegle_company.shihonid | Shihon + BeegleCompany.shihonId | - |
| 売上高（レンジ） | CompanyFinanceEntity.net_sales | cleansed_financial_data_riskmonster | Revenue + BeegleCompany.revenueId | - |

### 1.3 財務・成長

| データ項目 | DKS | Snowflake (ETL_S3) | PlanetScale | BigQuery |
|---|---|---|---|---|
| 売上成長率 | FinanceTransitionRecord.salesGrowthRate | cleansed_ib_tag_employeetransitionrecord | ActivityTag (タグ化のみ) | - |
| 営業利益率 | FinanceTransitionRecord.operationMargin | cleansed_financial_data_riskmonster | ActivityTag (タグ化のみ) | - |
| 純利益率 | FinanceTransitionRecord.netIncomeMargin | cleansed_edinet_financialreport | ActivityTag (タグ化のみ) | - |
| 従業員成長率 | EmployeeTransitionRecord.employee_growth_rate_* | cleansed_ib_tag_employeetransitionrecord | EmpTransition + BeegleCompany.empTransition*Id | - |

### 1.4 キーマン・連絡先・部署

| データ項目 | DKS | Snowflake (ETL_S3) | PlanetScale | BigQuery |
|---|---|---|---|---|
| キーマン情報 | KeyMan | KEYMAN | KeyMan (112万件, 48万社) | - |
| 電話番号 | LocationEntity | int_beegle_company | BeegleCompany.tel (13.2%) | - |
| メールアドレス | CompanyEntity | int_beegle_company | BeegleCompany.mail (5.2%) | - |
| HP URL | CompanyEntity | int_beegle_company | BeegleCompany.hpUrl (31.6%) | - |
| 部署情報 | Department | DEPARTMENT (source定義のみ) | Department (196万件) | - |

### 1.5 求人動向

| データ項目 | DKS | Snowflake (ETL_S3) | PlanetScale | BigQuery |
|---|---|---|---|---|
| 求人数 | JobOpeningsV2 | - (API直接連携) | JobOpening + JobOpeningJobType | - |
| 採用活発度 | JobOpeningGrowthFlag | - | BeegleCompany (タグ経由) | - |

### 1.6 1stパーティインテント

| データ項目 | DKS | Snowflake (ETL_S3) | PlanetScale | BigQuery (production_infobox) |
|---|---|---|---|---|
| 1stパーティスコア（企業単位） | - | FIRSTPARTYSCORECHANGE / HISTORY | FirstPartyScoreChange | first_party_score_company_all_history |
| 1stパーティスコア（サイト単位） | - | 同上 (GROUP_ID付き) | 同上 | first_party_score_company_all_history_by_group |
| サイト訪問ログ（企業単位） | - | FIRSTPARTYVISITLOGS | FirstPartyVisitLogs | first_party_visitors_log |
| サイト訪問ログ（サイト単位） | - | 同上 | 同上 | first_party_visitors_log_by_group |
| 顧客サイトマスタ | - | FIRSTPARTYGROUP | FirstPartyGroup | master_first_party_group |

### 1.7 2ndパーティインテント

| データ項目 | DKS | Snowflake (ETL_S3) | PlanetScale | BigQuery (production_infobox) |
|---|---|---|---|---|
| 2ndパーティスコア | - | INTENTSCORECHANGE / HISTORY | IntentScoreChange | score_company_category_change_v3_all_history |
| インテントキーワードマスタ | - | - | OriginalProductCategory | master_original_product_category |
| 比較サイトアクセスログ | - | - | - | company_category_daily_v3 |

### 1.8 CRM（リード・商談）

| データ項目 | DKS | Snowflake (ETL_S3) | PlanetScale | BigQuery |
|---|---|---|---|---|
| リード（人物） | - | LEAD | - | - |
| 商談 | - | NEGOTIATION | - | - |
| リード×人物リスト | - | _LEADTOPEOPLELIST | - | - |
| インポート履歴 | - | LEADIMPORTEVENT | LeadImportEvent | - |

### 1.9 ユーザー行動ログ

| データ項目 | DKS | Snowflake (ETL_S3) | PlanetScale | BigQuery |
|---|---|---|---|---|
| 企業リスト追加 | - | COMPANYLIST + _BEEGLECOMPANYTOCOMPANYLIST | CompanyList | - |
| 人物リスト追加 | - | PEOPLELIST + _KEYMANTOPEOPLELIST | PeopleList | - |
| CSVダウンロード | - | CSVDOWNLOADLOG + _BEEGLECOMPANYTOCSVDOWNLOADLOG | CsvDownloadLog | - |
| 活動履歴（MEMO） | - | MEMO | Memo | - |

---

## 2. ダミー変数候補一覧

企業（BEEGLECOMPANY.ID / 法人番号）単位で作成可能なダミー変数の候補。

### 2.1 基本属性

| 変数名 | 型 | 元テーブル.カラム | 説明 |
|---|---|---|---|
| `company_name` | string | BEEGLECOMPANY.SHOGO | 会社名 |
| `compno` | string | BEEGLECOMPANY.COMPNO | 法人番号 |
| `pref_code` | string | Pref.code via BeegleCompany.prefId | 都道府県コード |
| `city_code` | string | City.code via BeegleCompany.cityId | 市区町村コード |
| `is_listed` | 0/1 | StockMarket.id via BeegleCompany.stockMarketId | 上場=1, 非上場=0 |
| `stock_market_order` | int | StockMarket.order | 上場市場の順序 (1=プライム...9=札証アンビシャス, NULL=非上場) |
| `established_years` | int | BeegleCompany.seturitu から算出 | 設立年数 |

### 2.2 業種

| 変数名 | 型 | 元テーブル.カラム | 説明 |
|---|---|---|---|
| `gyoshu_dai_code` | string | GyoshuDai.code | 業種大分類コード |
| `gyoshu_dai_name` | string | GyoshuDai.name | 業種大分類名 |
| `gyoshu_sho_code` | string | GyoshuSho.code | 業種小分類コード |
| `gyoshu_sho_name` | string | GyoshuSho.name | 業種小分類名 |

### 2.3 企業規模

| 変数名 | 型 | 元テーブル.カラム | 説明 |
|---|---|---|---|
| `emp_range_order` | int | Emp.order via BeegleCompany.empId | 従業員数レンジ (1-8) |
| `emp_count` | int | BeegleCompany.empCount | 従業員数（実数値） |
| `shihon_range_order` | int | Shihon.order via BeegleCompany.shihonId | 資本金レンジ (1-6) |
| `revenue_range_order` | int | Revenue.order via BeegleCompany.revenueId | 売上レンジ (1-5) |

### 2.4 財務・成長

| 変数名 | 型 | 元テーブル.カラム | 説明 |
|---|---|---|---|
| `sales_growth_rate` | float | FinanceTransitionRecord.salesGrowthRate (DKS/SF) | 売上成長率 |
| `operation_margin` | float | FinanceTransitionRecord.operationMargin (DKS/SF) | 営業利益率 |
| `net_income_margin` | float | FinanceTransitionRecord.netIncomeMargin (DKS/SF) | 純利益率 |
| `emp_growth_rate_1y` | float | EmployeeTransitionRecord.employee_growth_rate_last_year | 従業員増加率(1年) |
| `emp_growth_rate_6m` | float | EmployeeTransitionRecord.employee_growth_rate_last_half | 従業員増加率(6ヶ月) |
| `emp_growth_rate_3m` | float | EmployeeTransitionRecord.employee_growth_rate_last_quarter | 従業員増加率(3ヶ月) |
| `emp_transition_quarter` | category | EmpTransition via BeegleCompany.empTransitionLastQuarterId | 従業員推移(直近四半期) |
| `emp_transition_half` | category | EmpTransition via BeegleCompany.empTransitionLastHalfId | 従業員推移(直近半期) |

### 2.5 キーマン・連絡先

| 変数名 | 型 | 元テーブル.カラム | 説明 |
|---|---|---|---|
| `has_keyman` | 0/1 | KEYMAN.BEEGLECOMPANYID EXISTS | キーマン情報の有無 |
| `keyman_count` | int | COUNT(KEYMAN) per BEEGLECOMPANYID | キーマン数 |
| `has_ceo_keyman` | 0/1 | KEYMAN.POSITION LIKE '%代表%' | 代表者キーマンの有無 |
| `has_tel` | 0/1 | BeegleCompany.tel IS NOT NULL | 電話番号の有無 |
| `has_mail` | 0/1 | BeegleCompany.mail IS NOT NULL | メールアドレスの有無 |
| `has_hp_url` | 0/1 | BeegleCompany.hpUrl IS NOT NULL | HP URLの有無 |
| `department_count` | int | COUNT(Department) per companyId | 部署数 |

### 2.6 求人動向

| 変数名 | 型 | 元テーブル.カラム | 説明 |
|---|---|---|---|
| `job_opening_count` | int | COUNT(JobOpening) per companyId | 掲載中求人数 |
| `job_opening_active_1m` | 0/1 | JobOpeningGrowthFlag.one_month_ago | 1ヶ月前より求人増加 |
| `job_opening_active_2m` | 0/1 | JobOpeningGrowthFlag.two_months_ago | 2ヶ月前より求人増加 |
| `job_opening_active_3m` | 0/1 | JobOpeningGrowthFlag.three_months_ago | 3ヶ月前より求人増加 |

### 2.7 1stパーティインテント

| 変数名 | 型 | 元テーブル.カラム | 説明 |
|---|---|---|---|
| `fp_score_latest` | int(1-3) | first_party_score_company_all_history.intent_level (最新change_date) | 1stパーティスコア最新値 |
| `fp_score_max` | int(1-3) | MAX(intent_level) | 1stパーティスコア最大値 |
| `fp_score_is_high` | 0/1 | intent_level = 3 (最新) | 1stパーティHigh判定 |
| `fp_visit_count_30d` | int | COUNT(first_party_visitors_log) WHERE view_date >= 30日前 | 直近30日訪問回数 |
| `fp_visit_days_30d` | int | COUNT(DISTINCT view_date) WHERE view_date >= 30日前 | 直近30日訪問日数 |
| `fp_referrer_type_count` | int | COUNT(DISTINCT page_referrer_type) | 流入経路種別数 |
| `fp_group_count` | int | COUNT(group_id) in master_first_party_group per client_id | 登録サイト数 |

### 2.8 2ndパーティインテント

| 変数名 | 型 | 元テーブル.カラム | 説明 |
|---|---|---|---|
| `sp_score_latest` | int(1-3) | score_company_category_change_v3_all_history.intent_level (最新) | 2ndパーティスコア最新値 |
| `sp_score_max` | int(1-3) | MAX(intent_level) | 2ndパーティスコア最大値 |
| `sp_score_is_high` | 0/1 | intent_level = 3 (最新) | 2ndパーティHigh判定 |
| `sp_keyword_count` | int | COUNT(DISTINCT original_category_id) per corporate_id | 関心インテントキーワード数 |
| `sp_keyword_high_count` | int | COUNT(DISTINCT original_category_id) WHERE intent_level = 3 | Highスコアのキーワード数 |
| `sp_access_count_30d` | int | COUNT(company_category_daily_v3) WHERE view_date >= 30日前 | 直近30日比較サイトアクセス回数 |
| `sp_service_type_count` | int | COUNT(DISTINCT service_type) via original_category_id | 関心大カテゴリ数 |

### 2.9 CRM（リード・商談）

| 変数名 | 型 | 元テーブル.カラム | 説明 |
|---|---|---|---|
| `lead_count` | int | COUNT(LEAD) per COMPANYID WHERE DELETED=0 | 有効リード数 |
| `lead_has_contact` | 0/1 | MAX(LEAD.HASCONTACT) | 連絡先ありリードの有無 |
| `lead_has_email` | 0/1 | LEAD.EMAIL IS NOT NULL のいずれか | メール付きリードの有無 |
| `negotiation_count` | int | COUNT(NEGOTIATION) per COMPANYID | 商談数 |
| `has_negotiation` | 0/1 | NEGOTIATION EXISTS per COMPANYID | 商談の有無 |

### 2.10 ユーザー行動ログ（顧客側のInfoBox利用状況）

| 変数名 | 型 | 元テーブル.カラム | 説明 |
|---|---|---|---|
| `list_add_count_30d` | int | COUNT(_BEEGLECOMPANYTOCOMPANYLIST) WHERE CREATEDAT >= 30日前 | 直近30日リスト追加回数 |
| `csv_download_count_30d` | int | COUNT(_BEEGLECOMPANYTOCSVDOWNLOADLOG) WHERE CREATEDAT >= 30日前 | 直近30日CSVダウンロード回数 |
| `memo_count` | int | COUNT(MEMO) per COMPANYID | 活動履歴数 |
| `memo_latest_priority` | enum | MEMO.PRIORITY (最新ACTIVITYDATE) | 最新活動の優先度(A/B/C) |
| `memo_status_dai` | string | MEMOSTATUSDAI.NAME via MEMO最新 | 最新活動状況(大分類) |
| `memo_status_sho` | string | MEMOSTATUSSHO.NAME via MEMO最新 | 最新活動状況(詳細) |

---

## 3. 備考

### 結合キーの対応表

| Snowflake (ETL_S3) | BigQuery (production_infobox) | 説明 |
|---|---|---|
| BEEGLECOMPANY.COMPNO | corporate_id | 法人番号で結合 |
| FIRSTPARTYGROUP.ID | master_first_party_group.group_id | 1stパーティサイトID |
| BEEGLECOMPANY.ID | LEAD.COMPANYID | Snowflake内BeegleCompany ID |
| BEEGLECOMPANY.ID | NEGOTIATION.COMPANYID | Snowflake内BeegleCompany ID |

### 未解決事項

- ~~**NEGOTIATION.ORGANIZATIONID**: 関連先テーブルが不明~~ → **解消**: USERORGANIZATION.ORGID で確定（2026-02 開発チーム回答）
- **利益率・売上成長率の数値データ**: PlanetScaleにはタグ化されたカテゴリのみ。数値が必要な場合はDKS/Snowflakeの cleansed_* テーブルから取得が必要
- **Departmentのdbtモデル**: source定義のみでCLEANSED以降は未実装。マートに含める場合はDepartmentテーブルから直接集計

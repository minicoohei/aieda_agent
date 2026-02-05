# セマンティック定義 質問ステータス（ローカル）

データ分析時にテーブル定義やカラムの意味を参照するためのドキュメントです。

- 更新日: 2026-02-03
- 出典: InfoBox開発チーム（Slack, 2026-01〜02）で共有された定義
- ER図: `snowflake_elt_entity.puml`, `snowflake_elt_entity_notion.puml`

---

## 解消済み質問一覧

| ID | 質問項目 | 回答 |
| --- | --- | --- |
| Q01 | インテントスコアの正本テーブル名は？ | **1st**: FIRSTPARTYSCORECHANGE / FIRSTPARTYSCORECHANGEHISTORY, **2nd**: INTENTSCORECHANGE / INTENTSCORECHANGEHISTORY |
| Q02 | インテントスコアのキーは？ | **1st**: 複合UK (FIRSTPARTYGROUPID, COMPANYID), **2nd**: 複合UK (ORIGINALPRODUCTCATEGORYID, COMPANYID) |
| Q05 | リストインポートの実テーブル名は？ | 人物リスト: **LEADIMPORTEVENT**に履歴保管, 企業リスト: インポート履歴を残さない |
| Q06 | _BEEGLECOMPANYTOCOMPANYLIST の A/B は？ | **A: company_id (BEEGLECOMPANY), B: list_id (COMPANYLIST)** テーブル名左側がA、右側がB |
| Q07 | _KEYMANTOPEOPLELIST の A/B は？ | **A: keyman_id (KEYMAN), B: list_id (PEOPLELIST)** テーブル名左側がA、右側がB |
| Q08 | CSVDOWNLOADLOG.USERID のID体系は？ | **USERIDではなくUSERORGRELATIONID**が正しい。USERORGRELATIONテーブルのIDに紐づく |
| Q09 | USERORGRELATION.ID は何を表す？ | **UUID主キー（意味なし）**。USERID=ログインユーザーID、ORGANIZATIONID=組織ID |
| Q10 | MEMOの紐づけ方法は？ | ユーザー: **MEMO → USERORGRELATION → USER**, 企業: **MEMO → BEEGLECOMPANY (COMPANYID)** |
| Q14 | FIRSTPARTYSCOREVISITOR.COMPANYID は？ | **ユーザー企業ID（ORGID）** |
| Q16 | PEOPLELIST と COMPANYLIST の違いは？ | **PEOPLELIST**: 人物リスト（人物検索/CSVインポート）、履歴はLEADIMPORTEVENT。**COMPANYLIST**: 企業リスト（企業検索/CSVインポート）、履歴なし |
| Q20 | COMPANYLIST/CSVDOWNLOADLOG のユーザーID体系は？ | 両方とも **USERORGRELATIONID** を使用（USERIDではない） |
| Q21 | MEMO.PRIORITY と STATUSSHOID の定義は？ | **PRIORITY**: A/B/C/NULL（優先度ENUM）。**STATUSSHOID**: MEMOSTATUSSHOへのFK。MEMOSTATUSSHO→MEMOSTATUSDAIがN:1で紐づき、詳細/大分類に対応 |
| Q24 | MEMOの user→company 判別方法は？ | ユーザー: MEMO.USERORGRELATIONID → USERORGRELATION → USER、企業: MEMO.COMPANYID → BEEGLECOMPANY |

---

## 未解消質問一覧

| ID | 質問項目 | 状態 | メモ |
| --- | --- | --- | --- |

| Q13 | CSVダウンロードの対象企業は BEEGLECOMPANY で確定か？除外ルールは？ | 確定です |  |
| Q17 | KEYMAN テーブルの主要カラムとデータソースは？ | 確認して |  |
| Q18 | FIRSTPARTYVISITLOGS と FIRSTPARTYSCOREVISITOR の違いは？ | 一部解消 | 定義上の違いは整理済み、更新タイミングは不明 |
| Q19 | リスト追加とCSVダウンロードのユーザー重複率の意味は？ | 無視してOk |  |
| Q22 | MEMO.ANYFLOWJOBID の意味は？AnyFlowとの連携方法は？ | 使う予定なし |  |
| Q23 | FIRSTPARTYSCOREVISITOR.PAGEREFERRERTYPE の値の種類は？ | 自分で確認して |  |

---

## セマンティック定義リファレンス

### ID体系

| 概念 | カラム名 | 説明 |
| --- | --- | --- |
| ユーザー企業ID | `ORGID` | infoboxを使用しているユーザー企業を識別するID |
| BeegleCompany ID | `COMPANYID` | 企業マスタ（BEEGLECOMPANY）のユニークID |
| FirstPartyGroup ID | `FIRSTPARTYGROUPID` | 1stパーティスコアタグを埋め込んだWebサイトID |
| ログインユーザーID | `USERID` | infoboxログインユーザーのID |
| ユーザー組織関係ID | `USERORGRELATIONID` | USERORGRELATION.ID（UUID主キー）|

### 主要テーブル概要

| テーブル | 説明 |
| --- | --- |
| USERORGANIZATION | infoboxユーザー企業マスタ |
| USERORGRELATION | ログインユーザーと組織の組み合わせ管理 |
| USER | ログインユーザーマスタ |
| BEEGLECOMPANY | 企業マスタ |
| FIRSTPARTYGROUP | 1stパーティタグ埋込サイト |
| FIRSTPARTYVISITLOGS | サイト訪問ログ（VISITORCOMPANYID=訪問者BeegleCompanyID） |
| FIRSTPARTYSCOREVISITOR | 1stパーティスコア訪問者（COMPANYID=ユーザー企業ORGID） |
| FIRSTPARTYSCORECHANGE | 1stパーティースコア（複合UK: FIRSTPARTYGROUPID, COMPANYID） |
| FIRSTPARTYSCORECHANGEHISTORY | 1stパーティースコア履歴（+INTENTCHANGEDATE） |
| INTENTSCORECHANGE | 2ndパーティースコア（複合UK: ORIGINALPRODUCTCATEGORYID, COMPANYID） |
| INTENTSCORECHANGEHISTORY | 2ndパーティースコア履歴（+INTENTCHANGEDATE） |
| COMPANYLIST | 企業リスト（インポート履歴なし） |
| PEOPLELIST | 人物リスト（履歴はLEADIMPORTEVENT） |
| KEYMAN | キーマン（人物）マスタ |
| LEADIMPORTEVENT | 人物リストインポート履歴 |
| CSVDOWNLOADLOG | CSVダウンロード履歴 |
| MEMO | 活動履歴（PRIORITY: A/B/C/NULL, STATUSSHOID→MEMOSTATUSSHO） |
| MEMOSTATUSSHO | 活動状況（詳細）- 画面プルダウン細文字 |
| MEMOSTATUSDAI | 活動状況カテゴリ（大分類）- 画面プルダウン太文字 |

### 中間テーブル命名規則

**テーブル名左側のキーがA、右側がB**

| 中間テーブル | A | B |
| --- | --- | --- |
| _BEEGLECOMPANYTOCOMPANYLIST | company_id (BEEGLECOMPANY) | list_id (COMPANYLIST) |
| _KEYMANTOPEOPLELIST | keyman_id (KEYMAN) | list_id (PEOPLELIST) |
| _BEEGLECOMPANYTOCSVDOWNLOADLOG | log_id (CSVDOWNLOADLOG) | company_id (BEEGLECOMPANY) |

### MEMO関連の紐づけ

```
ユーザー判別:
  MEMO.USERORGRELATIONID → USERORGRELATION.ID → USER.ID

企業判別:
  MEMO.COMPANYID → BEEGLECOMPANY.ID

活動状況:
  MEMO.STATUSSHOID → MEMOSTATUSSHO.ID → MEMOSTATUSDAI.ID
```

### リストインポート

| リスト種別 | インポート履歴 |
| --- | --- |
| 企業リスト (COMPANYLIST) | 履歴を残さない |
| 人物リスト (PEOPLELIST) | LEADIMPORTEVENT に保管 |

---

## 出典

- InfoBox開発チーム（Yuta Sato, Fumiya Doi）との Slack やり取り（2026年1月〜2月）

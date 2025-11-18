"""AI Data Lab marimo portal (WIP)."""

import marimo

__generated_with__ = "0.0.0"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo

    return mo


@app.cell
def __():
    intro = """
    # AI Data Lab Portal (WIP)

    - BigQuery / S3 / Google Sheets / DuckDB への接続UIは順次実装予定です。
    - まずは `src/ai_data_lab` 以下でコネクタと設定ロジックを整備します。
    - EDA タブでは YData Profiling / AutoViz / Squarify / Colab Charts 風の可視化を選択的に利用予定です。
    - 目的・仮説・処理内容は Notebook 冒頭で明示してください（詳細は `.cursor.rules` を参照）。
    """

    return intro


@app.cell
def __(intro, mo):
    mo.md(intro)


if __name__ == "__main__":
    app.run()


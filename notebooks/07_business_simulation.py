import marimo

__generated_with = "0.17.8"
app = marimo.App(width="full")


@app.cell
def _(mo):
    mo.md("""
    # 事業計画シミュレーター (IRC Project)

    ## コンセプト
    - **TAM（市場）**: Xで呟いているアイドルファン全体（目標: 10,000 DAU）
    - **サービス獲得率**: 市場のうち、IRC Appを使うユーザーの割合（3% → 30%）
    - **施策**: Global/Vote/Organicは、その獲得を実現するための手段

    ## シミュレーション期間
    - **開始**: 2024/11/28 (Service Launch)
    - **終了**: 2025/03/15 (Idol Runway Collection)

    ## 目的
    経営層との対話において、市場獲得率と施策インパクトをリアルタイムに可視化する。
    """)
    return


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import seaborn as sns
    from datetime import datetime, timedelta

    # プロット設定
    plt.rcParams['figure.dpi'] = 300
    sns.set_style("whitegrid")
    # 日本語フォント設定 (Mac用)
    import matplotlib.font_manager as fm
    font_candidates = ['Hiragino Sans', 'Arial Unicode MS', 'AppleGothic']
    for font in font_candidates:
        try:
            if fm.findfont(font, fallback_to_default=False) != fm.findfont('DejaVu Sans'):
                plt.rcParams['font.family'] = font
                break
        except:
            continue
    else:
        plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
        plt.rcParams['font.family'] = 'sans-serif'
    return datetime, mo, np, pd, plt


@app.cell
def _(mo):
    mo.md("""
    ## パラメータ設定
    """)
    return


@app.cell
def _(datetime, mo):
    # 期間設定
    start_date = datetime(2024, 11, 28).date()
    end_date = datetime(2025, 3, 15).date()

    # イベント日程
    ticket_cp_start = datetime(2024, 11, 28).date()
    ticket_cp_end = datetime(2024, 12, 8).date()
    vote1_start = datetime(2024, 12, 5).date()
    vote1_end = datetime(2024, 12, 15).date()
    vote2_start = datetime(2025, 1, 15).date()
    vote2_end = datetime(2025, 1, 30).date()

    mo.md(f"""
    **定義済スケジュール**:
    - ローンチ: {start_date}
    - チケットCP: {ticket_cp_start} - {ticket_cp_end}
    - 投票第1弾: {vote1_start} - {vote1_end}
    - 投票第2弾: {vote2_start} - {vote2_end}
    - IRC開催: {end_date}
    """)
    return (
        end_date,
        start_date,
        ticket_cp_end,
        ticket_cp_start,
        vote1_end,
        vote1_start,
        vote2_end,
        vote2_start,
    )


@app.cell
def _(mo):
    # UI コンポーネント定義
    tam_dau_input = mo.ui.number(
        value=10000, step=100, label="TAM - 市場全体DAU（Xで呟いているファン数）", full_width=True
    )

    # 市場獲得率（これがサービスDAUを決める）
    initial_share_input = mo.ui.slider(
        start=0.0, stop=20.0, step=0.5, value=3.0, label="ローンチ時の市場獲得率 (%)", full_width=True
    )
    final_share_input = mo.ui.slider(
        start=0.0, stop=50.0, step=1.0, value=30.0, label="IRC開催時の市場獲得率 (%)", full_width=True
    )

    # IRCチャレンジ参加率（サービスDAUのうち、チャレンジに参加する割合）
    irc_challenge_ratio_input = mo.ui.slider(
        start=0.0, stop=1.0, step=0.05, value=0.5, label="IRCチャレンジ参加率（サービスDAUに対して）", full_width=True
    )

    mo.md(f"""
    ### パラメータ調整

    #### 市場規模（TAM）
    {tam_dau_input}

    #### 市場獲得率（サービスDAUを決定）
    {initial_share_input}
    {final_share_input}

    #### IRCチャレンジ参加率
    {irc_challenge_ratio_input}
    """)
    return (
        final_share_input,
        initial_share_input,
        irc_challenge_ratio_input,
        tam_dau_input,
    )


@app.cell
def _(
    end_date,
    final_share_input,
    initial_share_input,
    np,
    pd,
    start_date,
    tam_dau_input,
    ticket_cp_end,
    ticket_cp_start,
    vote1_end,
    vote1_start,
    vote2_end,
    vote2_start,
):
    # シミュレーション計算ロジック
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    df_sim = pd.DataFrame(index=dates)
    df_sim['date'] = df_sim.index.date

    # 市場獲得率の推移（線形成長）
    total_days = len(df_sim)
    market_share_progression = np.linspace(initial_share_input.value, final_share_input.value, total_days)

    # 曜日傾向を反映（週末は活動が活発）
    dow_factors = {
        0: 1.0,   # 月曜日
        1: 1.0,   # 火曜日
        2: 1.0,   # 水曜日
        3: 1.0,   # 木曜日
        4: 1.05,  # 金曜日
        5: 1.15,  # 土曜日
        6: 1.10   # 日曜日
    }

    df_sim['day_of_week'] = df_sim.index.dayofweek
    df_sim['dow_factor'] = df_sim['day_of_week'].map(dow_factors)
    df_sim['market_share'] = market_share_progression * df_sim['dow_factor']

    # サービスDAU = TAM × 市場獲得率
    df_sim['service_dau'] = (tam_dau_input.value * df_sim['market_share'] / 100).astype(int)

    # イベント期間フラグ
    df_sim['is_ticket_cp'] = (df_sim['date'] >= ticket_cp_start) & (df_sim['date'] <= ticket_cp_end)
    df_sim['is_vote1'] = (df_sim['date'] >= vote1_start) & (df_sim['date'] <= vote1_end)
    df_sim['is_vote2'] = (df_sim['date'] >= vote2_start) & (df_sim['date'] <= vote2_end)
    df_sim['is_irc_day'] = (df_sim['date'] == end_date)

    # IRC当日ブースト（配信視聴者による一時的なDAU増）
    df_sim['irc_day_boost'] = 0
    df_sim.loc[df_sim['is_irc_day'], 'irc_day_boost'] = 5000

    # 最終DAU = サービスDAU + IRC当日ブースト
    df_sim['total_dau'] = df_sim['service_dau'] + df_sim['irc_day_boost']

    return (df_sim,)


@app.cell
def _(df_sim, mo, tam_dau_input):
    # KPI 集計
    final_dau = int(df_sim['service_dau'].iloc[-1])
    avg_dau = int(df_sim['service_dau'].mean())
    max_dau = int(df_sim['total_dau'].max())
    final_share = df_sim['market_share'].iloc[-1]
    avg_share = df_sim['market_share'].mean()

    mo.md(f"""
    ## 予測サマリー (着地見込み)

    - **TAM（市場全体DAU）**: {tam_dau_input.value:,} 人
    - **3/15時点のサービスDAU**: {final_dau:,} 人
    - **期間平均サービスDAU**: {avg_dau:,} 人/日
    - **最大DAU（IRC当日）**: {max_dau:,} 人
    - **3/15時点の市場獲得率**: {final_share:.1f}%
    - **期間平均の市場獲得率**: {avg_share:.1f}%
    """)
    return


@app.cell
def _(df_sim, plt, tam_dau_input):
    # グラフ描画
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 12))

    # 1. サービスDAU推移
    ax1.plot(df_sim['date'], df_sim['service_dau'], color='#3498db', linewidth=3, marker='o', markersize=2, label='Service DAU')
    ax1.fill_between(df_sim['date'], df_sim['service_dau'], alpha=0.3, color='#3498db')

    # IRC当日ブーストを別で表示
    ax1.plot(df_sim['date'], df_sim['total_dau'], color='#e74c3c', linewidth=2, linestyle='--', alpha=0.7, label='Total DAU (with IRC boost)')

    # TAMライン
    ax1.axhline(y=tam_dau_input.value, color='black', linestyle='--', alpha=0.5, linewidth=2, label=f'TAM: {tam_dau_input.value:,}')

    ax1.set_title('Service DAU Growth vs TAM', fontsize=16, pad=15)
    ax1.set_ylabel('DAU', fontsize=12)
    ax1.legend(loc='upper left')
    ax1.grid(True, alpha=0.3)

    # イベント期間のハイライト
    ymin, ymax = ax1.get_ylim()
    ax1.axvspan(df_sim[df_sim['is_ticket_cp']]['date'].min(), df_sim[df_sim['is_ticket_cp']]['date'].max(), color='yellow', alpha=0.1)
    ax1.text(df_sim[df_sim['is_ticket_cp']]['date'].min(), ymax*0.9, 'Ticket CP', rotation=90, verticalalignment='top')
    ax1.axvspan(df_sim[df_sim['is_vote1']]['date'].min(), df_sim[df_sim['is_vote1']]['date'].max(), color='orange', alpha=0.1)
    ax1.text(df_sim[df_sim['is_vote1']]['date'].min(), ymax*0.9, 'Vote #1', rotation=90, verticalalignment='top')
    ax1.axvspan(df_sim[df_sim['is_vote2']]['date'].min(), df_sim[df_sim['is_vote2']]['date'].max(), color='red', alpha=0.1)
    ax1.text(df_sim[df_sim['is_vote2']]['date'].min(), ymax*0.9, 'Vote #2', rotation=90, verticalalignment='top')

    # 2. 市場獲得率の推移
    ax2.plot(df_sim['date'], df_sim['market_share'], color='#9b59b6', linewidth=3, marker='o', markersize=2)
    ax2.fill_between(df_sim['date'], df_sim['market_share'], alpha=0.3, color='#9b59b6')
    ax2.set_title('Market Share Growth', fontsize=16, pad=15)
    ax2.set_ylabel('Market Share (%)', fontsize=12)
    ax2.set_xlabel('Date', fontsize=12)
    ax2.grid(True, alpha=0.3)
    ax2.set_ylim(0, max(50, df_sim['market_share'].max() * 1.2))

    # 開始・終了値のアノテーション
    first_date = df_sim['date'].iloc[0]
    first_share = df_sim['market_share'].iloc[0]
    last_date = df_sim['date'].iloc[-1]
    last_share = df_sim['market_share'].iloc[-1]

    ax2.annotate(f'{first_share:.1f}%', xy=(first_date, first_share), 
                xytext=(first_date, first_share + 3),
                arrowprops=dict(facecolor='black', shrink=0.05),
                fontsize=11, fontweight='bold')
    ax2.annotate(f'{last_share:.1f}%', xy=(last_date, last_share), 
                xytext=(last_date, last_share + 3),
                arrowprops=dict(facecolor='black', shrink=0.05),
                fontsize=11, fontweight='bold')

    plt.tight_layout()
    fig
    return


@app.cell
def _(df_sim, mo):
    # データテーブル表示
    display_cols = {
        'date': '日付',
        'market_share': '市場獲得率(%)',
        'service_dau': 'サービスDAU',
        'irc_day_boost': 'IRC当日ブースト',
        'total_dau': '合計DAU'
    }

    df_display = df_sim[list(display_cols.keys())].rename(columns=display_cols)
    for col in df_display.columns:
        if col != '日付':
            if '率' in col or '%' in col:
                df_display[col] = df_display[col].apply(lambda x: f"{x:.2f}%")
            else:
                df_display[col] = df_display[col].apply(lambda x: f"{int(x):,}")

    mo.md("### 日次シミュレーション・データ")
    mo.ui.table(df_display)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## IRCチャレンジ参加会員の推移

    サービスDAUのうち、IRCチャレンジに参加登録している会員の割合と人数を表示します。
    """)
    return


@app.cell
def _(df_sim, irc_challenge_ratio_input, plt):
    # IRCチャレンジ参加会員の計算
    df_irc = df_sim.copy()

    # チャレンジ参加率を適用
    challenge_ratio = irc_challenge_ratio_input.value
    df_irc['irc_challenge_members'] = (df_irc['service_dau'] * challenge_ratio).astype(int)

    # グラフ描画（2段構成）
    fig_irc, (ax1_irc, ax2_irc) = plt.subplots(2, 1, figsize=(12, 10))

    # 1. サービスDAU vs IRCチャレンジ会員数
    ax1_irc.plot(df_irc['date'], df_irc['service_dau'], color='#3498db', linewidth=3, label='Service DAU')
    ax1_irc.fill_between(df_irc['date'], df_irc['service_dau'], alpha=0.2, color='#3498db')
    ax1_irc.plot(df_irc['date'], df_irc['irc_challenge_members'], color='#e67e22', linewidth=3, label=f'IRC Challenge Members ({challenge_ratio:.0%})')
    ax1_irc.fill_between(df_irc['date'], df_irc['irc_challenge_members'], alpha=0.2, color='#e67e22')

    ax1_irc.set_title('Service DAU vs IRC Challenge Members', fontsize=16, pad=15)
    ax1_irc.set_ylabel('Users', fontsize=12)
    ax1_irc.legend(loc='upper left')
    ax1_irc.grid(True, alpha=0.3)

    # 2. チャレンジ参加率の視覚化
    df_irc['challenge_ratio_display'] = challenge_ratio * 100
    ax2_irc.fill_between(df_irc['date'], df_irc['challenge_ratio_display'], alpha=0.4, color='#e67e22')
    ax2_irc.axhline(y=challenge_ratio * 100, color='#e67e22', linewidth=2, linestyle='--')
    ax2_irc.set_title(f'IRC Challenge Participation Rate: {challenge_ratio:.0%}', fontsize=16, pad=15)
    ax2_irc.set_ylabel('Participation Rate (%)', fontsize=12)
    ax2_irc.set_xlabel('Date', fontsize=12)
    ax2_irc.grid(True, alpha=0.3)
    ax2_irc.set_ylim(0, 100)

    plt.tight_layout()
    fig_irc
    return challenge_ratio, df_irc


@app.cell
def _(challenge_ratio, df_irc, df_sim, mo, tam_dau_input):
    # サマリー
    final_service_dau = int(df_sim['service_dau'].iloc[-1])
    final_market_share = df_sim['market_share'].iloc[-1]
    final_challenge_members = int(df_irc['irc_challenge_members'].iloc[-1])

    mo.md(f"""
    ### サマリー

    #### 市場獲得
    - **TAM（市場全体DAU）**: {tam_dau_input.value:,} 人
    - **3/15時点のサービスDAU**: {final_service_dau:,} 人
    - **3/15時点の市場獲得率**: {final_market_share:.1f}%

    #### IRCチャレンジ
    - **チャレンジ参加率**: {challenge_ratio:.0%}
    - **3/15時点のチャレンジ参加者数**: {final_challenge_members:,} 人

    **解釈**: 
    - Xで呟いている市場全体（TAM: {tam_dau_input.value:,}人）のうち、{final_market_share:.1f}%がIRC Appを使用
    - そのサービスユーザーのうち、{challenge_ratio:.0%}がIRCチャレンジに参加
    - 曜日傾向を反映（週末はやや高め）
    """)
    return


if __name__ == "__main__":
    app.run()

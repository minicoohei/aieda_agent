#!/bin/bash
# 包括的分析を開始するクイックスタートスクリプト

set -e

# カラー出力
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🎯 アイドル・グループ・ファンダム包括的比較分析${NC}"
echo -e "${BLUE}==========================================${NC}"
echo ""

# プロジェクトルートに移動
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

# 環境変数設定
export MARIMO_PORT_RANGE="${MARIMO_PORT_RANGE:-41000-41999}"
export REPORTS_DIR="${REPORTS_DIR:-reports/comprehensive_analysis}"

echo -e "${YELLOW}📋 設定${NC}"
echo "  - プロジェクトルート: $PROJECT_ROOT"
echo "  - ポート範囲: $MARIMO_PORT_RANGE"
echo "  - レポート出力先: $REPORTS_DIR"
echo ""

# レポートディレクトリ作成
mkdir -p "$REPORTS_DIR"

# 依存関係チェック
echo -e "${YELLOW}🔍 依存関係チェック...${NC}"
if ! command -v uv &> /dev/null; then
    echo -e "${RED}❌ uvがインストールされていません${NC}"
    echo "インストール方法: curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

# BigQuery認証チェック
echo -e "${YELLOW}🔐 BigQuery認証チェック...${NC}"
if [ ! -f "$HOME/.config/gcloud/application_default_credentials.json" ]; then
    echo -e "${YELLOW}⚠️  BigQueryの認証が必要です${NC}"
    echo "実行してください: gcloud auth application-default login"
    read -p "今すぐ実行しますか？ (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        gcloud auth application-default login
    else
        echo "認証をスキップします（Phase 1でエラーが発生する可能性があります）"
    fi
fi

# セッションクリーンアップ
echo -e "${YELLOW}🧹 既存セッションのクリーンアップ...${NC}"
if [ -f ".marimo/sessions.json" ]; then
    echo "既存のセッションファイルをバックアップします"
    cp .marimo/sessions.json .marimo/sessions.json.bak
fi

# マスターコーディネーター起動
echo ""
echo -e "${GREEN}🚀 マスターコーディネーターを起動します...${NC}"
echo ""
echo -e "${BLUE}ブラウザで以下のURLを開いてください：${NC}"
echo -e "  ${GREEN}http://localhost:40000${NC}"
echo ""
echo -e "${YELLOW}📌 実行手順：${NC}"
echo "  1. Phase 1を起動してデータを収集"
echo "  2. Phase 2 & 3を並列起動して統計/テキスト分析"
echo "  3. Phase 4で比較分析"
echo "  4. Phase 5で最終レポート統合"
echo ""
echo -e "${YELLOW}💡 各Phaseの詳細は以下で確認できます：${NC}"
echo "  - セッション一覧: http://localhost:40000 の「実行中セッション」テーブル"
echo "  - ログ: 各エージェントのターミナル出力を確認"
echo ""
echo -e "${GREEN}Ctrl+C で終了します${NC}"
echo ""

# Marimo起動
uv run marimo edit notebooks/05_comprehensive_analysis_coordinator.py --host 0.0.0.0 --port 40000


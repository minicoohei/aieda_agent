import requests
from bs4 import BeautifulSoup
import csv
import time
import os

# 設定
BASE_URL = "https://achikochi-data.com/twitter_follower_count_ranking_femaleidol/"
OUTPUT_FILE = "data/idol_followers_ranking.csv"
MAX_PAGES = 8

def get_page_url(page_num):
    if page_num == 1:
        return BASE_URL
    return f"{BASE_URL}{page_num}/"

def scrape_data():
    # 保存先ディレクトリの確認
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    all_data = []
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    for page in range(1, MAX_PAGES + 1):
        url = get_page_url(page)
        print(f"Processing page {page}: {url}")
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # テーブルを探す
            tables = soup.find_all('table')
            target_table = None
            for table in tables:
                if "順位" in table.get_text() and "フォロワー数" in table.get_text():
                    target_table = table
                    break
            
            if not target_table:
                print(f"Warning: Table not found on page {page}")
                continue

            # 行を取得 (ヘッダーを除く)
            rows = target_table.find_all('tr')
            
            for row in rows:
                cols = row.find_all(['td', 'th'])
                # ヘッダー行やデータが足りない行はスキップ
                if not cols or len(cols) < 3:
                    continue
                
                first_cell_text = cols[0].get_text(strip=True)
                # ヘッダー行スキップ
                if "順位" in first_cell_text:
                    continue

                # ランク
                rank = first_cell_text
                
                # 名前 (2列目)
                # 構造: <td>...<h4>名前</h4>...<strong>人物概要</strong>...</td> のような形が多い
                name_cell = cols[1]
                name_tag = name_cell.find('h4')
                
                if name_tag:
                    name = name_tag.get_text(strip=True)
                else:
                    # h4がない場合のフォールバック
                    # リンクがあれば取得、なければテキストから人物概要を除去
                    link_tag = name_cell.find('a')
                    if link_tag and link_tag.get_text(strip=True):
                        name = link_tag.get_text(strip=True)
                    else:
                        full_text = name_cell.get_text(strip=True)
                        if "人物概要" in full_text:
                            name = full_text.split("人物概要")[0].strip()
                        else:
                            name = full_text

                # フォロワー数 (3列目)
                follower_count = cols[2].get_text(strip=True)
                
                item = {
                    "rank": rank,
                    "name": name,
                    "follower_count": follower_count
                }
                all_data.append(item)
                # 進捗表示 (最初の数件のみ)
                if len(all_data) <= 3:
                    print(f"  Sample: {rank} - {name} - {follower_count}")

            time.sleep(1) # サーバー負荷軽減

        except Exception as e:
            print(f"Error on page {page}: {e}")
            # エラーが出ても取得できた分は保存するようにbreakで抜ける
            break

    # CSV保存
    if all_data:
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["rank", "name", "follower_count"])
            writer.writeheader()
            writer.writerows(all_data)
        print(f"\nSuccessfully saved {len(all_data)} records to {OUTPUT_FILE}")
    else:
        print("\nNo data collected.")

if __name__ == "__main__":
    scrape_data()


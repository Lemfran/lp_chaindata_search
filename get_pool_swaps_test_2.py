import requests, csv

CSV_FILE  = "factories_bundles.csv"
URL       = "https://gateway.thegraph.com/api/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"

QUERY = """
{
  factories(first: 5) {
    id
    poolCount
    txCount
    totalVolumeUSD
  }
  bundles(first: 5) {
    id
    ethPriceUSD
  }
}
"""

def fetch_data():
    headers = {
        "Authorization": "Bearer 5b537fdf4dd90fd409ca3da2c04b223f"
    }
    resp = requests.post(URL, json={'query': QUERY}, headers=headers)
    try:
        res = resp.json()
    except Exception:
        print("API返回内容不是JSON：", resp.text)
        return None
    if 'data' not in res:
        print("API返回异常：", res)
        return None
    return res['data']

def write_csv(data):
    with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        # 写 factories
        w.writerow(["factories_id", "poolCount", "txCount", "totalVolumeUSD"])
        for fac in data.get('factories', []):
            w.writerow([fac['id'], fac['poolCount'], fac['txCount'], fac['totalVolumeUSD']])
        # 写 bundles
        w.writerow([])
        w.writerow(["bundles_id", "ethPriceUSD"])
        for bun in data.get('bundles', []):
            w.writerow([bun['id'], bun['ethPriceUSD']])
    print(f"✅ 已导出 → {CSV_FILE}")

if __name__ == '__main__':
    data = fetch_data()
    if data:
        write_csv(data)
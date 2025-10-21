import requests

URL = "https://gateway.thegraph.com/api/subgraphs/id/HyW7A86UEdYVt5b9Lrw8W2F98yKecerHKutZTRbSCX27"
API_KEY = "5762403578020d8bca2128a9f926a746"  # 请替换为你的真实 API key

def build_query(pool_id, block_number):
    return f"""
{{
  pool(
    id: "{pool_id}"
    block: {{number: {block_number}}}
  ) {{
    feeGrowthGlobal1X128
    feeGrowthGlobal0X128
  }}
}}
"""

def fetch_pool_data(pool_id, block_number):
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }
    query = build_query(pool_id, block_number)
    resp = requests.post(URL, json={'query': query}, headers=headers)
    try:
        res = resp.json()
    except Exception:
        print("API返回内容不是JSON：", resp.text)
        return None
    if res is None or 'data' not in res or res['data'] is None or 'pool' not in res['data']:
        print("API返回异常：", res)
        return None
    return res['data']['pool']

#测试代码
if __name__ == '__main__':
    pool_id = input("请输入 pool id: ")
    block_number = input("请输入 block number: ")
    pool_data = fetch_pool_data(pool_id, block_number)
    if pool_data:
        print("pool数据：", pool_data)
    else:
        print("没有查询到 pool 数据")
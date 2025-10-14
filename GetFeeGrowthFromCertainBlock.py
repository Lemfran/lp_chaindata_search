import requests

URL = "https://gateway.thegraph.com/api/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"
API_KEY = "5b537fdf4dd90fd409ca3da2c04b223f"  # 请替换为你的真实 API key

def build_query(pool_id, block_number, tick):
    tick_id = f"{pool_id}#{tick}"
    return f"""
{{
  pool(
    id: "{pool_id}"
    block: {{number: {block_number}}}
  ) {{
    tick
    ticks(where: {{id: "{tick_id}"}}) {{
      feeGrowthOutside0X128
      feeGrowthOutside1X128
      pool {{
        feeGrowthGlobal0X128
        feeGrowthGlobal1X128
      }}
    }}
  }}
}}
"""

def fetch_pool_data(pool_id, block_number, tick):
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }
    query = build_query(pool_id, block_number, tick)
    resp = requests.post(URL, json={'query': query}, headers=headers)
    try:
        res = resp.json()
    except Exception:
        print("API返回内容不是JSON：", resp.text)
        return None
    if 'data' not in res or 'pool' not in res['data']:
        print("API返回异常：", res)
        return None
    return res['data']['pool']

#测试代码
if __name__ == '__main__':
    pool_id = input("请输入 pool id: ")
    block_number = input("请输入 block number: ")
    tick = input("请输入 tick: ")
    pool_data = fetch_pool_data(pool_id, block_number, tick)
    if pool_data and 'ticks' in pool_data and pool_data['ticks']:
        print("tick数据：", pool_data['ticks'][0])
    else:
        print("没有查询到 tick 数据")
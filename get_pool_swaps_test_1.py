import requests, csv

POOL      = "0xddaaed8b88ac0ccfdbfabdceba1c619391760f7f"
CSV_FILE  = "0xddaaed_swap_1year.csv"
URL       = "https://gateway.thegraph.com/api/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"
API_KEY   = "5b537fdf4dd90fd409ca3da2c04b223f"

QUERY = """
{
  swaps(first: 10, skip: 5) {
    sender
    sqrtPriceX96
    tick
    timestamp
    transaction {
      id
      timestamp
    }
    origin
    id
    amountUSD
    amount1
    amount0
    recipient
  }
}
"""

def sqrtx96_to_price(sqrtX96, dec0, dec1):
    """sqrtPriceX96 → token1/token0 price (人类可读)"""
    price = (int(sqrtX96) / (1 << 96)) ** 2
    return price * (10 ** (dec0 - dec1))

def fetch_all():
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }
    resp = requests.post(URL, json={'query': QUERY}, headers=headers)
    try:
        res = resp.json()
    except Exception:
        print("API返回内容不是JSON：", resp.text)
        return []
    if 'data' not in res or 'swaps' not in res['data']:
        print("API返回异常：", res)
        return []
    return res['data']['swaps']

def write_csv(swaps):
    with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(["sender","recipient","amount0","amount1",
                    "transaction.id","transaction.timestamp","origin",
                    "amountUSD","token0Price","token1Price"])
        # 获取池子 decimals（静态查一次）
        pool_q = """{ pool(id:"%s"){ token0{decimals} token1{decimals} } }""" % POOL
        headers = {
            "Authorization": f"Bearer {API_KEY}"
        }
        pool_resp = requests.post(URL, json={'query': pool_q}, headers=headers)
        try:
            pool_data = pool_resp.json()['data']['pool']
            dec0, dec1 = int(pool_data['token0']['decimals']), int(pool_data['token1']['decimals'])
        except Exception:
            print("池子信息获取失败：", pool_resp.text)
            dec0, dec1 = 18, 18  # 默认值

        for s in swaps:
            sqrt = s['sqrtPriceX96']
            p0 = sqrtx96_to_price(sqrt, dec0, dec1)   # token1/token0
            p1 = 1 / p0 if p0 != 0 else 0             # token0/token1
            w.writerow([
                s['id'],
                s['sender'],
                s['recipient'],
                s['amount0'],
                s['amount1'],
                s['transaction']['id'],
                s['transaction']['timestamp'],
                s.get('origin', ''),                  # origin 字段
                s['amountUSD'],
                p1,                                   # token0Price
                p0                                    # token1Price
            ])
    print(f"✅ 已导出 {len(swaps)} 条 → {CSV_FILE}")

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
      id
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

if __name__ == '__main__':
    swaps = fetch_all()
    if swaps:
        write_csv(swaps)
    pool_id = input("请输入 pool id: ")
    block_number = input("请输入 block number: ")
    tick = input("请输入 tick: ")
    pool_data = fetch_pool_data(pool_id, block_number, tick)
    print("API返回：", pool_data)
    if pool_data and 'ticks' in pool_data and pool_data['ticks']:
        print("tick数据：", pool_data['ticks'][0])
    else:
        print("没有查询到 tick 数据")
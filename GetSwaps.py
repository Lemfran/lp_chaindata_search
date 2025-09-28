import requests
import csv
import time
import requests

URL = "https://gateway.thegraph.com/api/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"
API_KEY = "5b537fdf4dd90fd409ca3da2c04b223f"  # 请替换为你的真实 API key

# 查询参数已写死
POOL_ID = "0x4e68ccd3e89f51c3074ca5072bbac773960dfa36"
TIMESTAMP_GTE = "1704067200"
FIRST = 1000  # 每次查询的记录数
INITIAL_SKIP = 1000  # 初始跳过的记录数

# CSV文件保存路径
CSV_FILE = "swaps_data.csv"


def build_query(skip):
    """构建GraphQL查询语句"""
    return """
    {{ 
      swaps(
        where: {{pool: "{POOL_ID}", timestamp_gte: "{TIMESTAMP_GTE}"}},
        orderBy: timestamp,
        orderDirection: asc,
        first: {FIRST},
        skip: {skip}
      ) {{ 
        transaction {{ 
          blockNumber
          timestamp
          id
          gasPrice
          gasUsed
        }}
        tick
        sqrtPriceX96
        amount0
        amount1
        origin
        amountUSD
        sender
        recipient
      }}
    }}
    """.format(
        POOL_ID=POOL_ID,
        TIMESTAMP_GTE=TIMESTAMP_GTE,
        FIRST=FIRST,
        skip=skip
    )


def fetch_swaps_data(skip):
    """获取swaps数据"""
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }
    query = build_query(skip)
    resp = requests.post(URL, json={'query': query}, headers=headers)
    try:
        res = resp.json()
    except Exception:
        print("API返回内容不是JSON：", resp.text)
        return None
    
    # 检查错误
    if 'errors' in res:
        print("API查询错误：", res['errors'])
        return None
    
    if 'data' not in res or 'swaps' not in res['data']:
        print("API返回异常：", res)
        return None
    
    return res['data']['swaps']


def save_to_csv(swaps_data):
    """将数据保存到CSV文件"""
    # 检查是否有数据
    if not swaps_data:
        print("没有数据可保存")
        return
    
    # 定义CSV文件的列标题，确保包含所有要写入的字段
    fieldnames = [
        'transaction_id',
        'transaction_blockNumber',
        'transaction_timestamp',
        'transaction_gasPrice',
        'transaction_gasUsed',
        'tick',
        'sqrtPriceX96',
        'amount0',
        'amount1',
        'origin',
        'amountUSD',
        'sender',
        'recipient'
    ]
    
    # 写入CSV文件
    with open(CSV_FILE, mode='w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        
        for swap in swaps_data:
            writer.writerow({
                'transaction_id': swap['transaction']['id'],
                'transaction_blockNumber': swap['transaction']['blockNumber'],
                'transaction_timestamp': swap['transaction']['timestamp'],
                'transaction_gasPrice': swap['transaction'].get('gasPrice', ''),
                'transaction_gasUsed': swap['transaction'].get('gasUsed', ''),
                'tick': swap['tick'],
                'sqrtPriceX96': swap['sqrtPriceX96'],
                'amount0': swap['amount0'],
                'amount1': swap['amount1'],
                'origin': swap.get('origin', ''),
                'amountUSD': swap.get('amountUSD', ''),
                'sender': swap.get('sender', ''),
                'recipient': swap.get('recipient', '')
            })
    
    print(f"数据已成功保存到 {CSV_FILE}，共 {len(swaps_data)} 条记录")


def main():
    """主函数，实现分页查询并保存数据"""
    all_swaps = []
    skip = INITIAL_SKIP
    total_records = 0
    
    print("开始获取数据...")
    print("注意：程序将一直查询到最新的数据，直到没有更多数据为止")

    
    try:
        while True:
            print(f"获取第 {skip+1} 到 {skip+FIRST} 条记录...")
            swaps = fetch_swaps_data(skip)
            
            # 检查是否获取到数据
            if not swaps:
                print("没有获取到更多数据或发生错误")
                break
            
            # 检查每条数据的时间戳是否超过限制
            has_exceeded_timestamp = False
            filtered_swaps = []
            for swap in swaps:
                # 将字符串类型的timestamp转换为整数
                current_timestamp = int(swap['transaction']['timestamp'])
                if current_timestamp > 1759024355:
                    has_exceeded_timestamp = True
                    # 不添加这条记录，因为它超过了时间戳限制
                else:
                    filtered_swaps.append(swap)

            # 将过滤后的数据添加到总列表中
            all_swaps.extend(filtered_swaps)
            total_records += len(filtered_swaps)

            # 如果有记录的时间戳超过限制，停止获取更多数据
            if has_exceeded_timestamp:
                print("检测到时间戳大于1759024355的记录，停止获取更多数据")
                break

            
            # 检查是否没有更多数据
            if len(swaps) < FIRST:
                break
            
            # 更新skip值，准备获取下一页数据
            skip += FIRST
            
            # 添加延迟，避免请求过于频繁
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n程序被用户中断")
        # 确保在中断时也能保存已获取的数据
        if all_swaps:
            save_to_csv(all_swaps)
            return
        else:
            print("没有获取到任何数据")
            return
    
    # 保存数据到CSV文件
    if all_swaps:
        save_to_csv(all_swaps)
        print(f"总记录数：{total_records}")
    else:
        print("没有获取到任何数据")


if __name__ == '__main__':
    main()
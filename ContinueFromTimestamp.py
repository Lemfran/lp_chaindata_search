import requests
import csv
import time

URL = "https://gateway.thegraph.com/api/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"
API_KEY = "5762403578020d8bca2128a9f926a746"  # 请替换为你的真实 API key

# 查询参数已写死
POOL_ID = "0x11950d141ecb863f01007add7d1a342041227b58"
CUSTOM_TIMESTAMP_GTE = "1704067200"  # 用户指定的起始时间戳
FIRST = 1000  # 每次查询的记录数
INITIAL_SKIP = 0  # 初始跳过的记录数，保持不变
FILE_BATCH_SIZE = 1000000  # 每100000条记录创建一个新的CSV文件

# CSV文件基础保存路径
CSV_BASE_PATH = "/Users/fanjinchen/python/learn/swaps_data_batch_"  # 从这里开始是新的文件存储路径
# 时间戳日志文件路径
TIMESTAMP_LOG_FILE = "last_successful_timestamp.csv"


def build_query(skip, timestamp_gte):
    """构建GraphQL查询语句"""
    return """
    {{ 
      swaps(
        where: {{pool: "{POOL_ID}", timestamp_gte: "{timestamp_gte}"}},
        orderBy: timestamp,
        orderDirection: asc,
        first: {FIRST},
        skip: {skip}
      ) {{ 
        transaction {{ 
          blockNumber
          id
          timestamp
        }}
        tick
        sqrtPriceX96
        amount0
        amount1
      }}
    }}
    """.format(
            POOL_ID=POOL_ID,
            timestamp_gte=timestamp_gte,
            FIRST=FIRST,
            skip=skip
        )


def fetch_swaps_data(skip, timestamp_gte):
    """获取swaps数据"""
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }
    query = build_query(skip, timestamp_gte)
    
    max_retries = 8
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            resp = requests.post(URL, json={'query': query}, headers=headers, timeout=30)
            
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
            
        except requests.exceptions.SSLError as e:
            retry_count += 1
            print(f"SSL连接错误 (第{retry_count}次尝试):", str(e))
            if retry_count >= max_retries:
                print("达到最大重试次数，放弃请求")
                return None
            print(f"等待15秒后重试...")
            time.sleep(15)
        except requests.exceptions.RequestException as e:
            retry_count += 1
            print(f"请求异常 (第{retry_count}次尝试):", str(e))
            if retry_count >= max_retries:
                print("达到最大重试次数，放弃请求")
                return None
            print(f"等待15秒后重试...")
            time.sleep(15)
    
    return None


def log_last_timestamp(last_timestamp, error_message=""):
    """记录最后成功处理的时间戳到日志文件"""
    # 检查文件是否存在，不存在则创建并写入表头
    file_exists = False
    try:
        with open(TIMESTAMP_LOG_FILE, 'r', encoding='utf-8') as f:
            file_exists = True
    except FileNotFoundError:
        pass
    
    # 写入CSV文件
    mode = 'a' if file_exists else 'w'
    with open(TIMESTAMP_LOG_FILE, mode=mode, newline='', encoding='utf-8') as csv_file:
        fieldnames = ['timestamp', 'error_message', 'record_time']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        
        writer.writerow({
            'timestamp': last_timestamp,
            'error_message': error_message,
            'record_time': str(int(time.time()))
        })
    
    print(f"已记录最后成功处理的时间戳 {last_timestamp} 到 {TIMESTAMP_LOG_FILE}")


def get_current_csv_file(total_records_processed):
    """根据已处理的总记录数获取当前应该使用的CSV文件名"""
    batch_number = total_records_processed // FILE_BATCH_SIZE + 1
    return f"{CSV_BASE_PATH}{batch_number}.csv"


def append_to_csv(swaps_data, total_records_processed):
    """将数据写入到当前批次的CSV文件"""
    # 检查是否有数据
    if not swaps_data:
        print("没有数据可保存")
        return
    
    # 获取当前应该使用的CSV文件名
    current_csv_file = get_current_csv_file(total_records_processed)
    
    # 定义CSV文件的列标题，只包含查询返回的字段
    fieldnames = [
        'transaction_id',
        'transaction_blockNumber',
        'transaction_timestamp',
        'tick',
        'sqrtPriceX96',
        'amount0',
        'amount1'
    ]
    
    # 检查文件是否存在，不存在则创建并写入表头
    file_exists = False
    try:
        with open(current_csv_file, 'r', encoding='utf-8') as f:
            file_exists = True
    except FileNotFoundError:
        pass
    
    # 写入CSV文件
    mode = 'a' if file_exists else 'w'
    with open(current_csv_file, mode=mode, newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        
        for swap in swaps_data:
            writer.writerow({
                'transaction_id': swap['transaction']['id'],
                'transaction_blockNumber': swap['transaction']['blockNumber'],
                'transaction_timestamp': swap['transaction']['timestamp'],
                'tick': swap['tick'],
                'sqrtPriceX96': swap['sqrtPriceX96'],
                'amount0': swap['amount0'],
                'amount1': swap['amount1']
            })
    
    # 统计CSV文件中的总记录数
    total_records_in_file = 0
    try:
        with open(current_csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            # 跳过表头
            next(reader)
            # 统计总行数
            total_records_in_file = sum(1 for row in reader)
    except Exception as e:
        print(f"统计CSV文件记录数时出错: {str(e)}")
    
    print(f"数据已成功写入到 {current_csv_file}，新增 {len(swaps_data)} 条记录，文件总记录数: {total_records_in_file}")


def main():
    """主函数，实现分页查询并按批次保存数据到不同的CSV文件"""
    all_swaps = []  # 用于跟踪所有数据以便记录最后时间戳
    skip = INITIAL_SKIP
    total_records = 0  # 已处理的总记录数
    timestamp_gte = CUSTOM_TIMESTAMP_GTE
    
    print(f"开始从时间戳 {timestamp_gte} 获取数据...")
    print("注意：程序将一直查询到最新的数据，直到没有更多数据为止")
    print(f"每 {FILE_BATCH_SIZE} 条记录将创建一个新的CSV文件")
    print(f"数据将从新的文件开始存储: {get_current_csv_file(0)}")

    try:
        while True:
            print(f"获取第 {skip+1} 到 {skip+FIRST} 条记录...")
            swaps = fetch_swaps_data(skip, timestamp_gte)
            
            # 检查是否获取到数据
            if not swaps:
                error_msg = "API返回空数据或请求失败"
                print(f"没有获取到更多数据或发生错误: {error_msg}")
                # 已经实时写入，无需额外处理
                # 如果已经获取了一些数据，记录最后一条的时间戳
                if all_swaps:
                    last_timestamp = all_swaps[-1]['transaction']['timestamp']
                    log_last_timestamp(last_timestamp, error_msg)
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

            # 将过滤后的数据添加到总列表
            all_swaps.extend(filtered_swaps)
            total_records += len(filtered_swaps)
            
            # 写入数据到当前批次的CSV文件
            if filtered_swaps:
                # 计算写入前的总记录数，用于确定应该写入哪个文件
                records_before_append = total_records - len(filtered_swaps)
                append_to_csv(filtered_swaps, records_before_append)

            # 如果有记录的时间戳超过限制，停止获取更多数据
            if has_exceeded_timestamp:
                print("检测到时间戳大于1759024355的记录，停止获取更多数据")
                # 已经实时写入，无需额外处理
                # 记录最后一条记录的时间戳
                if all_swaps:
                    last_timestamp = all_swaps[-1]['transaction']['timestamp']
                    log_last_timestamp(last_timestamp, "达到时间戳上限")
                break

            # 检查是否没有更多数据
            if len(filtered_swaps) < FIRST:
                # 已经实时写入，无需额外处理
                if all_swaps:
                    last_timestamp = all_swaps[-1]['transaction']['timestamp']
                    log_last_timestamp(last_timestamp, "已获取所有可用数据")
                break
            
            # 更新skip值，准备获取下一页数据
            # 使用原始的swaps长度来更新skip，因为API是按原始数量分页的
            skip += len(swaps)
            
            # 添加延迟，避免请求过于频繁
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n程序被用户中断")
        # 已经实时写入，无需额外处理
        if all_swaps:
            last_timestamp = all_swaps[-1]['transaction']['timestamp']
            log_last_timestamp(last_timestamp, "程序被用户中断")
            print(f"已保存 {len(all_swaps)} 条新记录")
            return
        else:
            print("没有获取到任何新数据")
            return
    except Exception as e:
        error_msg = str(e)
        print(f"发生未预期的错误: {error_msg}")
        # 已经实时写入，无需额外处理
        if all_swaps:
            last_timestamp = all_swaps[-1]['transaction']['timestamp']
            log_last_timestamp(last_timestamp, error_msg)
            print(f"已保存 {len(all_swaps)} 条新记录")
            return
        else:
            print("没有获取到任何新数据")
            return
    
    # 已经实时写入，无需额外处理
    
    # 在正常完成数据获取时记录最后一条记录的时间戳
    if all_swaps:
        last_timestamp = all_swaps[-1]['transaction']['timestamp']
        log_last_timestamp(last_timestamp, "正常完成数据获取")
        print(f"总新增记录数：{total_records}")
    else:
        print("没有获取到任何新数据")


if __name__ == '__main__':
    main()
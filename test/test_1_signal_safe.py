import requests
from datetime import datetime
import time
import os
import csv
import signal

# Infura API Key
INFURA_API_KEY = "fd24004ba99a4d04b506eea4ec406ba3"

# 从 CryptoCompare 获取的免费 API Key
CRYPTOCOMPARE_API_KEY = "e3afacafbf111a887fdc7ac301e9813eeb4fa9205995a2e8d80f09f47cab9d82"

# 输入参数配置 - 类似PEPE版本的参数化设置
TARGET_DATE = '2024-09-23'  # 测试的日期
INITIAL_TRANSACTION_INDEX = 0  # 初始交易索引
TRANSACTION_HASHES = []  # 空的交易哈希列表

# 输出文件名基础路径 - 采用PEPE版本的文件命名方式
CSV_BASE_PATH = "/Users/fanjinchen/python/learn/mint_enriched_PEPE_style_"  # 新的文件存储路径
# 进度日志文件路径
TRANSACTION_LOG_FILE = "last_successful_transaction_mint.csv"

# 文件批次大小 - 每达到这个数量就创建新文件
FILE_BATCH_SIZE = 1000000  # 每1000000条记录创建一个新的CSV文件

# Infura API 的地址
INFURA_URL = f"https://mainnet.infura.io/v3/{INFURA_API_KEY}"

# 用于缓存每日价格
price_cache = {}

# 全局变量用于存储最后的处理状态
global_last_transaction_info = None
global_total_records = 0

def signal_handler(sig, frame):
    """处理终端关闭信号，确保日志被记录"""
    print(f"\n接收到信号 {sig}，正在保存日志...")
    if global_last_transaction_info:
        log_last_transaction(global_last_transaction_info['transaction_hash'], global_total_records, f"接收到信号 {sig}，程序终止")
        print(f"已保存 {global_total_records} 条记录")
    else:
        print("没有获取到任何数据需要保存")
    # 强制退出程序
    os._exit(0)

# 注册信号处理器
signal.signal(signal.SIGTERM, signal_handler)  # 处理kill命令
signal.signal(signal.SIGINT, signal_handler)   # 处理Ctrl+C
signal.signal(signal.SIGHUP, signal_handler)   # 处理终端关闭

def log_last_transaction(transaction_hash, total_records, error_message=""):
    """记录最后成功处理的交易哈希到日志文件"""
    # 检查文件是否存在，不存在则创建并写入表头
    file_exists = False
    try:
        with open(TRANSACTION_LOG_FILE, 'r', encoding='utf-8') as f:
            file_exists = True
    except FileNotFoundError:
        pass
    
    # 写入CSV文件
    mode = 'a' if file_exists else 'w'
    with open(TRANSACTION_LOG_FILE, mode=mode, newline='', encoding='utf-8') as csv_file:
        fieldnames = ['transaction_hash', 'total_records', 'error_message', 'record_time']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        
        writer.writerow({
            'transaction_hash': transaction_hash,
            'total_records': total_records,
            'error_message': error_message,
            'record_time': str(int(time.time()))
        })
    
    print(f"已记录最后成功处理的交易哈希 {transaction_hash} 到 {TRANSACTION_LOG_FILE}")

def get_last_processed_transaction():
    """从日志文件中获取最后处理的交易哈希"""
    try:
        with open(TRANSACTION_LOG_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            # 获取最后一行数据
            last_row = None
            for row in reader:
                last_row = row
            
            if last_row:
                # 如果有最后一行数据，返回交易哈希
                transaction_hash = last_row['transaction_hash']
                # 找到这个交易哈希在列表中的索引
                try:
                    index = TRANSACTION_HASHES.index(transaction_hash)
                    return index + 1  # 返回下一个要处理的索引
                except ValueError:
                    print(f"最后处理的交易哈希 {transaction_hash} 不在列表中，从头开始")
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"读取日志文件时出错: {str(e)}")
    
    # 如果没有日志文件或读取失败，返回初始索引
    return INITIAL_TRANSACTION_INDEX

def get_current_csv_file(total_records_processed):
    """根据已处理的总记录数获取当前应该使用的CSV文件名"""
    batch_number = total_records_processed // FILE_BATCH_SIZE + 1
    return f"{CSV_BASE_PATH}{batch_number}.csv"

def get_gas_fee(tx_hash):
    """根据交易哈希获取Gas费(ETH)和详细信息，包含重试机制"""
    payload = { "id": 1, "jsonrpc": "2.0", "method": "eth_getTransactionReceipt", "params": [tx_hash] }
    
    max_retries = 10
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            response = requests.post(INFURA_URL, json=payload, timeout=30)
            response.raise_for_status()
            receipt = response.json().get('result')
            if receipt:
                gas_used = int(receipt['gasUsed'], 16)
                effective_gas_price = int(receipt['effectiveGasPrice'], 16)
                gas_fee_eth = (gas_used * effective_gas_price) / 1e18
                return gas_fee_eth, gas_used, effective_gas_price
        except requests.exceptions.RequestException as e:
            retry_count += 1
            print(f"获取Gas费时出错 (Tx: {tx_hash}, 第{retry_count}次尝试): {e}")
            if retry_count < max_retries:
                print(f"等待5秒后重试...")
                time.sleep(5)  # 等待5秒后重试
    return None, None, None

def get_eth_price_on_date(date_str):
    """根据日期获取当天的ETH美元价格，并使用缓存"""
    # 如果已经在缓存中，直接返回
    if date_str in price_cache:
        return price_cache[date_str]

    print(f"  -> 正在为日期 {date_str} 查询新价格 (from CryptoCompare)...")
    
    try:
        # 将日期字符串转换为时间戳
        dt_object = datetime.strptime(date_str, '%Y-%m-%d')
        unix_timestamp = int(dt_object.timestamp())
    except ValueError:
        print(f"无效的日期格式: {date_str}")
        return None
    
    cc_url = f"https://min-api.cryptocompare.com/data/pricehistorical?fsym=ETH&tsyms=USD&ts={unix_timestamp}&api_key={CRYPTOCOMPARE_API_KEY}"
    
    max_retries = 10
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            response = requests.get(cc_url, timeout=30)
            response.raise_for_status()
            data = response.json()
            if 'ETH' in data and 'USD' in data['ETH']:
                price = data['ETH']['USD']
                price_cache[date_str] = price
                time.sleep(0.5) 
                return price
            else:
                print(f"  -> CryptoCompare 返回了数据，但格式不正确: {data}")
        except requests.exceptions.RequestException as e:
            retry_count += 1
            print(f"获取价格时出错 (Date: {date_str}, 第{retry_count}次尝试): {e}")
            if retry_count < max_retries:
                print(f"等待5秒后重试...")
                time.sleep(5)  # 等待5秒后重试
    
    price_cache[date_str] = None
    return None

def append_to_csv(transaction_data, total_records_processed):
    """将交易数据写入到当前批次的CSV文件"""
    # 检查是否有数据
    if not transaction_data:
        print("没有数据可保存")
        return 0
    
    # 获取当前应该使用的CSV文件名
    current_csv_file = get_current_csv_file(total_records_processed)
    
    # 确保目录存在
    directory = os.path.dirname(current_csv_file)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        print(f"已创建目录: {directory}")
    
    # 定义CSV文件的列标题
    fieldnames = [
        'transaction_hash',
        'block_timestamp',
        'gas_used',
        'effective_gas_price_wei',
        'gas_fee_eth',
        'weth_price_in_usd',
        'gas_fee_usd',
        'effective_gas_price_gwei'
    ]
    
    # 检查文件是否存在，不存在则创建并写入表头
    file_exists = os.path.exists(current_csv_file)
    
    # 写入CSV文件
    mode = 'a' if file_exists else 'w'
    with open(current_csv_file, mode=mode, newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        
        # 写入数据
        writer.writerow(transaction_data)
    
    # 返回新增记录数
    print(f"数据已成功写入到 {current_csv_file}")
    return 1

def main():
    """主函数，实现信号安全的数据处理，支持断点续传 - PEPE版本风格"""
    global global_last_transaction_info, global_total_records
    
    global_total_records = 0  # 已处理的总记录数
    global_last_transaction_info = None  # 保存最后处理的交易信息
    
    # 从日志文件中获取最后一次处理的交易索引，如果没有则使用初始值
    start_index = get_last_processed_transaction()
    
    total_transactions = len(TRANSACTION_HASHES)
    
    print(f"开始从索引 {start_index} 处理交易数据...")
    print(f"总共有 {total_transactions} 条交易需要处理")
    print(f"每 {FILE_BATCH_SIZE} 条记录将创建一个新的CSV文件")
    print(f"数据将从新的文件开始存储: {get_current_csv_file(0)}")
    print("注意：程序已启用信号处理，即使直接关闭终端也能保存日志")

    try:
        # 从最后处理的索引开始处理
        for index in range(start_index, total_transactions):
            transaction_hash = TRANSACTION_HASHES[index]
            print(f"正在处理第 {index + 1} / {total_transactions} 条交易: {transaction_hash}")
            
            # 获取Gas费用信息
            gas_fee_eth, gas_used, effective_gas_price = get_gas_fee(transaction_hash)
            
            # 获取ETH价格 - 使用目标日期
            eth_price = get_eth_price_on_date(TARGET_DATE)
            
            # 计算额外的字段
            gas_fee_usd = gas_fee_eth * eth_price if gas_fee_eth and eth_price else None
            effective_gas_price_gwei = effective_gas_price / 1e9 if effective_gas_price else None
            
            # 构建交易数据字典
            transaction_data = {
                'transaction_hash': transaction_hash,
                'block_timestamp': f"{TARGET_DATE} 12:00:00 UTC",  # 使用目标日期作为时间戳
                'gas_used': gas_used,
                'effective_gas_price_wei': effective_gas_price,
                'gas_fee_eth': gas_fee_eth,
                'weth_price_in_usd': eth_price,
                'gas_fee_usd': gas_fee_usd,
                'effective_gas_price_gwei': effective_gas_price_gwei
            }
            
            # 更新全局变量
            global_last_transaction_info = transaction_data
            
            # 写入数据到当前批次的CSV文件
            added_records = append_to_csv(transaction_data, global_total_records)
            global_total_records += added_records
            
            # 记录进度
            log_last_transaction(transaction_hash, global_total_records)
            
            # 添加延迟，避免请求过于频繁
            time.sleep(1)
        
        # 在正常完成数据获取时记录最后一条记录
        if global_last_transaction_info:
            log_last_transaction(global_last_transaction_info['transaction_hash'], global_total_records, "正常完成数据获取")
            print(f"总新增记录数：{global_total_records}")
        else:
            print("没有处理任何新数据")

    except KeyboardInterrupt:
        print("\n程序被用户中断")
        # 如果已经处理了一些数据，记录最后一条
        if global_last_transaction_info:
            log_last_transaction(global_last_transaction_info['transaction_hash'], global_total_records, "程序被用户中断")
            print(f"已保存 {global_total_records} 条新记录")
        else:
            print("没有处理任何新数据")
        return
    except Exception as e:
        error_msg = str(e)
        print(f"发生未预期的错误: {error_msg}")
        # 如果已经处理了一些数据，记录最后一条
        if global_last_transaction_info:
            log_last_transaction(global_last_transaction_info['transaction_hash'], global_total_records, error_msg)
            print(f"已保存 {global_total_records} 条新记录")
        else:
            print("没有处理任何新数据")
        return

if __name__ == '__main__':
    main()
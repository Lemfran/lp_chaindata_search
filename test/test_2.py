import pandas as pd
import requests
from datetime import datetime
import time


# Infura API Key
INFURA_API_KEY = "fd24004ba99a4d04b506eea4ec406ba3"#f2aec2734b8047d2a7497aaf804b4f72

# 从 CryptoCompare 获取的免费 API Key
CRYPTOCOMPARE_API_KEY = "e3afacafbf111a887fdc7ac301e9813eeb4fa9205995a2e8d80f09f47cab9d82"

INPUT_CSV_PATH = 'MINT.csv' 
TARGET_DATE = '2024-09-23' # 您想测试的日期

# 4. 输出文件名
OUTPUT_CSV_PATH = f'mint_enriched_ONEDAY_TEST_{TARGET_DATE}.csv'
# ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲

# Infura API 的地址
INFURA_URL = f"https://mainnet.infura.io/v3/{INFURA_API_KEY}"

# 用于缓存每日价格
price_cache = {}

# --- 2. 定义函数来获取数据 ---

def get_gas_fee(tx_hash):
    """根据交易哈希获取Gas费(ETH)和详细信息"""
    payload = { "id": 1, "jsonrpc": "2.0", "method": "eth_getTransactionReceipt", "params": [tx_hash] }
    try:
        response = requests.post(INFURA_URL, json=payload)
        response.raise_for_status()
        receipt = response.json().get('result')
        if receipt:
            gas_used = int(receipt['gasUsed'], 16)
            effective_gas_price = int(receipt['effectiveGasPrice'], 16)
            gas_fee_eth = (gas_used * effective_gas_price) / 1e18
            return gas_fee_eth, gas_used, effective_gas_price
    except requests.exceptions.RequestException as e:
        print(f"获取Gas费时出错 (Tx: {tx_hash}): {e}")
    return None, None, None

def get_eth_price_on_date(timestamp_str):
    """根据时间戳获取当天的ETH美元价格，并使用缓存"""
    try:
        dt_object = datetime.fromisoformat(str(timestamp_str).replace(' UTC', ''))
        unix_timestamp = int(dt_object.timestamp())
        date_key = dt_object.strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        print(f"无效的时间戳格式: {timestamp_str}")
        return None

    if date_key in price_cache:
        return price_cache[date_key]

    print(f"  -> 正在为日期 {date_key} 查询新价格 (from CryptoCompare)...")
    cc_url = f"https://min-api.cryptocompare.com/data/pricehistorical?fsym=ETH&tsyms=USD&ts={unix_timestamp}&api_key={CRYPTOCOMPARE_API_KEY}"
    
    try:
        response = requests.get(cc_url)
        response.raise_for_status()
        data = response.json()
        if 'ETH' in data and 'USD' in data['ETH']:
            price = data['ETH']['USD']
            price_cache[date_key] = price
            time.sleep(0.5) 
            return price
        else:
            print(f"  -> CryptoCompare 返回了数据，但格式不正确: {data}")
    except requests.exceptions.RequestException as e:
        print(f"获取价格时出错 (Date: {date_key}): {e}")
    
    price_cache[date_key] = None
    return None

# --- 3. 主逻辑：读取、筛选、处理、保存 ---

try:
    df_full = pd.read_csv(INPUT_CSV_PATH)
    print(f"成功读取文件 '{INPUT_CSV_PATH}'，包含 {len(df_full)} 条总记录。")
except FileNotFoundError:
    print(f"错误：找不到输入文件 '{INPUT_CSV_PATH}'。请确保文件名正确且文件与脚本在同一目录下。")
    exit()

print(f"正在筛选日期为 {TARGET_DATE} 的数据...")
df_full['block_timestamp'] = pd.to_datetime(df_full['block_timestamp'])
df = df_full[df_full['block_timestamp'].dt.date == pd.to_datetime(TARGET_DATE).date()].copy()
print(f"筛选完成，找到 {len(df)} 条记录进行处理。")

if df.empty:
    print("指定日期内没有数据，程序退出。")
    exit()

# 【修改一】: 为 gas_used 和 effective_gas_price 创建新列
df['gas_used'] = 0
df['effective_gas_price_wei'] = 0 # 原始单位是 wei
df['gas_fee_eth'] = 0.0
df['weth_price_in_usd'] = 0.0

# 遍历筛选后的DataFrame的每一行
for index, row in df.iterrows():
    print(f"正在处理第 {index + 1} / {len(df)} 条记录: {row['transaction_hash']}")
    
    # 【修改二】: 接收所有三个返回值
    gas_fee_eth, gas_used, effective_gas_price = get_gas_fee(row['transaction_hash'])
    
    # 将所有获取到的值填入DataFrame
    df.at[index, 'gas_fee_eth'] = gas_fee_eth
    df.at[index, 'gas_used'] = gas_used
    df.at[index, 'effective_gas_price_wei'] = effective_gas_price
    
    eth_price = get_eth_price_on_date(row['block_timestamp'])
    df.at[index, 'weth_price_in_usd'] = eth_price

# 向量化计算美元Gas费
df['gas_fee_usd'] = df['gas_fee_eth'] * df['weth_price_in_usd']

# 【修改三】: (可选但建议) 添加一列更易读的 Gwei 单位的 Gas 价格
# 1 Gwei = 1,000,000,000 wei
df['effective_gas_price_gwei'] = df['effective_gas_price_wei'] / 1e9


# 将丰富后的数据保存到新的CSV文件
df.to_csv(OUTPUT_CSV_PATH, index=False, encoding='utf-8-sig')

print(f"\n处理完成！测试数据已保存到 {OUTPUT_CSV_PATH}")
import time
import requests  # 确保导入requests库
import os  # 添加os模块用于创建目录
import csv  # 确保导入csv库
import signal  # 添加signal模块用于处理终端关闭信号

URL = "https://gateway.thegraph.com/api/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"
API_KEY = "34165bc4253b84ee026aaae314fd9022"  # 请替换为你的真实 API key

# 查询参数已写死
POOL_ID = "0x11950d141ecb863f01007add7d1a342041227b58"
INITIAL_BLOCK_NUMBER = 20855607  # 初始区块号
MAX_BLOCK_NUMBER = 23483607  # 最大区块号限制
BLOCK_STEP = 60  # 每次增加的区块号
INITIAL_TICK_IDX_GT = -220001  # 初始的tickIdx_gt值
TICK_IDX_LTE = -180000  # tickIdx_lte值
FIRST = 2000  # 每次查询的记录数
FILE_BATCH_SIZE = 1000000  # 每1000000条记录创建一个新的CSV文件

# CSV文件基础保存路径
CSV_BASE_PATH = "/Users/fanjinchen/python/learn/ticks_data_PEPE_"  # 从这里开始是新的文件存储路径
# 区块号和tickIdx日志文件路径
BLOCK_LOG_FILE = "last_successful_block_tick_PEPE.csv"

# 全局变量用于存储最后的处理状态
global_last_tick_info = None
global_last_block = None
global_total_records = 0

def signal_handler(sig, frame):
    """处理终端关闭信号，确保日志被记录"""
    print(f"\n接收到信号 {sig}，正在保存日志...")
    if global_last_tick_info and global_last_block:
        log_last_block_and_tick(global_last_block, global_last_tick_info['tickIdx'], f"接收到信号 {sig}，程序终止")
        print(f"已保存 {global_total_records} 条记录")
    else:
        print("没有获取到任何数据需要保存")
    # 强制退出程序
    os._exit(0)

# 注册信号处理器
signal.signal(signal.SIGTERM, signal_handler)  # 处理kill命令
signal.signal(signal.SIGINT, signal_handler)   # 处理Ctrl+C
signal.signal(signal.SIGHUP, signal_handler)   # 处理终端关闭
def build_query(block_number, tick_idx_gt, tick_idx_lte):
    """构建GraphQL查询语句"""
    return """
    {{ 
      ticks(
        where: {{pool: "{POOL_ID}", tickIdx_gt: "{tick_idx_gt}", tickIdx_lte: "{tick_idx_lte}"}} 
        first: {FIRST} 
        block: {{number: {block_number}}} 
        orderBy: tickIdx 
        orderDirection: asc 
      ) {{ 
        tickIdx 
        feeGrowthOutside1X128 
        feeGrowthOutside0X128 
        liquidityNet 
      }} 
    }}""".format(
                POOL_ID=POOL_ID,
                tick_idx_gt=tick_idx_gt,
                tick_idx_lte=tick_idx_lte,
                FIRST=FIRST,
                block_number=block_number
            )

def fetch_ticks_data(block_number, tick_idx_gt, tick_idx_lte):
    """获取ticks数据"""
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }
    query = build_query(block_number, tick_idx_gt, tick_idx_lte)
    
    max_retries = 10
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
            
            if 'data' not in res or 'ticks' not in res['data']:
                print("API返回异常：", res)
                return None
            
            return res['data']['ticks'], block_number
            
        except requests.exceptions.SSLError as e:
            retry_count += 1
            print(f"SSL连接错误 (第{retry_count}次尝试):", str(e))
            if retry_count >= max_retries:
                print("达到最大重试次数，放弃请求")
                return None, block_number
            print(f"等待20秒后重试...")
            time.sleep(20)
        except requests.exceptions.RequestException as e:
            retry_count += 1
            print(f"请求异常 (第{retry_count}次尝试):", str(e))
            if retry_count >= max_retries:
                print("达到最大重试次数，放弃请求")
                return None, block_number
            print(f"等待20秒后重试...")
            time.sleep(20)
    
    return None, block_number

def log_last_block_and_tick(block_number, tick_idx, error_message=""):
    """记录最后成功处理的区块号和tickIdx到日志文件"""
    # 检查文件是否存在，不存在则创建并写入表头
    file_exists = False
    try:
        with open(BLOCK_LOG_FILE, 'r', encoding='utf-8') as f:
            file_exists = True
    except FileNotFoundError:
        pass
    
    # 写入CSV文件
    mode = 'a' if file_exists else 'w'
    with open(BLOCK_LOG_FILE, mode=mode, newline='', encoding='utf-8') as csv_file:
        fieldnames = ['block_number', 'tick_idx', 'error_message', 'record_time']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        
        writer.writerow({
            'block_number': block_number,
            'tick_idx': tick_idx,
            'error_message': error_message,
            'record_time': str(int(time.time()))
        })
    
    print(f"已记录最后成功处理的区块号 {block_number} 和tickIdx {tick_idx} 到 {BLOCK_LOG_FILE}")

def get_last_block_and_tick():
    """从日志文件中获取最后一次的区块号和tickIdx"""
    try:
        with open(BLOCK_LOG_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            # 获取最后一行数据
            last_row = None
            for row in reader:
                last_row = row
            
            if last_row:
                # 如果有最后一行数据，返回区块号和tickIdx
                return int(last_row['block_number']), int(last_row['tick_idx'])
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"读取日志文件时出错: {str(e)}")
    
    # 如果没有日志文件或读取失败，返回初始值
    return INITIAL_BLOCK_NUMBER, INITIAL_TICK_IDX_GT

def get_current_csv_file(total_records_processed):
    """根据已处理的总记录数获取当前应该使用的CSV文件名"""
    batch_number = total_records_processed // FILE_BATCH_SIZE + 1
    return f"{CSV_BASE_PATH}{batch_number}.csv"

def append_to_csv(ticks_data, block_number, total_records_processed):
    """将数据写入到当前批次的CSV文件"""
    # 检查是否有数据
    if not ticks_data:
        print("没有数据可保存")
        return 0
    
    # 获取当前应该使用的CSV文件名
    current_csv_file = get_current_csv_file(total_records_processed)
    
    # 确保目录存在
    directory = os.path.dirname(current_csv_file)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
    
    # 定义CSV文件的列标题，只包含查询返回的字段
    fieldnames = [
        'block_number',
        'tickIdx',
        'feeGrowthOutside1X128',
        'feeGrowthOutside0X128',
        'liquidityNet'
    ]
    
    # 检查文件是否存在，不存在则创建并写入表头
    file_exists = os.path.exists(current_csv_file)
    
    # 写入CSV文件
    mode = 'a' if file_exists else 'w'
    with open(current_csv_file, mode=mode, newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        
        # 直接写入数据，不保存到额外的列表
        for tick in ticks_data:
            writer.writerow({
                'block_number': block_number,
                'tickIdx': tick['tickIdx'],
                'feeGrowthOutside1X128': tick['feeGrowthOutside1X128'],
                'feeGrowthOutside0X128': tick['feeGrowthOutside0X128'],
                'liquidityNet': tick['liquidityNet']
            })
    
    # 返回新增记录数，不需要统计文件中的总记录数来减少I/O操作
    print(f"数据已成功写入到 {current_csv_file}，新增 {len(ticks_data)} 条记录")
    return len(ticks_data)

def main():
    """主函数，实现按区块号查询并按批次保存数据到不同的CSV文件 - 信号安全版本"""
    global global_last_tick_info, global_last_block, global_total_records
    
    global_total_records = 0  # 已处理的总记录数
    global_last_tick_info = None  # 只保存最后一条tick信息，而不是全部
    global_last_block = None  # 保存最后处理的区块号
    
    # 从日志文件中获取最后一次的区块号和tickIdx，如果没有则使用初始值
    block_number, tick_idx_gt = get_last_block_and_tick()

    if tick_idx_gt >= TICK_IDX_LTE:
        tick_idx_gt = INITIAL_TICK_IDX_GT
        block_number = block_number + BLOCK_STEP
    
    print(f"开始从区块号 {block_number} 和tickIdx_gt {tick_idx_gt} 获取数据...")
    print(f"每次查询区块号增加 {BLOCK_STEP}")
    print(f"每 {FILE_BATCH_SIZE} 条记录将创建一个新的CSV文件")
    print(f"数据将从新的文件开始存储: {get_current_csv_file(0)}")
    print("注意：程序已启用信号处理，即使直接关闭终端也能保存日志")

    try:
        while block_number <= MAX_BLOCK_NUMBER:
                
            print(f"获取区块号 {block_number} 的数据...")
            ticks, current_block = fetch_ticks_data(block_number, tick_idx_gt, TICK_IDX_LTE)
            
            # 检查是否获取到数据
            if not ticks:
                error_msg = "API返回空数据或请求失败"
                print(f"没有获取到更多数据或发生错误: {error_msg}")
                # 如果已经获取了一些数据，记录最后一条的区块号和tickIdx
                if global_last_tick_info and global_last_block:
                    log_last_block_and_tick(global_last_block, global_last_tick_info['tickIdx'], error_msg)
                break
            
            # 只保存最后一条tick信息，而不是全部数据
            if ticks:
                global_last_tick_info = ticks[-1]  # 只保存最后一条记录
                global_last_block = current_block
            
            # 写入数据到当前批次的CSV文件
            if ticks:
                # 计算写入前的总记录数，用于确定应该写入哪个文件
                records_before_append = global_total_records
                # 直接写入数据，并更新总记录数
                added_records = append_to_csv(ticks, current_block, records_before_append)
                global_total_records += added_records
                
                # 及时清理不再需要的数据，帮助垃圾回收
                del ticks  # 删除当前批次的数据，释放内存

            # 更新区块号，准备获取下一个区块的数据
            tick_idx_gt = INITIAL_TICK_IDX_GT
            block_number += BLOCK_STEP

            
            # 检查是否超过最大区块号限制
            if block_number > MAX_BLOCK_NUMBER:
                print(f"即将超过最大区块号限制 {MAX_BLOCK_NUMBER}，停止数据获取")
                # 记录最后成功处理的区块号和tickIdx
                if global_last_tick_info:
                    log_last_block_and_tick(global_last_block, global_last_tick_info['tickIdx'], "达到最大区块号限制")
                break
            
            # 添加延迟，避免请求过于频繁
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n程序被用户中断")
        # 如果已经获取了一些数据，记录最后一条的区块号和tickIdx
        if global_last_tick_info:
            log_last_block_and_tick(global_last_block, global_last_tick_info['tickIdx'], "程序被用户中断")
            print(f"已保存 {global_total_records} 条新记录")
        else:
            print("没有获取到任何新数据")
        return
    except Exception as e:
        error_msg = str(e)
        print(f"发生未预期的错误: {error_msg}")
        # 如果已经获取了一些数据，记录最后一条的区块号和tickIdx
        if global_last_tick_info:
            log_last_block_and_tick(global_last_block, global_last_tick_info['tickIdx'], error_msg)
            print(f"已保存 {global_total_records} 条新记录")
        else:
            print("没有获取到任何新数据")
        return
    
    # 在正常完成数据获取时记录最后一条记录的区块号和tickIdx
    if global_last_tick_info:
        log_last_block_and_tick(global_last_block, global_last_tick_info['tickIdx'], "正常完成数据获取")
        print(f"总新增记录数：{global_total_records}")
    else:
        print("没有获取到任何新数据")


if __name__ == '__main__':
    main()
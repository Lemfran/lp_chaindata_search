#!/usr/bin/env python3
"""为ETH transactions数据实时添加价格信息的工具。"""

import os
import csv
import time
import sys
import json
import ssl
import shutil
import gc
import urllib.request
import urllib.error
import urllib.parse
import datetime as dt
import http.client as http_client
from typing import Dict, List, Any, Optional, OrderedDict
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed

# 配置DEBUG模式
try:
    http_client.HTTPConnection.debuglevel = 0  # 设置为1开启调试模式
except Exception:
    pass

# 创建SSL上下文以解决连接问题
try:
    SSL_CONTEXT = ssl.create_default_context()
    SSL_CONTEXT.check_hostname = False
    SSL_CONTEXT.verify_mode = ssl.CERT_NONE
except Exception:
    SSL_CONTEXT = None

# 配置参数
BASE_DIR = "/Users/fanjinchen/python/learn" #/crypto_ETH/data
INPUT_FILE_PREFIX = "transactions_data_ETH_"  # 修改为ETH文件前缀
CHECK_INTERVAL = 5  # 监控文件变化的间隔时间（秒）
MAX_WORKERS = 10  # 并发请求的最大线程数
STATE_FILE = os.path.join(BASE_DIR, "enrichment_state_eth.json")  # ETH专用状态记录文件
MAX_CACHE_SIZE = 10000  # 价格缓存的最大条目数
STATE_CLEAN_INTERVAL = 60  # 状态清理间隔（秒）
COMPLETED_FILE_RETENTION = 3600  # 已完成文件状态的保留时间（秒）

# Binance API配置
BASE_URL = "https://api.binance.com"
AGG_TRADES_PATH = "/api/v3/aggTrades"
DEFAULT_WINDOW_SECONDS = 60

# 已处理文件和行的记录
processed_state = {
    "files": {}
}  # 文件 -> {last_processed_line: int, is_complete: bool}

class PriceFetcher:
    """价格获取器，使用Binance API获取指定时间戳的价格"""
    
    def __init__(self, window_seconds: int = DEFAULT_WINDOW_SECONDS, max_cache_size: int = MAX_CACHE_SIZE):
        self.window_seconds = window_seconds
        self.max_cache_size = max_cache_size
        # 使用OrderedDict以便于缓存管理
        self.price_cache: Dict[str, OrderedDict[int, float]] = {
            "ETHUSDC": OrderedDict()
        }
        self.last_cache_clean = time.time()
        self.cache_clean_interval = 60  # 缓存清理间隔（秒）  # 只缓存ETHUSDC价格
    
    def parse_timestamp(self, timestamp_str: str) -> int:
        """将时间戳字符串转换为毫秒级时间戳"""
        try:
            value = float(timestamp_str)
        except ValueError:
            raise ValueError(f"无效的时间戳: {timestamp_str}")
        
        # 处理秒级或毫秒级时间戳
        if value <= 0:
            raise ValueError("时间戳必须为正数")
        
        # 如果时间戳小于1e12，视为秒级时间戳
        if value < 1e12:
            millis = int(value * 1000)
        else:
            millis = int(value)
        
        return millis
    
    def binance_request(self, params: Dict[str, Any]) -> Any:
        """发送Binance API请求"""
        query = urllib.parse.urlencode(params)
        url = f"{BASE_URL}{AGG_TRADES_PATH}?{query}"
        request = urllib.request.Request(url=url, method="GET")

        # 添加User-Agent头来模拟浏览器请求
        request.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')

        try:
            # 使用SSL上下文（如果可用）
            if SSL_CONTEXT:
                with urllib.request.urlopen(request, timeout=15, context=SSL_CONTEXT) as response:
                    data = response.read()
            else:
                with urllib.request.urlopen(request, timeout=15) as response:
                    data = response.read()
        except urllib.error.HTTPError as exc:
            try:
                error_msg = exc.read().decode()
            except:
                error_msg = "无法读取错误信息"
            raise RuntimeError(
                f"Binance API错误 {exc.code}: {error_msg}"
            ) from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"无法连接到Binance API: {exc.reason}") from exc
        except Exception as e:
            raise RuntimeError(f"请求Binance API时发生未知错误: {str(e)}") from e

        try:
            return json.loads(data)
        except json.JSONDecodeError:
            raise RuntimeError("从Binance API接收了无效的JSON")
    
    def find_trade_at_timestamp(self, trades: List[Dict[str, Any]], target_ms: int) -> Optional[Dict[str, Any]]:
        """找到最接近目标时间戳的交易"""
        if not isinstance(trades, list):
            raise RuntimeError("Binance响应格式异常: 预期为交易列表")

        # 优先选择目标时间戳或之前的交易
        before_or_at = [trade for trade in trades if int(trade.get("T", 0)) <= target_ms]
        if before_or_at:
            return max(before_or_at, key=lambda trade: int(trade.get("T", 0)))

        # 如果没有找到之前的交易，选择之后最近的交易
        if trades:
            return min(trades, key=lambda trade: int(trade.get("T", 0)))

        return None
    
    def _clean_cache(self):
        """清理旧缓存数据，保持缓存大小在限制范围内"""
        current_time = time.time()
        
        # 定期清理
        if current_time - self.last_cache_clean >= self.cache_clean_interval:
            self.last_cache_clean = current_time
            
            for symbol in self.price_cache:
                # 如果缓存超过最大限制，删除最旧的条目
                while len(self.price_cache[symbol]) > self.max_cache_size:
                    # 删除OrderedDict中的第一个（最旧的）条目
                    self.price_cache[symbol].popitem(last=False)
    
    def fetch_price(self, symbol: str, timestamp_ms: int) -> float:
        """获取指定时间戳的价格，如果有缓存则直接使用缓存"""
        # 先清理缓存
        self._clean_cache()
        
        # 检查缓存
        if symbol in self.price_cache and timestamp_ms in self.price_cache[symbol]:
            # 更新访问顺序（移到最后，表示最近访问）
            price = self.price_cache[symbol][timestamp_ms]
            del self.price_cache[symbol][timestamp_ms]
            self.price_cache[symbol][timestamp_ms] = price
            return price
        
        window_ms = self.window_seconds * 1000
        start_time = max(timestamp_ms - window_ms, 0)
        end_time = timestamp_ms + window_ms

        params = {
            "symbol": symbol.upper(),
            "startTime": start_time,
            "endTime": end_time,
            "limit": 1000,
        }

        max_retries = 5
        retry_count = 0
        last_error = None
        
        # 使用渐进式退避策略
        base_delay = 1
        
        while retry_count < max_retries:
            try:
                trades = self.binance_request(params)
                trade = self.find_trade_at_timestamp(trades, timestamp_ms)
                
                if not trade:
                    retry_count += 1
                    last_error = f"在请求的时间戳附近未找到{symbol}交易，尝试扩大窗口"
                    print(f"警告: {last_error} (重试 {retry_count}/{max_retries})")
                    # 扩大窗口并重试
                    window_ms *= 2
                    start_time = max(timestamp_ms - window_ms, 0)
                    end_time = timestamp_ms + window_ms
                    params["startTime"] = start_time
                    params["endTime"] = end_time
                    time.sleep(base_delay * (2 ** (retry_count - 1)))
                    continue
                
                price = float(trade["p"])
                # 缓存价格
                self.price_cache[symbol][timestamp_ms] = price
                # 如果添加后超过限制，删除最旧的条目
                if len(self.price_cache[symbol]) > self.max_cache_size:
                    self.price_cache[symbol].popitem(last=False)
                
                # 打印获取到的价格信息
                timestamp_dt = dt.datetime.fromtimestamp(timestamp_ms / 1000)
                print(f"获取到{symbol}在{timestamp_dt}的价格: {price}")
                
                return price
            
            except Exception as e:
                retry_count += 1
                last_error = str(e)
                delay = base_delay * (2 ** (retry_count - 1))
                print(f"获取{symbol}价格失败 (重试 {retry_count}/{max_retries}): {str(e)}，{delay}秒后重试...")
                time.sleep(delay)
        
        # 达到最大重试次数，记录详细信息
        timestamp_dt = dt.datetime.fromtimestamp(timestamp_ms / 1000)
        print(f"警告: 经过{max_retries}次重试后仍无法获取{symbol}在{timestamp_dt} (时间戳: {timestamp_ms})的价格，使用默认值0")
        # 记录失败的请求，避免重复尝试
        self.price_cache[symbol][timestamp_ms] = 0.0
        return 0.0

def clean_processed_state():
    """清理过期的状态记录"""
    global processed_state
    current_time = time.time()
    expired_files = []
    
    for filename, state in processed_state['files'].items():
        # 检查是否是已完成且长时间未更新的文件
        if state.get('is_complete', False) and 'last_updated' in state:
            if current_time - state['last_updated'] > COMPLETED_FILE_RETENTION:
                expired_files.append(filename)
    
    # 删除过期记录
    for filename in expired_files:
        del processed_state['files'][filename]
        print(f"清理过期状态记录: {filename}")
    
    # 记录状态清理完成
    if expired_files:
        print(f"已清理 {len(expired_files)} 条过期状态记录")
        save_state()

def load_state() -> None:
    """加载处理状态"""
    global processed_state
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r', encoding='utf-8') as f:
                processed_state = json.load(f)
            
            # 确保所有文件状态都有last_updated字段
            current_time = time.time()
            for filename in processed_state['files']:
                if 'last_updated' not in processed_state['files'][filename]:
                    processed_state['files'][filename]['last_updated'] = current_time
            
            print(f"已加载ETH处理状态，记录了 {len(processed_state['files'])} 个文件")
            
            # 加载后立即进行一次状态清理
            clean_processed_state()
            
        except Exception as e:
            print(f"加载ETH状态文件失败: {str(e)}")
            processed_state = {"files": {}}

def save_state() -> None:
    """保存处理状态"""
    try:
        # 更新所有文件的最后更新时间
        current_time = time.time()
        for filename in processed_state['files']:
            processed_state['files'][filename]['last_updated'] = current_time
            
        with open(STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(processed_state, f, indent=2)
    except Exception as e:
        print(f"保存ETH状态文件失败: {str(e)}")

def get_input_files() -> List[str]:
    """获取所有需要处理的transactions_data_ETH_*.csv文件"""
    files = []
    for filename in os.listdir(BASE_DIR):
        if filename.startswith(INPUT_FILE_PREFIX) and filename.endswith(".csv"):
            # 无论是否已处理都返回，因为需要检查是否有新数据
            files.append(os.path.join(BASE_DIR, filename))
    return files

def is_file_modified_since_last_check(file_path: str) -> bool:
    """检查文件是否在最后检查后被修改过"""
    filename = os.path.basename(file_path)
    if filename not in processed_state['files']:
        return True
    
    # 检查文件大小变化
    try:
        current_size = os.path.getsize(file_path)
        # 如果文件大小没变，可能没有新数据
        if 'last_size' in processed_state['files'][filename] and \
           current_size == processed_state['files'][filename]['last_size']:
            return False
    except Exception:
        pass
    
    return True

def process_file(input_file: str, price_fetcher: PriceFetcher) -> bool:
    """实时处理单个CSV文件，直接在原始文件上添加价格信息（优化版）"""
    try:
        filename = os.path.basename(input_file)
        print(f"开始处理ETH文件: {input_file}")
        
        # 获取文件状态信息
        file_state = processed_state['files'].get(filename, {
            'last_processed_line': 0,
            'is_complete': False,
            'last_size': 0
        })
        
        # 检查是否已经完全处理过
        if file_state['is_complete']:
            print(f"文件 {filename} 已完全处理，跳过")
            return True
        
        # 检查文件是否存在且可读
        if not os.path.exists(input_file):
            print(f"错误: 文件 {input_file} 不存在")
            return False
        
        current_size = os.path.getsize(input_file)
        # 检查文件是否为空
        if current_size == 0:
            print("文件为空，标记为已完成")
            try:
                processed_state['files'][filename] = {
                    'last_processed_line': 0,
                    'is_complete': True,
                    'last_size': 0
                }
                save_state()
            except Exception:
                pass
            return True
        
        # 读取文件头部信息
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            header = reader.fieldnames.copy()  # 保存原始表头
            
            # 检查是否需要添加价格列
            if 'eth_price' not in header:
                header.append('eth_price')
            
            # 计算文件总行数（高效方式）
            f.seek(0, 2)  # 移动到文件末尾
            total_bytes = f.tell()
            f.seek(0)  # 回到文件开头
            next(reader)  # 跳过表头
            
            # 估算行数（不加载整个文件到内存）
            line_count = 0
            for _ in reader:
                line_count += 1
                # 如果文件很大，只计算前1000行用于估算
                if line_count > 1000 and total_bytes > 1024 * 1024 * 10:  # 大于10MB的文件
                    avg_line_size = total_bytes / (line_count + 1)  # +1 包含表头
                    total_rows = int(total_bytes / avg_line_size)
                    break
            else:
                total_rows = line_count
        
        print(f"文件共有约 {total_rows} 条记录")
        
        # 计算需要处理的起始行
        start_line = file_state['last_processed_line']
        lines_to_process = max(0, total_rows - start_line)
        
        if lines_to_process == 0:
            print("没有新的记录需要处理")
            # 更新文件大小但不标记为完成，因为可能还会有新数据写入
            try:
                file_state['last_size'] = current_size
                processed_state['files'][filename] = file_state
                save_state()
            except Exception:
                pass
            return True
        
        print(f"需要处理从第 {start_line + 1} 行开始的 {lines_to_process} 条新记录")
        
        # 使用流式处理来减少内存占用
        temp_file = input_file + '.tmp'
        header_written = False
        processed_count = 0
        
        # 为了更好的错误恢复，分批次处理
        batch_size = 100  # 每批处理100条记录
        batch = []
        batch_indices = []
        
        try:
            # 读取旧文件并处理新行
            with open(input_file, 'r', encoding='utf-8') as infile, \
                 open(temp_file, 'w', newline='', encoding='utf-8') as outfile:
                reader = csv.DictReader(infile)
                writer = csv.DictWriter(outfile, fieldnames=header)
                
                # 写入表头
                writer.writeheader()
                header_written = True
                
                # 处理已有的行（直接复制）
                for i, row in enumerate(reader):
                    if i < start_line:
                        # 如果缺少价格列，添加默认值
                        if 'eth_price' not in row:
                            row['eth_price'] = '0.0'
                        writer.writerow(row)
                    else:
                        # 收集需要处理的新行到批次
                        batch.append((i, row.copy()))
                        
                        # 当批次满时处理
                        if len(batch) >= batch_size:
                            # 处理当前批次
                            processed_batch = process_row_batch(batch, price_fetcher)
                            # 写入处理后的批次
                            for _, processed_row in processed_batch:
                                writer.writerow(processed_row)
                            processed_count += len(batch)
                            
                            # 更新进度
                            print(f"已处理 {processed_count}/{lines_to_process} 条新记录")
                            
                            # 清空批次并进行垃圾回收
                            batch = []
                            gc.collect()  # 显式调用垃圾回收
                            
                            # 每处理1000条记录保存一次状态
                            if processed_count % 1000 == 0:
                                try:
                                    current_processed_line = start_line + processed_count
                                    processed_state['files'][filename] = {
                                        'last_processed_line': current_processed_line,
                                        'is_complete': False,
                                        'last_size': current_size
                                    }
                                    save_state()
                                    print(f"已保存进度到第 {current_processed_line} 行")
                                except Exception as e:
                                    print(f"保存中间进度时出错: {str(e)}")
                
                # 处理最后一个不完整的批次
                if batch:
                    processed_batch = process_row_batch(batch, price_fetcher)
                    for _, processed_row in processed_batch:
                        writer.writerow(processed_row)
                    processed_count += len(batch)
            
            # 原子性地替换原始文件
            shutil.move(temp_file, input_file)
            
            # 更新处理状态
            try:
                new_last_line = start_line + processed_count
                # 只有当处理了所有预期记录且文件大小不再变化时才标记为完成
                is_complete = processed_count >= lines_to_process and file_state.get('last_size', 0) == current_size
                
                processed_state['files'][filename] = {
                    'last_processed_line': new_last_line,
                    'is_complete': is_complete,
                    'last_size': current_size
                }
                save_state()
                
                print(f"成功更新ETH文件: {input_file}")
                print(f"已处理到第 {new_last_line} 行")
                if is_complete:
                    print(f"文件 {filename} 已完全处理完毕")
                else:
                    print(f"文件 {filename} 处理完成，将继续监控更新")
            except Exception as e:
                print(f"更新最终状态时出错: {str(e)}")
        
        except Exception as e:
            # 如果处理失败，清理临时文件
            if os.path.exists(temp_file) and not header_written:
                try:
                    os.remove(temp_file)
                except:
                    pass
            raise
        
        return True
        
    except Exception as e:
        print(f"处理文件{input_file}时发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def process_row_batch(batch: List[tuple], price_fetcher: PriceFetcher) -> List[tuple]:
    """处理一批数据行，并发获取价格信息"""
    processed_batch = []
    
    # 使用线程池并发获取价格
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(batch))) as executor:
        futures = {}
        
        # 提交当前批次的价格查询任务
        for idx, row in batch:
            timestamp = row['timestamp']
            try:
                timestamp_ms = price_fetcher.parse_timestamp(timestamp)
                futures[executor.submit(get_prices_for_row, row, timestamp_ms, price_fetcher)] = idx
            except Exception as e:
                print(f"准备处理记录时出错: {str(e)}")
                row['eth_price'] = '0'
                processed_batch.append((idx, row))
        
        # 处理结果
        for future in as_completed(futures):
            idx = futures[future]
            try:
                processed_row = future.result()
                processed_batch.append((idx, processed_row))
            except Exception as e:
                print(f"处理记录时出错: {str(e)}")
                # 查找原始行并设置默认值
                for original_idx, original_row in batch:
                    if original_idx == idx:
                        if 'eth_price' not in original_row:
                            original_row['eth_price'] = '0'
                        processed_batch.append((idx, original_row))
                        break
    
    return processed_batch

def get_prices_for_row(row: Dict[str, str], timestamp_ms: int, price_fetcher: PriceFetcher) -> Dict[str, str]:
    """为单行数据获取ETH价格信息"""
    try:
        # 获取ETHUSDC价格
        eth_price = price_fetcher.fetch_price("ETHUSDC", timestamp_ms)
        
        # 添加价格列
        row['eth_price'] = f"{eth_price:.2f}"  # ETH价格保留2位小数
    except Exception as e:
        print(f"处理价格时出错: {str(e)}")
        row['eth_price'] = '0.0'
    
    return row

def main():
    """主函数"""
    print("=== ETH交易数据价格信息添加工具（内存优化版） ===")
    print("使用真实Binance API获取ETHUSDC价格数据")
    
    # 创建带缓存限制的价格获取器
    price_fetcher = PriceFetcher(max_cache_size=MAX_CACHE_SIZE)
    
    # 加载历史处理状态
    load_state()
    
    print(f"开始实时监控目录: {BASE_DIR}")
    print(f"监控文件前缀: {INPUT_FILE_PREFIX}")
    print(f"检查间隔: {CHECK_INTERVAL}秒")
    print(f"按Ctrl+C停止")
    print(f"状态文件: {STATE_FILE}")
    print(f"Binance API: {BASE_URL}")
    print(f"SSL上下文状态: {'已启用' if SSL_CONTEXT else '未启用'}")
    print(f"最大缓存大小: {MAX_CACHE_SIZE} 条记录")
    print(f"状态清理间隔: {STATE_CLEAN_INTERVAL}秒")
    
    last_state_clean = time.time()
    
    try:
        while True:
            # 定期清理状态数据
            current_time = time.time()
            if current_time - last_state_clean >= STATE_CLEAN_INTERVAL:
                last_state_clean = current_time
                clean_processed_state()
                
            # 显式调用垃圾回收以回收内存
            gc.collect()
            
            # 获取所有需要监控的文件
            input_files = get_input_files()
            
            modified_files = []
            for file_path in input_files:
                if is_file_modified_since_last_check(file_path):
                    modified_files.append(file_path)
            
            if modified_files:
                print(f"发现 {len(modified_files)} 个有更新的ETH文件")
                
                for input_file in modified_files:
                    success = process_file(input_file, price_fetcher)
                    if success:
                        print(f"文件处理成功: {input_file}")
                    else:
                        print(f"文件处理失败: {input_file}")
                    
                    # 处理完一个文件后短暂休息并进行垃圾回收
                    time.sleep(1)
                    gc.collect()
            else:
                print(f"没有发现更新的ETH文件，{CHECK_INTERVAL}秒后再次检查...")
            
            # 等待一段时间后再次检查
            time.sleep(CHECK_INTERVAL)
            
    except KeyboardInterrupt:
        print("\n程序被用户中断")
        # 保存最后的状态
        try:
            save_state()
        except Exception as e:
            print(f"保存最终状态时出错: {str(e)}")
    except Exception as e:
        print(f"发生未预期的错误: {str(e)}")
        import traceback
        traceback.print_exc()
        # 尝试保存状态
        try:
            save_state()
        except:
            pass
    finally:
        # 最后进行一次垃圾回收
        try:
            gc.collect()
        except:
            pass
        print("程序已停止")

if __name__ == "__main__":
    main()
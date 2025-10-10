import math

def calculate_tick_id(price):
    # Uniswap V3 的价格基数
    base_price = 1.0001
    
    # 计算 tick id
    # 由于价格是 1.0001 的 tick id 次方，所以我们需要做对数运算来找到 tick id
    tick_id = math.log(price) / math.log(base_price)
    
    # 由于 tick id 必须是整数，我们需要对其进行四舍五入
    tick_id = round(tick_id)
    
    return tick_id

# 假设我们想要计算价格为 2000 美元的以太坊对应的 tick id
price = 4500
tick_id = calculate_tick_id(price)
print(f"The tick id for a price of {price} USD is: {tick_id}")
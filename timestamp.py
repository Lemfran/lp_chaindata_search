from datetime import datetime
from zoneinfo import ZoneInfo

# 定义日期时间字符串
a = "Sep-28-2025 01:52:35 AM +UTC"
date_str = "Jan-01-2024 12:00:00 AM +UTC"

# 处理 '+UTC' 格式，将其替换为 'UTC'
a_with_utc = a.replace('+UTC', 'UTC')
date_str_with_utc = date_str.replace('+UTC', 'UTC')

# 解析日期时间字符串，注意这里需要指定时区为 UTC
# %p 用于解析 AM/PM
dt = datetime.strptime(date_str_with_utc, '%b-%d-%Y %I:%M:%S %p %Z')
a_dt = datetime.strptime(a_with_utc, '%b-%d-%Y %I:%M:%S %p %Z')



# 确保 datetime 对象是带时区信息的（使用Python 3.9+自带的ZoneInfo）
dt_with_tz = dt.replace(tzinfo=ZoneInfo('UTC'))
a_dt_with_tz = a_dt.replace(tzinfo=ZoneInfo('UTC'))

# 将 datetime 对象转换为时间戳（秒为单位）
timestamp = int(dt_with_tz.timestamp())
a_timestamp = int(a_dt_with_tz.timestamp())

print(timestamp)
print(a_timestamp)

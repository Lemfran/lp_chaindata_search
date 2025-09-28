#!/bin/bash

# 创建一个新的虚拟环境
python3 -m venv venv_get_swaps

# 激活虚拟环境
source venv_get_swaps/bin/activate

# 安装requests库
pip3 install requests

# 安装完成提示
echo "依赖安装完成！"
echo "要运行GetSwaps.py脚本，请先激活虚拟环境："
echo "source venv_get_swaps/bin/activate"
echo "然后运行："
echo "python3 GetSwaps.py"
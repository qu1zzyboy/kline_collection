#!/bin/bash

# K线采集服务重启脚本
# 先停止服务，再启动服务

set -e

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo -e "${YELLOW}正在重启服务...${NC}"

# 停止服务
"$SCRIPT_DIR/stop.sh"

# 等待一秒
sleep 1

# 启动服务
"$SCRIPT_DIR/start.sh"

echo -e "${GREEN}服务重启完成${NC}"


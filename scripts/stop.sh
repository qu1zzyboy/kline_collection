#!/bin/bash

# K线采集服务停止脚本
# 用于优雅地停止 kline_col 服务

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# 服务名称
SERVICE_NAME="kline_col"
BINARY_NAME="kline_col"
PID_FILE="$PROJECT_DIR/${BINARY_NAME}.pid"

echo -e "${YELLOW}正在停止 ${SERVICE_NAME} 服务...${NC}"

# 从 PID 文件读取进程 ID
if [ ! -f "$PID_FILE" ]; then
    echo -e "${YELLOW}PID 文件不存在，尝试查找进程...${NC}"
    PID=$(pgrep -f "target.*${BINARY_NAME}" || true)
    if [ -z "$PID" ]; then
        echo -e "${YELLOW}服务未运行${NC}"
        exit 0
    fi
else
    PID=$(cat "$PID_FILE" 2>/dev/null || true)
    if [ -z "$PID" ]; then
        echo -e "${YELLOW}PID 文件为空，删除并退出${NC}"
        rm -f "$PID_FILE"
        exit 0
    fi
    
    # 验证进程是否存在
    if ! kill -0 "$PID" 2>/dev/null; then
        echo -e "${YELLOW}进程不存在 (PID: ${PID})，删除 PID 文件${NC}"
        rm -f "$PID_FILE"
        exit 0
    fi
fi

echo -e "${YELLOW}找到进程 PID: ${PID}${NC}"

# 发送 SIGTERM 信号（优雅停止）
echo -e "${YELLOW}发送停止信号...${NC}"
kill -TERM "$PID" 2>/dev/null || true

# 等待进程结束（最多等待 10 秒）
TIMEOUT=10
ELAPSED=0
while [ $ELAPSED -lt $TIMEOUT ]; do
    if ! kill -0 "$PID" 2>/dev/null; then
        echo -e "${GREEN}服务已成功停止${NC}"
        rm -f "$PID_FILE"
        exit 0
    fi
    sleep 1
    ELAPSED=$((ELAPSED + 1))
    echo -e "${YELLOW}等待服务停止... (${ELAPSED}/${TIMEOUT})${NC}"
done

# 如果还在运行，强制停止
if kill -0 "$PID" 2>/dev/null; then
    echo -e "${RED}服务未在 ${TIMEOUT} 秒内停止，强制终止...${NC}"
    kill -KILL "$PID" 2>/dev/null || true
    sleep 1
    
    if ! kill -0 "$PID" 2>/dev/null; then
        echo -e "${GREEN}服务已强制停止${NC}"
        rm -f "$PID_FILE"
        exit 0
    else
        echo -e "${RED}无法停止服务，请手动检查${NC}"
        exit 1
    fi
fi

# 删除 PID 文件
rm -f "$PID_FILE"
exit 0


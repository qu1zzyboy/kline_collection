#!/bin/bash

# Binance Futures K线采集服务开发模式启动脚本

set -e

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# 切换到项目目录
cd "$PROJECT_DIR"

# 服务名称
BINARY_NAME="kline_col"

# 开发环境配置
export REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379}"
export MAX_KLINES_PER_SYMBOL="${MAX_KLINES_PER_SYMBOL:-600}"
export REDIS_KEY_PREFIX="${REDIS_KEY_PREFIX:-binance:kline:1s}"
export LOG_DIRECTORY="${LOG_DIRECTORY:-logs}"
export LOG_LEVEL_STDOUT="${LOG_LEVEL_STDOUT:-Debug}"
export LOG_LEVEL_FILE="${LOG_LEVEL_FILE:-Debug}"
export BINANCE_PRODUCT_TYPE="${BINANCE_PRODUCT_TYPE:-UsdM}"
export BINANCE_ENVIRONMENT="${BINANCE_ENVIRONMENT:-Mainnet}"

# 创建日志目录
mkdir -p "$LOG_DIRECTORY"

# PID 文件路径
PID_FILE="$PROJECT_DIR/${BINARY_NAME}.pid"

# 检查服务是否已在运行
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "错误: 服务已在运行 (PID: $OLD_PID)"
        echo "如需重启，请先运行: ./scripts/stop.sh"
        exit 1
    else
        # PID 文件存在但进程不存在，删除旧的 PID 文件
        rm -f "$PID_FILE"
    fi
fi

# 显示配置信息
echo "=== Binance Futures K线采集服务 (开发模式) ==="
echo "配置信息:"
echo "  Redis URL: $REDIS_URL"
echo "  每个品种最大 K 线数: $MAX_KLINES_PER_SYMBOL"
echo "  Redis Key 前缀: $REDIS_KEY_PREFIX"
echo "  日志目录: $LOG_DIRECTORY"
echo "  日志级别 (stdout): $LOG_LEVEL_STDOUT"
echo "  日志级别 (file): $LOG_LEVEL_FILE"
echo "  Binance 产品类型: $BINANCE_PRODUCT_TYPE"
echo "  Binance 环境: $BINANCE_ENVIRONMENT"
echo ""

# 先编译（如果还没有编译）
echo "正在编译服务..."
cargo build --bin kline_col

# 启动服务（后台运行）
echo "正在启动服务..."
BINARY_PATH="$PROJECT_DIR/target/debug/${BINARY_NAME}"
nohup "$BINARY_PATH" > /dev/null 2>&1 &
PID=$!

# 保存 PID
echo $PID > "$PID_FILE"

# 等待一下，检查进程是否还在运行
sleep 2
if ! kill -0 "$PID" 2>/dev/null; then
    rm -f "$PID_FILE"
    echo "错误: 服务启动失败"
    exit 1
fi

echo "服务已启动 (PID: $PID)"
echo "PID 文件: $PID_FILE"
echo ""
echo "停止服务: ./scripts/stop.sh"
echo "查看日志: tail -f $LOG_DIRECTORY/*.log"


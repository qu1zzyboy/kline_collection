#!/bin/bash

# Binance Futures K线采集服务启动脚本

set -e

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# 切换到项目目录
cd "$PROJECT_DIR"

# 默认配置（可通过环境变量覆盖）
export REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379}"
export MAX_KLINES_PER_SYMBOL="${MAX_KLINES_PER_SYMBOL:-600}"
export REDIS_KEY_PREFIX="${REDIS_KEY_PREFIX:-binance:kline:1s}"
export LOG_DIRECTORY="${LOG_DIRECTORY:-logs}"
export LOG_LEVEL_STDOUT="${LOG_LEVEL_STDOUT:-Info}"
export LOG_LEVEL_FILE="${LOG_LEVEL_FILE:-Info}"
export BINANCE_PRODUCT_TYPE="${BINANCE_PRODUCT_TYPE:-UsdM}"
export BINANCE_ENVIRONMENT="${BINANCE_ENVIRONMENT:-Mainnet}"

# 创建日志目录
mkdir -p "$LOG_DIRECTORY"

# 显示配置信息
echo "=== Binance Futures K线采集服务 ==="
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

# 运行服务
exec cargo run --release --bin kline_col


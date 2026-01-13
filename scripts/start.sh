#!/bin/bash

# Binance Futures 数据采集服务启动脚本（K线 + 订单簿）

set -e

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# 切换到项目目录
cd "$PROJECT_DIR"

# 服务名称
BINARY_NAME="kline_col"

# 先加载 .env 文件（如果存在），这样 .env 中的值会被优先使用
if [ -f "$PROJECT_DIR/.env" ]; then
    echo "加载 .env 文件..."
    set -a  # 自动导出所有变量
    source "$PROJECT_DIR/.env"
    set +a  # 关闭自动导出
fi

# 默认配置（只有在环境变量未设置时才使用默认值）
export REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379}"
export REDIS_PASSWORD="${REDIS_PASSWORD:-}"
export REDIS_DATABASE="${REDIS_DATABASE:-0}"

# K 线采集配置
export MAX_KLINES_PER_SYMBOL="${MAX_KLINES_PER_SYMBOL:-600}"
export KLINE_REDIS_KEY_PREFIX="${KLINE_REDIS_KEY_PREFIX:-binance:kline:1s}"

# 订单簿采集配置
export MAX_SNAPSHOTS_PER_SYMBOL="${MAX_SNAPSHOTS_PER_SYMBOL:-1200}"
export OB_REDIS_KEY_PREFIX="${OB_REDIS_KEY_PREFIX:-binance:ob:500ms}"
export ORDER_BOOK_DEPTH="${ORDER_BOOK_DEPTH:-5}"
export SNAPSHOT_INTERVAL_MS="${SNAPSHOT_INTERVAL_MS:-500}"

# 日志配置
export LOG_DIRECTORY="${LOG_DIRECTORY:-logs}"
export LOG_LEVEL_STDOUT="${LOG_LEVEL_STDOUT:-Info}"
export LOG_LEVEL_FILE="${LOG_LEVEL_FILE:-Info}"

# Binance 配置
export BINANCE_PRODUCT_TYPE="${BINANCE_PRODUCT_TYPE:-UsdM}"
export BINANCE_ENVIRONMENT="${BINANCE_ENVIRONMENT:-Mainnet}"

# 创建日志目录
mkdir -p "$LOG_DIRECTORY"

# PID 文件路径
PID_FILE="$PROJECT_DIR/${BINARY_NAME}.pid"
BINARY_NAME="kline_col"

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
echo "=== Binance Futures 数据采集服务 (K线 + 订单簿) ==="
echo "配置信息:"
echo "  Redis URL: $REDIS_URL"
echo "  Redis 数据库: $REDIS_DATABASE"
echo ""
echo "K 线采集配置:"
echo "  每个品种最大 K 线数: $MAX_KLINES_PER_SYMBOL"
echo "  Redis Key 前缀: $KLINE_REDIS_KEY_PREFIX"
echo ""
echo "订单簿采集配置:"
echo "  每个品种最大快照数: $MAX_SNAPSHOTS_PER_SYMBOL"
echo "  Redis Key 前缀: $OB_REDIS_KEY_PREFIX"
echo "  订单簿深度: $ORDER_BOOK_DEPTH 档"
echo "  快照间隔: $SNAPSHOT_INTERVAL_MS ms"
echo ""
echo "其他配置:"
echo "  日志目录: $LOG_DIRECTORY"
echo "  日志级别 (stdout): $LOG_LEVEL_STDOUT"
echo "  日志级别 (file): $LOG_LEVEL_FILE"
echo "  Binance 产品类型: $BINANCE_PRODUCT_TYPE"
echo "  Binance 环境: $BINANCE_ENVIRONMENT"
echo ""

# 先编译（如果还没有编译）
echo "正在编译服务..."
cargo build --release --bin kline_col

# 启动服务（后台运行）
echo "正在启动服务..."
BINARY_PATH="$PROJECT_DIR/target/release/${BINARY_NAME}"

# 创建日志文件用于捕获启动错误
STARTUP_LOG="$LOG_DIRECTORY/startup.log"
nohup "$BINARY_PATH" > "$STARTUP_LOG" 2>&1 &
PID=$!

# 保存 PID
echo $PID > "$PID_FILE"

# 等待一下，检查进程是否还在运行
sleep 3
if ! kill -0 "$PID" 2>/dev/null; then
    rm -f "$PID_FILE"
    echo "错误: 服务启动失败"
    echo "查看启动日志: $STARTUP_LOG"
    if [ -f "$STARTUP_LOG" ]; then
        echo "最近的错误信息:"
        tail -20 "$STARTUP_LOG"
    fi
    exit 1
fi

echo "服务已启动 (PID: $PID)"
echo "PID 文件: $PID_FILE"
echo ""
echo "停止服务: ./scripts/stop.sh"
echo "查看日志: tail -f $LOG_DIRECTORY/*.log"


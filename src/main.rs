//! Binance Futures K线采集服务入口
//!
//! 全量采集 Binance Futures 的秒级别 K 线数据，并存储到 Redis。
//! 每个品种最多存储 600 根 K 线。

mod config;
mod collection;
mod log;

use anyhow::Result;

/// K线采集服务入口
#[tokio::main]
async fn main() -> Result<()> {
    // 加载 .env 文件（如果存在）
    // 如果文件不存在，不会报错，会继续使用系统环境变量
    dotenvy::dotenv().ok();
    
    // 加载配置
    let config = config::KlineCollectionConfig::from_env();
    
    // 初始化日志
    let _log_guard = log::init_logging(&config)?;
    
    // 运行采集服务
    collection::run_collection_service(config).await
}

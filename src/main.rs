//! Binance Futures 数据采集服务入口
//!
//! 同时运行 K 线采集服务和订单簿采集服务：
//! - K 线采集：全量采集 Binance Futures 的秒级别 K 线数据
//! - 订单簿采集：全量采集 Binance Futures 的5档订单簿快照（500ms级别）

mod config;
mod collection;
mod log;

use anyhow::Result;

/// 数据采集服务入口
#[tokio::main]
async fn main() -> Result<()> {
    // 加载 .env 文件（如果存在）
    // 如果文件不存在，不会报错，会继续使用系统环境变量
    dotenvy::dotenv().ok();
    
    // 加载配置
    let kline_config = config::KlineCollectionConfig::from_env();
    let ob_config = config::ObCollectionConfig::from_env();
    
    // 初始化日志（使用 K 线配置的日志设置，两个服务共享日志）
    let _log_guard = log::init_logging(&kline_config.base)?;
    
    // 启动数据采集服务：K 线采集 + 订单簿采集
    // 使用 tokio::spawn 同时运行两个服务
    let kline_handle = tokio::spawn(async move {
        collection::kline_collection::run_kline_collection_service(kline_config).await
    });
    
    let ob_handle = tokio::spawn(async move {
        collection::ob_collection::run_ob_collection_service(ob_config).await
    });
    
    // 等待两个服务完成（任何一个退出都会导致程序退出）
    tokio::select! {
        result = kline_handle => {
            match result {
                Ok(Ok(())) => {
                    eprintln!("K 线采集服务正常退出");
                }
                Ok(Err(e)) => {
                    eprintln!("K 线采集服务异常退出: {}", e);
                    return Err(e);
                }
                Err(e) => {
                    eprintln!("K 线采集服务任务异常: {}", e);
                    return Err(anyhow::anyhow!("K 线采集服务任务异常: {}", e));
                }
            }
        }
        result = ob_handle => {
            match result {
                Ok(Ok(())) => {
                    eprintln!("订单簿采集服务正常退出");
                }
                Ok(Err(e)) => {
                    eprintln!("订单簿采集服务异常退出: {}", e);
                    return Err(e);
                }
                Err(e) => {
                    eprintln!("订单簿采集服务任务异常: {}", e);
                    return Err(anyhow::anyhow!("订单簿采集服务任务异常: {}", e));
                }
            }
        }
    }
    
    Ok(())
}

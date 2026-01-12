//! 日志管理模块
//!
//! 负责初始化日志系统，按日期输出到文件夹

use anyhow::Result;
use log::LevelFilter;
use nautilus_common::logging::{
    logger::{Logger, LoggerConfig},
    writer::FileWriterConfig,
};
use nautilus_core::UUID4;
use nautilus_model::identifiers::TraderId;
use std::str::FromStr;

use crate::config::KlineCollectionConfig;

/// 初始化日志系统
///
/// # Errors
///
/// 如果日志初始化失败，返回错误
pub fn init_logging(config: &KlineCollectionConfig) -> Result<nautilus_common::logging::logger::LogGuard> {
    let trader_id = TraderId::from("KLINE_COL-001");
    let instance_id = UUID4::new();
    
    // 解析日志级别
    let stdout_level = LevelFilter::from_str(&config.log_level_stdout)
        .unwrap_or(LevelFilter::Info);
    let fileout_level = LevelFilter::from_str(&config.log_level_file)
        .unwrap_or(LevelFilter::Info);
    
    // 配置日志
    let logger_config = LoggerConfig {
        stdout_level,
        fileout_level,
        ..Default::default()
    };
    
    // 配置文件输出：按日期自动轮转
    // 不设置 file_name 时，会自动使用格式：{trader_id}_{YYYY-MM-DD}_{instance_id}.log
    // 每天 UTC 午夜会自动创建新文件
    let file_config = FileWriterConfig {
        directory: Some(config.log_directory.clone()),
        file_name: None, // None 表示使用默认命名，会自动按日期轮转（每天一个新文件）
        file_format: None, // None 表示使用纯文本格式（.log），"json" 表示 JSON 格式（.json）
        file_rotate: None, // None 表示按日期轮转（默认行为），设置后可按文件大小轮转
    };
    
    let log_guard = Logger::init_with_config(trader_id, instance_id, logger_config, file_config)
        .map_err(|e| anyhow::anyhow!("Failed to initialize logging: {e}"))?;
    
    log::info!("日志已初始化，输出到 {} 文件夹", config.log_directory);
    log::info!("日志级别 - stdout: {}, file: {}", config.log_level_stdout, config.log_level_file);
    
    Ok(log_guard)
}


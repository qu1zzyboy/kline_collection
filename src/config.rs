//! 配置模块
//!
//! 管理 K 线采集服务的配置参数

use std::env;

/// K 线采集服务配置
#[derive(Debug, Clone)]
pub struct KlineCollectionConfig {
    /// Redis 连接 URL（不包含密码和数据库编号）
    pub redis_url: String,
    /// Redis 密码（可选，默认为空）
    pub redis_password: String,
    /// Redis 数据库编号（0-15，默认为 0）
    pub redis_database: u8,
    /// 每个品种最多存储的 K 线数量
    pub max_klines_per_symbol: usize,
    /// Redis Key 前缀
    pub redis_key_prefix: String,
    /// 日志目录
    pub log_directory: String,
    /// 日志级别（stdout）
    pub log_level_stdout: String,
    /// 日志级别（file）
    pub log_level_file: String,
    /// Binance 产品类型
    pub binance_product_type: String,
    /// Binance 环境（mainnet/testnet）
    pub binance_environment: String,
}

impl Default for KlineCollectionConfig {
    fn default() -> Self {
        Self {
            redis_url: env::var("REDIS_URL")
                .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string()),
            redis_password: env::var("REDIS_PASSWORD")
                .unwrap_or_else(|_| String::new()),
            redis_database: env::var("REDIS_DATABASE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            max_klines_per_symbol: 600,
            redis_key_prefix: "binance:kline:1s".to_string(),
            log_directory: "logs".to_string(),
            log_level_stdout: "Info".to_string(),
            log_level_file: "Info".to_string(),
            binance_product_type: "UsdM".to_string(),
            binance_environment: "Mainnet".to_string(),
        }
    }
}

impl KlineCollectionConfig {
    /// 从环境变量创建配置
    pub fn from_env() -> Self {
        Self {
            redis_url: env::var("REDIS_URL")
                .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string()),
            redis_password: env::var("REDIS_PASSWORD")
                .unwrap_or_else(|_| String::new()),
            redis_database: env::var("REDIS_DATABASE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            max_klines_per_symbol: env::var("MAX_KLINES_PER_SYMBOL")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(600),
            redis_key_prefix: env::var("REDIS_KEY_PREFIX")
                .unwrap_or_else(|_| "binance:kline:1s".to_string()),
            log_directory: env::var("LOG_DIRECTORY")
                .unwrap_or_else(|_| "logs".to_string()),
            log_level_stdout: env::var("LOG_LEVEL_STDOUT")
                .unwrap_or_else(|_| "Info".to_string()),
            log_level_file: env::var("LOG_LEVEL_FILE")
                .unwrap_or_else(|_| "Info".to_string()),
            binance_product_type: env::var("BINANCE_PRODUCT_TYPE")
                .unwrap_or_else(|_| "UsdM".to_string()),
            binance_environment: env::var("BINANCE_ENVIRONMENT")
                .unwrap_or_else(|_| "Mainnet".to_string()),
        }
    }
    
    /// 构建包含密码和数据库编号的 Redis 连接 URL
    /// 格式: redis://[:password@]host:port[/database]
    pub fn build_redis_url(&self) -> String {
        // 先处理密码
        let url_with_auth = if self.redis_password.is_empty() {
            self.redis_url.clone()
        } else {
            // 解析原始 URL，插入密码
            // 格式: redis://host:port -> redis://:password@host:port
            if let Some(rest) = self.redis_url.strip_prefix("redis://") {
                format!("redis://:{}@{}", self.redis_password, rest)
            } else if let Some(rest) = self.redis_url.strip_prefix("rediss://") {
                format!("rediss://:{}@{}", self.redis_password, rest)
            } else {
                // 如果格式不符合预期，直接返回原 URL
                self.redis_url.clone()
            }
        };
        
        // 处理数据库编号
        // Redis URL 格式: redis://host:port 或 redis://host:port/database
        // 需要找到最后一个 '/' 之后的部分，替换为数据库编号
        if let Some(pos) = url_with_auth.rfind('/') {
            // 检查 '/' 之后是否是端口号（包含 ':'）
            let after_slash = &url_with_auth[pos + 1..];
            if after_slash.contains(':') || after_slash.is_empty() {
                // '/' 后面是端口号或为空，添加数据库编号
                format!("{}/{}", url_with_auth, self.redis_database)
            } else {
                // '/' 后面可能是数据库编号，替换它
                format!("{}/{}", &url_with_auth[..pos], self.redis_database)
            }
        } else {
            // URL 没有 '/'，添加数据库编号
            format!("{}/{}", url_with_auth, self.redis_database)
        }
    }
}


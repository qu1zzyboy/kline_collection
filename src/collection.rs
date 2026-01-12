//! K 线采集业务逻辑模块
//!
//! 负责连接 Binance WebSocket，采集 K 线数据并存储到 Redis

use anyhow::{Context, Result};
use futures_util::StreamExt;
use nautilus_binance::futures::websocket::client::BinanceFuturesWebSocketClient;
use nautilus_binance::futures::websocket::messages::NautilusFuturesWsMessage;
use nautilus_binance::futures::http::client::BinanceFuturesHttpClient;
use nautilus_binance::common::enums::{BinanceEnvironment, BinanceProductType};
use nautilus_binance::common::symbol::format_binance_stream_symbol;
use nautilus_model::instruments::Instrument;
use redis::aio::ConnectionManager;
use redis::cmd;
use std::time::Duration;
use tokio::time::sleep;

use crate::config::KlineCollectionConfig;

/// Lua 脚本：原子性去重写入
/// 使用 SET key "1" NX EX ttl 来判断是否已存在
/// KEYS[1]: listKey (binance:kline:1s:{symbol})
/// KEYS[2]: dedupKey (binance:kline:1s:dedup:{symbol}:{ts_event})
/// ARGV[1]: klineJson (K线数据 JSON)
/// ARGV[2]: maxKlines (最大K线数量)
/// ARGV[3]: ttl (去重 key 的过期时间，秒)
/// 返回: 1 表示新写入，0 表示重复（已存在）
const LUA_SCRIPT_DEDUP_WRITE: &str = r#"
local listKey = KEYS[1]
local dedupKey = KEYS[2]
local klineJson = ARGV[1]
local maxKlines = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

-- 使用 SET key "1" NX EX ttl 来判断是否已存在
-- 如果 key 不存在，SET 返回 "OK"，如果已存在，返回 nil
local result = redis.call('SET', dedupKey, '1', 'NX', 'EX', ttl)
if result == nil then
    -- key 已存在，返回0表示重复
    return 0
end

-- key 不存在，执行写入操作
redis.call('LPUSH', listKey, klineJson)
redis.call('LTRIM', listKey, 0, maxKlines - 1)

-- 返回1表示新写入
return 1
"#;

/// 去重 key 的过期时间（秒），10秒
const DEDUP_KEY_TTL: u64 = 10;

/// 加载 Lua 脚本到 Redis 并返回 SHA1 哈希
///
/// # Errors
///
/// 如果加载失败，返回错误
async fn load_lua_script(
    redis_conn: &mut ConnectionManager,
) -> Result<String> {
    // 使用 SCRIPT LOAD 加载 Lua 脚本，返回 SHA1 哈希
    let sha: String = cmd("SCRIPT")
        .arg("LOAD")
        .arg(LUA_SCRIPT_DEDUP_WRITE)
        .query_async(redis_conn)
        .await
        .context("Failed to load Lua script")?;
    
    log::debug!("Lua 脚本已加载，SHA: {}", sha);
    Ok(sha)
}

/// 运行 K 线采集服务
///
/// # Errors
///
/// 如果服务启动失败或运行过程中出错，返回错误
pub async fn run_collection_service(config: KlineCollectionConfig) -> Result<()> {
    log::info!("Binance Futures K线采集服务启动");

    // 连接 Redis
    let redis_url = config.build_redis_url();
    // 构建用于日志的 URL（隐藏密码）
    let redis_url_log = if config.redis_password.is_empty() {
        redis_url.clone()
    } else {
        redis_url.replace(&config.redis_password, "***")
    };
    log::info!("连接 Redis: {} (数据库: {})", redis_url_log, config.redis_database);
    let redis_client = redis::Client::open(redis_url)?;
    
    // 配置 Redis 连接管理器：增加超时时间以处理高并发场景
    let connection_manager_config = redis::aio::ConnectionManagerConfig::new()
        .set_connection_timeout(Some(Duration::from_secs(30)))  // 连接超时：30秒
        .set_response_timeout(Some(Duration::from_secs(10)))  // 响应超时：10秒（Lua 脚本执行）
        .set_number_of_retries(3)                              // 重试次数：3次
        .set_exponent_base(2.0)                                 // 指数退避基数
        .set_min_delay(Duration::from_millis(100))              // 最小延迟：100ms
        .set_max_delay(Duration::from_secs(2));                 // 最大延迟：2秒
    
    let mut redis_conn: ConnectionManager = redis_client
        .get_connection_manager_with_config(connection_manager_config)
        .await
        .context("Failed to connect to Redis")?;
    log::info!("Redis 连接成功 (数据库: {})", config.redis_database);
    
    // 加载 Lua 脚本
    let lua_script_sha = load_lua_script(&mut redis_conn).await?;
    log::info!("Lua 脚本已加载，准备开始采集");

    // 解析 Binance 配置
    let product_type = match config.binance_product_type.as_str() {
        "UsdM" => BinanceProductType::UsdM,
        "CoinM" => BinanceProductType::CoinM,
        _ => {
            log::warn!("未知的产品类型: {}，使用默认值 UsdM", config.binance_product_type);
            BinanceProductType::UsdM
        }
    };

    let environment = match config.binance_environment.as_str() {
        "Mainnet" => BinanceEnvironment::Mainnet,
        "Testnet" => BinanceEnvironment::Testnet,
        _ => {
            log::warn!("未知的环境: {}，使用默认值 Mainnet", config.binance_environment);
            BinanceEnvironment::Mainnet
        }
    };

    // 获取所有交易对
    log::info!("正在获取交易对信息...");
    let http_client = BinanceFuturesHttpClient::new(
        product_type,
        environment,
        None,
        None,
        None,
        None,
        None,
        None,
    )?;

    let instruments = http_client.request_instruments().await?;
    log::info!("获取到 {} 个交易对", instruments.len());

    // 构建所有流名称
    let mut streams = Vec::new();
    for instrument in &instruments {
        let symbol_lower = format_binance_stream_symbol(&instrument.id());
        let stream = format!("{}_perpetual@continuousKline_1s", symbol_lower);
        streams.push(stream);
    }

    // Binance 限制：每个连接最多 200 个流
    // 需要创建多个连接来处理所有流
    const MAX_STREAMS_PER_CONNECTION: usize = 200;
    let total_connections = (streams.len() + MAX_STREAMS_PER_CONNECTION - 1) / MAX_STREAMS_PER_CONNECTION;
    
    log::info!("订阅 {} 个流（需要 {} 个连接，每个最多 {} 个流）", 
        streams.len(), total_connections, MAX_STREAMS_PER_CONNECTION);
    
    // 创建多个 WebSocket 客户端，每个处理最多 200 个流
    let mut clients = Vec::new();
    let streams_iter = streams.chunks(MAX_STREAMS_PER_CONNECTION);
    
    for (conn_idx, chunk) in streams_iter.enumerate() {
        log::info!("创建连接 {} / {}，处理 {} 个流", conn_idx + 1, total_connections, chunk.len());
        
        let mut client = BinanceFuturesWebSocketClient::new(
            product_type,
            environment,
            None,
            None,
            None,
            Some(60),
        )?;
        
        // 缓存 instruments（所有客户端共享相同的 instruments）
        client.cache_instruments(instruments.clone());
        
        log::info!("连接 Binance Futures WebSocket (连接 {})...", conn_idx + 1);
        client.connect().await?;
        log::info!("WebSocket 连接成功 (连接 {})", conn_idx + 1);
        
        // 等待连接稳定
        sleep(Duration::from_millis(500)).await;
        
        // 订阅当前连接的流
        client.subscribe(chunk.to_vec()).await?;
        log::info!("连接 {} 订阅成功，已订阅 {} 个流", conn_idx + 1, chunk.len());
        
        clients.push(client);
        
        // 在连接之间稍作延迟，避免过快创建连接
        if conn_idx < total_connections - 1 {
            sleep(Duration::from_millis(200)).await;
        }
    }
    
    log::info!("所有连接创建成功，开始采集 K 线数据");

    // 使用 channel 来合并所有流的消息
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tokio::sync::mpsc;
    
    let (tx, mut rx) = mpsc::unbounded_channel::<NautilusFuturesWsMessage>();
    let lua_script_sha = Arc::new(Mutex::new(lua_script_sha));
    
    // 为每个客户端创建独立任务来处理消息
    for (conn_idx, client) in clients.into_iter().enumerate() {
        let tx_clone = tx.clone();
        let stream = client.stream();
        tokio::spawn(async move {
            tokio::pin!(stream);
            while let Some(msg) = stream.next().await {
                if let Err(e) = tx_clone.send(msg) {
                    log::error!("连接 {} 发送消息失败: {}", conn_idx + 1, e);
                    break;
                }
            }
            log::warn!("连接 {} 的消息流已结束", conn_idx + 1);
        });
    }
    drop(tx); // 关闭发送端，当所有任务结束时，接收端会收到 None

    // 持续运行
    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(NautilusFuturesWsMessage::Data(data_vec)) => {
                        for data in data_vec {
                            if let nautilus_model::data::Data::Bar(bar) = data {
                                // 存储到 Redis
                                let instrument_id = bar.bar_type.instrument_id();
                                let symbol_raw = instrument_id.symbol.as_str();
                                // 去掉 -PERP 后缀
                                let symbol = symbol_raw.strip_suffix("-PERP").unwrap_or(symbol_raw);
                                let redis_key = format!("{}:{}", config.redis_key_prefix, symbol);
                                
                                // 使用 ts_event 作为去重标识（对应 Go 中的 closeTime）
                                let ts_event = bar.ts_event.as_u64();
                                let dedup_key = format!("{}:dedup:{}:{}", config.redis_key_prefix, symbol, ts_event);
                                
                                // 序列化 K 线数据（使用去掉后缀的 symbol）
                                let kline_data = serde_json::json!({
                                    "symbol": symbol,
                                    "open": bar.open.to_string(),
                                    "high": bar.high.to_string(),
                                    "low": bar.low.to_string(),
                                    "close": bar.close.to_string(),
                                    "volume": bar.volume.to_string(),
                                    "ts_event": ts_event,
                                    "ts_init": bar.ts_init.as_u64(),
                                });
                                
                                let kline_json = serde_json::to_string(&kline_data)?;
                                
                                // 使用 Lua 脚本进行原子性去重写入
                                // KEYS[1]: listKey, KEYS[2]: dedupKey
                                // ARGV[1]: klineJson, ARGV[2]: maxKlines, ARGV[3]: ttl
                                // 返回: 1 表示新写入，0 表示重复（已存在）
                                let sha_guard = lua_script_sha.lock().await;
                                let current_sha = sha_guard.clone();
                                drop(sha_guard);
                                
                                let _result: i64 = match cmd("EVALSHA")
                                    .arg(&current_sha)
                                    .arg(2)
                                    .arg(&redis_key)
                                    .arg(&dedup_key)
                                    .arg(&kline_json)
                                    .arg(config.max_klines_per_symbol)
                                    .arg(DEDUP_KEY_TTL)
                                    .query_async(&mut redis_conn)
                                    .await
                                {
                                    Ok(result) => result,
                                    Err(e) => {
                                        // 如果脚本不存在（可能 Redis 重启），重新加载
                                        if e.to_string().contains("NOSCRIPT") {
                                            log::warn!("Lua 脚本未找到，重新加载...");
                                            match load_lua_script(&mut redis_conn).await {
                                                Ok(new_sha) => {
                                                    // 更新共享的 SHA
                                                    *lua_script_sha.lock().await = new_sha.clone();
                                                    
                                                    // 使用新的 SHA 重试
                                                    cmd("EVALSHA")
                                                        .arg(&new_sha)
                                                        .arg(2)
                                                        .arg(&redis_key)
                                                        .arg(&dedup_key)
                                                        .arg(&kline_json)
                                                        .arg(config.max_klines_per_symbol)
                                                        .arg(DEDUP_KEY_TTL)
                                                        .query_async(&mut redis_conn)
                                                        .await
                                                        .unwrap_or_else(|e| {
                                                            log::warn!("Lua 脚本执行失败 [{}]: {}", symbol, e);
                                                            0 // 返回 0 表示失败，跳过
                                                        })
                                                }
                                                Err(e) => {
                                                    log::warn!("重新加载 Lua 脚本失败 [{}]: {}", symbol, e);
                                                    0
                                                }
                                            }
                                        } else {
                                            log::warn!("Lua 脚本执行失败 [{}]: {}", symbol, e);
                                            0
                                        }
                                    }
                                };
                                
                                // result 为 1 表示新写入，0 表示重复或失败
                                // 重复时不打印日志，避免日志过多
                            }
                        }
                    }
                    Some(NautilusFuturesWsMessage::Reconnected) => {
                        log::info!("WebSocket 已重新连接");
                    }
                    Some(NautilusFuturesWsMessage::Error(err)) => {
                        log::error!("WebSocket 错误 - 代码: {}, 消息: {}", err.code, err.msg);
                    }
                    Some(NautilusFuturesWsMessage::Deltas(_)) => {
                        // 忽略订单簿更新
                    }
                    Some(NautilusFuturesWsMessage::RawJson(_)) => {
                        // 忽略原始 JSON
                    }
                    Some(NautilusFuturesWsMessage::Instrument(_)) => {
                        // 忽略 instrument 更新
                    }
                    Some(NautilusFuturesWsMessage::AccountUpdate(_)) => {
                        // 忽略账户更新
                    }
                    Some(NautilusFuturesWsMessage::OrderUpdate(_)) => {
                        // 忽略订单更新
                    }
                    Some(NautilusFuturesWsMessage::MarginCall(_)) => {
                        log::warn!("收到保证金追加通知");
                    }
                    Some(NautilusFuturesWsMessage::AccountConfigUpdate(_)) => {
                        // 忽略账户配置更新
                    }
                    Some(NautilusFuturesWsMessage::ListenKeyExpired) => {
                        log::warn!("Listen key 已过期，需要重新连接");
                    }
                    None => {
                        log::error!("消息流已关闭");
                        break;
                    }
                }
            }
        }
    }

    log::info!("正在关闭连接...");
    // 注意：客户端已经在任务中使用，当任务结束时连接会自动关闭
    // 这里只需要等待所有任务完成即可
    log::info!("服务已停止");

    Ok(())
}


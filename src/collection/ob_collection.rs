//! 订单簿采集业务逻辑模块
//!
//! 负责连接 Binance WebSocket，采集全量市场5档订单簿快照（500ms级别）并存储到 Redis

use anyhow::{Context, Result};
use futures_util::StreamExt;
use nautilus_binance::futures::websocket::client::BinanceFuturesWebSocketClient;
use nautilus_binance::futures::websocket::messages::NautilusFuturesWsMessage;
use nautilus_binance::futures::http::client::BinanceFuturesHttpClient;
use nautilus_binance::common::enums::{BinanceEnvironment, BinanceProductType};
use nautilus_binance::common::symbol::format_binance_stream_symbol;
use nautilus_model::instruments::Instrument;
use nautilus_model::data::OrderBookDeltas;
use nautilus_model::enums::{BookAction, OrderSide};
use redis::aio::ConnectionManager;
use redis::cmd;
use std::time::Duration;
use tokio::time::sleep;
use std::collections::BTreeMap;

use crate::config::ObCollectionConfig;

/// Lua 脚本：原子性去重写入订单簿快照（使用 Redis Streams）
/// KEYS[1]: streamKey (按交易对分的 Stream key，格式: BINANCE_OB_500MS:{symbol})
/// KEYS[2]: dedupKey (binance:ob:500ms:dedup:{symbol}:{ts_event})
/// ARGV[1]: obJson (订单簿快照 JSON)
/// ARGV[2]: maxLen (最大长度，1200条，相当于10分钟数据，500ms * 1200 = 600秒)
/// ARGV[3]: ttl (去重 key 的过期时间，秒)
/// 返回: 1 表示新写入，0 表示重复（已存在）
const LUA_SCRIPT_DEDUP_WRITE: &str = r#"
local streamKey = KEYS[1]
local dedupKey = KEYS[2]
local obJson = ARGV[1]
local maxLen = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

-- 使用 SET key "1" NX EX ttl 来判断是否已存在
local result = redis.call('SET', dedupKey, '1', 'NX', 'EX', ttl)
if result == nil then
    -- key 已存在，返回0表示重复
    return 0
end

-- key 不存在，执行写入操作（使用 XADD 存储到 Stream）
redis.call('XADD', streamKey, 'MAXLEN', '~', maxLen, '*', 'data', obJson)

-- 返回1表示新写入
return 1
"#;

/// 去重 key 的过期时间（秒），10秒
const DEDUP_KEY_TTL: u64 = 10;

/// 订单簿状态（维护每个交易对的当前订单簿）
#[derive(Clone, Debug)]
struct OrderBookState {
    /// 买盘（价格从高到低排序，只保留前5档）
    bids: BTreeMap<u64, (String, String)>, // price_key -> (price, size)
    /// 卖盘（价格从低到高排序，只保留前5档）
    asks: BTreeMap<u64, (String, String)>, // price_key -> (price, size)
}

impl OrderBookState {
    fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    /// 应用订单簿增量更新
    fn apply_deltas(&mut self, deltas: &OrderBookDeltas) {
        for delta in &deltas.deltas {
            let price_key = (delta.order.price.as_f64() * 1_000_000_000.0) as u64; // 转换为整数用于排序
            let price_str = delta.order.price.to_string();
            let size_str = delta.order.size.to_string();

            match delta.order.side {
                OrderSide::Buy => {
                    match delta.action {
                        BookAction::Add | BookAction::Update => {
                            if delta.order.size.as_f64() > 0.0 {
                                self.bids.insert(price_key, (price_str, size_str));
                            }
                        }
                        BookAction::Delete => {
                            self.bids.remove(&price_key);
                        }
                        BookAction::Clear => {
                            self.bids.clear();
                        }
                    }
                }
                OrderSide::Sell => {
                    match delta.action {
                        BookAction::Add | BookAction::Update => {
                            if delta.order.size.as_f64() > 0.0 {
                                self.asks.insert(price_key, (price_str, size_str));
                            }
                        }
                        BookAction::Delete => {
                            self.asks.remove(&price_key);
                        }
                        BookAction::Clear => {
                            self.asks.clear();
                        }
                    }
                }
                OrderSide::NoOrderSide => {
                    // 忽略没有方向的订单
                }
            }
        }

    }

    /// 裁剪到指定深度
    fn trim_to_depth(&mut self, depth: usize) {
        // 买盘：价格从高到低，保留前 depth 档
        if self.bids.len() > depth {
            let keys_to_remove: Vec<u64> = self.bids
                .keys()
                .rev()
                .skip(depth)
                .copied()
                .collect();
            for key in keys_to_remove {
                self.bids.remove(&key);
            }
        }

        // 卖盘：价格从低到高，保留前 depth 档
        if self.asks.len() > depth {
            let keys_to_remove: Vec<u64> = self.asks
                .keys()
                .take(self.asks.len() - depth)
                .copied()
                .collect();
            for key in keys_to_remove {
                self.asks.remove(&key);
            }
        }
    }

    /// 生成快照 JSON
    fn to_snapshot_json(&self, symbol: &str, ts_event: u64, ts_init: u64, depth: usize) -> serde_json::Value {
        // 买盘：价格从高到低排序，取前 depth 档
        let bids: Vec<[String; 2]> = self.bids
            .iter()
            .rev()
            .take(depth)
            .map(|(_, (price, size))| [price.clone(), size.clone()])
            .collect();

        // 卖盘：价格从低到高排序，取前 depth 档
        let asks: Vec<[String; 2]> = self.asks
            .iter()
            .take(depth)
            .map(|(_, (price, size))| [price.clone(), size.clone()])
            .collect();

        serde_json::json!({
            "symbol": symbol,
            "bids": bids,
            "asks": asks,
            "ts_event": ts_event,
            "ts_init": ts_init,
        })
    }
}

/// 加载 Lua 脚本到 Redis 并返回 SHA1 哈希
async fn load_lua_script(
    redis_conn: &mut ConnectionManager,
) -> Result<String> {
    let sha: String = cmd("SCRIPT")
        .arg("LOAD")
        .arg(LUA_SCRIPT_DEDUP_WRITE)
        .query_async(redis_conn)
        .await
        .context("Failed to load Lua script")?;
    
    log::debug!("Lua 脚本已加载，SHA: {}", sha);
    Ok(sha)
}

/// 运行订单簿采集服务
///
/// # Errors
///
/// 如果服务启动失败或运行过程中出错，返回错误
pub async fn run_ob_collection_service(config: ObCollectionConfig) -> Result<()> {
    log::info!("Binance Futures 订单簿采集服务启动（5档，500ms级别）");

    // 连接 Redis
    let redis_url = config.build_redis_url();
    let redis_url_log = if config.base.redis_password.is_empty() {
        redis_url.clone()
    } else {
        redis_url.replace(&config.base.redis_password, "***")
    };
    log::info!("连接 Redis: {} (数据库: {})", redis_url_log, config.base.redis_database);
    let redis_client = redis::Client::open(redis_url)?;
    
    let connection_manager_config = redis::aio::ConnectionManagerConfig::new()
        .set_connection_timeout(Some(Duration::from_secs(30)))
        .set_response_timeout(Some(Duration::from_secs(10)))
        .set_number_of_retries(3)
        .set_exponent_base(2.0)
        .set_min_delay(Duration::from_millis(100))
        .set_max_delay(Duration::from_secs(2));
    
    let mut redis_conn: ConnectionManager = redis_client
        .get_connection_manager_with_config(connection_manager_config)
        .await
        .context("Failed to connect to Redis")?;
    log::info!("Redis 连接成功 (数据库: {})", config.base.redis_database);
    
    // 加载 Lua 脚本
    let lua_script_sha = load_lua_script(&mut redis_conn).await?;
    log::info!("Lua 脚本已加载，准备开始采集");

    // 解析 Binance 配置
    let product_type = match config.base.binance_product_type.as_str() {
        "UsdM" => BinanceProductType::UsdM,
        "CoinM" => BinanceProductType::CoinM,
        _ => {
            log::warn!("未知的产品类型: {}，使用默认值 UsdM", config.base.binance_product_type);
            BinanceProductType::UsdM
        }
    };

    let environment = match config.base.binance_environment.as_str() {
        "Mainnet" => BinanceEnvironment::Mainnet,
        "Testnet" => BinanceEnvironment::Testnet,
        _ => {
            log::warn!("未知的环境: {}，使用默认值 Mainnet", config.base.binance_environment);
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

    // 构建所有流名称（订阅 depth@500ms）
    let mut streams = Vec::new();
    for instrument in &instruments {
        let symbol_lower = format_binance_stream_symbol(&instrument.id());
        let stream = format!("{}@depth@500ms", symbol_lower);
        streams.push(stream);
    }

    // Binance 限制：每个连接最多 200 个流
    const MAX_STREAMS_PER_CONNECTION: usize = 200;
    let total_connections = (streams.len() + MAX_STREAMS_PER_CONNECTION - 1) / MAX_STREAMS_PER_CONNECTION;
    
    log::info!("订阅 {} 个流（需要 {} 个连接，每个最多 {} 个流）", 
        streams.len(), total_connections, MAX_STREAMS_PER_CONNECTION);
    
    // 创建多个 WebSocket 客户端
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
        
        client.cache_instruments(instruments.clone());
        
        log::info!("连接 Binance Futures WebSocket (连接 {})...", conn_idx + 1);
        client.connect().await?;
        log::info!("WebSocket 连接成功 (连接 {})", conn_idx + 1);
        
        sleep(Duration::from_millis(500)).await;
        
        client.subscribe(chunk.to_vec()).await?;
        log::info!("连接 {} 订阅成功，已订阅 {} 个流", conn_idx + 1, chunk.len());
        
        clients.push(client);
        
        if conn_idx < total_connections - 1 {
            sleep(Duration::from_millis(200)).await;
        }
    }
    
    log::info!("所有连接创建成功，开始采集订单簿数据");

    // 使用 channel 来合并所有流的消息
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tokio::sync::mpsc;
    
    let (tx, mut rx) = mpsc::unbounded_channel::<NautilusFuturesWsMessage>();
    let lua_script_sha = Arc::new(Mutex::new(lua_script_sha));
    
    // 维护每个交易对的订单簿状态
    let order_books: Arc<Mutex<std::collections::HashMap<String, OrderBookState>>> = 
        Arc::new(Mutex::new(std::collections::HashMap::new()));
    
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
    drop(tx);

    // 启动定时任务：按配置的间隔生成快照
    let order_books_snapshot = order_books.clone();
    let redis_conn_snapshot = Arc::new(Mutex::new(redis_conn));
    let lua_script_sha_snapshot = lua_script_sha.clone();
    let config_snapshot = config.clone();
    
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(config_snapshot.snapshot_interval_ms));
        loop {
            interval.tick().await;
            
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            
            let books_guard = order_books_snapshot.lock().await;
            let books = books_guard.clone();
            drop(books_guard);
            
            for (symbol, book_state) in books {
                let snapshot_json = book_state.to_snapshot_json(&symbol, now, now, config_snapshot.order_book_depth);
                let snapshot_str = match serde_json::to_string(&snapshot_json) {
                    Ok(s) => s,
                    Err(e) => {
                        log::warn!("序列化订单簿快照失败 [{}]: {}", symbol, e);
                        continue;
                    }
                };
                
                let base_key = config_snapshot.redis_key_prefix.to_uppercase().replace(":", "_");
                let stream_key = format!("{}:{}", base_key, symbol);
                let dedup_key = format!("{}:dedup:{}:{}", config_snapshot.redis_key_prefix, symbol, now / 500_000_000); // 500ms 粒度去重
                
                let sha_guard = lua_script_sha_snapshot.lock().await;
                let current_sha = sha_guard.clone();
                drop(sha_guard);
                
                let mut conn = redis_conn_snapshot.lock().await;
                let _result: i64 = match cmd("EVALSHA")
                    .arg(&current_sha)
                    .arg(2)
                    .arg(&stream_key)
                    .arg(&dedup_key)
                    .arg(&snapshot_str)
                    .arg(config_snapshot.max_snapshots_per_symbol)
                    .arg(DEDUP_KEY_TTL)
                    .query_async(&mut *conn)
                    .await
                {
                    Ok(result) => result,
                    Err(e) => {
                        if e.to_string().contains("NOSCRIPT") {
                            log::warn!("Lua 脚本未找到，重新加载...");
                            match load_lua_script(&mut *conn).await {
                                Ok(new_sha) => {
                                    *lua_script_sha_snapshot.lock().await = new_sha.clone();
                                    cmd("EVALSHA")
                                        .arg(&new_sha)
                                        .arg(2)
                                        .arg(&stream_key)
                                        .arg(&dedup_key)
                                        .arg(&snapshot_str)
                                        .arg(config_snapshot.max_snapshots_per_symbol)
                                        .arg(DEDUP_KEY_TTL)
                                        .query_async(&mut *conn)
                                        .await
                                        .unwrap_or(0)
                                }
                                Err(_) => 0,
                            }
                        } else {
                            log::warn!("Lua 脚本执行失败 [{}]: {}", symbol, e);
                            0
                        }
                    }
                };
            }
        }
    });

    // 持续运行，处理订单簿增量更新
    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(NautilusFuturesWsMessage::Deltas(deltas)) => {
                        let instrument_id = deltas.instrument_id;
                        let symbol_raw = instrument_id.symbol.as_str();
                        let symbol = symbol_raw.strip_suffix("-PERP").unwrap_or(symbol_raw);
                        
                        let mut books_guard = order_books.lock().await;
                        let book_state = books_guard
                            .entry(symbol.to_string())
                            .or_insert_with(OrderBookState::new);
                        book_state.apply_deltas(&deltas);
                        book_state.trim_to_depth(config.order_book_depth);
                    }
                    Some(NautilusFuturesWsMessage::Reconnected) => {
                        log::info!("WebSocket 已重新连接");
                    }
                    Some(NautilusFuturesWsMessage::Error(err)) => {
                        log::error!("WebSocket 错误 - 代码: {}, 消息: {}", err.code, err.msg);
                    }
                    Some(NautilusFuturesWsMessage::Data(_)) => {
                        // 忽略其他数据
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
    log::info!("服务已停止");

    Ok(())
}


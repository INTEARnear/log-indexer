use async_trait::async_trait;
use inevents_redis::RedisEventStream;
use intear_events::events::log::{
    log_nep297::{LogNep297Event, LogNep297EventData},
    log_text::{LogTextEvent, LogTextEventData},
};
use redis::aio::ConnectionManager;

use crate::LogEventHandler;

pub struct PushToRedisStream {
    text_stream: RedisEventStream<LogTextEventData>,
    nep297_stream: RedisEventStream<LogNep297EventData>,
    max_stream_size: usize,
}

impl PushToRedisStream {
    pub async fn new(connection: ConnectionManager, max_stream_size: usize, testnet: bool) -> Self {
        Self {
            text_stream: RedisEventStream::new(
                connection.clone(),
                if testnet {
                    format!("{}_testnet", LogTextEvent::ID)
                } else {
                    LogTextEvent::ID.to_string()
                },
            ),
            nep297_stream: RedisEventStream::new(
                connection.clone(),
                if testnet {
                    format!("{}_testnet", LogNep297Event::ID)
                } else {
                    LogNep297Event::ID.to_string()
                },
            ),
            max_stream_size,
        }
    }
}

#[async_trait]
impl LogEventHandler for PushToRedisStream {
    async fn handle_text(&mut self, event: LogTextEventData) {
        self.text_stream
            .emit_event(event.block_height, event, self.max_stream_size)
            .expect("Failed to emit text event");
    }

    async fn handle_nep297(&mut self, event: LogNep297EventData) {
        self.nep297_stream
            .emit_event(event.block_height, event, self.max_stream_size)
            .expect("Failed to emit nep297 event");
    }
}

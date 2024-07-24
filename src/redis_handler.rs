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
    pub async fn new(connection: ConnectionManager, max_stream_size: usize) -> Self {
        Self {
            text_stream: RedisEventStream::new(connection.clone(), LogTextEvent::ID),
            nep297_stream: RedisEventStream::new(connection.clone(), LogNep297Event::ID),
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
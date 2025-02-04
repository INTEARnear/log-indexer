use async_trait::async_trait;
use inevents_redis::RedisEventStream;
use inindexer::near_indexer_primitives::types::BlockHeight;
use intear_events::events::log::{log_nep297::LogNep297Event, log_text::LogTextEvent};
use redis::aio::ConnectionManager;

use crate::LogEventHandler;

pub struct PushToRedisStream {
    text_stream: RedisEventStream<LogTextEvent>,
    nep297_stream: RedisEventStream<LogNep297Event>,
    max_stream_size: usize,
}

impl PushToRedisStream {
    pub async fn new(connection: ConnectionManager, max_stream_size: usize) -> Self {
        Self {
            text_stream: RedisEventStream::new(connection.clone(), LogTextEvent::ID),
            nep297_stream: RedisEventStream::new(
                connection.clone(),
                LogNep297Event::ID.to_string(),
            ),
            max_stream_size,
        }
    }
}

#[async_trait]
impl LogEventHandler for PushToRedisStream {
    async fn handle_text(&mut self, event: LogTextEvent) {
        self.text_stream.add_event(event);
    }

    async fn handle_nep297(&mut self, event: LogNep297Event) {
        self.nep297_stream.add_event(event);
    }

    async fn flush_events(&mut self, block_height: BlockHeight) {
        self.text_stream
            .flush_events(block_height, self.max_stream_size)
            .await
            .expect("Failed to flush text stream");
        self.nep297_stream
            .flush_events(block_height, self.max_stream_size)
            .await
            .expect("Failed to flush nep297 stream");
    }
}

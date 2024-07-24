pub mod redis_handler;

use async_trait::async_trait;
use inindexer::near_indexer_primitives::StreamerMessage;
use inindexer::near_utils::EventLogData;
use inindexer::{IncompleteTransaction, Indexer, TransactionReceipt};
use intear_events::events::log::log_nep297::LogNep297EventData;
use intear_events::events::log::log_text::LogTextEventData;

#[async_trait]
pub trait LogEventHandler: Send + Sync {
    async fn handle_text(&mut self, event: LogTextEventData);
    async fn handle_nep297(&mut self, event: LogNep297EventData);
}

pub struct LogIndexer<T: LogEventHandler + Send + Sync + 'static>(pub T);

#[async_trait]
impl<T: LogEventHandler + Send + Sync + 'static> Indexer for LogIndexer<T> {
    type Error = String;

    async fn on_receipt(
        &mut self,
        receipt: &TransactionReceipt,
        transaction: &IncompleteTransaction,
        _block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        if !receipt.is_successful(false) {
            return Ok(());
        }
        for log in receipt.receipt.execution_outcome.outcome.logs.iter() {
            let text_event = LogTextEventData {
                block_height: receipt.block_height,
                block_timestamp_nanosec: receipt.block_timestamp_nanosec,
                transaction_id: transaction.transaction.transaction.hash,
                receipt_id: receipt.receipt.receipt.receipt_id,

                account_id: receipt.receipt.receipt.receiver_id.clone(),
                predecessor_id: receipt.receipt.receipt.predecessor_id.clone(),

                log_text: log.clone(),
            };
            self.0.handle_text(text_event).await;
            if let Ok(event) = EventLogData::deserialize(log) {
                let nep297_event = LogNep297EventData {
                    block_height: receipt.block_height,
                    block_timestamp_nanosec: receipt.block_timestamp_nanosec,
                    transaction_id: transaction.transaction.transaction.hash,
                    receipt_id: receipt.receipt.receipt.receipt_id,

                    account_id: receipt.receipt.receipt.receiver_id.clone(),
                    predecessor_id: receipt.receipt.receipt.predecessor_id.clone(),

                    event_standard: event.standard,
                    event_version: event.version,
                    event_event: event.event,
                    event_data: event.data,
                };
                self.0.handle_nep297(nep297_event).await;
            }
        }
        Ok(())
    }
}

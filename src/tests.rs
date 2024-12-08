use std::collections::HashMap;

use async_trait::async_trait;
use inindexer::{
    near_indexer_primitives::types::AccountId, near_indexer_primitives::types::BlockHeight,
    neardata::NeardataProvider, run_indexer, BlockIterator, IndexerOptions,
    PreprocessTransactionsSettings,
};
use intear_events::events::log::{log_nep297::LogNep297Event, log_text::LogTextEvent};
use log_indexer::{LogEventHandler, LogIndexer};

#[derive(Default)]
struct TestIndexer {
    text_logs: HashMap<AccountId, Vec<LogTextEvent>>,
    nep297_logs: HashMap<AccountId, Vec<LogNep297Event>>,
}

#[async_trait]
impl LogEventHandler for TestIndexer {
    async fn handle_text(&mut self, event: LogTextEvent) {
        self.text_logs
            .entry(event.account_id.clone())
            .or_default()
            .push(event);
    }

    async fn handle_nep297(&mut self, event: LogNep297Event) {
        self.nep297_logs
            .entry(event.account_id.clone())
            .or_default()
            .push(event);
    }

    async fn flush_events(&mut self, _block_height: BlockHeight) {}
}

#[tokio::test]
async fn handles_nep297_events() {
    let mut indexer = LogIndexer(TestIndexer::default());

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(124099140..=124099142),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(
        indexer
            .0
            .text_logs
            .get(&"sbt.honeybot.near".parse::<AccountId>().unwrap())
            .unwrap()
            .iter()
            .map(|event| event.log_text.clone())
            .collect::<Vec<_>>(),
        vec!["EVENT_JSON:{\"standard\":\"nep171\",\"version\":\"1.2.0\",\"event\":\"nft_mint\",\"data\":[{\"owner_id\":\"thickpuma7542.near\",\"token_ids\":[\"thickpuma7542.near\"],\"memo\":\"\"}]}".to_string()]
    );
    assert_eq!(
        indexer
            .0
            .nep297_logs
            .get(&"sbt.honeybot.near".parse::<AccountId>().unwrap())
            .unwrap()
            .iter()
            .map(|event| (
                event.event_standard.clone(),
                event.event_version.clone(),
                event.event_event.clone(),
                event.event_data.clone()
            ))
            .collect::<Vec<_>>(),
        vec![(
            "nep171".to_string(),
            "1.2.0".to_string(),
            "nft_mint".to_string(),
            Some(
                serde_json::json!([{"owner_id":"thickpuma7542.near","token_ids":["thickpuma7542.near"],"memo":""}])
            )
        )]
    )
}

#[tokio::test]
async fn handles_text_events() {
    let mut indexer = LogIndexer(TestIndexer::default());

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(124105249..=124105254),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(
        indexer
            .0
            .text_logs
            .get(&"intel.tkn.near".parse::<AccountId>().unwrap())
            .unwrap()
            .iter()
            .map(|event| event.log_text.clone())
            .collect::<Vec<_>>(),
        vec!["Transfer 107135422747761180476 from shit.0xshitzu.near to kahbib.tg".to_string()]
    );
    assert!(!indexer
        .0
        .nep297_logs
        .contains_key(&"intel.tkn.near".parse::<AccountId>().unwrap()))
}

use anyhow::Context;
use pathfinder_common::{BlockHash, BlockId, GasPrice, TransactionHash};
use pathfinder_executor::CallError;
use primitive_types::U256;
use serde::{Deserialize, Serialize};

use crate::{compose_executor_transaction, context::RpcContext, executor::ExecutionStateError};

use super::simulate_transactions::dto::TransactionTrace;

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TraceBlockTransactionsInput {
    block_id: BlockId,
}

#[derive(Debug, Serialize, Eq, PartialEq)]
pub struct Trace {
    pub transaction_hash: TransactionHash,
    pub trace_root: TransactionTrace,
}

#[derive(Debug, Serialize, Eq, PartialEq)]
pub struct TraceBlockTransactionsOutput(pub Vec<Trace>);

#[derive(Debug)]
pub enum TraceBlockTransactionsError {
    Internal(anyhow::Error),
    Custom(anyhow::Error),
    BlockNotFound,
    ContractErrorV05 { revert_error: String },
}

impl From<anyhow::Error> for TraceBlockTransactionsError {
    fn from(value: anyhow::Error) -> Self {
        Self::Internal(value)
    }
}

impl From<TraceBlockTransactionsError> for crate::error::ApplicationError {
    fn from(value: TraceBlockTransactionsError) -> Self {
        match value {
            TraceBlockTransactionsError::Internal(e) => Self::Internal(e, ()),
            TraceBlockTransactionsError::BlockNotFound => Self::BlockNotFound,
            TraceBlockTransactionsError::ContractErrorV05 { revert_error } => {
                Self::ContractErrorV05 { revert_error }
            }
            TraceBlockTransactionsError::Custom(e) => Self::Custom(e),
        }
    }
}

impl From<ExecutionStateError> for TraceBlockTransactionsError {
    fn from(value: ExecutionStateError) -> Self {
        match value {
            ExecutionStateError::BlockNotFound => Self::BlockNotFound,
            ExecutionStateError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<CallError> for TraceBlockTransactionsError {
    fn from(value: CallError) -> Self {
        match value {
            CallError::ContractNotFound => Self::Custom(anyhow::anyhow!("Contract not found")),
            CallError::InvalidMessageSelector => {
                Self::Custom(anyhow::anyhow!("Invalid message selector"))
            }
            CallError::Reverted(revert_error) => Self::ContractErrorV05 { revert_error },
            CallError::Internal(e) => Self::Internal(e),
            CallError::Custom(e) => Self::Custom(e),
        }
    }
}

pub async fn trace_block_transactions(
    context: RpcContext,
    input: TraceBlockTransactionsInput,
) -> Result<TraceBlockTransactionsOutput, TraceBlockTransactionsError> {
    let (transactions, gas_price, parent_block_hash) =
        fetch_transactions(context.clone(), input.block_id).await?;

    let parent_block_id = pathfinder_common::BlockId::Hash(parent_block_hash);
    let execution_state =
        crate::executor::execution_state(context, parent_block_id, Some(U256::from(gas_price.0)))
            .await?;

    let span = tracing::Span::current();
    let traces = tokio::task::spawn_blocking(move || {
        let _g = span.enter();
        pathfinder_executor::trace_all(execution_state, transactions, true, true)
    })
    .await
    .context("trace_block_transactions: execution")??;

    let result = traces
        .into_iter()
        .map(|(hash, trace)| Trace {
            transaction_hash: hash,
            trace_root: trace.into(),
        })
        .collect();

    Ok(TraceBlockTransactionsOutput(result))
}

async fn fetch_transactions(
    context: RpcContext,
    block_id: BlockId,
) -> Result<(Vec<pathfinder_executor::Transaction>, GasPrice, BlockHash), TraceBlockTransactionsError>
{
    let storage = context.storage.clone();
    let span = tracing::Span::current();
    tokio::task::spawn_blocking(move || {
        let _g = span.enter();
        let mut db = storage.connection()?;
        let tx = db.transaction()?;

        if block_id.is_pending() {
            let pending_data = context
                .pending_data
                .get(&tx)
                .context("Querying pending data")?;

            let transactions = pending_data
                .block
                .transactions
                .iter()
                .map(|transaction| compose_executor_transaction(transaction.clone(), &tx))
                .collect::<Result<Vec<_>, _>>()?;

            return Ok((
                transactions,
                pending_data.block.gas_price,
                pending_data.block.parent_hash,
            ));
        }

        let block_id = block_id.try_into().expect("Only pending cast should fail");
        fetch_block_transactions(&tx, block_id)
    })
    .await
    .context("Fetching transactions")?
}

pub(super) fn fetch_block_transactions(
    tx: &pathfinder_storage::Transaction<'_>,
    block_id: pathfinder_storage::BlockId,
) -> Result<(Vec<pathfinder_executor::Transaction>, GasPrice, BlockHash), TraceBlockTransactionsError>
{
    let header = tx
        .block_header(block_id)?
        .ok_or(TraceBlockTransactionsError::BlockNotFound)?;

    let (transactions, _): (Vec<_>, Vec<_>) = tx
        .transaction_data_for_block(block_id)?
        .ok_or(TraceBlockTransactionsError::BlockNotFound)?
        .into_iter()
        .unzip();

    let transactions = transactions
        .into_iter()
        .map(|transaction| compose_executor_transaction(transaction, tx))
        .collect::<anyhow::Result<Vec<_>, _>>()?;

    Ok::<_, TraceBlockTransactionsError>((transactions, header.gas_price, header.parent_hash))
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use pathfinder_common::{
        felt, BlockHeader, ChainId, GasPrice, SierraHash, StateUpdate, TransactionIndex,
    };
    use starknet_gateway_types::reply::transaction::{ExecutionStatus, Receipt};

    use super::*;

    pub(crate) async fn setup_multi_tx_trace_test(
    ) -> anyhow::Result<(RpcContext, BlockHeader, Vec<Trace>)> {
        use super::super::simulate_transactions::tests::fixtures;
        use super::super::simulate_transactions::tests::setup_storage;

        let (
            storage,
            last_block_header,
            account_contract_address,
            universal_deployer_address,
            test_storage_value,
        ) = setup_storage().await;
        let context = RpcContext::for_tests().with_storage(storage.clone());

        let transactions = vec![
            fixtures::input::declare(account_contract_address),
            fixtures::input::universal_deployer(
                account_contract_address,
                universal_deployer_address,
            ),
            fixtures::input::invoke(account_contract_address),
        ];

        let traces = vec![
            fixtures::expected_output::declare(account_contract_address, &last_block_header)
                .transaction_trace,
            fixtures::expected_output::universal_deployer(
                account_contract_address,
                &last_block_header,
                universal_deployer_address,
            )
            .transaction_trace,
            fixtures::expected_output::invoke(
                account_contract_address,
                &last_block_header,
                test_storage_value,
            )
            .transaction_trace,
        ];

        let next_block_header = {
            let mut db = storage.connection()?;
            let tx = db.transaction()?;

            tx.insert_sierra_class(
                &SierraHash(fixtures::SIERRA_HASH.0),
                fixtures::SIERRA_DEFINITION,
                &fixtures::CASM_HASH,
                fixtures::CASM_DEFINITION,
                "compiler version",
            )?;

            let next_block_header = BlockHeader::builder()
                .with_number(last_block_header.number + 1)
                .with_gas_price(GasPrice(1))
                .with_parent_hash(last_block_header.hash)
                .finalize_with_hash(BlockHash(felt!("0x1")));
            tx.insert_block_header(&next_block_header)?;

            let dummy_receipt: Receipt = Receipt {
                actual_fee: None,
                events: vec![],
                execution_resources: None,
                l1_to_l2_consumed_message: None,
                l2_to_l1_messages: vec![],
                transaction_hash: TransactionHash(felt!("0x1")),
                transaction_index: TransactionIndex::new_or_panic(0),
                execution_status: ExecutionStatus::default(),
                revert_error: None,
            };
            tx.insert_transaction_data(
                next_block_header.hash,
                next_block_header.number,
                &[
                    (transactions[0].clone().into(), dummy_receipt.clone()),
                    (transactions[1].clone().into(), dummy_receipt.clone()),
                    (transactions[2].clone().into(), dummy_receipt.clone()),
                ],
            )?;
            tx.commit()?;

            next_block_header
        };

        let traces = vec![
            Trace {
                transaction_hash: transactions[0]
                    .transaction_hash(ChainId::TESTNET, Some(fixtures::SIERRA_HASH)),
                trace_root: traces[0].clone(),
            },
            Trace {
                transaction_hash: transactions[1].transaction_hash(ChainId::TESTNET, None),
                trace_root: traces[1].clone(),
            },
            Trace {
                transaction_hash: transactions[2].transaction_hash(ChainId::TESTNET, None),
                trace_root: traces[2].clone(),
            },
        ];

        Ok((context, next_block_header, traces))
    }

    #[tokio::test]
    async fn test_multiple_transactions() -> anyhow::Result<()> {
        let (context, next_block_header, traces) = setup_multi_tx_trace_test().await?;

        let input = TraceBlockTransactionsInput {
            block_id: next_block_header.hash.into(),
        };
        let output = trace_block_transactions(context, input).await.unwrap();
        let expected = TraceBlockTransactionsOutput(traces);

        pretty_assertions::assert_eq!(output, expected);
        Ok(())
    }

    pub(crate) async fn setup_multi_tx_trace_pending_test(
    ) -> anyhow::Result<(RpcContext, Vec<Trace>)> {
        use super::super::simulate_transactions::tests::fixtures;
        use super::super::simulate_transactions::tests::setup_storage;

        let (
            storage,
            last_block_header,
            account_contract_address,
            universal_deployer_address,
            test_storage_value,
        ) = setup_storage().await;
        let context = RpcContext::for_tests().with_storage(storage.clone());

        let transactions = vec![
            fixtures::input::declare(account_contract_address),
            fixtures::input::universal_deployer(
                account_contract_address,
                universal_deployer_address,
            ),
            fixtures::input::invoke(account_contract_address),
        ];

        let traces = vec![
            fixtures::expected_output::declare(account_contract_address, &last_block_header)
                .transaction_trace,
            fixtures::expected_output::universal_deployer(
                account_contract_address,
                &last_block_header,
                universal_deployer_address,
            )
            .transaction_trace,
            fixtures::expected_output::invoke(
                account_contract_address,
                &last_block_header,
                test_storage_value,
            )
            .transaction_trace,
        ];

        let pending_block = {
            let mut db = storage.connection()?;
            let tx = db.transaction()?;

            tx.insert_sierra_class(
                &SierraHash(fixtures::SIERRA_HASH.0),
                fixtures::SIERRA_DEFINITION,
                &fixtures::CASM_HASH,
                fixtures::CASM_DEFINITION,
                "compiler version",
            )?;

            let dummy_receipt: Receipt = Receipt {
                actual_fee: None,
                events: vec![],
                execution_resources: None,
                l1_to_l2_consumed_message: None,
                l2_to_l1_messages: vec![],
                transaction_hash: TransactionHash(felt!("0x1")),
                transaction_index: TransactionIndex::new_or_panic(0),
                execution_status: ExecutionStatus::default(),
                revert_error: None,
            };

            let transaction_receipts =
                vec![dummy_receipt.clone(), dummy_receipt.clone(), dummy_receipt];

            let pending_block = starknet_gateway_types::reply::PendingBlock {
                gas_price: GasPrice(1),
                parent_hash: last_block_header.hash,
                sequencer_address: last_block_header.sequencer_address,
                status: starknet_gateway_types::reply::Status::Pending,
                timestamp: last_block_header.timestamp,
                transaction_receipts,
                transactions: transactions.iter().cloned().map(Into::into).collect(),
                starknet_version: last_block_header.starknet_version,
            };

            tx.commit()?;

            pending_block
        };

        let pending_data = crate::pending::PendingData {
            block: pending_block,
            state_update: StateUpdate::default(),
            number: last_block_header.number + 1,
        };

        let (tx, rx) = tokio::sync::watch::channel(Default::default());
        tx.send(Arc::new(pending_data)).unwrap();

        let context = context.with_pending_data(rx);

        let traces = vec![
            Trace {
                transaction_hash: transactions[0]
                    .transaction_hash(ChainId::TESTNET, Some(fixtures::SIERRA_HASH)),
                trace_root: traces[0].clone(),
            },
            Trace {
                transaction_hash: transactions[1].transaction_hash(ChainId::TESTNET, None),
                trace_root: traces[1].clone(),
            },
            Trace {
                transaction_hash: transactions[2].transaction_hash(ChainId::TESTNET, None),
                trace_root: traces[2].clone(),
            },
        ];

        Ok((context, traces))
    }

    #[tokio::test]
    async fn test_multiple_pending_transactions() -> anyhow::Result<()> {
        let (context, traces) = setup_multi_tx_trace_pending_test().await?;

        let input = TraceBlockTransactionsInput {
            block_id: BlockId::Pending,
        };
        let output = trace_block_transactions(context, input).await.unwrap();
        let expected = TraceBlockTransactionsOutput(traces);

        pretty_assertions::assert_eq!(output, expected);
        Ok(())
    }
}

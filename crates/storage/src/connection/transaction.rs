//! Contains starknet transaction related code and __not__ database transaction.

use anyhow::Context;
use pathfinder_common::{BlockHash, BlockNumber, TransactionHash};
use starknet_gateway_types::reply::transaction as gateway;

use crate::{prelude::*, BlockId};

pub enum TransactionStatus {
    L1Accepted,
    L2Accepted,
}

pub(super) fn insert_transactions(
    tx: &Transaction<'_>,
    block_hash: BlockHash,
    block_number: BlockNumber,
    transaction_data: &[(gateway::Transaction, gateway::Receipt)],
) -> anyhow::Result<()> {
    if transaction_data.is_empty() {
        return Ok(());
    }

    let mut compressor = zstd::bulk::Compressor::new(10).context("Create zstd compressor")?;
    for (i, (transaction, receipt)) in transaction_data.iter().enumerate() {
        // Serialize and compress transaction data.
        let tx_data = serde_json::to_vec(&transaction).context("Serializing transaction")?;
        let tx_data = compressor
            .compress(&tx_data)
            .context("Compressing transaction")?;

        let serialized_receipt = serde_json::to_vec(&receipt).context("Serializing receipt")?;
        let serialized_receipt = compressor
            .compress(&serialized_receipt)
            .context("Compressing receipt")?;

        let execution_status = match receipt.execution_status {
            gateway::ExecutionStatus::Succeeded => 0,
            gateway::ExecutionStatus::Reverted => 1,
        };

        tx.inner().execute(r"INSERT OR REPLACE INTO starknet_transactions (hash,  idx,  block_hash,  tx,  receipt,  execution_status) 
                                                                  VALUES (:hash, :idx, :block_hash, :tx, :receipt, :execution_status)",
            named_params![
            ":hash": &transaction.hash(),
            ":idx": &i.try_into_sql_int()?,
            ":block_hash": &block_hash,
            ":tx": &tx_data,
            ":receipt": &serialized_receipt,
            ":execution_status": &execution_status,
        ]).context("Inserting transaction data")?;

        // insert events from receipt
        super::event::insert_events(tx, block_number, receipt.transaction_hash, &receipt.events)
            .context("Inserting events")?;
    }

    Ok(())
}

pub(super) fn transaction(
    tx: &Transaction<'_>,
    transaction: TransactionHash,
) -> anyhow::Result<Option<gateway::Transaction>> {
    let mut stmt = tx
        .inner()
        .prepare("SELECT tx FROM starknet_transactions WHERE hash = ?")
        .context("Preparing statement")?;

    let mut rows = stmt
        .query(params![&transaction])
        .context("Executing query")?;

    let row = match rows.next()? {
        Some(row) => row,
        None => return Ok(None),
    };

    let transaction = row.get_ref_unwrap(0).as_blob()?;
    let transaction = zstd::decode_all(transaction).context("Decompressing transaction")?;
    let transaction = serde_json::from_slice(&transaction).context("Deserializing transaction")?;

    Ok(Some(transaction))
}

pub(super) fn transaction_with_receipt(
    tx: &Transaction<'_>,
    txn_hash: TransactionHash,
) -> anyhow::Result<Option<(gateway::Transaction, gateway::Receipt, BlockHash)>> {
    let mut stmt = tx
        .inner()
        .prepare("SELECT tx, receipt, block_hash FROM starknet_transactions WHERE hash = ?1")
        .context("Preparing statement")?;

    let mut rows = stmt.query(params![&txn_hash]).context("Executing query")?;

    let row = match rows.next()? {
        Some(row) => row,
        None => return Ok(None),
    };

    let transaction = row.get_ref_unwrap("tx").as_blob()?;
    let transaction = zstd::decode_all(transaction).context("Decompressing transaction")?;
    let transaction = serde_json::from_slice(&transaction).context("Deserializing transaction")?;

    let receipt = match row.get_ref_unwrap("receipt").as_blob_or_null()? {
        Some(data) => data,
        None => return Ok(None),
    };
    let receipt = zstd::decode_all(receipt).context("Decompressing receipt")?;
    let receipt = serde_json::from_slice(&receipt).context("Deserializing receipt")?;

    let block_hash = row.get_block_hash("block_hash")?;

    Ok(Some((transaction, receipt, block_hash)))
}

pub(super) fn transaction_at_block(
    tx: &Transaction<'_>,
    block: BlockId,
    index: usize,
) -> anyhow::Result<Option<gateway::Transaction>> {
    // Identify block hash
    let Some((_, block_hash)) = tx.block_id(block)? else {
        return Ok(None);
    };

    let mut stmt = tx
        .inner()
        .prepare("SELECT tx FROM starknet_transactions WHERE block_hash = ? AND idx = ?")
        .context("Preparing statement")?;

    let mut rows = stmt
        .query(params![&block_hash, &index.try_into_sql_int()?])
        .context("Executing query")?;

    let row = match rows.next()? {
        Some(row) => row,
        None => return Ok(None),
    };

    let transaction = match row.get_ref_unwrap(0).as_blob_or_null()? {
        Some(data) => data,
        None => return Ok(None),
    };

    let transaction = zstd::decode_all(transaction).context("Decompressing transaction")?;
    let transaction = serde_json::from_slice(&transaction).context("Deserializing transaction")?;

    Ok(Some(transaction))
}

pub(super) fn transaction_count(tx: &Transaction<'_>, block: BlockId) -> anyhow::Result<usize> {
    match block {
        BlockId::Number(number) => tx
            .inner()
            .query_row(
                "SELECT COUNT(*) FROM starknet_transactions
                JOIN block_headers ON starknet_transactions.block_hash = block_headers.hash
                WHERE number = ?1",
                params![&number],
                |row| row.get(0),
            )
            .context("Counting transactions"),
        BlockId::Hash(hash) => tx
            .inner()
            .query_row(
                "SELECT COUNT(*) FROM starknet_transactions WHERE block_hash = ?1",
                params![&hash],
                |row| row.get(0),
            )
            .context("Counting transactions"),
        BlockId::Latest => {
            // First get the latest block
            let block = match tx.block_id(BlockId::Latest)? {
                Some((number, _)) => number,
                None => return Ok(0),
            };

            transaction_count(tx, block.into())
        }
    }
}

pub(super) fn transaction_data_for_block(
    tx: &Transaction<'_>,
    block: BlockId,
) -> anyhow::Result<Option<Vec<(gateway::Transaction, gateway::Receipt)>>> {
    let Some((_, block_hash)) = tx.block_id(block)? else {
        return Ok(None);
    };

    let mut stmt = tx
        .inner()
        .prepare(
            "SELECT tx, receipt FROM starknet_transactions WHERE block_hash = ? ORDER BY idx ASC",
        )
        .context("Preparing statement")?;

    let mut rows = stmt
        .query(params![&block_hash])
        .context("Executing query")?;

    let mut data = Vec::new();
    while let Some(row) = rows.next()? {
        let receipt = row
            .get_ref_unwrap("receipt")
            .as_blob_or_null()?
            .context("Receipt data missing")?;
        let receipt = zstd::decode_all(receipt).context("Decompressing transaction receipt")?;
        let receipt =
            serde_json::from_slice(&receipt).context("Deserializing transaction receipt")?;

        let transaction = row
            .get_ref_unwrap("tx")
            .as_blob_or_null()?
            .context("Transaction data missing")?;
        let transaction = zstd::decode_all(transaction).context("Decompressing transaction")?;
        let transaction =
            serde_json::from_slice(&transaction).context("Deserializing transaction")?;

        data.push((transaction, receipt));
    }

    Ok(Some(data))
}

pub(super) fn transaction_block_hash(
    tx: &Transaction<'_>,
    hash: TransactionHash,
) -> anyhow::Result<Option<BlockHash>> {
    tx.inner()
        .query_row(
            "SELECT block_hash FROM starknet_transactions WHERE hash = ?",
            params![&hash],
            |row| row.get_block_hash(0),
        )
        .optional()
        .map_err(|e| e.into())
}

#[cfg(test)]
mod tests {
    use crate::JournalMode;
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::{BlockHeader, TransactionIndex, TransactionVersion};
    use starknet_gateway_types::reply::transaction::{
        DeclareTransactionV0V1, DeclareTransactionV2, DeployAccountTransaction, DeployTransaction,
        InvokeTransactionV0, InvokeTransactionV1,
    };
    use std::num::NonZeroU32;
    use std::path::Path;
    use std::time::{Duration, Instant};

    use super::*;

    fn setup() -> (
        crate::Connection,
        BlockHeader,
        Vec<(gateway::Transaction, gateway::Receipt)>,
    ) {
        let header = BlockHeader::builder().finalize_with_hash(block_hash_bytes!(b"block hash"));

        // Create one of each transaction type.
        let transactions = vec![
            gateway::Transaction::Declare(gateway::DeclareTransaction::V0(
                DeclareTransactionV0V1 {
                    class_hash: class_hash_bytes!(b"declare v0 class hash"),
                    max_fee: fee_bytes!(b"declare v0 max fee"),
                    nonce: transaction_nonce_bytes!(b"declare v0 tx nonce"),
                    sender_address: contract_address_bytes!(b"declare v0 contract address"),
                    signature: vec![
                        transaction_signature_elem_bytes!(b"declare v0 tx sig 0"),
                        transaction_signature_elem_bytes!(b"declare v0 tx sig 1"),
                    ],
                    transaction_hash: transaction_hash_bytes!(b"declare v0 tx hash"),
                },
            )),
            gateway::Transaction::Declare(gateway::DeclareTransaction::V1(
                DeclareTransactionV0V1 {
                    class_hash: class_hash_bytes!(b"declare v1 class hash"),
                    max_fee: fee_bytes!(b"declare v1 max fee"),
                    nonce: transaction_nonce_bytes!(b"declare v1 tx nonce"),
                    sender_address: contract_address_bytes!(b"declare v1 contract address"),
                    signature: vec![
                        transaction_signature_elem_bytes!(b"declare v1 tx sig 0"),
                        transaction_signature_elem_bytes!(b"declare v1 tx sig 1"),
                    ],
                    transaction_hash: transaction_hash_bytes!(b"declare v1 tx hash"),
                },
            )),
            gateway::Transaction::Declare(gateway::DeclareTransaction::V2(DeclareTransactionV2 {
                class_hash: class_hash_bytes!(b"declare v2 class hash"),
                max_fee: fee_bytes!(b"declare v2 max fee"),
                nonce: transaction_nonce_bytes!(b"declare v2 tx nonce"),
                sender_address: contract_address_bytes!(b"declare v2 contract address"),
                signature: vec![
                    transaction_signature_elem_bytes!(b"declare v2 tx sig 0"),
                    transaction_signature_elem_bytes!(b"declare v2 tx sig 1"),
                ],
                transaction_hash: transaction_hash_bytes!(b"declare v2 tx hash"),
                compiled_class_hash: casm_hash_bytes!(b"declare v2 casm hash"),
            })),
            gateway::Transaction::Deploy(DeployTransaction {
                contract_address: contract_address_bytes!(b"deploy contract address"),
                contract_address_salt: contract_address_salt_bytes!(
                    b"deploy contract address salt"
                ),
                class_hash: class_hash_bytes!(b"deploy class hash"),
                constructor_calldata: vec![
                    constructor_param_bytes!(b"deploy call data 0"),
                    constructor_param_bytes!(b"deploy call data 1"),
                ],
                transaction_hash: transaction_hash_bytes!(b"deploy tx hash"),
                version: TransactionVersion::ZERO,
            }),
            gateway::Transaction::DeployAccount(DeployAccountTransaction {
                contract_address: contract_address_bytes!(b"deploy account contract address"),
                transaction_hash: transaction_hash_bytes!(b"deploy account tx hash"),
                max_fee: fee_bytes!(b"deploy account max fee"),
                version: TransactionVersion::ZERO,
                signature: vec![
                    transaction_signature_elem_bytes!(b"deploy account tx sig 0"),
                    transaction_signature_elem_bytes!(b"deploy account tx sig 1"),
                ],
                nonce: transaction_nonce_bytes!(b"deploy account tx nonce"),
                contract_address_salt: contract_address_salt_bytes!(b"deploy account address salt"),
                constructor_calldata: vec![
                    call_param_bytes!(b"deploy account call data 0"),
                    call_param_bytes!(b"deploy account call data 1"),
                ],
                class_hash: class_hash_bytes!(b"deploy account class hash"),
            }),
            gateway::Transaction::Invoke(gateway::InvokeTransaction::V0(InvokeTransactionV0 {
                calldata: vec![
                    call_param_bytes!(b"invoke v0 call data 0"),
                    call_param_bytes!(b"invoke v0 call data 1"),
                ],
                sender_address: contract_address_bytes!(b"invoke v0 contract address"),
                entry_point_selector: entry_point_bytes!(b"invoke v0 entry point"),
                entry_point_type: None,
                max_fee: fee_bytes!(b"invoke v0 max fee"),
                signature: vec![
                    transaction_signature_elem_bytes!(b"invoke v0 tx sig 0"),
                    transaction_signature_elem_bytes!(b"invoke v0 tx sig 1"),
                ],
                transaction_hash: transaction_hash_bytes!(b"invoke v0 tx hash"),
            })),
            gateway::Transaction::Invoke(gateway::InvokeTransaction::V1(InvokeTransactionV1 {
                calldata: vec![
                    call_param_bytes!(b"invoke v1 call data 0"),
                    call_param_bytes!(b"invoke v1 call data 1"),
                ],
                sender_address: contract_address_bytes!(b"invoke v1 contract address"),
                max_fee: fee_bytes!(b"invoke v1 max fee"),
                signature: vec![
                    transaction_signature_elem_bytes!(b"invoke v1 tx sig 0"),
                    transaction_signature_elem_bytes!(b"invoke v1 tx sig 1"),
                ],
                nonce: transaction_nonce_bytes!(b"invoke v1 tx nonce"),
                transaction_hash: transaction_hash_bytes!(b"invoke v1 tx hash"),
            })),
            gateway::Transaction::L1Handler(gateway::L1HandlerTransaction {
                contract_address: contract_address_bytes!(b"L1 handler contract address"),
                entry_point_selector: entry_point_bytes!(b"L1 handler entry point"),
                nonce: transaction_nonce_bytes!(b"L1 handler tx nonce"),
                calldata: vec![
                    call_param_bytes!(b"L1 handler call data 0"),
                    call_param_bytes!(b"L1 handler call data 1"),
                ],
                transaction_hash: transaction_hash_bytes!(b"L1 handler tx hash"),
                version: TransactionVersion::ZERO,
            }),
        ];

        // Generate a random receipt for each transaction. Note that these won't make physical sense
        // but its enough for the tests.
        let receipts: Vec<gateway::Receipt> = transactions
            .iter()
            .enumerate()
            .map(|(i, t)| gateway::Receipt {
                actual_fee: None,
                events: vec![],
                execution_resources: None,
                l1_to_l2_consumed_message: None,
                l2_to_l1_messages: vec![],
                transaction_hash: t.hash(),
                transaction_index: TransactionIndex::new_or_panic(i as u64),
                execution_status: Default::default(),
                revert_error: Default::default(),
            })
            .collect();
        assert_eq!(transactions.len(), receipts.len());

        let body = transactions
            .into_iter()
            .zip(receipts)
            .map(|(t, r)| (t, r))
            .collect::<Vec<_>>();

        let mut db = crate::Storage::in_memory().unwrap().connection().unwrap();
        let db_tx = db.transaction().unwrap();

        db_tx.insert_block_header(&header).unwrap();
        db_tx
            .insert_transaction_data(header.hash, header.number, &body)
            .unwrap();

        db_tx.commit().unwrap();

        (db, header, body)
    }

    #[test]
    fn convert() {
        println!("Started");
        // TODO Metrics we're interested in: compressed size, compression time, decompression time
        // TODO Time metrics would require batching. Also black boxing the output
        // TODO Allocator?
        // TODO Investigate the possibility to join the tx & receipt field for better compression
        let storage_manager = crate::Storage::migrate(
            Path::new("/home/pierre/workspace/pathfinder/testnet2.sqlite").to_path_buf(),
            JournalMode::WAL,
        )
        .unwrap();
        let mut connection = storage_manager
            .create_pool(NonZeroU32::new(8).unwrap())
            .unwrap()
            .connection()
            .unwrap();
        let transaction = connection.transaction().unwrap();
        println!("Connected");

        /* TODO
        let count = transaction
            .inner()
            .query_row(
                "SELECT COUNT(idx) as count FROM starknet_transactions",
                params!(),
                |row| Ok(row.get::<&str, i32>("count").unwrap()),
            )
            .unwrap();
        println!("Number of transactions: {}", count);
         */

        let mut statement = transaction
            .inner()
            .prepare(
                "SELECT hash, tx, receipt FROM starknet_transactions ORDER BY hash ASC LIMIT ? OFFSET ?",
            )
            .unwrap();

        let mut json_zstd_counter = Counter {
            acc_duration: Default::default(),
            total_size: 0,
            processed_items: 0,
        };

        let mut bincode_zstd_counter = Counter {
            acc_duration: Default::default(),
            total_size: 0,
            processed_items: 0,
        };

        let mut bson_zstd_counter = Counter {
            acc_duration: Default::default(),
            total_size: 0,
            processed_items: 0,
        };

        const BATCH_SIZE: i32 = 100;
        let mut last_batch_size = BATCH_SIZE;
        let mut batch_index = -1;
        while last_batch_size == BATCH_SIZE {
            batch_index += 1;
            println!("Querying batch {}", batch_index);
            let mut rows = statement
                .query(params!(&BATCH_SIZE, &((batch_index + 1) * BATCH_SIZE)))
                .unwrap();

            let decompression_duration = Duration::from_secs(0);

            last_batch_size = 0;
            while let Some(row) = rows.next().unwrap() {
                last_batch_size += 1;
                let tx = row.get_ref_unwrap("tx").as_blob().unwrap();
                let receipt = row.get_ref_unwrap("receipt").as_blob().unwrap();
                // TODO println!("Transaction compressed len: {}", tx.len());

                if let Ok((tx, receipt)) = json_decompression(tx, receipt) {
                    // TODO Move the clones into measure+closure params
                    let tx_clone = tx.clone();
                    let receipt_clone = receipt.clone();
                    measure(&mut json_zstd_counter, move || {
                        json_roundtrip(tx_clone, receipt_clone)
                    });
                    let tx_clone = tx.clone();
                    let receipt_clone = receipt.clone();
                    measure(&mut bincode_zstd_counter, move || {
                        bincode_zstd_roundtrip(tx_clone, receipt_clone)
                    });
                    let tx_clone = tx.clone();
                    let receipt_clone = receipt.clone();
                    measure(&mut bson_zstd_counter, move || {
                        bson_zstd_roundtrip(tx_clone, receipt_clone)
                    });
                    // TODO Other formats
                }
            }

            if json_zstd_counter.processed_items > 100_000 {
                println!(
                    "JSON+ZSTD Average compressed size: {}",
                    json_zstd_counter.total_size / json_zstd_counter.processed_items
                );
                println!(
                    "Bincode+ZSTD Average compressed size: {}",
                    bincode_zstd_counter.total_size / bincode_zstd_counter.processed_items
                );
                println!(
                    "BSON+ZSTD Average compressed size: {}",
                    bson_zstd_counter.total_size / bson_zstd_counter.processed_items
                );

                return;
            }
        }

        println!("Done");
    }

    fn json_decompression(
        tx: &[u8],
        receipt: &[u8],
    ) -> anyhow::Result<(gateway::Transaction, gateway::Receipt)> {
        let transaction = zstd::decode_all(tx).context("Transaction initial decompression")?;
        let transaction =
            serde_json::from_slice(&transaction).context("Transaction initial deser")?;

        let receipt = zstd::decode_all(receipt).context("Receipt initial decompression")?;
        let receipt = serde_json::from_slice(&receipt).context("Receipt initial deser")?;

        Ok((transaction, receipt))
    }

    struct Counter {
        acc_duration: Duration,
        total_size: usize,
        processed_items: usize,
    }

    fn measure<F>(counter: &mut Counter, work: F)
    where
        F: FnOnce() -> anyhow::Result<usize>,
    {
        let start = Instant::now();

        match work() {
            Ok(compressed_size) => {
                counter.acc_duration += start.elapsed();
                counter.total_size += compressed_size;
                counter.processed_items += 1;
            }
            Err(err) => {
                println!("Measurement failed: {}", err);
            }
        }
    }

    fn json_roundtrip(tx: gateway::Transaction, rct: gateway::Receipt) -> anyhow::Result<usize> {
        let mut compressor = zstd::bulk::Compressor::new(10).context("Create zstd compressor")?;
        let tx_data = serde_json::to_vec(&tx).context("Serializing transaction")?;
        let tx_data = compressor
            .compress(&tx_data)
            .context("Compressing transaction")?;

        let serialized_receipt = serde_json::to_vec(&rct).context("Serializing receipt")?;
        let serialized_receipt = compressor
            .compress(&serialized_receipt)
            .context("Compressing receipt")?;

        let compressed_size = tx_data.len() + serialized_receipt.len();

        let transaction =
            zstd::decode_all(tx_data.as_slice()).context("Decompressing transaction")?;
        let transaction: gateway::Transaction =
            serde_json::from_slice(&transaction).context("Deserializing transaction")?;
        let receipt =
            zstd::decode_all(serialized_receipt.as_slice()).context("Decompressing receipt")?;
        let receipt: gateway::Receipt =
            serde_json::from_slice(&receipt).context("Deserializing receipt")?;

        Ok(compressed_size)
    }

    fn bson_zstd_roundtrip(
        tx: gateway::Transaction,
        rct: gateway::Receipt,
    ) -> anyhow::Result<usize> {
        let mut compressor = zstd::bulk::Compressor::new(10).context("Create zstd compressor")?;

        let tx_data = bson::to_vec(&tx).context("Serializing transaction")?;
        let serialized_receipt = bson::to_vec(&rct).context("Serializing receipt")?;

        let tx_data = compressor
            .compress(&tx_data)
            .context("Compressing transaction")?;
        let serialized_receipt = compressor
            .compress(&serialized_receipt)
            .context("Compressing receipt")?;

        let compressed_size = tx_data.len() + serialized_receipt.len();

        let transaction =
            zstd::decode_all(tx_data.as_slice()).context("Decompressing transaction")?;
        let receipt =
            zstd::decode_all(serialized_receipt.as_slice()).context("Decompressing receipt")?;

        let transaction: gateway::Transaction =
            bson::from_slice(&transaction).context("Deserializing transaction")?;
        let receipt: gateway::Receipt =
            bson::from_slice(&receipt).context("Deserializing receipt")?;

        Ok(compressed_size)
    }

    fn bincode_zstd_roundtrip(
        tx: gateway::Transaction,
        rct: gateway::Receipt,
    ) -> anyhow::Result<usize> {
        let mut compressor = zstd::bulk::Compressor::new(10).context("Create zstd compressor")?;
        let tx_data = bincode::serialize(&tx).context("Serializing transaction")?;
        let serialized_receipt = bincode::serialize(&rct).context("Serializing receipt")?;

        let tx_data = compressor
            .compress(&tx_data)
            .context("Compressing transaction")?;
        let serialized_receipt = compressor
            .compress(&serialized_receipt)
            .context("Compressing receipt")?;

        let compressed_size = tx_data.len() + serialized_receipt.len();

        let transaction =
            zstd::decode_all(tx_data.as_slice()).context("Decompressing transaction")?;
        let receipt =
            zstd::decode_all(serialized_receipt.as_slice()).context("Decompressing receipt")?;

        // TODO Fails with "DeserializeAnyNotSupported"
        // TODO https://github.com/bincode-org/bincode/issues/548
        /*
        let transaction: gateway::Transaction =
            bincode::deserialize(&transaction).context("Deserializing transaction")?;
        let receipt: gateway::Receipt =
            bincode::deserialize(&receipt).context("Deserializing receipt")?;
         */

        Ok(compressed_size)
    }

    #[test]
    fn transaction() {
        let (mut db, _, body) = setup();
        let tx = db.transaction().unwrap();

        let (expected, _) = body.first().unwrap().clone();

        let result = super::transaction(&tx, expected.hash()).unwrap().unwrap();
        assert_eq!(result, expected);

        let invalid = super::transaction(&tx, transaction_hash_bytes!(b"invalid")).unwrap();
        assert_eq!(invalid, None);
    }

    #[test]
    fn transaction_wtih_receipt() {
        let (mut db, header, body) = setup();
        let tx = db.transaction().unwrap();

        let (transaction, receipt) = body.first().unwrap().clone();

        let result = super::transaction_with_receipt(&tx, transaction.hash())
            .unwrap()
            .unwrap();
        assert_eq!(result.0, transaction);
        assert_eq!(result.1, receipt);
        assert_eq!(result.2, header.hash);

        let invalid =
            super::transaction_with_receipt(&tx, transaction_hash_bytes!(b"invalid")).unwrap();
        assert_eq!(invalid, None);
    }

    #[test]
    fn transaction_at_block() {
        let (mut db, header, body) = setup();
        let tx = db.transaction().unwrap();

        let idx = 5;
        let expected = Some(body[idx].0.clone());

        let by_number = super::transaction_at_block(&tx, header.number.into(), idx).unwrap();
        assert_eq!(by_number, expected);
        let by_hash = super::transaction_at_block(&tx, header.hash.into(), idx).unwrap();
        assert_eq!(by_hash, expected);
        let by_latest = super::transaction_at_block(&tx, BlockId::Latest, idx).unwrap();
        assert_eq!(by_latest, expected);

        let invalid_index =
            super::transaction_at_block(&tx, header.number.into(), body.len() + 1).unwrap();
        assert_eq!(invalid_index, None);

        let invalid_index = super::transaction_at_block(&tx, BlockNumber::MAX.into(), idx).unwrap();
        assert_eq!(invalid_index, None);
    }

    #[test]
    fn transaction_count() {
        let (mut db, header, body) = setup();
        let tx = db.transaction().unwrap();

        let by_latest = super::transaction_count(&tx, BlockId::Latest).unwrap();
        assert_eq!(by_latest, body.len());
        let by_number = super::transaction_count(&tx, header.number.into()).unwrap();
        assert_eq!(by_number, body.len());
        let by_hash = super::transaction_count(&tx, header.hash.into()).unwrap();
        assert_eq!(by_hash, body.len());
    }

    #[test]
    fn transaction_data_for_block() {
        let (mut db, header, body) = setup();
        let tx = db.transaction().unwrap();

        let expected = Some(body);

        let by_number = super::transaction_data_for_block(&tx, header.number.into()).unwrap();
        assert_eq!(by_number, expected);
        let by_hash = super::transaction_data_for_block(&tx, header.hash.into()).unwrap();
        assert_eq!(by_hash, expected);
        let by_latest = super::transaction_data_for_block(&tx, BlockId::Latest).unwrap();
        assert_eq!(by_latest, expected);

        let invalid_block =
            super::transaction_data_for_block(&tx, BlockNumber::MAX.into()).unwrap();
        assert_eq!(invalid_block, None);
    }

    #[test]
    fn transaction_block_hash() {
        let (mut db, header, body) = setup();
        let tx = db.transaction().unwrap();

        let target = body.first().unwrap().0.hash();
        let valid = super::transaction_block_hash(&tx, target).unwrap().unwrap();
        assert_eq!(valid, header.hash);

        let invalid =
            super::transaction_block_hash(&tx, transaction_hash_bytes!(b"invalid hash")).unwrap();
        assert_eq!(invalid, None);
    }
}

//! Contains starknet transaction related code and __not__ database transaction.

use anyhow::Context;
use pathfinder_common::{BlockHash, BlockNumber, TransactionHash};
use starknet_gateway_types::reply::transaction as gateway;

use crate::{prelude::*, BlockId};
use pathfinder_common::receipt::Receipt as CommonRct;
use pathfinder_common::receipt::Receipt as StoredRct;
use pathfinder_common::transaction::Transaction as CommonTx;
use pathfinder_common::transaction::Transaction as StoredTx;

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
    use anyhow::Error;
    use bincode::Options;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use lz4::EncoderBuilder;
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::{BlockHeader, TransactionIndex, TransactionVersion};
    use starknet_gateway_types::reply::transaction::{
        DeclareTransactionV0V1, DeclareTransactionV2, DeployAccountTransaction, DeployTransaction,
        InvokeTransactionV0, InvokeTransactionV1,
    };
    use std::hint::black_box;
    use std::io::{BufReader, BufWriter, Write};
    use std::num::NonZeroU32;
    use std::path::Path;
    use std::time::{Duration, Instant};
    use zstd::bulk::Compressor;

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

        let mut counters = [
            Counter::new("json/zstd".to_string(), json_serializer, zstd_compressor),
            Counter::new("flex/zstd".to_string(), rmp_serializer, zstd_compressor),
            Counter::new("flex/raw".to_string(), rmp_serializer, noop_compressor),
        ];

        const BATCH_SIZE: i32 = 1_000;
        const BENCHMARK_LIMIT: usize = 100_000;
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
                    let tx = CommonTx::from(tx);
                    let rct = CommonRct::from(receipt);
                    for counter in &mut counters {
                        counter.measure(&tx, &rct);
                    }
                }
            }

            if counters[0].processed_items > BENCHMARK_LIMIT {
                for counter in counters {
                    counter.print_results()
                }

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
        name: String,
        acc_duration: Duration,
        total_size: usize,
        processed_items: usize,
        serializer: Box<dyn Fn(StoredTx, StoredRct) -> anyhow::Result<(Vec<u8>, Vec<u8>)>>,
        compressor: Box<dyn Fn(&[u8], &[u8]) -> anyhow::Result<(Vec<u8>, Vec<u8>)>>,
    }

    impl Counter {
        pub fn new<F, C>(name: String, roundtrip: F, compressor: C) -> Self
        where
            F: Fn(StoredTx, StoredRct) -> anyhow::Result<(Vec<u8>, Vec<u8>)> + 'static,
            C: Fn(&[u8], &[u8]) -> anyhow::Result<(Vec<u8>, Vec<u8>)> + 'static,
        {
            Self {
                name,
                acc_duration: Default::default(),
                total_size: 0,
                processed_items: 0,
                serializer: Box::new(roundtrip),
                compressor: Box::new(compressor),
            }
        }

        fn perform(&self, tx: StoredTx, rct: StoredRct) -> anyhow::Result<(Duration, usize)> {
            let start = Instant::now();
            let (tx_data, rct_data) = (self.serializer)(tx.clone(), rct.clone())?;
            let duration = start.elapsed();
            let (tx, rct) = (self.compressor)(&tx_data, &rct_data)?;
            Ok((duration, tx.len() + rct.len()))
        }

        fn measure(&mut self, tx: &StoredTx, rct: &StoredRct) {
            match self.perform(tx.clone(), rct.clone()) {
                Ok((duration, compressed_size)) => {
                    self.acc_duration += duration;
                    self.total_size += compressed_size;
                    self.processed_items += 1;
                }
                Err(err) => {
                    panic!("Measurement failed: {}", err);
                }
            }
        }

        pub fn print_results(self) {
            println!(
                "{} avg size: {}",
                self.name,
                self.total_size / self.processed_items
            );
            println!(
                "{} ser/de duration: {}µs",
                self.name,
                self.acc_duration.as_micros() / self.processed_items as u128 / 100
            );
        }
    }

    fn zstd_compressor(tx_data: &[u8], rct_data: &[u8]) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
        let mut compressor = zstd::bulk::Compressor::new(10).context("Create zstd compressor")?;
        zstd_compression(&tx_data, &rct_data, &mut compressor)
    }

    fn zstd22_compressor(tx_data: &[u8], rct_data: &[u8]) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
        let mut compressor = zstd::bulk::Compressor::new(22).context("Create zstd compressor")?;
        zstd_compression(&tx_data, &rct_data, &mut compressor)
    }

    fn zstd_compression(
        tx_data: &&[u8],
        rct_data: &&[u8],
        compressor: &mut Compressor,
    ) -> Result<(Vec<u8>, Vec<u8>), Error> {
        let tx_data = compressor
            .compress(&tx_data)
            .context("Compressing transaction")?;

        let rct_data = compressor
            .compress(&rct_data)
            .context("Compressing receipt")?;

        Ok((tx_data, rct_data))
    }

    fn zstd_dual_compressor(tx_data: &[u8], rct_data: &[u8]) -> Result<(Vec<u8>, Vec<u8>), Error> {
        let mut compressor = zstd::bulk::Compressor::new(10).context("Create zstd compressor")?;
        let mut all_data = vec![];
        std::io::Write::write_all(&mut all_data, tx_data).unwrap();
        std::io::Write::write_all(&mut all_data, rct_data).unwrap();
        let dual = compressor
            .compress(&all_data)
            .context("Compressing transaction")?;

        Ok((dual, vec![]))
    }

    fn gz_compressor(tx_data: &[u8], rct_data: &[u8]) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
        let mut tx_compressor = GzEncoder::new(Vec::new(), Compression::best());
        tx_compressor
            .write_all(&tx_data)
            .context("Compressing transaction")?;
        let mut rct_compressor = GzEncoder::new(Vec::new(), Compression::best());
        rct_compressor
            .write_all(&rct_data)
            .context("Compressing receipt")?;
        let tx_data = tx_compressor.finish().context("Tx finish")?;

        let rct_data = rct_compressor.finish().context("Rct finish")?;

        Ok((tx_data, rct_data))
    }

    fn noop_compressor(tx_data: &[u8], rct_data: &[u8]) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
        Ok((tx_data.to_vec(), rct_data.to_vec()))
    }

    fn lz4_compressor(tx_data: &[u8], rct_data: &[u8]) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
        let mut output = vec![];
        let mut encoder = EncoderBuilder::new()
            .level(4)
            .build(BufWriter::new(output))?;
        std::io::copy(&mut BufReader::new(tx_data), &mut encoder)?;
        let (tx_data, result) = encoder.finish();
        let tx_data = tx_data.into_inner().unwrap();

        let mut output = vec![];
        let mut encoder = EncoderBuilder::new()
            .level(4)
            .build(BufWriter::new(output))?;
        std::io::copy(&mut BufReader::new(rct_data), &mut encoder)?;
        let (rct_data, result) = encoder.finish();
        let rct_data = rct_data.into_inner().unwrap();

        Ok((tx_data, rct_data))
    }

    fn json_serializer(tx: StoredTx, rct: StoredRct) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
        let tx_data = serde_json::to_vec(&tx).unwrap();
        let rct_data = serde_json::to_vec(&rct).unwrap();

        let dec_tx: StoredTx = serde_json::from_slice(&tx_data).unwrap();
        assert_eq!(dec_tx, tx);
        let dec_rct: StoredRct = serde_json::from_slice(&rct_data).unwrap();
        assert_eq!(dec_rct, rct);

        Ok((tx_data, rct_data))
    }

    fn flexbuffers_serializer(tx: StoredTx, rct: StoredRct) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
        let mut tx_data = flexbuffers::to_vec(&tx).unwrap();
        let mut rct_data = flexbuffers::to_vec(&rct).unwrap();

        let dec_tx: StoredTx = flexbuffers::from_slice(&tx_data).unwrap();
        assert_eq!(dec_tx, tx);
        let dec_rct: StoredRct = flexbuffers::from_slice(&rct_data).unwrap();
        assert_eq!(dec_rct, rct);

        Ok((tx_data, rct_data))
    }

    fn rmp_serializer(tx: StoredTx, rct: StoredRct) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
        let mut tx_data = rmp_serde::to_vec(&tx).unwrap();
        let mut rct_data = rmp_serde::to_vec(&rct).unwrap();

        /* TODO Deser failed
        let dec_tx: gateway::Transaction = rmp_serde::from_slice(&tx_data).unwrap();
        assert_eq!(dec_tx, tx);
        let dec_rct: gateway::Receipt = rmp_serde::from_slice(&rct_data).unwrap();
        assert_eq!(dec_rct, rct);
         */

        Ok((tx_data, rct_data))
    }

    fn bson_serializer(tx: StoredTx, rct: StoredRct) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
        let tx_data = bson::to_vec(&tx).context("Serializing transaction")?;
        let rct_data = bson::to_vec(&rct).context("Serializing receipt")?;

        Ok((tx_data, rct_data))
    }

    fn bincode_serializer(tx: StoredTx, rct: StoredRct) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
        let tx_data = bincode::serialize(&tx).context("Serializing transaction")?;
        let rct_data = bincode::serialize(&rct).context("Serializing receipt")?;

        // TODO Fails to deserialize with "DeserializeAnyNotSupported"
        // TODO https://github.com/bincode-org/bincode/issues/548

        Ok((tx_data, rct_data))
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

use crate::reply::transaction::{
    DeclareTransaction, DeclareTransactionV0V1, DeclareTransactionV2, DeployAccountTransaction,
    DeployTransaction, InvokeTransaction, InvokeTransactionV0, InvokeTransactionV1,
    L1HandlerTransaction, Receipt, Transaction,
};

impl From<Transaction> for pathfinder_common::transaction::Transaction {
    fn from(value: Transaction) -> Self {
        use pathfinder_common::transaction::TransactionVariant;

        let hash = value.hash();
        let variant = match value {
            Transaction::Declare(DeclareTransaction::V0(DeclareTransactionV0V1 {
                class_hash,
                max_fee,
                nonce,
                sender_address,
                signature,
                transaction_hash: _,
            })) => TransactionVariant::DeclareV0(
                pathfinder_common::transaction::DeclareTransactionV0V1 {
                    class_hash,
                    max_fee,
                    nonce,
                    sender_address,
                    signature,
                },
            ),
            Transaction::Declare(DeclareTransaction::V1(DeclareTransactionV0V1 {
                class_hash,
                max_fee,
                nonce,
                sender_address,
                signature,
                transaction_hash: _,
            })) => TransactionVariant::DeclareV1(
                pathfinder_common::transaction::DeclareTransactionV0V1 {
                    class_hash,
                    max_fee,
                    nonce,
                    sender_address,
                    signature,
                },
            ),
            Transaction::Declare(DeclareTransaction::V2(DeclareTransactionV2 {
                class_hash,
                max_fee,
                nonce,
                sender_address,
                signature,
                transaction_hash: _,
                compiled_class_hash,
            })) => TransactionVariant::DeclareV2(
                pathfinder_common::transaction::DeclareTransactionV2 {
                    class_hash,
                    max_fee,
                    nonce,
                    sender_address,
                    signature,
                    compiled_class_hash,
                },
            ),
            Transaction::Deploy(DeployTransaction {
                contract_address,
                contract_address_salt,
                class_hash,
                constructor_calldata,
                transaction_hash: _,
                version,
            }) => TransactionVariant::Deploy(pathfinder_common::transaction::DeployTransaction {
                contract_address,
                contract_address_salt,
                class_hash,
                constructor_calldata,
                version,
            }),
            Transaction::DeployAccount(DeployAccountTransaction {
                contract_address,
                transaction_hash: _,
                max_fee,
                version,
                signature,
                nonce,
                contract_address_salt,
                constructor_calldata,
                class_hash,
            }) => TransactionVariant::DeployAccount(
                pathfinder_common::transaction::DeployAccountTransaction {
                    contract_address,
                    max_fee,
                    version,
                    signature,
                    nonce,
                    contract_address_salt,
                    constructor_calldata,
                    class_hash,
                },
            ),
            Transaction::Invoke(InvokeTransaction::V0(InvokeTransactionV0 {
                calldata,
                sender_address,
                entry_point_selector,
                entry_point_type,
                max_fee,
                signature,
                transaction_hash: _,
            })) => {
                TransactionVariant::InvokeV0(pathfinder_common::transaction::InvokeTransactionV0 {
                    calldata,
                    sender_address,
                    entry_point_selector,
                    entry_point_type: entry_point_type.map(Into::into),
                    max_fee,
                    signature,
                })
            }
            Transaction::Invoke(InvokeTransaction::V1(InvokeTransactionV1 {
                calldata,
                sender_address,
                max_fee,
                signature,
                nonce,
                transaction_hash: _,
            })) => {
                TransactionVariant::InvokeV1(pathfinder_common::transaction::InvokeTransactionV1 {
                    calldata,
                    sender_address,
                    max_fee,
                    signature,
                    nonce,
                })
            }
            Transaction::L1Handler(L1HandlerTransaction {
                contract_address,
                entry_point_selector,
                nonce,
                calldata,
                transaction_hash: _,
                version,
            }) => TransactionVariant::L1Handler(
                pathfinder_common::transaction::L1HandlerTransaction {
                    contract_address,
                    entry_point_selector,
                    nonce,
                    calldata,
                    version,
                },
            ),
        };

        pathfinder_common::transaction::Transaction { hash, variant }
    }
}

impl From<Receipt> for pathfinder_common::receipt::Receipt {
    fn from(value: Receipt) -> Self {
        pathfinder_common::receipt::Receipt {
            actual_fee: value.actual_fee,
            events: value.events,
            execution_resources: value.execution_resources.map(Into::into),
            l1_to_l2_consumed_message: value.l1_to_l2_consumed_message.map(Into::into),
            l2_to_l1_messages: value
                .l2_to_l1_messages
                .into_iter()
                .map(Into::into)
                .collect(),
            transaction_hash: value.transaction_hash,
            transaction_index: value.transaction_index,
        }
    }
}

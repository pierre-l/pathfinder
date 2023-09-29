use crate::reply::transaction::{
    DeclareTransaction, DeclareTransactionV0V1, DeclareTransactionV2, DeployAccountTransaction,
    DeployTransaction, EntryPointType, ExecutionResources, InvokeTransaction, InvokeTransactionV0,
    InvokeTransactionV1, L1HandlerTransaction, L1ToL2Message, L2ToL1Message, Receipt, Transaction,
};
use pathfinder_common::receipt::BuiltinInstanceCounter;

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

impl From<L2ToL1Message> for pathfinder_common::receipt::L2ToL1Message {
    fn from(value: L2ToL1Message) -> Self {
        pathfinder_common::receipt::L2ToL1Message {
            from_address: value.from_address,
            payload: value.payload,
            to_address: value.to_address,
        }
    }
}

impl From<L1ToL2Message> for pathfinder_common::receipt::L1ToL2Message {
    fn from(value: L1ToL2Message) -> Self {
        pathfinder_common::receipt::L1ToL2Message {
            from_address: value.from_address,
            payload: value.payload,
            selector: value.selector,
            to_address: value.to_address,
            nonce: value.nonce,
        }
    }
}

impl From<EntryPointType> for pathfinder_common::transaction::EntryPointType {
    fn from(value: EntryPointType) -> Self {
        match value {
            EntryPointType::External => pathfinder_common::transaction::EntryPointType::External,
            EntryPointType::L1Handler => pathfinder_common::transaction::EntryPointType::L1Handler,
        }
    }
}

impl From<ExecutionResources> for pathfinder_common::receipt::ExecutionResources {
    fn from(value: ExecutionResources) -> Self {
        Self {
            // TODO builtin_instance_counter: value.builtin_instance_counter.into(),
            builtin_instance_counter: BuiltinInstanceCounter::Empty,
            n_steps: value.n_steps,
            n_memory_holes: value.n_memory_holes,
        }
    }
}

/// Types used when deserializing L2 execution resources related data.
pub mod execution_resources {
    use serde::{Deserialize, Serialize};

    /// Sometimes `builtin_instance_counter` JSON object is returned empty.
    #[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(untagged)]
    #[serde(deny_unknown_fields)]
    pub enum BuiltinInstanceCounter {
        Normal(NormalBuiltinInstanceCounter),
        Empty(EmptyBuiltinInstanceCounter),
    }

    impl From<BuiltinInstanceCounter> for pathfinder_common::receipt::BuiltinInstanceCounter {
        fn from(value: BuiltinInstanceCounter) -> Self {
            match value {
                BuiltinInstanceCounter::Normal(NormalBuiltinInstanceCounter {
                    bitwise_builtin,
                    ecdsa_builtin,
                    ec_op_builtin,
                    output_builtin,
                    pedersen_builtin,
                    range_check_builtin,
                }) => pathfinder_common::receipt::BuiltinInstanceCounter::Normal {
                    bitwise_builtin,
                    ecdsa_builtin,
                    ec_op_builtin,
                    output_builtin,
                    pedersen_builtin,
                    range_check_builtin,
                },
                BuiltinInstanceCounter::Empty(_) => {
                    pathfinder_common::receipt::BuiltinInstanceCounter::Empty
                }
            }
        }
    }

    #[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    pub struct NormalBuiltinInstanceCounter {
        bitwise_builtin: u64,
        ecdsa_builtin: u64,
        ec_op_builtin: u64,
        output_builtin: u64,
        pedersen_builtin: u64,
        range_check_builtin: u64,
    }

    #[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    pub struct EmptyBuiltinInstanceCounter {}
}

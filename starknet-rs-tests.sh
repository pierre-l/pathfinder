#!/usr/bin/env bash

# Stop at the first error
set -eux

REPO_ROOT_DIR=$(git rev-parse --show-toplevel);
CI_DIR=$REPO_ROOT_DIR/ci
STARKNET_RS_DIR=$CI_DIR/starknet-rs

cd $CI_DIR

# Clone starknet-rs if it doesn't exist
if [ ! -d ./starknet-rs ]; then
    git clone https://github.com/xJonathanLEI/starknet-rs
fi

cd $STARKNET_RS_DIR

# TODO Remove the test filter
# TODO nocapture?
#   RUST_LOG="starknet-providers::jsonrpc::transport::http=trace" \
RUST_BACKTRACE=1 \
    STARKNET_RPC=http://127.0.0.1:9545 \
    RUST_LOG="trace" \
    cargo test --all jsonrpc_get_block_with_tx_hashes -- --nocapture

# TODO Make this a TearDown cli option
rm -Rf $STARKNET_RS_DIR
rm -Rf tmp
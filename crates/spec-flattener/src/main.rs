#[tokio::main]
async fn main() {
    let file = "starknet_api_openrpc.json";
    let spec = reqwest::get(format!(
        "https://github.com/starkware-libs/starknet-specs/blob/{}/api/{}",
        "v0.5.0", file
    ))
    .await
    .unwrap()
    .text()
    .await
    .unwrap();

    std::fs::write(format!("reference/{}", file), spec).unwrap();
}

use serde_json::Value;

#[tokio::main]
async fn main() {
    let file = "starknet_api_openrpc.json";
    let spec = reqwest::get(format!(
        "https://raw.githubusercontent.com/starkware-libs/starknet-specs/{}/api/{}",
        "v0.5.0", file
    ))
    .await
    .unwrap()
    .text()
    .await
    .unwrap();

    let json = serde_json::from_str::<Value>(&spec).unwrap();

    std::fs::write(
        format!("reference/{}", file),
        serde_json::to_string_pretty(&json).unwrap(),
    )
    .unwrap();
}

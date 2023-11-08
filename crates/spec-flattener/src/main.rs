use serde_json::{Map, Value};

#[tokio::main]
async fn main() {
    let version = "v0.5.0";
    let file = "starknet_api_openrpc.json";
    let url = format!(
        "https://raw.githubusercontent.com/starkware-libs/starknet-specs/{}/api/{}",
        version, file
    );
    println!("Fetching {}", url);
    let spec = reqwest::get(url).await.unwrap().text().await.unwrap();

    let mut root = serde_json::from_str::<Value>(&spec).unwrap();

    let mut flattened_schemas = serde_json::Map::new();

    flatten_section(&mut root, &mut flattened_schemas, "/components/errors");
    flatten_section(&mut root, &mut flattened_schemas, "/components/schemas");
    // TODO Find a better solution?
    {
        // Just make the methods an object.
        let mut object = serde_json::Map::new();
        root.pointer("/methods")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
            .for_each(|(i, value)| {
                object.insert(
                    value.get("name").unwrap().as_str().unwrap().to_string(),
                    value.clone(),
                );
            });
        *root.pointer_mut("/methods").unwrap() = Value::Object(object);
    }
    flatten_section(&mut root, &mut flattened_schemas, "/methods");

    std::fs::write(
        format!("reference/{}", file),
        serde_json::to_string_pretty(&flattened_schemas).unwrap(),
    )
    .unwrap();
}

fn flatten_section(root: &mut Value, flattened_schemas: &mut Map<String, Value>, pointer: &str) {
    let mut schemas = root.pointer(pointer).unwrap().as_object().unwrap().clone();
    let reference_prefix = "#".to_string() + pointer + "/";

    println!("Schemas: {}", schemas.len());

    // TODO ugly
    let mut schemas_left = 999;
    while schemas_left > 0 {
        // ollect the schemas that are flat
        // TODO Ugly
        let mut flat = vec![];
        for (name, definition) in schemas.iter_mut() {
            if !object_fields(definition)
                .into_iter()
                .any(|(key, value)| key == "$ref" && value.as_str().is_some())
            {
                flat.push(name.clone());
            }
        }

        for flat in flat {
            let definition = schemas.remove(flat.as_str()).unwrap();
            if let Some(_) =
                flattened_schemas.insert(reference_prefix.to_string() + &flat, definition)
            {
                panic!("A schema was replaced")
            }
        }

        // Perform a flattening pass
        for definition in schemas.values_mut() {
            object_fields(definition)
                .into_iter()
                .for_each(|(key, value)| {
                    if key == "$ref" {
                        let reference = value.as_str().unwrap();
                        if !reference.starts_with("#") {
                            panic!()
                        }

                        if let Some(definition) = flattened_schemas.get(reference) {
                            let mut flattened_reference = serde_json::Map::new();
                            if let Some(_) = flattened_reference
                                .insert(reference.to_string(), definition.clone())
                            {
                                panic!("A schema was replaced")
                            }
                            *value = Value::Object(flattened_reference);
                        }
                    }
                })
        }

        if schemas_left == schemas.len() {
            panic!("No replacement during the last pass.")
        }
        schemas_left = schemas.len();
    }
}

// TODO Doc
// TODO Return an iterator?
fn object_fields(value: &mut Value) -> Vec<(&str, &mut Value)> {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            vec![]
        }
        Value::Array(array) => array.iter_mut().map(object_fields).flatten().collect(),
        Value::Object(obj) => obj
            .iter_mut()
            .map(|(key, value)| match value {
                Value::Object(_) => object_fields(value),
                Value::Array(array) => array.iter_mut().map(object_fields).flatten().collect(),
                Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
                    vec![(key.as_str(), value)]
                }
            })
            .flatten()
            .collect(),
    }
}

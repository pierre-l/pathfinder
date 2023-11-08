use serde_json::{Map, Value};

#[tokio::main]
async fn main() {
    let version = "v0.5.0";
    let file = "starknet_api_openrpc.json";
    let spec = reqwest::get(format!(
        "https://raw.githubusercontent.com/starkware-libs/starknet-specs/{}/api/{}",
        version, file
    ))
    .await
    .unwrap()
    .text()
    .await
    .unwrap();

    let mut root = serde_json::from_str::<Value>(&spec).unwrap();

    let mut flattened_schemas = serde_json::Map::new();

    // TODO flatten_section(&mut root, &mut flattened_schemas, "/components/errors");
    flatten_section(&mut root, &mut flattened_schemas, "/components/schemas");

    std::fs::write(
        format!("reference/{}", file),
        serde_json::to_string_pretty(&flattened_schemas).unwrap(),
    )
    .unwrap();
}

fn flatten_section(root: &mut Value, flattened_schemas: &mut Map<String, Value>, pointer: &str) {
    let schemas = root.pointer_mut(pointer).unwrap().as_object_mut().unwrap();
    let reference_prefix = "#".to_string() + pointer + "/";

    println!("Schemas: {}", schemas.len());

    // TODO ugly
    let mut schemas_left = 999;
    while schemas_left > 0 {
        // TODO Collect the schemas that are flat
        // TODO Ugly
        let mut flat = vec![];
        for (name, definition) in schemas.iter_mut() {
            if !object_fields(definition)
                .into_iter()
                .any(|(key, value)| key == "$ref" && value.as_str().is_some())
            {
                println!("Flat: {}", name);
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

        // TODO Flatten all the refs.
        for (name, definition) in schemas.iter_mut() {
            object_fields(definition)
                .into_iter()
                .for_each(|(key, value)| {
                    if key == "$ref" {
                        let reference = value.as_str().unwrap();
                        if !reference.starts_with(&reference_prefix) {
                            panic!()
                        }

                        let name = reference.split(&reference_prefix).collect::<Vec<&str>>()[1];

                        // TODO REMOVE
                        println!("Name {}", name);

                        if let Some(definition) = flattened_schemas.get(reference) {
                            let mut flattened_reference = serde_json::Map::new();
                            if let Some(_) = flattened_reference
                                .insert(reference.to_string(), definition.clone())
                            {
                                panic!("A schema was replaced")
                            }
                            *value = Value::Object(flattened_reference);
                            // TODO dbg!(&value);
                        }
                    }
                })
        }

        if schemas_left == schemas.len() {
            panic!("No replacement during the last pass.")
        }
        schemas_left = schemas.len();
        dbg!(&schemas_left);
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

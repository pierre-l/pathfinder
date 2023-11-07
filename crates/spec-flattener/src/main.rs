use serde_json::Value;
use std::collections::HashMap;

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

    {
        let mut schemas = root
            .pointer_mut("/components/schemas")
            .unwrap()
            .as_object_mut()
            .unwrap();

        let mut flattened_schemas = HashMap::new();

        // TODO ugly
        let mut schemas_left = 999;
        while schemas_left > 0 {
            dbg!(&schemas_left);
            // TODO Collect the schemas that are flat
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
                flattened_schemas.insert(flat, definition);
            }

            // TODO Flatten all the refs.
            for (name, definition) in schemas.iter_mut() {
                object_fields(definition)
                    .into_iter()
                    .for_each(|(key, mut value)| {
                        if key == "$ref" {
                            let reference = value.as_str().unwrap();
                            if let Some(definition) = flattened_schemas.get(reference) {
                                let mut flattened_reference = serde_json::Map::new();
                                flattened_reference
                                    .insert(reference.to_string(), definition.clone());
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
        /* TODO
        for (name, definition) in schemas {
            object_fields(definition)
                .into_iter()
                .for_each(|(ref_, value)| {
                    // TODO Extract all components
                    // TODO Flatten all the refs.
                });

            // TODO
            if let None = replace_refs() {
                flattened_schemas.insert(name.clone(), definition.clone());
        }
         */
    }

    std::fs::write(
        format!("reference/{}", file),
        serde_json::to_string_pretty(&root).unwrap(),
    )
    .unwrap();
}

// TODO Doc
// TODO Return an iterator?
fn object_fields(value: &mut Value) -> Vec<(&str, &mut Value)> {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            vec![]
        }
        Value::Array(array) => array
            .iter_mut()
            .map(|value| object_fields(value))
            .flatten()
            .collect(),
        Value::Object(obj) => obj
            .iter_mut()
            .map(|(key, value)| (key.as_str(), value))
            .collect(),
    }
}

/* TODO Remove?
mod flattened {
    use serde_json::Value;

    struct Flattened(Value);

    impl Flattened {}

    impl From<Value> for Flattened {
        fn from(value: Value) -> Self {
            Self::check_for_refs();
            todo!()
        }
    }
}
*/

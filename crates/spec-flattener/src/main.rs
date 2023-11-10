mod all_of;

use crate::all_of::{AllOfInput, MergedAllOf};
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
            .for_each(|value| {
                object.insert(
                    value.get("name").unwrap().as_str().unwrap().to_string(),
                    value.clone(),
                );
            });
        *root.pointer_mut("/methods").unwrap() = Value::Object(object);
    }
    flatten_section(&mut root, &mut flattened_schemas, "/methods");

    let sorted = {
        let mut raw: Vec<(String, Value)> = flattened_schemas.into_iter().collect();
        raw.sort_by(|(name_a, _), (name_b, _)| name_a.cmp(name_b));
        serde_json::Map::from_iter(raw)
    };

    // Trim: remove the intermediate layers of flattened references: the "$ref" field, the pointer object.
    let trimmed = {
        let mut clone = Value::Object(sorted.clone());
        for_each_object(&mut clone, &|object| {
            trim_ref_layer(object);
        });
        clone.as_object().unwrap().clone()
    };

    let merged_allof = {
        let mut clone = Value::Object(trimmed.clone());
        for_each_object(&mut clone, &|object| {
            merge_allofs(object);
        });
        clone.as_object().unwrap().clone()
    };

    // TODO We could have an output dir per step and write output for every step for easier debugging
    // Write the method files
    merged_allof
        .iter()
        .filter_map(|(pointer, schema)| {
            if pointer.starts_with("#/methods") {
                let name = pointer.split('/').last().unwrap();
                Some((name, schema))
            } else {
                None
            }
        })
        .for_each(|(name, schema)| {
            write_to_file(schema, "output/methods/".to_string() + name + "json")
        });

    // Write the whole file
    write_to_file(&Value::Object(merged_allof), format!("output/{}", file));
}

fn trim_ref_layer(obj: &mut Map<String, Value>) {
    for (key, value) in obj.clone() {
        if key == "$ref" {
            // References should now be an object with a single field whose key is the pointer.
            let pointer_object = value.as_object().unwrap();
            assert_eq!(pointer_object.keys().len(), 1);
            let (pointer, flat) = pointer_object.iter().next().unwrap();
            // Remove the original reference.
            obj.remove(&key);
            // Replace it with the pointer object
            let pointer_end = pointer.split("/").last().unwrap().to_string();
            obj.insert(pointer_end, flat.clone());
        }
    }
}

fn merge_allofs(obj: &mut Map<String, Value>) {
    for (key, value) in obj.clone() {
        if value.get("allOf").is_some() {
            let allof = serde_json::from_value::<AllOfInput>(value.clone()).unwrap();
            let merged = MergedAllOf::from(allof);
            obj.insert(key, serde_json::to_value(&merged).unwrap());
        }
        /* TODO
        if key == "allOf" {
            if let Ok(allof) = serde_json::from_value::<AllOfInput>(value.clone()) {
                let merged = MergedAllOf::from(allof);
                obj.insert(key, serde_json::to_value(&merged).unwrap());
            }
            // TODO
            let allof = serde_json::from_value::<AllOfInput>(value.clone()).unwrap();
            let merged = MergedAllOf::from(allof);
            obj.insert(key, serde_json::to_value(&merged).unwrap());
        }
         */
    }
}

fn write_to_file(sorted: &Value, path: String) {
    std::fs::write(path, serde_json::to_string_pretty(&sorted).unwrap()).unwrap();
}

// TODO Document the transformations
fn flatten_section(root: &mut Value, flattened_schemas: &mut Map<String, Value>, pointer: &str) {
    let mut schemas = root.pointer(pointer).unwrap().as_object().unwrap().clone();
    let reference_prefix = "#".to_string() + pointer + "/";

    println!("Schemas: {}", schemas.len());

    // TODO ugly
    let mut schemas_left = 999;
    while schemas_left > 0 {
        // Collect the schemas that are flat
        // TODO Ugly
        let mut flat = vec![];
        for (name, definition) in schemas.iter_mut() {
            if !leaf_fields(definition)
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
            leaf_fields(definition)
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
fn leaf_fields(value: &mut Value) -> Vec<(&str, &mut Value)> {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            vec![]
        }
        Value::Array(array) => array.iter_mut().map(leaf_fields).flatten().collect(),
        Value::Object(obj) => obj
            .iter_mut()
            .map(|(key, value)| match value {
                Value::Object(_) => leaf_fields(value),
                Value::Array(array) => array.iter_mut().map(leaf_fields).flatten().collect(),
                Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
                    // TODO This whole match could probably be merge with the outer match
                    vec![(key.as_str(), value)]
                }
            })
            .flatten()
            .collect(),
    }
}

// TODO The closure ref could be avoided?
fn for_each_object<F>(value: &mut Value, f: &F)
where
    F: Fn(&mut Map<String, Value>),
{
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        Value::Array(array) => array.iter_mut().for_each(|v| for_each_object(v, f)),
        Value::Object(obj) => {
            obj.iter_mut()
                .for_each(|(_key, value)| for_each_object(value, f));

            f(obj)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::leaf_fields;
    use serde_json::json;

    #[test]
    fn test_leaf_fields() {
        let mut value = json!({
            "field1": {
                "subfield1": {
                    "subsubfield1": "subsubvalue1"
                }
            },
            "field2": "value2"
        });

        assert_eq!(
            leaf_fields(&mut value),
            vec![
                ("subsubfield1", &mut json!("subsubvalue1")),
                ("field2", &mut json!("value2"))
            ]
        )
    }
}

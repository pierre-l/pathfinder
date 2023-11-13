mod all_of;

use crate::all_of::{AllOfInput, MergedAllOf};
use anyhow::Context;
use serde_json::{Map, Value};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let version = "v0.5.0";
    let file = "starknet_api_openrpc.json";
    let url = format!(
        "https://raw.githubusercontent.com/starkware-libs/starknet-specs/{}/api/{}",
        version, file
    );

    let mut root = fetch_spec_file(url).await;
    write_to_file(&root, format!("original/{}", file))?;

    // Flatten the $ref pointers into objects, retrieve a pointer->definition map.
    let flattened_schemas = flatten_openrpc_spec(&mut root)?;
    // Sort the top-level fields so they're ordered by pointer (section, then name)
    let sorted = sort_map_fields(flattened_schemas);
    write_output("1-flatten", file, sorted.clone())?;

    // Trim the "$ref" layer, effectively inlining the pointer object and getting completely rid of the original "$ref" field
    let mut trimmed = {
        let mut object_as_value = Value::Object(sorted);
        for_each_object(&mut object_as_value, &|object| {
            trim_ref_layer(object).unwrap();
        });
        object_as_value.as_object().unwrap().clone()
    };
    write_output("2-trimmed", file, trimmed.clone())?;

    // For allOf objects that have a "required" array field, embed that as a boolean in the property objects.
    let mut embedded_required = {
        trimmed
            .iter_mut()
            .for_each(|(_key, value)| for_each_object(value, &embed_required));
        trimmed
    };
    write_output("3-embedded-required", file, embedded_required.clone())?;

    // Merge the "allOf" arrays, regroup their item properties into a single object (name -> property)
    let merged_allof = {
        embedded_required.iter_mut().for_each(|(_key, value)| {
            for_each_object(value, &|object| {
                merge_allofs(object).unwrap();
            })
        });
        embedded_required
    };
    write_output("4-merged-allOf", file, merged_allof)
}

fn write_output(directory: &str, file: &str, result: Map<String, Value>) -> anyhow::Result<()> {
    // Write the method files
    result
        .iter()
        .filter_map(|(pointer, schema)| {
            if pointer.starts_with("#/methods") {
                let name = pointer.split('/').last().unwrap_or(pointer);
                Some((name, schema))
            } else {
                None
            }
        })
        .for_each(|(name, schema)| {
            write_to_file(schema, format!("{}/methods/{}.json", directory, name)).unwrap()
        });

    // Write the whole file
    write_to_file(&Value::Object(result), format!("{}/{}", directory, file))
}

/// Reconstructs the `Map`, sorting the fields by their key.
/// TODO Might be unnecessary. Is this something that `serde_json` already does?
fn sort_map_fields(flattened_schemas: Map<String, Value>) -> Map<String, Value> {
    let mut raw: Vec<(String, Value)> = flattened_schemas.into_iter().collect();
    raw.sort_by(|(name_a, _), (name_b, _)| name_a.cmp(name_b));
    serde_json::Map::from_iter(raw)
}

fn flatten_openrpc_spec(root: &mut Value) -> anyhow::Result<Map<String, Value>> {
    let mut flattened_schemas = serde_json::Map::new();
    flatten_refs(root, &mut flattened_schemas, "/components/errors");
    flatten_refs(root, &mut flattened_schemas, "/components/schemas");

    {
        // The methods section is an array. Make it an object. Use the method name as the key.
        let mut object = serde_json::Map::new();
        root.pointer("/methods")
            .expect("The spec file must have a methods section")
            .as_array()
            .expect("The methods section must be an array")
            .iter()
            .for_each(|value| {
                object.insert(
                    value
                        .get("name")
                        .expect("Methods must have a name")
                        .as_str()
                        .expect("Method names must be strings")
                        .to_string(),
                    value.clone(),
                );
            });
        *root
            .pointer_mut("/methods")
            .context("The spec file must have a methods section")? = Value::Object(object);
    }
    flatten_refs(root, &mut flattened_schemas, "/methods");
    Ok(flattened_schemas)
}

async fn fetch_spec_file(url: String) -> Value {
    println!("Fetching {}", url);
    let spec = reqwest::get(url)
        .await
        .expect("Failed to retrieve the spec file")
        .text()
        .await
        .expect("Failed to parse the spec file to a string");

    serde_json::from_str::<Value>(&spec).expect("The spec file isn't valid JSON")
}

/// Trim: remove the intermediate layers of flattened references: the "$ref" field, the pointer object.
///
/// Example input:
/// ```json
/// {
///     "$ref": {
///         "POINTER_OBJECT_NAME": {
///             "pointer_object_field": "value"
///         }
///     },
///     "other_field": "value"
/// }
/// ```
///
/// Example output:
/// ```json
/// {
///     "POINTER_OBJECT_NAME": {
///         "pointer_object_field": "value"
///     },
///     "other_field": "value"
/// }
/// ```
///
/// TODO Unit test
fn trim_ref_layer(obj: &mut Map<String, Value>) -> anyhow::Result<()> {
    for (key, value) in obj.clone() {
        if key == "$ref" {
            // References should now be an object with a single field whose key is the pointer.
            let pointer_object = value
                .as_object()
                .context("\"$ref\" fields are expected to be objects at this point")?;
            assert_eq!(pointer_object.keys().len(), 1);
            let (pointer, flat) = pointer_object
                .iter()
                .next()
                .expect("Shouldn't happen as the vec len is supposedly checked above");
            // Remove the original reference, the "$ref" field.
            obj.remove(&key);
            // Replace it with the pointer object, effectively removing the "$ref" layer
            let pointer_end = pointer.split('/').last().unwrap_or(pointer).to_string();
            obj.insert(pointer_end, flat.clone());
        }
    }

    Ok(())
}

/// When an object has a "properties" field and a "required" array field, embed the "required" prop
/// as a boolean inside the corresponding property object.
/// TODO Unit test
fn embed_required(obj: &mut Map<String, Value>) {
    for child in obj.values_mut() {
        let Some(child) = child.as_object_mut() else {
            continue;
        };

        let Some(required) = child.get("required").and_then(|v| v.as_array().cloned()) else {
            continue;
        };

        let Some(properties) = child
            .get_mut("properties")
            .and_then(|value| value.as_object_mut())
        else {
            continue;
        };

        required.into_iter().for_each(|property_name| {
            let child_prop = properties
                .get_mut(
                    property_name
                        .as_str()
                        .context("Property names in the required array are supposed to be strings")
                        .unwrap(),
                )
                .expect(
                    "A property marked as \"required\" should already exist in the property map",
                )
                .as_object_mut()
                .expect("Properties are expected to be objects");
            child_prop.insert("required".to_string(), Value::Bool(true));
        });

        // TODO This isn't perfect, two occurrences are left. Write unit tests.
        child.remove("required");
    }
}

fn merge_allofs(obj: &mut Map<String, Value>) -> anyhow::Result<()> {
    for (key, value) in obj.clone() {
        if value.get("allOf").is_some() {
            let allof = serde_json::from_value::<AllOfInput>(value.clone())
                .context("allOf deserialization failed")?;
            let merged = MergedAllOf::from(allof);
            obj.insert(
                key,
                serde_json::to_value(&merged).context("Merged allOf serialization failed")?,
            );
        }
    }

    Ok(())
}

fn write_to_file(sorted: &Value, path: String) -> anyhow::Result<()> {
    std::fs::write(
        &path,
        serde_json::to_string_pretty(&sorted).context("Serialization failed")?,
    )
    .context(format!("Failed to write to file: {}", path))
}

/// Flattens the "$ref" fields.
/// "$ref" fields originally are strings formatted like this: `"$ref": "#/components/schemas/EVENT"`
/// This function lists all schemas, and for every "$ref", replaces the pointer string with the schema object.
/// By doing this iteratively, eventually all the schemas gets fully flat and all "$ref" fields contain objects.
///
/// # Panics
/// This function will panic in two main cases:
/// * the values don't fit the expected schema (eg: $refs are anything else than strings and objects,
///     or the pointer value is formatted in an unexpected way)
/// * a flattening pass doesn't result in fewer schemas to flatten (eg: there's a $ref cycle).
fn flatten_refs(root: &mut Value, flattened_schemas: &mut Map<String, Value>, pointer: &str) {
    let mut schemas = root.pointer(pointer).unwrap().as_object().unwrap().clone();
    let reference_prefix = "#".to_string() + pointer + "/";

    println!("Schemas: {}", schemas.len());

    let mut schemas_left = usize::MAX;
    while schemas_left > 0 {
        // Collect the names of the flat schemas
        let flat = schemas
            .iter_mut()
            .filter_map(|(name, definition)| {
                if !leaf_fields(definition)
                    .into_iter()
                    .any(|(key, value)| key == "$ref" && value.as_str().is_some())
                {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<String>>();

        for flat in flat {
            let definition = schemas.remove(flat.as_str()).unwrap();
            if flattened_schemas
                .insert(reference_prefix.to_string() + &flat, definition)
                .is_some()
            {
                panic!("A schema was replaced")
            }
        }

        // Perform a flattening pass: if a "$ref" is mentioned, make it a pointer object.
        for definition in schemas.values_mut() {
            leaf_fields(definition)
                .into_iter()
                .for_each(|(key, value)| {
                    if key == "$ref" {
                        let reference = value.as_str().unwrap();
                        if !reference.starts_with('#') {
                            panic!()
                        }

                        if let Some(definition) = flattened_schemas.get(reference) {
                            let mut flattened_reference = serde_json::Map::new();
                            if flattened_reference
                                .insert(reference.to_string(), definition.clone())
                                .is_some()
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

// TODO Return an iterator?
/// Returns mutable references to the leaf fields of all `Value::Object`s in the tree.
/// Leaf values are `Value::Null`, `Value::Bool`, `Value::Number` and `Value::String`.
///
/// ⚠ returns an empty array if the input is already a leaf field.
fn leaf_fields(value: &mut Value) -> Vec<(&str, &mut Value)> {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            vec![]
        }
        Value::Array(array) => array.iter_mut().flat_map(leaf_fields).collect(),
        Value::Object(obj) => obj
            .iter_mut()
            .flat_map(|(key, value)| match value {
                Value::Object(_) => leaf_fields(value),
                Value::Array(array) => array.iter_mut().flat_map(leaf_fields).collect(),
                Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
                    vec![(key.as_str(), value)]
                }
            })
            .collect(),
    }
}

// TODO The closure ref could be avoided?
/// Apply the closure to every `Value::Object` in the tree.
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

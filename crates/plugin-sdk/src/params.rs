//! Lenient case-insensitive JSON params deserializer for plugin factories.
//!
//! Plugin `FfiNodeFactory::create()` receives manifest params as a JSON
//! string. Plugin authors disagree on `rename_all` conventions — some
//! structs use `snake_case` (Rust-idiomatic), others `camelCase`
//! (JSON/JS-idiomatic). That makes manifests fragile: a `bundle_path`
//! key silently falls back to `Default` when the struct expects
//! `bundlePath`, surfacing only as an obscure ENOENT at load time.
//!
//! [`deserialize_params`] removes the trap by pre-walking the JSON
//! tree and emitting BOTH snake_case AND camelCase variants of every
//! object key before handing the value off to serde. The struct's
//! existing `rename_all` picks whichever form it expects; the other
//! is silently dropped as an unknown field. No per-field annotations
//! required.
//!
//! ## Use from a plugin
//!
//! ```ignore
//! use remotemedia_plugin_sdk::params::deserialize_params;
//!
//! impl FfiNodeFactory for MyNodeFactory {
//!     fn create(&self, params: RString) -> RResult<FfiNodeBox, RString> {
//!         let parsed: MyConfig = match deserialize_params(params.as_str()) {
//!             Ok(p) => p,
//!             Err(e) => return RErr(RString::from(format!("params: {e}"))),
//!         };
//!         // ...
//!     }
//! }
//! ```
//!
//! Manifest writers can use either form interchangeably:
//!
//! ```json
//! { "bundle_path": "/path", "smoothingAlpha": 0.1 }
//! ```

use serde::de::DeserializeOwned;

/// Parse `json` into `T` while accepting both snake_case and camelCase
/// keys at every object level. See module docs.
///
/// Limitations:
/// - Keys with both forms present in the input keep the original
///   (already-present form wins; the alias is not inserted on top).
/// - `#[serde(deny_unknown_fields)]` structs may now reject inputs
///   that previously succeeded — the alias gets fed in alongside the
///   real key. Avoid `deny_unknown_fields` on plugin param structs.
pub fn deserialize_params<T>(json: &str) -> Result<T, serde_json::Error>
where
    T: DeserializeOwned,
{
    let mut value: serde_json::Value = serde_json::from_str(json)?;
    add_case_aliases(&mut value);
    serde_json::from_value(value)
}

fn add_case_aliases(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            // Snapshot existing keys so we can mutate the map below.
            let existing: Vec<String> = map.keys().cloned().collect();
            for key in existing {
                let val = map.get(&key).cloned().unwrap_or(serde_json::Value::Null);
                let snake = to_snake_case(&key);
                let camel = to_camel_case(&key);
                if snake != key && !map.contains_key(&snake) {
                    map.insert(snake, val.clone());
                }
                if camel != key && !map.contains_key(&camel) {
                    map.insert(camel, val);
                }
            }
            // Recurse into every value (including the freshly-inserted
            // aliases — cheap, since they're shallow clones at this point).
            for v in map.values_mut() {
                add_case_aliases(v);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                add_case_aliases(v);
            }
        }
        _ => {}
    }
}

fn to_snake_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    let mut prev_lower_or_digit = false;
    for ch in s.chars() {
        if ch.is_ascii_uppercase() {
            if prev_lower_or_digit {
                out.push('_');
            }
            for low in ch.to_lowercase() {
                out.push(low);
            }
            prev_lower_or_digit = false;
        } else {
            out.push(ch);
            prev_lower_or_digit = ch.is_ascii_lowercase() || ch.is_ascii_digit();
        }
    }
    out
}

fn to_camel_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut upper_next = false;
    for ch in s.chars() {
        if ch == '_' || ch == '-' {
            upper_next = true;
            continue;
        }
        if upper_next {
            for up in ch.to_uppercase() {
                out.push(up);
            }
            upper_next = false;
        } else {
            out.push(ch);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[test]
    fn snake_case_conversion() {
        assert_eq!(to_snake_case("bundlePath"), "bundle_path");
        assert_eq!(to_snake_case("bundle_path"), "bundle_path");
        assert_eq!(to_snake_case("modelPath2"), "model_path2");
        assert_eq!(to_snake_case("X"), "x");
        assert_eq!(to_snake_case(""), "");
    }

    #[test]
    fn camel_case_conversion() {
        assert_eq!(to_camel_case("bundle_path"), "bundlePath");
        assert_eq!(to_camel_case("bundlePath"), "bundlePath");
        assert_eq!(to_camel_case("model_path_2"), "modelPath2");
        assert_eq!(to_camel_case("x"), "x");
        assert_eq!(to_camel_case(""), "");
    }

    #[derive(Deserialize, Debug, PartialEq)]
    #[serde(default, rename_all = "camelCase")]
    struct CamelStruct {
        bundle_path: String,
        smoothing_alpha: f32,
    }

    impl Default for CamelStruct {
        fn default() -> Self {
            Self {
                bundle_path: String::new(),
                smoothing_alpha: 0.0,
            }
        }
    }

    #[test]
    fn camel_struct_accepts_snake_keys() {
        let s: CamelStruct =
            deserialize_params(r#"{"bundle_path": "/foo", "smoothing_alpha": 0.5}"#).unwrap();
        assert_eq!(s.bundle_path, "/foo");
        assert_eq!(s.smoothing_alpha, 0.5);
    }

    #[test]
    fn camel_struct_accepts_camel_keys() {
        let s: CamelStruct =
            deserialize_params(r#"{"bundlePath": "/foo", "smoothingAlpha": 0.5}"#).unwrap();
        assert_eq!(s.bundle_path, "/foo");
        assert_eq!(s.smoothing_alpha, 0.5);
    }

    #[derive(Deserialize, Debug, PartialEq)]
    #[serde(default)]
    struct SnakeStruct {
        glb_path: String,
        realtime_mode: bool,
    }

    impl Default for SnakeStruct {
        fn default() -> Self {
            Self {
                glb_path: String::new(),
                realtime_mode: false,
            }
        }
    }

    #[test]
    fn snake_struct_accepts_camel_keys() {
        let s: SnakeStruct =
            deserialize_params(r#"{"glbPath": "/foo", "realtimeMode": true}"#).unwrap();
        assert_eq!(s.glb_path, "/foo");
        assert_eq!(s.realtime_mode, true);
    }
}

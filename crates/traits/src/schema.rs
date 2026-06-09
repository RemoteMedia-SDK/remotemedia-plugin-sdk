//! Node schema definitions for automatic TypeScript type generation
//!
//! All pipeline nodes receive and emit `RuntimeData` enum variants.
//! This module provides metadata about:
//! - Which RuntimeData variants a node accepts as input
//! - Which RuntimeData variants a node produces as output
//! - The node's configuration parameters (JSON Schema)
//!
//! # Design Philosophy
//!
//! Nodes don't have custom input/output types - they work with `RuntimeData`:
//! - `RuntimeData::Audio { samples, sample_rate, channels, .. }`
//! - `RuntimeData::Video { pixel_data, width, height, format, .. }`
//! - `RuntimeData::Json(Value)`
//! - `RuntimeData::Text(String)`
//! - `RuntimeData::Binary(Vec<u8>)`
//! - `RuntimeData::Tensor { .. }`
//! - `RuntimeData::ControlMessage { .. }`
//!
//! The schema describes which variants are accepted/produced, plus config params.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

fn default_true() -> bool {
    true
}

/// Model source declaration for a node (in its schema metadata).
/// Distinct from `remotemedia_core::manifest::ModelSourceFile` which is used
/// in pipeline manifests' `params["model_sources"]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeModelSourceFile {
    /// Local path where the model should be placed (relative to model cache dir)
    pub path: String,
    /// Filename to download (key for lookup)
    pub filename: String,
    /// Download URL
    pub url: String,
    /// Expected file size in bytes (for progress/validation)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_size: Option<u64>,
    /// Whether this model is required for the node to function
    #[serde(default = "default_true")]
    pub required: bool,
}

/// Collection of model sources for a node
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NodeModelSources {
    pub files: Vec<NodeModelSourceFile>,
}

/// RuntimeData variant types
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeDataType {
    Audio,
    Video,
    Image,
    Json,
    Text,
    Binary,
    Tensor,
    Numpy,
    ControlMessage,
}

impl RuntimeDataType {
    /// Get all variant types
    pub fn all() -> &'static [RuntimeDataType] {
        &[
            RuntimeDataType::Audio,
            RuntimeDataType::Video,
            RuntimeDataType::Image,
            RuntimeDataType::Json,
            RuntimeDataType::Text,
            RuntimeDataType::Binary,
            RuntimeDataType::Tensor,
            RuntimeDataType::Numpy,
            RuntimeDataType::ControlMessage,
        ]
    }

    /// Convert to TypeScript type
    pub fn to_typescript(&self) -> &'static str {
        match self {
            RuntimeDataType::Audio => "'audio'",
            RuntimeDataType::Video => "'video'",
            RuntimeDataType::Image => "'image'",
            RuntimeDataType::Json => "'json'",
            RuntimeDataType::Text => "'text'",
            RuntimeDataType::Binary => "'binary'",
            RuntimeDataType::Tensor => "'tensor'",
            RuntimeDataType::Numpy => "'numpy'",
            RuntimeDataType::ControlMessage => "'control'",
        }
    }
}

/// Complete schema for a pipeline node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSchema {
    /// Node type identifier (e.g., "KokoroTTSNode", "AudioResample")
    pub node_type: String,

    /// Human-readable description
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Category for grouping (e.g., "audio", "text", "ml", "video")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,

    /// Accepted input RuntimeData variants (empty = accepts any)
    #[serde(default)]
    pub accepts: Vec<RuntimeDataType>,

    /// Produced output RuntimeData variants (empty = same as input)
    #[serde(default)]
    pub produces: Vec<RuntimeDataType>,

    /// Configuration JSON Schema (for node params in manifest)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_schema: Option<serde_json::Value>,

    /// Default configuration values
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_defaults: Option<serde_json::Value>,

    /// Whether this is a Python node
    #[serde(default)]
    pub is_python: bool,

    /// Whether this node supports streaming (multiple outputs per input)
    #[serde(default = "default_true")]
    pub streaming: bool,

    /// Whether this node is multi-output (yields many outputs per input)
    #[serde(default)]
    pub multi_output: bool,

    /// Execution characteristics
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<NodeCapabilitiesSchema>,

    /// Model sources required by this node (download URLs, paths, etc.)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_sources: Option<NodeModelSources>,
}

/// Execution characteristics for scheduling
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NodeCapabilitiesSchema {
    /// Can process inputs in parallel
    #[serde(default)]
    pub parallelizable: bool,

    /// Benefits from batched inputs
    #[serde(default)]
    pub batch_aware: bool,

    /// Supports control messages
    #[serde(default)]
    pub supports_control: bool,

    /// Typical latency class
    #[serde(default)]
    pub latency_class: LatencyClass,
}

/// Processing latency classification
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LatencyClass {
    /// Sub-millisecond (< 1ms) - audio resampling
    Realtime,
    /// Fast (1-10ms) - VAD, format conversion
    #[default]
    Fast,
    /// Medium (10-100ms) - text processing
    Medium,
    /// Slow (100ms-1s) - TTS synthesis
    Slow,
    /// Very slow (> 1s) - ML inference, transcription
    Batch,
}

/// Parameter type for node configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ParameterType {
    String,
    Number,
    Integer,
    Boolean,
    Array,
    Object,
}

/// A single configuration parameter for a node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeParameter {
    /// Parameter name
    pub name: String,
    /// Parameter type
    pub param_type: ParameterType,
    /// Human-readable description
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Default value (as JSON)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,
    /// Whether the parameter is required
    #[serde(default)]
    pub required: bool,
    /// Allowed values (enum constraint)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub enum_values: Vec<serde_json::Value>,
    /// Minimum value (for numbers)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimum: Option<f64>,
    /// Maximum value (for numbers)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum: Option<f64>,
}

impl NodeSchema {
    /// Create a new node schema
    pub fn new(node_type: impl Into<String>) -> Self {
        Self {
            node_type: node_type.into(),
            description: None,
            category: None,
            accepts: Vec::new(),
            produces: Vec::new(),
            config_schema: None,
            config_defaults: None,
            is_python: false,
            streaming: true,
            multi_output: false,
            capabilities: None,
            model_sources: None,
        }
    }

    /// Set description
    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// Set category
    pub fn category(mut self, cat: impl Into<String>) -> Self {
        self.category = Some(cat.into());
        self
    }

    /// Set accepted input types
    pub fn accepts(mut self, types: impl IntoIterator<Item = RuntimeDataType>) -> Self {
        self.accepts = types.into_iter().collect();
        self
    }

    /// Set produced output types
    pub fn produces(mut self, types: impl IntoIterator<Item = RuntimeDataType>) -> Self {
        self.produces = types.into_iter().collect();
        self
    }

    /// Set config schema from JSON Schema value
    pub fn config_schema(mut self, schema: serde_json::Value) -> Self {
        self.config_schema = Some(schema);
        self
    }

    /// Set config schema from a type that implements schemars::JsonSchema
    ///
    /// This automatically generates a JSON Schema from the Rust struct's type definition,
    /// eliminating the need to manually define schemas. Just derive `JsonSchema` on your
    /// config struct and call this method.
    ///
    /// # Example
    /// ```ignore
    /// #[derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
    /// pub struct MyNodeConfig {
    ///     /// Sample rate in Hz
    ///     pub sample_rate: u32,
    ///     /// Threshold value (0.0-1.0)
    ///     pub threshold: f32,
    /// }
    ///
    /// NodeSchema::new("MyNode")
    ///     .config_schema_from::<MyNodeConfig>()
    /// ```
    pub fn config_schema_from<T: schemars::JsonSchema>(mut self) -> Self {
        let schema = schemars::schema_for!(T);
        self.config_schema = Some(serde_json::to_value(schema).unwrap_or_default());
        self
    }

    /// Set default config values
    pub fn config_defaults(mut self, defaults: serde_json::Value) -> Self {
        self.config_defaults = Some(defaults);
        self
    }

    /// Mark as Python node
    pub fn python(mut self) -> Self {
        self.is_python = true;
        self
    }

    /// Mark as multi-output streaming
    pub fn multi_output(mut self) -> Self {
        self.multi_output = true;
        self
    }

    /// Set capabilities
    pub fn capabilities(mut self, caps: NodeCapabilitiesSchema) -> Self {
        self.capabilities = Some(caps);
        self
    }

    /// Set model sources required by this node
    pub fn model_sources(mut self, sources: NodeModelSources) -> Self {
        self.model_sources = Some(sources);
        self
    }

    /// Extract parameters from config_schema as a structured list
    pub fn get_parameters(&self) -> Vec<NodeParameter> {
        let Some(schema) = &self.config_schema else {
            return Vec::new();
        };

        let Some(obj) = schema.as_object() else {
            return Vec::new();
        };

        let Some(props) = obj.get("properties").and_then(|p| p.as_object()) else {
            return Vec::new();
        };

        // Get required fields
        let required: Vec<&str> = obj
            .get("required")
            .and_then(|r| r.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();

        props
            .iter()
            .map(|(name, prop)| {
                let prop_obj = prop.as_object();

                let param_type = prop_obj
                    .and_then(|o| o.get("type"))
                    .and_then(|t| t.as_str())
                    .map(|t| match t {
                        "string" => ParameterType::String,
                        "number" => ParameterType::Number,
                        "integer" => ParameterType::Integer,
                        "boolean" => ParameterType::Boolean,
                        "array" => ParameterType::Array,
                        "object" => ParameterType::Object,
                        _ => ParameterType::String,
                    })
                    .unwrap_or(ParameterType::String);

                let description = prop_obj
                    .and_then(|o| o.get("description"))
                    .and_then(|d| d.as_str())
                    .map(|s| s.to_string());

                let default = prop_obj.and_then(|o| o.get("default")).cloned();

                let enum_values = prop_obj
                    .and_then(|o| o.get("enum"))
                    .and_then(|e| e.as_array())
                    .cloned()
                    .unwrap_or_default();

                let minimum = prop_obj
                    .and_then(|o| o.get("minimum"))
                    .and_then(|m| m.as_f64());

                let maximum = prop_obj
                    .and_then(|o| o.get("maximum"))
                    .and_then(|m| m.as_f64());

                NodeParameter {
                    name: name.clone(),
                    param_type,
                    description,
                    default,
                    required: required.contains(&name.as_str()),
                    enum_values,
                    minimum,
                    maximum,
                }
            })
            .collect()
    }
}

/// Trait for types that provide their schema
pub trait HasNodeSchema {
    /// Get the schema for this node type
    fn schema() -> NodeSchema;
}

// =============================================================================
// NodeConfig derive support (zero-boilerplate config registration)
// =============================================================================

/// Trait implemented by `#[derive(NodeConfig)]` for automatic schema registration.
///
/// This trait is used by the proc-macro to generate schema metadata from
/// Rust config structs. Users should not implement this manually.
pub trait NodeConfigSchema: schemars::JsonSchema + serde::Serialize + Default + 'static {
    /// The node type name (e.g., "SpeculativeVADGate")
    fn node_type() -> &'static str;

    /// Category for grouping (e.g., "audio", "video", "ml")
    fn category() -> Option<String> {
        None
    }

    /// Human-readable description
    fn description() -> Option<String> {
        None
    }

    /// Accepted input RuntimeData types
    fn accepts() -> Vec<RuntimeDataType> {
        Vec::new()
    }

    /// Produced output RuntimeData types
    fn produces() -> Vec<RuntimeDataType> {
        Vec::new()
    }

    /// Whether this node produces multiple outputs per input
    fn multi_output() -> bool {
        false
    }

    /// Generate JSON Schema from the type
    fn config_json_schema() -> serde_json::Value;

    /// Generate default config as JSON
    fn default_config() -> Option<serde_json::Value>
    where
        Self: Default + serde::Serialize,
    {
        serde_json::to_value(Self::default()).ok()
    }

    /// Build a complete NodeSchema from this config
    fn to_node_schema() -> NodeSchema {
        let mut schema = NodeSchema::new(Self::node_type());

        if let Some(desc) = Self::description() {
            schema = schema.description(desc);
        }
        if let Some(cat) = Self::category() {
            schema = schema.category(cat);
        }

        let accepts = Self::accepts();
        if !accepts.is_empty() {
            schema = schema.accepts(accepts);
        }

        let produces = Self::produces();
        if !produces.is_empty() {
            schema = schema.produces(produces);
        }

        if Self::multi_output() {
            schema = schema.multi_output();
        }

        schema.config_schema = Some(Self::config_json_schema());
        schema.config_defaults = Self::default_config();

        schema
    }
}

/// A registered node config for inventory-based auto-collection.
///
/// This struct is submitted to inventory by the `#[derive(NodeConfig)]` macro.
pub struct RegisteredNodeConfig {
    /// Function to build the NodeSchema
    pub build_schema: fn() -> NodeSchema,
}

impl RegisteredNodeConfig {
    /// Create a new registration for a NodeConfigSchema implementor
    pub const fn new(build_schema: fn() -> NodeSchema) -> Self {
        Self { build_schema }
    }
}

// Inventory collection for auto-registered configs
inventory::collect!(RegisteredNodeConfig);

/// Collect all auto-registered node configs into a registry
pub fn collect_registered_configs() -> NodeSchemaRegistry {
    let mut registry = NodeSchemaRegistry::new();

    for registered in inventory::iter::<RegisteredNodeConfig> {
        let schema = (registered.build_schema)();
        registry.register(schema);
    }

    registry
}

// =============================================================================
// TypeScript Generation
// =============================================================================

/// Generate TypeScript type definitions from all registered node schemas.
///
/// This produces a `.d.ts` file with interfaces for all node configs.
///
/// # Example output
///
/// ```typescript
/// /** Speculative VAD gate for low-latency voice interaction */
/// export interface SpeculativeVADGateConfig {
///   /** Lookback window in milliseconds */
///   lookback_ms?: number;
///   /** Sample rate of audio */
///   sample_rate?: number;
/// }
/// ```
pub fn generate_typescript(registry: &NodeSchemaRegistry) -> String {
    let mut output = String::new();

    output.push_str("// Auto-generated by remotemedia-core\n");
    output.push_str("// Do not edit manually\n\n");

    // Generate RuntimeDataType enum
    output.push_str("export type RuntimeDataType = ");
    output.push_str(
        "'audio' | 'video' | 'json' | 'text' | 'binary' | 'tensor' | 'numpy' | 'control';\n\n",
    );

    // Generate config interfaces for each node
    let mut schemas: Vec<_> = registry.iter().collect();
    schemas.sort_by(|a, b| a.node_type.cmp(&b.node_type));

    for schema in schemas {
        output.push_str(&generate_typescript_interface(schema));
        output.push('\n');
    }

    // Generate union type of all configs
    output.push_str("/** All node configuration types */\n");
    output.push_str("export type NodeConfigs = {\n");
    for schema in registry.iter() {
        output.push_str(&format!(
            "  {}: {}Config;\n",
            schema.node_type, schema.node_type
        ));
    }
    output.push_str("};\n");

    output
}

/// Generate TypeScript interface for a single node schema
fn generate_typescript_interface(schema: &NodeSchema) -> String {
    let mut output = String::new();

    // Doc comment
    if let Some(desc) = &schema.description {
        output.push_str(&format!("/** {} */\n", desc));
    }

    output.push_str(&format!("export interface {}Config {{\n", schema.node_type));

    // Extract properties from config_schema
    if let Some(config_schema) = &schema.config_schema {
        let required = config_schema
            .get("required")
            .and_then(|r| r.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
            .unwrap_or_default();

        if let Some(props) = config_schema.get("properties").and_then(|p| p.as_object()) {
            for (name, prop) in props {
                // Doc comment for property
                if let Some(desc) = prop.get("description").and_then(|d| d.as_str()) {
                    output.push_str(&format!("  /** {} */\n", desc));
                }

                // Property with type
                let is_required = required.contains(&name.as_str());
                let ts_type = json_schema_to_typescript(prop);
                let optional = if is_required { "" } else { "?" };

                output.push_str(&format!("  {}{}: {};\n", name, optional, ts_type));
            }
        }
    }

    output.push_str("}\n");
    output
}

/// Convert JSON Schema type to TypeScript type
fn json_schema_to_typescript(prop: &serde_json::Value) -> String {
    // Handle enum first
    if let Some(enum_values) = prop.get("enum").and_then(|e| e.as_array()) {
        let literals: Vec<String> = enum_values
            .iter()
            .map(|v| match v {
                serde_json::Value::String(s) => format!("'{}'", s),
                serde_json::Value::Number(n) => n.to_string(),
                serde_json::Value::Bool(b) => b.to_string(),
                _ => "unknown".to_string(),
            })
            .collect();
        return literals.join(" | ");
    }

    // Handle oneOf (union types)
    if let Some(one_of) = prop.get("oneOf").and_then(|o| o.as_array()) {
        let types: Vec<String> = one_of.iter().map(json_schema_to_typescript).collect();
        return types.join(" | ");
    }

    // Handle basic types
    let type_str = prop.get("type").and_then(|t| t.as_str()).unwrap_or("any");

    match type_str {
        "string" => "string".to_string(),
        "number" | "integer" => "number".to_string(),
        "boolean" => "boolean".to_string(),
        "null" => "null".to_string(),
        "array" => {
            if let Some(items) = prop.get("items") {
                format!("{}[]", json_schema_to_typescript(items))
            } else {
                "unknown[]".to_string()
            }
        }
        "object" => {
            if let Some(props) = prop.get("properties").and_then(|p| p.as_object()) {
                let mut fields: Vec<String> = props
                    .iter()
                    .map(|(k, v)| format!("{}: {}", k, json_schema_to_typescript(v)))
                    .collect();
                fields.sort();
                format!("{{ {} }}", fields.join("; "))
            } else if let Some(additional) = prop.get("additionalProperties") {
                format!("Record<string, {}>", json_schema_to_typescript(additional))
            } else {
                "Record<string, unknown>".to_string()
            }
        }
        _ => "unknown".to_string(),
    }
}

/// Registry of all node schemas
#[derive(Debug, Default, Clone)]
pub struct NodeSchemaRegistry {
    schemas: HashMap<String, NodeSchema>,
}

impl NodeSchemaRegistry {
    /// Create empty registry
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a schema
    pub fn register(&mut self, schema: NodeSchema) {
        self.schemas.insert(schema.node_type.clone(), schema);
    }

    /// Register from HasNodeSchema implementor
    pub fn register_node<T: HasNodeSchema>(&mut self) {
        self.register(T::schema());
    }

    /// Get schema by node type
    pub fn get(&self, node_type: &str) -> Option<&NodeSchema> {
        self.schemas.get(node_type)
    }

    /// Iterate all schemas
    pub fn iter(&self) -> impl Iterator<Item = &NodeSchema> {
        self.schemas.values()
    }

    /// Get all schemas as JSON array
    pub fn to_json(&self) -> serde_json::Value {
        let schemas: Vec<_> = self.schemas.values().collect();
        serde_json::to_value(schemas).unwrap_or_default()
    }

    /// Number of registered schemas
    pub fn len(&self) -> usize {
        self.schemas.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.schemas.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_builder() {
        let schema = NodeSchema::new("TestNode")
            .description("A test node")
            .category("test")
            .accepts([RuntimeDataType::Audio, RuntimeDataType::Text])
            .produces([RuntimeDataType::Json])
            .python();

        assert_eq!(schema.node_type, "TestNode");
        assert_eq!(schema.accepts.len(), 2);
        assert_eq!(schema.produces.len(), 1);
        assert!(schema.is_python);
    }
}

//! Proc-macros for RemoteMedia core
//!
//! Provides two attribute macros:
//!
//! ## `#[node_config]` - Config struct generation
//!
//! For defining node configuration structs with automatic TypeScript type generation.
//!
//! ```ignore
//! #[node_config(
//!     node_type = "SpeculativeVADGate",
//!     category = "audio",
//!     description = "Speculative VAD gate for low-latency voice interaction"
//! )]
//! pub struct SpeculativeVADConfig {
//!     pub lookback_ms: u32,
//!     pub sample_rate: u32,
//! }
//! ```
//!
//! ## `#[node]` - Unified node definition (NEW)
//!
//! For defining complete streaming nodes with minimal boilerplate. Combines config struct
//! generation, `AsyncStreamingNode` trait implementation, and TypeScript types into one
//! declarative definition.
//!
//! ```ignore
//! #[node(
//!     node_type = "Echo",
//!     category = "utility",
//!     description = "Echoes input with optional prefix",
//!     accepts = "text",
//!     produces = "text"
//! )]
//! pub struct EchoNode {
//!     #[config(default = "Echo: ".to_string())]
//!     pub prefix: String,
//!
//!     #[state]
//!     call_count: u64,
//! }
//!
//! impl EchoNode {
//!     async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
//!         // Your implementation here
//!     }
//! }
//! ```
//!
//! This automatically:
//! 1. Generates `EchoNodeConfig` struct from `#[config]` fields
//! 2. Derives `Debug`, `Clone`, `Serialize`, `Deserialize`, `JsonSchema` on config
//! 3. Adds `#[serde(default, rename_all = "camelCase")]` to config
//! 4. Rewrites `EchoNode` to hold `config: EchoNodeConfig` + state fields
//! 5. Generates `new(config)` and `with_default()` constructors
//! 6. Implements `AsyncStreamingNode` trait with delegation to user's `process` method
//! 7. Registers schema for TypeScript generation via `inventory`

use proc_macro::TokenStream;
use quote::quote;
use syn::parse::Parser;
use syn::{parse_macro_input, DeriveInput, Lit, Meta, NestedMeta};

// =============================================================================
// #[node] macro - Unified node definition
// =============================================================================

/// Arguments parsed from `#[node(...)]` attribute
struct NodeArgs {
    /// Node type identifier (defaults to struct name minus "Node" suffix)
    node_type: Option<String>,
    /// Category for grouping ("audio", "video", "ml", "text", "utility")
    category: Option<String>,
    /// Human-readable description
    description: Option<String>,
    /// Accepted input RuntimeData types (comma-separated)
    accepts: Option<String>,
    /// Produced output RuntimeData types (comma-separated)
    produces: Option<String>,
    /// Whether this node produces multiple outputs per input
    multi_output: bool,
    /// Override `AsyncStreamingNode::process_streaming` from a
    /// user-supplied `process_streaming_impl` method WHILE keeping
    /// `process` backed by `process_impl`.
    ///
    /// Differs from `multi_output = true`: the latter REPLACES
    /// `process` with a "use streaming instead" error. `streaming`
    /// is the additive variant — both single-output and multi-output
    /// callers stay correct, with the single-output path taking the
    /// first emission. Useful for nodes like SileroVAD whose in-tree
    /// behaviour is multi-output (Json + audio passthrough) but whose
    /// single-output FFI back-compat path must still return the first
    /// (or canonical) output.
    streaming: bool,
    /// Capability behavior: static, configured, passthrough, adaptive, runtime_discovered
    capabilities: Option<String>,
    /// Input capability constraints DSL (e.g., "audio(sample_rate=16000, channels=1)")
    input_caps: Option<String>,
    /// Output capability constraints DSL (e.g., "audio(sample_rate=16000..48000)")
    output_caps: Option<String>,
}

/// Classification of a field as either config or state
#[derive(Debug, Clone)]
enum FieldKind {
    /// Config field - included in generated {NodeName}Config struct
    Config {
        /// Optional default value expression
        default_expr: Option<syn::Expr>,
    },
    /// State field - kept in node struct, excluded from config
    State {
        /// Optional default value expression
        default_expr: Option<syn::Expr>,
    },
}

/// A field that has been classified with its kind
struct ClassifiedField {
    /// The original field
    field: syn::Field,
    /// The classification (config or state)
    kind: FieldKind,
}

/// Parse `#[node(...)]` attribute arguments
fn parse_node_args(args: &[NestedMeta]) -> Result<NodeArgs, syn::Error> {
    let mut node_type = None;
    let mut category = None;
    let mut description = None;
    let mut accepts = None;
    let mut produces = None;
    let mut multi_output = false;
    let mut streaming = false;
    let mut capabilities = None;
    let mut input_caps = None;
    let mut output_caps = None;

    for arg in args {
        match arg {
            NestedMeta::Meta(Meta::NameValue(nv)) => {
                let ident = nv
                    .path
                    .get_ident()
                    .ok_or_else(|| syn::Error::new_spanned(&nv.path, "expected identifier"))?;

                match ident.to_string().as_str() {
                    "node_type" => {
                        if let Lit::Str(s) = &nv.lit {
                            node_type = Some(s.value());
                        }
                    }
                    "category" => {
                        if let Lit::Str(s) = &nv.lit {
                            category = Some(s.value());
                        }
                    }
                    "description" => {
                        if let Lit::Str(s) = &nv.lit {
                            description = Some(s.value());
                        }
                    }
                    "accepts" => {
                        if let Lit::Str(s) = &nv.lit {
                            accepts = Some(s.value());
                        }
                    }
                    "produces" => {
                        if let Lit::Str(s) = &nv.lit {
                            produces = Some(s.value());
                        }
                    }
                    "multi_output" => {
                        if let Lit::Bool(b) = &nv.lit {
                            multi_output = b.value;
                        }
                    }
                    "streaming" => {
                        if let Lit::Bool(b) = &nv.lit {
                            streaming = b.value;
                        }
                    }
                    "capabilities" => {
                        if let Lit::Str(s) = &nv.lit {
                            capabilities = Some(s.value());
                        }
                    }
                    "input_caps" => {
                        if let Lit::Str(s) = &nv.lit {
                            input_caps = Some(s.value());
                        }
                    }
                    "output_caps" => {
                        if let Lit::Str(s) = &nv.lit {
                            output_caps = Some(s.value());
                        }
                    }
                    other => {
                        return Err(syn::Error::new_spanned(
                            &nv.path,
                            format!("unknown attribute: {}", other),
                        ));
                    }
                }
            }
            NestedMeta::Meta(Meta::Path(path)) => {
                if path.is_ident("multi_output") {
                    multi_output = true;
                } else if path.is_ident("streaming") {
                    streaming = true;
                }
            }
            _ => {}
        }
    }

    Ok(NodeArgs {
        node_type,
        category,
        description,
        accepts,
        produces,
        multi_output,
        streaming,
        capabilities,
        input_caps,
        output_caps,
    })
}

/// Parse `#[config]` or `#[config(default = expr)]` attribute on a field
fn parse_config_attr(attr: &syn::Attribute) -> Result<Option<syn::Expr>, syn::Error> {
    // Check if it's just `#[config]` with no arguments
    if attr.tokens.is_empty() {
        return Ok(None);
    }

    // Try to parse as a list with arbitrary expression: #[config(default = expr)]
    // We need custom parsing since syn's Meta::NameValue only accepts literals
    let parser = |input: syn::parse::ParseStream| -> syn::Result<Option<syn::Expr>> {
        let content;
        syn::parenthesized!(content in input);

        // Look for "default = expr"
        let ident: syn::Ident = content.parse()?;
        if ident != "default" {
            return Err(syn::Error::new(ident.span(), "expected 'default'"));
        }
        content.parse::<syn::Token![=]>()?;
        let expr: syn::Expr = content.parse()?;
        Ok(Some(expr))
    };

    match parser.parse2(attr.tokens.clone()) {
        Ok(expr) => Ok(expr),
        Err(_) => {
            // Fall back to Meta parsing for simple literals
            let meta = attr.parse_meta()?;
            match meta {
                Meta::Path(_) => Ok(None),
                Meta::List(list) => {
                    for nested in &list.nested {
                        if let NestedMeta::Meta(Meta::NameValue(nv)) = nested {
                            if nv.path.is_ident("default") {
                                let expr = lit_to_expr(&nv.lit)?;
                                return Ok(Some(expr));
                            }
                        }
                    }
                    Ok(None)
                }
                Meta::NameValue(_) => Err(syn::Error::new_spanned(
                    attr,
                    "expected #[config] or #[config(default = expr)]",
                )),
            }
        }
    }
}

/// Parse `#[state]` or `#[state(default = expr)]` attribute on a field
fn parse_state_attr(attr: &syn::Attribute) -> Result<Option<syn::Expr>, syn::Error> {
    // Check if it's just `#[state]` with no arguments
    if attr.tokens.is_empty() {
        return Ok(None);
    }

    // Try to parse as a list with arbitrary expression: #[state(default = expr)]
    // We need custom parsing since syn's Meta::NameValue only accepts literals
    let parser = |input: syn::parse::ParseStream| -> syn::Result<Option<syn::Expr>> {
        let content;
        syn::parenthesized!(content in input);

        // Look for "default = expr"
        let ident: syn::Ident = content.parse()?;
        if ident != "default" {
            return Err(syn::Error::new(ident.span(), "expected 'default'"));
        }
        content.parse::<syn::Token![=]>()?;
        let expr: syn::Expr = content.parse()?;
        Ok(Some(expr))
    };

    match parser.parse2(attr.tokens.clone()) {
        Ok(expr) => Ok(expr),
        Err(_) => {
            // Fall back to Meta parsing for simple literals
            let meta = attr.parse_meta()?;
            match meta {
                Meta::Path(_) => Ok(None),
                Meta::List(list) => {
                    for nested in &list.nested {
                        if let NestedMeta::Meta(Meta::NameValue(nv)) = nested {
                            if nv.path.is_ident("default") {
                                let expr = lit_to_expr(&nv.lit)?;
                                return Ok(Some(expr));
                            }
                        }
                    }
                    Ok(None)
                }
                Meta::NameValue(_) => Err(syn::Error::new_spanned(
                    attr,
                    "expected #[state] or #[state(default = expr)]",
                )),
            }
        }
    }
}

/// Convert a syn::Lit to a syn::Expr
fn lit_to_expr(lit: &Lit) -> Result<syn::Expr, syn::Error> {
    Ok(syn::Expr::Lit(syn::ExprLit {
        attrs: vec![],
        lit: lit.clone(),
    }))
}

/// Classify all fields in a struct as either config or state
fn classify_fields(
    fields: &syn::punctuated::Punctuated<syn::Field, syn::token::Comma>,
) -> Result<Vec<ClassifiedField>, syn::Error> {
    let mut classified = Vec::new();

    for field in fields {
        let mut has_config = false;
        let mut has_state = false;
        let mut config_default = None;
        let mut state_default = None;

        for attr in &field.attrs {
            if attr.path.is_ident("config") {
                if has_state {
                    return Err(syn::Error::new_spanned(
                        field,
                        format!(
                            "field '{}' cannot have both #[config] and #[state] attributes",
                            field
                                .ident
                                .as_ref()
                                .map(|i| i.to_string())
                                .unwrap_or_default()
                        ),
                    ));
                }
                has_config = true;
                config_default = parse_config_attr(attr)?;
            } else if attr.path.is_ident("state") {
                if has_config {
                    return Err(syn::Error::new_spanned(
                        field,
                        format!(
                            "field '{}' cannot have both #[config] and #[state] attributes",
                            field
                                .ident
                                .as_ref()
                                .map(|i| i.to_string())
                                .unwrap_or_default()
                        ),
                    ));
                }
                has_state = true;
                state_default = parse_state_attr(attr)?;
            }
        }

        // Require explicit annotation
        if !has_config && !has_state {
            return Err(syn::Error::new_spanned(
                field,
                format!(
                    "field '{}' must have either #[config] or #[state] attribute",
                    field
                        .ident
                        .as_ref()
                        .map(|i| i.to_string())
                        .unwrap_or_default()
                ),
            ));
        }

        let kind = if has_config {
            FieldKind::Config {
                default_expr: config_default,
            }
        } else {
            FieldKind::State {
                default_expr: state_default,
            }
        };

        // Create a new field without the config/state attributes
        let mut clean_field = field.clone();
        clean_field
            .attrs
            .retain(|attr| !attr.path.is_ident("config") && !attr.path.is_ident("state"));

        classified.push(ClassifiedField {
            field: clean_field,
            kind,
        });
    }

    Ok(classified)
}

/// Generate the config struct from classified fields
fn generate_config_struct(
    struct_name: &syn::Ident,
    vis: &syn::Visibility,
    fields: &[ClassifiedField],
    struct_attrs: &[&syn::Attribute],
) -> proc_macro2::TokenStream {
    let config_name = syn::Ident::new(&format!("{}Config", struct_name), struct_name.span());

    // Collect config fields only
    let config_fields: Vec<_> = fields
        .iter()
        .filter(|f| matches!(f.kind, FieldKind::Config { .. }))
        .map(|f| &f.field)
        .collect();

    // Generate Default impl with custom defaults
    let default_field_inits: Vec<_> = fields
        .iter()
        .filter_map(|f| {
            if let FieldKind::Config { default_expr } = &f.kind {
                let field_name = &f.field.ident;
                let init = match default_expr {
                    Some(expr) => quote! { #field_name: #expr },
                    None => quote! { #field_name: Default::default() },
                };
                Some(init)
            } else {
                None
            }
        })
        .collect();

    quote! {
        #(#struct_attrs)*
        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
        #[serde(default, rename_all = "camelCase")]
        #vis struct #config_name {
            #(#config_fields),*
        }

        impl Default for #config_name {
            fn default() -> Self {
                Self {
                    #(#default_field_inits),*
                }
            }
        }
    }
}

/// Generate the rewritten node struct with config field and state fields
fn generate_node_struct(
    struct_name: &syn::Ident,
    vis: &syn::Visibility,
    fields: &[ClassifiedField],
    struct_attrs: &[&syn::Attribute],
) -> proc_macro2::TokenStream {
    let config_name = syn::Ident::new(&format!("{}Config", struct_name), struct_name.span());

    // Collect state fields only
    let state_fields: Vec<_> = fields
        .iter()
        .filter(|f| matches!(f.kind, FieldKind::State { .. }))
        .map(|f| &f.field)
        .collect();

    // Generate state field initializers for new()
    let state_field_inits: Vec<_> = fields
        .iter()
        .filter_map(|f| {
            if let FieldKind::State { default_expr } = &f.kind {
                let field_name = &f.field.ident;
                let init = match default_expr {
                    Some(expr) => quote! { #field_name: #expr },
                    None => quote! { #field_name: Default::default() },
                };
                Some(init)
            } else {
                None
            }
        })
        .collect();

    quote! {
        #(#struct_attrs)*
        #vis struct #struct_name {
            /// Node configuration
            pub config: #config_name,
            #(#state_fields),*
        }

        impl #struct_name {
            /// Create a new node with the given configuration
            pub fn new(config: #config_name) -> Self {
                Self {
                    config,
                    #(#state_field_inits),*
                }
            }

            /// Create a new node with default configuration
            pub fn with_default() -> Self {
                Self::new(#config_name::default())
            }

            /// Construct this node from a JSON params blob.
            ///
            /// Deserializes into the generated `Config` type and calls
            /// [`Self::new`]. Returns the serde error message as a
            /// string so callers (notably the loadable-export factory
            /// emitted by `#[node]` under
            /// `#[cfg(feature = "loadable-export")]`) can forward it
            /// across the FFI boundary without an `Error` enum dep.
            pub fn new_from_params(
                params: ::serde_json::Value,
            ) -> ::core::result::Result<Self, ::std::string::String> {
                let cfg: #config_name = ::serde_json::from_value(params)
                    .map_err(|e| format!("{e}"))?;
                ::core::result::Result::Ok(Self::new(cfg))
            }
        }
    }
}

/// Generate the NodeConfigSchema implementation for the config struct
fn generate_schema_impl(struct_name: &syn::Ident, args: &NodeArgs) -> proc_macro2::TokenStream {
    let config_name = syn::Ident::new(&format!("{}Config", struct_name), struct_name.span());

    // Default node_type: remove "Node" suffix from struct name
    let node_type = args.node_type.clone().unwrap_or_else(|| {
        let name = struct_name.to_string();
        name.strip_suffix("Node").unwrap_or(&name).to_string()
    });

    let category = args
        .category
        .as_ref()
        .map(|c| quote! { Some(#c.to_string()) })
        .unwrap_or(quote! { None });

    let description = args
        .description
        .as_ref()
        .map(|d| quote! { Some(#d.to_string()) })
        .unwrap_or(quote! { None });

    let accepts_tokens = parse_runtime_data_types(&args.accepts);
    let produces_tokens = parse_runtime_data_types(&args.produces);
    let multi_output = args.multi_output;

    quote! {
        impl remotemedia_traits::schema::NodeConfigSchema for #config_name {
            fn node_type() -> &'static str {
                #node_type
            }

            fn category() -> Option<String> {
                #category
            }

            fn description() -> Option<String> {
                #description
            }

            fn accepts() -> Vec<remotemedia_traits::schema::RuntimeDataType> {
                vec![#accepts_tokens]
            }

            fn produces() -> Vec<remotemedia_traits::schema::RuntimeDataType> {
                vec![#produces_tokens]
            }

            fn multi_output() -> bool {
                #multi_output
            }

            fn config_json_schema() -> serde_json::Value {
                let schema = schemars::schema_for!(#config_name);
                serde_json::to_value(schema).unwrap_or_default()
            }

            fn default_config() -> Option<serde_json::Value>
            where
                Self: Default + serde::Serialize,
            {
                serde_json::to_value(Self::default()).ok()
            }
        }
    }
}

/// Generate the AsyncStreamingNode trait implementation
fn generate_trait_impl(struct_name: &syn::Ident, args: &NodeArgs) -> proc_macro2::TokenStream {
    // Default node_type: remove "Node" suffix from struct name
    let node_type = args.node_type.clone().unwrap_or_else(|| {
        let name = struct_name.to_string();
        name.strip_suffix("Node").unwrap_or(&name).to_string()
    });

    let multi_output = args.multi_output;
    let streaming = args.streaming;

    // For multi_output nodes, process() returns an error, process_streaming is used.
    // For nodes with `streaming = true` (additive form), process() still maps to
    // `process_impl` — the user-provided single-output implementation — and
    // process_streaming gets a separate override below.
    let process_impl = if multi_output {
        quote! {
            async fn process(&self, _data: remotemedia_types::RuntimeData) -> Result<remotemedia_types::RuntimeData, remotemedia_types::Error> {
                Err(remotemedia_types::Error::Execution(
                    concat!(stringify!(#struct_name), " requires streaming mode - use process_streaming()").into()
                ))
            }
        }
    } else {
        quote! {
            async fn process(&self, data: remotemedia_types::RuntimeData) -> Result<remotemedia_types::RuntimeData, remotemedia_types::Error> {
                #struct_name::process_impl(self, data).await
            }
        }
    };

    // Override process_streaming when EITHER:
    // - `multi_output = true`: the canonical multi-output node (process() errors;
    //   process_streaming_impl is the only entry point), OR
    // - `streaming = true`: the additive variant (process() still works via
    //   process_impl, but process_streaming_impl provides the multi-emission
    //   path used by the loadable FFI multi-output adapter and any in-tree
    //   streaming wrapper).
    let streaming_impl = if multi_output || streaming {
        quote! {
            async fn process_streaming<F>(
                &self,
                data: remotemedia_types::RuntimeData,
                session_id: Option<String>,
                callback: F,
            ) -> Result<usize, remotemedia_types::Error>
            where
                F: FnMut(remotemedia_types::RuntimeData) -> Result<(), remotemedia_types::Error> + Send,
            {
                #struct_name::process_streaming_impl(self, data, session_id, callback).await
            }
        }
    } else {
        quote! {}
    };

    quote! {
        #[async_trait::async_trait]
        impl remotemedia_traits::streaming::AsyncStreamingNode for #struct_name {
            fn node_type(&self) -> &str {
                #node_type
            }

            async fn initialize(&self, _ctx: &dyn remotemedia_traits::runtime_context::InitializeContextRead) -> Result<(), remotemedia_types::Error> {
                Ok(())
            }

            #process_impl

            #streaming_impl
        }
    }
}

/// Generate inventory registration for the config struct
fn generate_inventory_registration(struct_name: &syn::Ident) -> proc_macro2::TokenStream {
    let config_name = syn::Ident::new(&format!("{}Config", struct_name), struct_name.span());

    quote! {
        inventory::submit! {
            remotemedia_traits::schema::RegisteredNodeConfig::new(
                <#config_name as remotemedia_traits::schema::NodeConfigSchema>::to_node_schema
            )
        }
    }
}

// =============================================================================
// Capability DSL Parsing and Code Generation (spec 023)
// =============================================================================

/// Parse a capability behavior string into the corresponding enum variant.
///
/// Valid values: "static", "configured", "passthrough", "adaptive", "runtime_discovered"
fn parse_capability_behavior(behavior: &str) -> proc_macro2::TokenStream {
    match behavior.to_lowercase().as_str() {
        "static" => quote! { remotemedia_traits::capabilities::CapabilityBehavior::Static },
        "configured" => quote! { remotemedia_traits::capabilities::CapabilityBehavior::Configured },
        "passthrough" => {
            quote! { remotemedia_traits::capabilities::CapabilityBehavior::Passthrough }
        }
        "adaptive" => quote! { remotemedia_traits::capabilities::CapabilityBehavior::Adaptive },
        "runtime_discovered" | "runtimediscovered" => {
            quote! { remotemedia_traits::capabilities::CapabilityBehavior::RuntimeDiscovered }
        }
        _ => {
            quote! { compile_error!(concat!("Unknown capability behavior: ", #behavior, ". Expected: static, configured, passthrough, adaptive, runtime_discovered")) }
        }
    }
}

/// Parse a constraint value DSL string into code.
///
/// DSL formats:
/// - Exact: `16000` or `"F32"` (for enums)
/// - Range: `16000..48000`
/// - Set: `[16000, 44100, 48000]`
fn parse_constraint_value_dsl(value: &str, field_type: &str) -> proc_macro2::TokenStream {
    let trimmed = value.trim();

    // Check for range syntax: min..max
    if trimmed.contains("..") {
        let parts: Vec<&str> = trimmed.split("..").collect();
        if parts.len() == 2 {
            let min = parts[0].trim();
            let max = parts[1].trim();

            // Try to parse as integers
            if let (Ok(min_val), Ok(max_val)) = (min.parse::<u32>(), max.parse::<u32>()) {
                return quote! {
                    Some(remotemedia_traits::capabilities::ConstraintValue::Range {
                        min: #min_val,
                        max: #max_val,
                    })
                };
            }

            // Try as floats
            if let (Ok(min_val), Ok(max_val)) = (min.parse::<f32>(), max.parse::<f32>()) {
                return quote! {
                    Some(remotemedia_traits::capabilities::ConstraintValue::Range {
                        min: #min_val,
                        max: #max_val,
                    })
                };
            }
        }
    }

    // Check for set syntax: [val1, val2, ...]
    if trimmed.starts_with('[') && trimmed.ends_with(']') {
        let inner = &trimmed[1..trimmed.len() - 1];
        let values: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();

        // Try to parse as integers
        let parsed_ints: Result<Vec<u32>, _> = values.iter().map(|s| s.parse::<u32>()).collect();
        if let Ok(int_vals) = parsed_ints {
            return quote! {
                Some(remotemedia_traits::capabilities::ConstraintValue::Set(
                    vec![#(#int_vals),*]
                ))
            };
        }

        // Try as audio sample formats
        if field_type == "format" {
            let format_tokens: Vec<_> = values
                .iter()
                .map(|s| parse_audio_format_dsl(s.trim().trim_matches('"')))
                .collect();
            return quote! {
                Some(remotemedia_traits::capabilities::ConstraintValue::Set(
                    vec![#(#format_tokens),*]
                ))
            };
        }
    }

    // Exact value
    // Try integer
    if let Ok(int_val) = trimmed.parse::<u32>() {
        return quote! {
            Some(remotemedia_traits::capabilities::ConstraintValue::Exact(#int_val))
        };
    }

    // Try float
    if let Ok(float_val) = trimmed.parse::<f32>() {
        return quote! {
            Some(remotemedia_traits::capabilities::ConstraintValue::Exact(#float_val))
        };
    }

    // Try audio sample format
    if field_type == "format" {
        let format_token = parse_audio_format_dsl(trimmed.trim_matches('"'));
        return quote! {
            Some(remotemedia_traits::capabilities::ConstraintValue::Exact(#format_token))
        };
    }

    // String value
    quote! {
        Some(remotemedia_traits::capabilities::ConstraintValue::Exact(#trimmed.to_string()))
    }
}

/// Parse audio sample format string to enum variant.
fn parse_audio_format_dsl(format: &str) -> proc_macro2::TokenStream {
    match format.to_uppercase().as_str() {
        "F32" => quote! { remotemedia_traits::capabilities::AudioSampleFormat::F32 },
        "I16" => quote! { remotemedia_traits::capabilities::AudioSampleFormat::I16 },
        "I32" => quote! { remotemedia_traits::capabilities::AudioSampleFormat::I32 },
        "U8" => quote! { remotemedia_traits::capabilities::AudioSampleFormat::U8 },
        _ => quote! { compile_error!(concat!("Unknown audio format: ", #format)) },
    }
}

/// Parse input/output caps DSL string into MediaConstraints code.
///
/// DSL format: `media_type(field1=value1, field2=value2, ...)`
///
/// Examples:
/// - `audio(sample_rate=16000, channels=1, format=F32)`
/// - `audio(sample_rate=16000..48000, channels=1..2)`
/// - `video(width=1920, height=1080, framerate=30.0)`
/// - `text(encoding="UTF-8")`
/// - `binary`
fn parse_media_constraints_dsl(dsl: &str) -> proc_macro2::TokenStream {
    let trimmed = dsl.trim();

    // Handle simple types without parameters
    match trimmed.to_lowercase().as_str() {
        "binary" => return quote! { remotemedia_traits::capabilities::MediaConstraints::Binary },
        "text" => {
            return quote! { remotemedia_traits::capabilities::MediaConstraints::Text(Default::default()) }
        }
        "json" => {
            return quote! { remotemedia_traits::capabilities::MediaConstraints::Json(Default::default()) }
        }
        _ => {}
    }

    // Parse type(params) format
    let paren_start = match trimmed.find('(') {
        Some(idx) => idx,
        None => {
            // No parameters - just the type
            return match trimmed.to_lowercase().as_str() {
                "audio" => {
                    quote! { remotemedia_traits::capabilities::MediaConstraints::Audio(Default::default()) }
                }
                "video" => {
                    quote! { remotemedia_traits::capabilities::MediaConstraints::Video(Default::default()) }
                }
                "tensor" => {
                    quote! { remotemedia_traits::capabilities::MediaConstraints::Tensor(Default::default()) }
                }
                "file" => {
                    quote! { remotemedia_traits::capabilities::MediaConstraints::File(Default::default()) }
                }
                _ => quote! { compile_error!(concat!("Unknown media type: ", #trimmed)) },
            };
        }
    };

    let media_type = &trimmed[..paren_start].trim().to_lowercase();
    let params_str = &trimmed[paren_start + 1..trimmed.len() - 1]; // Strip ( and )

    // Parse parameters
    let params: Vec<(&str, &str)> = params_str
        .split(',')
        .filter(|s| !s.is_empty())
        .filter_map(|param| {
            let parts: Vec<&str> = param.splitn(2, '=').collect();
            if parts.len() == 2 {
                Some((parts[0].trim(), parts[1].trim()))
            } else {
                None
            }
        })
        .collect();

    match media_type.as_str() {
        "audio" => {
            let mut sample_rate = quote! { None };
            let mut channels = quote! { None };
            let mut format = quote! { None };

            for (key, value) in params {
                match key {
                    "sample_rate" | "samplerate" => {
                        sample_rate = parse_constraint_value_dsl(value, "sample_rate");
                    }
                    "channels" => {
                        channels = parse_constraint_value_dsl(value, "channels");
                    }
                    "format" => {
                        format = parse_constraint_value_dsl(value, "format");
                    }
                    _ => {}
                }
            }

            quote! {
                remotemedia_traits::capabilities::MediaConstraints::Audio(
                    remotemedia_traits::capabilities::AudioConstraints {
                        sample_rate: #sample_rate,
                        channels: #channels,
                        format: #format,
                    }
                )
            }
        }
        "video" => {
            let mut width = quote! { None };
            let mut height = quote! { None };
            let mut framerate = quote! { None };
            let mut pixel_format = quote! { None };

            for (key, value) in params {
                match key {
                    "width" => {
                        width = parse_constraint_value_dsl(value, "width");
                    }
                    "height" => {
                        height = parse_constraint_value_dsl(value, "height");
                    }
                    "framerate" | "fps" => {
                        framerate = parse_constraint_value_dsl(value, "framerate");
                    }
                    "pixel_format" | "format" => {
                        pixel_format = parse_constraint_value_dsl(value, "pixel_format");
                    }
                    _ => {}
                }
            }

            quote! {
                remotemedia_traits::capabilities::MediaConstraints::Video(
                    remotemedia_traits::capabilities::VideoConstraints {
                        width: #width,
                        height: #height,
                        framerate: #framerate,
                        pixel_format: #pixel_format,
                    }
                )
            }
        }
        "text" => {
            let mut encoding = quote! { None };
            let mut text_format = quote! { None };

            for (key, value) in params {
                match key {
                    "encoding" => {
                        encoding = parse_constraint_value_dsl(value, "encoding");
                    }
                    "format" => {
                        text_format = parse_constraint_value_dsl(value, "format");
                    }
                    _ => {}
                }
            }

            quote! {
                remotemedia_traits::capabilities::MediaConstraints::Text(
                    remotemedia_traits::capabilities::TextConstraints {
                        encoding: #encoding,
                        format: #text_format,
                    }
                )
            }
        }
        _ => quote! { compile_error!(concat!("Unknown media type: ", #media_type)) },
    }
}

/// Generate capability_behavior() implementation for StreamingNode and StreamingNodeFactory.
fn generate_capability_behavior_impl(
    struct_name: &syn::Ident,
    args: &NodeArgs,
) -> proc_macro2::TokenStream {
    // If no capabilities attribute, don't generate any implementation
    let behavior = match &args.capabilities {
        Some(b) => parse_capability_behavior(b),
        None => return quote! {},
    };

    let config_name = syn::Ident::new(&format!("{}Config", struct_name), struct_name.span());

    quote! {
        impl #struct_name {
            /// Returns the capability behavior for this node type.
            pub fn capability_behavior_static() -> remotemedia_traits::capabilities::CapabilityBehavior {
                #behavior
            }
        }

        impl #config_name {
            /// Returns the capability behavior for this node type.
            pub fn capability_behavior() -> remotemedia_traits::capabilities::CapabilityBehavior {
                #behavior
            }
        }
    }
}

/// Generate media_capabilities() implementation from input_caps/output_caps DSL.
fn generate_media_capabilities_impl(
    struct_name: &syn::Ident,
    args: &NodeArgs,
) -> proc_macro2::TokenStream {
    // Only generate if at least one of input_caps or output_caps is specified
    if args.input_caps.is_none() && args.output_caps.is_none() {
        return quote! {};
    }

    let config_name = syn::Ident::new(&format!("{}Config", struct_name), struct_name.span());

    let inputs_code = match &args.input_caps {
        Some(dsl) => {
            let constraints = parse_media_constraints_dsl(dsl);
            quote! {
                inputs.insert("default".to_string(), #constraints);
            }
        }
        None => quote! {},
    };

    let outputs_code = match &args.output_caps {
        Some(dsl) => {
            let constraints = parse_media_constraints_dsl(dsl);
            quote! {
                outputs.insert("default".to_string(), #constraints);
            }
        }
        None => quote! {},
    };

    quote! {
        impl #struct_name {
            /// Returns the media capabilities for this node.
            pub fn media_capabilities_static() -> remotemedia_traits::capabilities::MediaCapabilities {
                use std::collections::HashMap;
                let mut inputs: HashMap<String, remotemedia_traits::capabilities::MediaConstraints> = HashMap::new();
                let mut outputs: HashMap<String, remotemedia_traits::capabilities::MediaConstraints> = HashMap::new();
                #inputs_code
                #outputs_code
                remotemedia_traits::capabilities::MediaCapabilities { inputs, outputs }
            }
        }

        impl #config_name {
            /// Returns the media capabilities for this node type.
            pub fn media_capabilities(_params: &serde_json::Value) -> remotemedia_traits::capabilities::MediaCapabilities {
                #struct_name::media_capabilities_static()
            }
        }
    }
}

/// Generate the `#[cfg(feature = "loadable-export")]` FFI plumbing.
///
/// Emits, gated on the `loadable-export` feature of the **consuming
/// crate** (per Phase B / Task B1 decision):
///
/// * `${StructName}LoadableFactory` — a unit struct implementing
///   [`FfiNodeFactory`].
/// * A free `fn` that constructs the factory and wraps it in
///   [`FfiNodeFactoryBox`]. Used as the `make` field of
///   [`LoadableFactoryEntry`] — must be a named `fn` (not a closure)
///   because `inventory::submit!` requires a `const`-constructible
///   value.
/// * An `inventory::submit!` entry registering the factory so a
///   downstream cdylib calling `plugin_export!()` (no-arg form) picks
///   it up automatically.
///
/// The whole block is wrapped in `const _: () = { ... }` so its
/// `use` statements and helper types do not leak into the consuming
/// crate's namespace. The factory unit struct itself is also defined
/// inside the const block — callers can still discover it through the
/// inventory at runtime.
///
/// # Requirements on the consuming crate
///
/// When the `loadable-export` feature is on, the consuming crate must:
///
/// 1. Have `remotemedia-plugin-sdk` in its dep tree (declared as an
///    optional dep, gated on the `loadable-export` cargo feature). If
///    missing, rustc emits `could not find 'remotemedia_plugin_sdk' in
///    the list of imported crates`.
/// 2. Provide a `${StructName}::new_from_params(serde_json::Value) ->
///    Result<Self, String>` constructor. The `#[node]` macro
///    auto-generates this from the `#[config]` fields, so users who
///    write nodes with `#[node]` get it for free.
fn generate_loadable_export(struct_name: &syn::Ident, args: &NodeArgs) -> proc_macro2::TokenStream {
    let factory_name = syn::Ident::new(
        &format!("{}LoadableFactory", struct_name),
        struct_name.span(),
    );

    // snake_case the struct name for the make-fn identifier so two
    // factories in the same module don't collide.
    let make_fn_name = syn::Ident::new(
        &format!(
            "__remotemedia_make_{}_factory",
            to_snake_case(&struct_name.to_string())
        ),
        struct_name.span(),
    );

    // Resolve node_type with the same precedence as
    // `generate_trait_impl` and `generate_schema_impl`: explicit
    // `node_type = "..."` macro arg wins; otherwise default to the
    // struct name with a trailing "Node" suffix stripped. Without this
    // unification the in-tree factory and the loadable-side factory
    // can advertise different wire identifiers — e.g. an explicit
    // `node_type = "LFM25AudioOnnxNode"` would be honoured in-tree but
    // silently shortened to `"LFM25AudioOnnx"` across the dlopen
    // boundary.
    let node_type_lit = args.node_type.clone().unwrap_or_else(|| {
        let name = struct_name.to_string();
        name.strip_suffix("Node").unwrap_or(&name).to_string()
    });

    quote! {
        #[cfg(feature = "loadable-export")]
        const _: () = {
            use ::remotemedia_plugin_sdk::abi_stable::sabi_trait::TD_Opaque;
            use ::remotemedia_plugin_sdk::abi_stable::std_types::{RErr, ROk, RResult, RString};
            use ::remotemedia_plugin_sdk::adapter::StreamingNodeFfiAdapter;
            use ::remotemedia_plugin_sdk::{
                FfiNodeBox, FfiNodeFactory, FfiNodeFactoryBox, FfiNodeFactory_TO, FfiNode_TO,
                LoadableFactoryEntry,
            };

            #[derive(Default)]
            pub struct #factory_name;

            impl FfiNodeFactory for #factory_name {
                fn node_type(&self) -> RString {
                    RString::from(#node_type_lit)
                }

                fn create(&self, params_json: RString) -> RResult<FfiNodeBox, RString> {
                    let value: ::serde_json::Value =
                        match ::serde_json::from_str(params_json.as_str()) {
                            Ok(v) => v,
                            Err(e) => return RErr(RString::from(format!("params parse: {e}"))),
                        };
                    let node = match #struct_name::new_from_params(value) {
                        Ok(n) => n,
                        Err(e) => return RErr(RString::from(format!("init: {e}"))),
                    };
                    ROk(FfiNode_TO::from_value(
                        StreamingNodeFfiAdapter::new(node),
                        TD_Opaque,
                    ))
                }
            }

            fn #make_fn_name() -> FfiNodeFactoryBox {
                FfiNodeFactory_TO::from_value(#factory_name::default(), TD_Opaque)
            }

            ::remotemedia_plugin_sdk::inventory::submit! {
                LoadableFactoryEntry { make: #make_fn_name }
            }
        };
    }
}

/// Convert a CamelCase / PascalCase identifier to snake_case.
fn to_snake_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for (i, ch) in s.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

/// Unified node definition macro.
///
/// Combines config struct generation, `AsyncStreamingNode` trait implementation,
/// and TypeScript type registration into a single declarative definition.
///
/// # Field Attributes
///
/// - `#[config]` - Field included in generated `{NodeName}Config` struct
/// - `#[config(default = expr)]` - Config field with custom default value
/// - `#[state]` - Field kept in node struct but excluded from config
/// - `#[state(default = expr)]` - State field with custom default value
///
/// # Node Attributes
///
/// - `node_type` - Node type identifier (defaults to struct name minus "Node")
/// - `category` - Category for grouping ("audio", "video", "ml", "text", "utility")
/// - `description` - Human-readable description
/// - `accepts` - Accepted input types (comma-separated: "audio", "text", etc.)
/// - `produces` - Produced output types (comma-separated)
/// - `multi_output` - Flag for multi-output streaming nodes
///
/// # Capability Attributes (spec 023)
///
/// - `capabilities` - Capability behavior: "static", "configured", "passthrough", "adaptive", "runtime_discovered"
/// - `input_caps` - Input capability constraints DSL (e.g., "audio(sample_rate=16000, channels=1)")
/// - `output_caps` - Output capability constraints DSL (e.g., "text")
///
/// ## Capability DSL Syntax
///
/// The `input_caps` and `output_caps` attributes use a simple DSL for declaring constraints:
///
/// ### Media Types
/// - `audio(sample_rate=16000, channels=1, format=F32)`
/// - `video(width=1920, height=1080, framerate=30.0)`
/// - `text(encoding="UTF-8")`
/// - `binary`
///
/// ### Constraint Values
/// - Exact: `16000` or `"F32"`
/// - Range: `16000..48000`
/// - Set: `[16000, 44100, 48000]`
///
/// # Examples
///
/// ## Basic Node
///
/// ```ignore
/// #[node(
///     node_type = "Echo",
///     category = "utility",
///     accepts = "text",
///     produces = "text"
/// )]
/// pub struct EchoNode {
///     #[config(default = "Echo: ".to_string())]
///     pub prefix: String,
///
///     #[state]
///     call_count: u64,
/// }
///
/// impl EchoNode {
///     async fn process_impl(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
///         // Your implementation here
///     }
/// }
/// ```
///
/// ## Node with Static Capabilities (spec 023)
///
/// ```ignore
/// #[node(
///     node_type = "Whisper",
///     category = "ml",
///     capabilities = "static",
///     input_caps = "audio(sample_rate=16000, channels=1, format=F32)",
///     output_caps = "text"
/// )]
/// pub struct WhisperNode {
///     #[config]
///     pub model: String,
///
///     #[state]
///     model_loaded: bool,
/// }
/// ```
///
/// ## Node with Adaptive Capabilities (spec 023)
///
/// ```ignore
/// #[node(
///     node_type = "AudioResample",
///     category = "audio",
///     capabilities = "adaptive",
///     input_caps = "audio(sample_rate=8000..192000)",
///     output_caps = "audio"  // Output adapts to downstream requirements
/// )]
/// pub struct AudioResampleNode {
///     #[state]
///     resampler: Option<Resampler>,
/// }
/// ```
#[proc_macro_attribute]
pub fn node(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let attr_args = parse_macro_input!(attr as syn::AttributeArgs);

    // Parse the attribute arguments
    let args = match parse_node_args(&attr_args) {
        Ok(args) => args,
        Err(e) => return e.into_compile_error().into(),
    };

    let struct_name = &input.ident;
    let vis = &input.vis;

    // Extract fields from struct
    let fields = match &input.data {
        syn::Data::Struct(data) => match &data.fields {
            syn::Fields::Named(named) => &named.named,
            _ => {
                return syn::Error::new_spanned(
                    &input,
                    "#[node] only supports structs with named fields",
                )
                .into_compile_error()
                .into()
            }
        },
        _ => {
            return syn::Error::new_spanned(&input, "#[node] only supports structs")
                .into_compile_error()
                .into()
        }
    };

    // Classify fields as config or state
    let classified_fields = match classify_fields(fields) {
        Ok(fields) => fields,
        Err(e) => return e.into_compile_error().into(),
    };

    // Preserve doc comments and other attributes from original struct
    let struct_attrs: Vec<_> = input
        .attrs
        .iter()
        .filter(|attr| attr.path.is_ident("doc") || attr.path.is_ident("allow"))
        .collect();

    // Generate all components
    let config_struct = generate_config_struct(struct_name, vis, &classified_fields, &struct_attrs);
    let node_struct = generate_node_struct(struct_name, vis, &classified_fields, &struct_attrs);
    let schema_impl = generate_schema_impl(struct_name, &args);
    let trait_impl = generate_trait_impl(struct_name, &args);
    let inventory_reg = generate_inventory_registration(struct_name);

    // Generate capability-related implementations (spec 023)
    let capability_behavior_impl = generate_capability_behavior_impl(struct_name, &args);
    let media_capabilities_impl = generate_media_capabilities_impl(struct_name, &args);

    // Generate FFI plumbing under the consuming crate's
    // `loadable-export` cargo feature (Phase B / Task B2). Always emit
    // the tokens; the `#[cfg(feature = "loadable-export")]` inside
    // `generate_loadable_export` decides whether they compile.
    let loadable_export = generate_loadable_export(struct_name, &args);

    let expanded = quote! {
        #config_struct
        #node_struct
        #schema_impl
        #trait_impl
        #inventory_reg
        #capability_behavior_impl
        #media_capabilities_impl
        #loadable_export
    };

    TokenStream::from(expanded)
}

// =============================================================================
// #[node_config] macro - Original config-only macro (preserved for compatibility)
// =============================================================================

/// Attribute macro for node configuration structs.
///
/// Automatically derives all necessary traits, adds serde attributes,
/// and registers the schema for TypeScript type generation.
///
/// # Attributes
///
/// - `node_type`: The node type name (defaults to struct name without "Config" suffix)
/// - `category`: Category for grouping ("audio", "video", "ml", "text", "utility")
/// - `description`: Human-readable description
/// - `accepts`: Accepted input types (comma-separated)
/// - `produces`: Produced output types (comma-separated)
/// - `multi_output`: Whether this node produces multiple outputs per input
///
/// # Example
///
/// ```ignore
/// #[node_config(node_type = "AudioResample", category = "audio")]
/// pub struct AudioResampleConfig {
///     pub target_sample_rate: u32,
/// }
/// ```
#[proc_macro_attribute]
pub fn node_config(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let attr_args = parse_macro_input!(attr as syn::AttributeArgs);

    // Parse the attribute arguments
    let args = match parse_node_config_args(&attr_args) {
        Ok(args) => args,
        Err(e) => return e.into_compile_error().into(),
    };

    let struct_name = &input.ident;
    let vis = &input.vis;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    // Extract fields from struct
    let fields = match &input.data {
        syn::Data::Struct(data) => match &data.fields {
            syn::Fields::Named(fields) => &fields.named,
            _ => {
                return syn::Error::new_spanned(
                    &input,
                    "node_config only supports structs with named fields",
                )
                .into_compile_error()
                .into()
            }
        },
        _ => {
            return syn::Error::new_spanned(&input, "node_config only supports structs")
                .into_compile_error()
                .into()
        }
    };

    // Preserve doc comments and other attributes from original struct
    let struct_attrs: Vec<_> = input
        .attrs
        .iter()
        .filter(|attr| attr.path.is_ident("doc") || attr.path.is_ident("allow"))
        .collect();

    // Default node_type: remove "Config" suffix from struct name
    let node_type = args.node_type.unwrap_or_else(|| {
        let name = struct_name.to_string();
        name.strip_suffix("Config").unwrap_or(&name).to_string()
    });

    let category = args
        .category
        .map(|c| quote! { Some(#c.to_string()) })
        .unwrap_or(quote! { None });
    let description = args
        .description
        .map(|d| quote! { Some(#d.to_string()) })
        .unwrap_or(quote! { None });

    // Parse accepts/produces as RuntimeDataType variants
    let accepts_tokens = parse_runtime_data_types(&args.accepts);
    let produces_tokens = parse_runtime_data_types(&args.produces);

    let multi_output = args.multi_output;
    let generics = &input.generics;

    let expanded = quote! {
        #(#struct_attrs)*
        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
        #[serde(default, rename_all = "camelCase")]
        #vis struct #struct_name #generics #where_clause {
            #fields
        }

        // Implement the NodeConfigSchema trait
        impl #impl_generics crate::nodes::schema::NodeConfigSchema for #struct_name #ty_generics #where_clause {
            fn node_type() -> &'static str {
                #node_type
            }

            fn category() -> Option<String> {
                #category
            }

            fn description() -> Option<String> {
                #description
            }

            fn accepts() -> Vec<crate::nodes::schema::RuntimeDataType> {
                vec![#accepts_tokens]
            }

            fn produces() -> Vec<crate::nodes::schema::RuntimeDataType> {
                vec![#produces_tokens]
            }

            fn multi_output() -> bool {
                #multi_output
            }

            fn config_json_schema() -> serde_json::Value {
                let schema = schemars::schema_for!(#struct_name);
                serde_json::to_value(schema).unwrap_or_default()
            }

            fn default_config() -> Option<serde_json::Value>
            where
                Self: Default + serde::Serialize,
            {
                serde_json::to_value(Self::default()).ok()
            }
        }

        // Auto-register via inventory
        inventory::submit! {
            crate::nodes::schema::RegisteredNodeConfig::new(
                <#struct_name as crate::nodes::schema::NodeConfigSchema>::to_node_schema
            )
        }
    };

    TokenStream::from(expanded)
}

/// Parsed arguments from `#[node_config(...)]`
struct NodeConfigArgs {
    node_type: Option<String>,
    category: Option<String>,
    description: Option<String>,
    accepts: Option<String>,
    produces: Option<String>,
    multi_output: bool,
}

fn parse_node_config_args(args: &[NestedMeta]) -> Result<NodeConfigArgs, syn::Error> {
    let mut node_type = None;
    let mut category = None;
    let mut description = None;
    let mut accepts = None;
    let mut produces = None;
    let mut multi_output = false;

    for arg in args {
        match arg {
            NestedMeta::Meta(Meta::NameValue(nv)) => {
                let ident = nv
                    .path
                    .get_ident()
                    .ok_or_else(|| syn::Error::new_spanned(&nv.path, "expected identifier"))?;

                match ident.to_string().as_str() {
                    "node_type" => {
                        if let Lit::Str(s) = &nv.lit {
                            node_type = Some(s.value());
                        }
                    }
                    "category" => {
                        if let Lit::Str(s) = &nv.lit {
                            category = Some(s.value());
                        }
                    }
                    "description" => {
                        if let Lit::Str(s) = &nv.lit {
                            description = Some(s.value());
                        }
                    }
                    "accepts" => {
                        if let Lit::Str(s) = &nv.lit {
                            accepts = Some(s.value());
                        }
                    }
                    "produces" => {
                        if let Lit::Str(s) = &nv.lit {
                            produces = Some(s.value());
                        }
                    }
                    "multi_output" => {
                        if let Lit::Bool(b) = &nv.lit {
                            multi_output = b.value;
                        }
                    }
                    other => {
                        return Err(syn::Error::new_spanned(
                            &nv.path,
                            format!("unknown attribute: {}", other),
                        ));
                    }
                }
            }
            NestedMeta::Meta(Meta::Path(path)) => {
                if path.is_ident("multi_output") {
                    multi_output = true;
                }
            }
            _ => {}
        }
    }

    Ok(NodeConfigArgs {
        node_type,
        category,
        description,
        accepts,
        produces,
        multi_output,
    })
}

/// Parse comma-separated RuntimeDataType names into tokens
fn parse_runtime_data_types(types: &Option<String>) -> proc_macro2::TokenStream {
    match types {
        Some(s) if !s.is_empty() => {
            let types: Vec<_> = s
                .split(',')
                .map(|t| t.trim())
                .filter(|t| !t.is_empty())
                .map(|t| {
                    let variant = match t.to_lowercase().as_str() {
                        "audio" => quote! { remotemedia_traits::schema::RuntimeDataType::Audio },
                        "video" => quote! { remotemedia_traits::schema::RuntimeDataType::Video },
                        "json" => quote! { remotemedia_traits::schema::RuntimeDataType::Json },
                        "text" => quote! { remotemedia_traits::schema::RuntimeDataType::Text },
                        "binary" => quote! { remotemedia_traits::schema::RuntimeDataType::Binary },
                        "tensor" => quote! { remotemedia_traits::schema::RuntimeDataType::Tensor },
                        "numpy" => quote! { remotemedia_traits::schema::RuntimeDataType::Numpy },
                        "control" | "controlmessage" => {
                            quote! { remotemedia_traits::schema::RuntimeDataType::ControlMessage }
                        }
                        _ => quote! { compile_error!(concat!("Unknown RuntimeDataType: ", #t)) },
                    };
                    variant
                })
                .collect();
            quote! { #(#types),* }
        }
        _ => quote! {},
    }
}

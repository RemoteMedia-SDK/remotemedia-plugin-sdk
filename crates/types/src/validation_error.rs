//! Validation error types shared by host + plugin code.
//!
//! Plain-data leaf types (no heavy deps). Lifted out of
//! `remotemedia-core::validation` so the trait-surface `Error` can
//! reference [`ValidationError`] without forcing plugins to link the
//! host crate.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Constraint type that was violated
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ValidationConstraint {
    /// Value has wrong JSON type (e.g., string instead of number)
    Type,
    /// Required parameter is missing
    Required,
    /// Numeric value below minimum
    Minimum,
    /// Numeric value above maximum
    Maximum,
    /// Numeric value at or below exclusive minimum
    ExclusiveMinimum,
    /// Numeric value at or above exclusive maximum
    ExclusiveMaximum,
    /// Value not in allowed enum set
    Enum,
    /// String doesn't match regex pattern
    Pattern,
    /// String shorter than minimum length
    MinLength,
    /// String longer than maximum length
    MaxLength,
    /// Array has fewer items than minimum
    MinItems,
    /// Array has more items than maximum
    MaxItems,
    /// Object has properties not allowed by schema
    AdditionalProperties,
    /// Value doesn't match format (e.g., "uri", "email")
    Format,
    /// Other JSON Schema constraint
    Other(String),
}

impl ValidationConstraint {
    /// Get a human-readable description of this constraint type
    pub fn description(&self) -> &str {
        match self {
            ValidationConstraint::Type => "type mismatch",
            ValidationConstraint::Required => "required parameter missing",
            ValidationConstraint::Minimum => "value below minimum",
            ValidationConstraint::Maximum => "value above maximum",
            ValidationConstraint::ExclusiveMinimum => "value at or below exclusive minimum",
            ValidationConstraint::ExclusiveMaximum => "value at or above exclusive maximum",
            ValidationConstraint::Enum => "value not in allowed set",
            ValidationConstraint::Pattern => "string doesn't match pattern",
            ValidationConstraint::MinLength => "string too short",
            ValidationConstraint::MaxLength => "string too long",
            ValidationConstraint::MinItems => "array has too few items",
            ValidationConstraint::MaxItems => "array has too many items",
            ValidationConstraint::AdditionalProperties => "unexpected property",
            ValidationConstraint::Format => "invalid format",
            ValidationConstraint::Other(_) => "constraint violation",
        }
    }
}

impl fmt::Display for ValidationConstraint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationConstraint::Other(s) => write!(f, "{}", s),
            _ => write!(f, "{}", self.description()),
        }
    }
}

/// A single validation error for a node parameter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationError {
    /// Node ID from the manifest
    pub node_id: String,
    /// Node type (e.g., "SileroVAD")
    pub node_type: String,
    /// JSON pointer path to the invalid parameter (e.g., "/threshold")
    pub path: String,
    /// Type of constraint violated
    pub constraint: ValidationConstraint,
    /// Human-readable expected value description
    pub expected: String,
    /// String representation of the actual value
    pub received: String,
    /// Complete error message
    pub message: String,
}

impl ValidationError {
    /// Create a new validation error with auto-generated message
    pub fn new(
        node_id: impl Into<String>,
        node_type: impl Into<String>,
        path: impl Into<String>,
        constraint: ValidationConstraint,
        expected: impl Into<String>,
        received: impl Into<String>,
    ) -> Self {
        let node_id = node_id.into();
        let node_type = node_type.into();
        let path = path.into();
        let expected = expected.into();
        let received = received.into();

        let message = Self::format_message(
            &node_id,
            &node_type,
            &path,
            &constraint,
            &expected,
            &received,
        );

        Self {
            node_id,
            node_type,
            path,
            constraint,
            expected,
            received,
            message,
        }
    }

    /// Format a human-readable error message
    fn format_message(
        node_id: &str,
        node_type: &str,
        path: &str,
        constraint: &ValidationConstraint,
        expected: &str,
        received: &str,
    ) -> String {
        // Extract parameter name from JSON pointer path
        let param_name = path.trim_start_matches('/').replace('/', ".");
        let param_display = if param_name.is_empty() {
            "(root)".to_string()
        } else {
            format!("'{}'", param_name)
        };

        match constraint {
            ValidationConstraint::Type => {
                format!(
                    "Node '{}' ({}): parameter {} expected type '{}', got {}",
                    node_id, node_type, param_display, expected, received
                )
            }
            ValidationConstraint::Required => {
                // For Required errors, the property name is in the expected field
                // (format: "property 'name'")
                format!(
                    "Node '{}' ({}): required parameter {} is missing",
                    node_id, node_type, expected
                )
            }
            ValidationConstraint::Minimum => {
                format!(
                    "Node '{}' ({}): parameter {} must be >= {}, got {}",
                    node_id, node_type, param_display, expected, received
                )
            }
            ValidationConstraint::Maximum => {
                format!(
                    "Node '{}' ({}): parameter {} must be <= {}, got {}",
                    node_id, node_type, param_display, expected, received
                )
            }
            ValidationConstraint::ExclusiveMinimum => {
                format!(
                    "Node '{}' ({}): parameter {} must be > {}, got {}",
                    node_id, node_type, param_display, expected, received
                )
            }
            ValidationConstraint::ExclusiveMaximum => {
                format!(
                    "Node '{}' ({}): parameter {} must be < {}, got {}",
                    node_id, node_type, param_display, expected, received
                )
            }
            ValidationConstraint::Enum => {
                format!(
                    "Node '{}' ({}): parameter {} must be one of [{}], got {}",
                    node_id, node_type, param_display, expected, received
                )
            }
            ValidationConstraint::Pattern => {
                format!(
                    "Node '{}' ({}): parameter {} must match pattern '{}', got {}",
                    node_id, node_type, param_display, expected, received
                )
            }
            ValidationConstraint::MinLength => {
                format!(
                    "Node '{}' ({}): parameter {} must have length >= {}, got {}",
                    node_id, node_type, param_display, expected, received
                )
            }
            ValidationConstraint::MaxLength => {
                format!(
                    "Node '{}' ({}): parameter {} must have length <= {}, got {}",
                    node_id, node_type, param_display, expected, received
                )
            }
            ValidationConstraint::MinItems => {
                format!(
                    "Node '{}' ({}): parameter {} must have at least {} items, got {}",
                    node_id, node_type, param_display, expected, received
                )
            }
            ValidationConstraint::MaxItems => {
                format!(
                    "Node '{}' ({}): parameter {} must have at most {} items, got {}",
                    node_id, node_type, param_display, expected, received
                )
            }
            ValidationConstraint::AdditionalProperties => {
                format!(
                    "Node '{}' ({}): parameter {} contains unexpected property",
                    node_id, node_type, param_display
                )
            }
            ValidationConstraint::Format => {
                format!(
                    "Node '{}' ({}): parameter {} must be a valid '{}', got {}",
                    node_id, node_type, param_display, expected, received
                )
            }
            ValidationConstraint::Other(constraint_name) => {
                format!(
                    "Node '{}' ({}): parameter {} failed constraint '{}': expected {}, got {}",
                    node_id, node_type, param_display, constraint_name, expected, received
                )
            }
        }
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ValidationError {}

/// Warning type for non-fatal issues
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WarningType {
    /// Node type has no schema defined
    MissingSchema,
    /// Parameter is deprecated but still accepted
    DeprecatedParameter,
}

impl fmt::Display for WarningType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WarningType::MissingSchema => write!(f, "missing_schema"),
            WarningType::DeprecatedParameter => write!(f, "deprecated_parameter"),
        }
    }
}

/// A validation warning (non-fatal)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationWarning {
    /// Node ID from the manifest
    pub node_id: String,
    /// Node type (e.g., "SileroVAD")
    pub node_type: String,
    /// Type of warning
    pub warning_type: WarningType,
    /// Warning message
    pub message: String,
}

impl fmt::Display for ValidationWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_error_type_mismatch() {
        let error = ValidationError::new(
            "vad_node",
            "SileroVAD",
            "/threshold",
            ValidationConstraint::Type,
            "number",
            "\"high\"",
        );

        assert_eq!(error.node_id, "vad_node");
        assert_eq!(error.node_type, "SileroVAD");
        assert_eq!(error.path, "/threshold");
        assert!(error.message.contains("expected type 'number'"));
        assert!(error.message.contains("got \"high\""));
    }

    #[test]
    fn test_validation_error_required() {
        // Note: For Required errors, the expected field contains the property name
        // (as produced by schema_validator.rs when converting jsonschema errors)
        let error = ValidationError::new(
            "tts",
            "KokoroTTSNode",
            "",
            ValidationConstraint::Required,
            "property 'voice'",
            "(missing)",
        );

        assert!(error
            .message
            .contains("required parameter property 'voice' is missing"));
    }

    #[test]
    fn test_validation_error_range() {
        let error = ValidationError::new(
            "vad",
            "SileroVAD",
            "/threshold",
            ValidationConstraint::Maximum,
            "1.0",
            "1.5",
        );

        assert!(error.message.contains("must be <= 1.0"));
        assert!(error.message.contains("got 1.5"));
    }

    #[test]
    fn test_validation_error_display() {
        let error = ValidationError::new(
            "node1",
            "TestNode",
            "/param",
            ValidationConstraint::Type,
            "number",
            "string",
        );

        let displayed = format!("{}", error);
        assert!(displayed.contains("Node 'node1'"));
        assert!(displayed.contains("TestNode"));
    }

    #[test]
    fn test_validation_constraint_display() {
        assert_eq!(format!("{}", ValidationConstraint::Type), "type mismatch");
        assert_eq!(
            format!("{}", ValidationConstraint::Other("custom".to_string())),
            "custom"
        );
    }

    #[test]
    fn test_nested_path_display() {
        let error = ValidationError::new(
            "node",
            "Type",
            "/config/audio/sample_rate",
            ValidationConstraint::Type,
            "integer",
            "string",
        );

        assert!(error.message.contains("'config.audio.sample_rate'"));
    }
}

//! The generator role (openspec: add-generator-cancellation).
//!
//! A node that generates text, audio, video or images is a *generator* and
//! must be [`Cancellable`]: it states how it stops a cancelled call
//! ([`CancelStrategy`]), may stop out-of-band work in
//! [`Cancellable::on_cancel`], and applies the cancel's
//! [`Disposition`](crate::cancel::Disposition) to its own state.
//!
//! A node declares the role from its streaming trait:
//!
//! ```ignore
//! impl AudioGenerator for MyTts {}
//! #[async_trait]
//! impl Cancellable for MyTts {
//!     fn cancel_strategy(&self) -> CancelStrategy { CancelStrategy::ObservesToken }
//!     fn default_disposition(&self, _: &CancelContext) -> Disposition {
//!         // Queued text belongs to the cancelled reply.
//!         Disposition { pending_input: Retention::Discard, ..Disposition::INTERRUPT }
//!     }
//! }
//! impl AsyncStreamingNode for MyTts {
//!     fn generator(&self) -> Option<GeneratorRole<'_>> { GeneratorRole::audio(self) }
//!     // ...
//! }
//! ```
//!
//! `GeneratorRole::audio` only accepts an `AudioGenerator`, which requires
//! `Cancellable`, so a node that claims the role without implementing
//! cancellation does not compile. Speech-to-text and voice conversion are not
//! generators.

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::cancel::{CancelContext, Cancellation, Disposition};
use crate::rpc::CallContext;

/// What a generator produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GeneratedModality {
    Text,
    Audio,
    /// Includes avatar, lip-sync and motion renderers.
    Video,
    Image,
}

impl GeneratedModality {
    pub const ALL: [GeneratedModality; 4] = [
        GeneratedModality::Text,
        GeneratedModality::Audio,
        GeneratedModality::Video,
        GeneratedModality::Image,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            GeneratedModality::Text => "text",
            GeneratedModality::Audio => "audio",
            GeneratedModality::Video => "video",
            GeneratedModality::Image => "image",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|m| m.as_str() == s)
    }
}

/// How a generator stops a cancelled call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CancelStrategy {
    /// All generation happens inside the call future; dropping it stops the
    /// work (a streamed HTTP response, an awaited token loop).
    DropStopsWork,
    /// Work outside the future polls or subscribes to the call's
    /// [`CancelToken`](crate::cancel::CancelToken).
    ObservesToken,
    /// The node stops work out of band in [`Cancellable::on_cancel`] (abort a
    /// remote request, stop a model thread), and may also observe the token.
    OnCancel,
}

impl CancelStrategy {
    pub const ALL: [CancelStrategy; 3] = [
        CancelStrategy::DropStopsWork,
        CancelStrategy::ObservesToken,
        CancelStrategy::OnCancel,
    ];
}

/// How a generator stops, and what it does with data, on a cancel.
#[async_trait]
pub trait Cancellable: Send + Sync {
    /// How this node stops a cancelled call. No default: choosing is the point.
    fn cancel_strategy(&self) -> CancelStrategy;

    /// The disposition this node wants when neither the canceller nor the
    /// manifest chose one. Defaults to the reason's default (barge-in and
    /// supersession interrupt, everything else aborts). A TTS node usually
    /// discards pending input (its queued text belongs to the cancelled
    /// reply); a node may decide per cancel, e.g. roll back a reply the user
    /// cut into early.
    fn default_disposition(&self, cancel: &CancelContext) -> Disposition {
        cancel.reason.default_disposition()
    }

    /// Called once per cancelled call, after the call's token fired and
    /// before its future is dropped (it may run concurrently with the tail of
    /// the call, so it must be idempotent). Stop out-of-band work and apply
    /// `cancellation.disposition` to the node's own state.
    async fn on_cancel(&self, _call: &CallContext, _cancellation: &Cancellation) {}
}

/// A node that generates text. See [`GeneratorRole::text`].
pub trait TextGenerator: Cancellable {}
/// A node that generates audio. See [`GeneratorRole::audio`].
pub trait AudioGenerator: Cancellable {}
/// A node that generates video, avatar, lip-sync or motion output.
pub trait VideoGenerator: Cancellable {}
/// A node that generates images.
pub trait ImageGenerator: Cancellable {}

/// A node's declared generator role: what it produces and its cancellation.
/// Built only through the typed constructors, so holding one proves the node
/// implements the matching generator trait.
#[derive(Clone, Copy)]
pub struct GeneratorRole<'a> {
    modality: GeneratedModality,
    node: &'a dyn Cancellable,
}

impl std::fmt::Debug for GeneratorRole<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GeneratorRole")
            .field("modality", &self.modality)
            .field("cancel", &self.node.cancel_strategy())
            .finish()
    }
}

impl<'a> GeneratorRole<'a> {
    pub fn text<T: TextGenerator>(node: &'a T) -> Option<Self> {
        Some(Self::new(GeneratedModality::Text, node))
    }
    pub fn audio<T: AudioGenerator>(node: &'a T) -> Option<Self> {
        Some(Self::new(GeneratedModality::Audio, node))
    }
    pub fn video<T: VideoGenerator>(node: &'a T) -> Option<Self> {
        Some(Self::new(GeneratedModality::Video, node))
    }
    pub fn image<T: ImageGenerator>(node: &'a T) -> Option<Self> {
        Some(Self::new(GeneratedModality::Image, node))
    }

    fn new(modality: GeneratedModality, node: &'a dyn Cancellable) -> Self {
        GeneratorRole { modality, node }
    }

    pub fn modality(&self) -> GeneratedModality {
        self.modality
    }

    pub fn cancellable(&self) -> &'a dyn Cancellable {
        self.node
    }

    pub fn declaration(&self) -> GeneratorDeclaration {
        GeneratorDeclaration {
            generates: Some(self.modality),
            cancel: Some(self.node.cancel_strategy()),
        }
    }
}

/// What a factory, schema or plugin description declares about the role
/// before a node exists. Both `None` declares nothing (not a generator, or a
/// plugin built before this contract).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GeneratorDeclaration {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generates: Option<GeneratedModality>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel: Option<CancelStrategy>,
}

/// Why a declaration is not admissible.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GeneratorDeclarationError {
    #[error("node `{node_type}` declares that it generates {modality} but declares no cancel strategy; generators must implement Cancellable")]
    MissingCancel {
        node_type: String,
        modality: &'static str,
    },
    #[error("node `{node_type}` declares a cancel strategy but no generated modality")]
    CancelWithoutGenerates { node_type: String },
    #[error("node `{node_type}`: its factory declares {declared:?} but the created node declares {actual:?}")]
    Mismatch {
        node_type: String,
        declared: GeneratorDeclaration,
        actual: GeneratorDeclaration,
    },
}

impl GeneratorDeclaration {
    pub const NONE: GeneratorDeclaration = GeneratorDeclaration {
        generates: None,
        cancel: None,
    };

    pub fn is_generator(&self) -> bool {
        self.generates.is_some()
    }

    /// The admission rule: `generates` requires `cancel`.
    pub fn validate(&self, node_type: &str) -> Result<(), GeneratorDeclarationError> {
        match (self.generates, self.cancel) {
            (Some(m), None) => Err(GeneratorDeclarationError::MissingCancel {
                node_type: node_type.to_string(),
                modality: m.as_str(),
            }),
            (None, Some(_)) => Err(GeneratorDeclarationError::CancelWithoutGenerates {
                node_type: node_type.to_string(),
            }),
            _ => Ok(()),
        }
    }

    /// A created node must declare what its factory declared, unless the
    /// factory declared nothing.
    pub fn check_instance(
        &self,
        node_type: &str,
        actual: GeneratorDeclaration,
    ) -> Result<(), GeneratorDeclarationError> {
        if *self == GeneratorDeclaration::NONE || *self == actual {
            return Ok(());
        }
        Err(GeneratorDeclarationError::Mismatch {
            node_type: node_type.to_string(),
            declared: *self,
            actual,
        })
    }
}

/// Schema categories whose nodes are expected to generate. A node in one of
/// these categories that declares no generator role fails the registry audit.
pub const GENERATIVE_CATEGORIES: &[&str] = &[
    "llm",
    "tts",
    "t2i",
    "video",
    "avatar",
    "motion",
    "text-generation",
];

pub fn is_generative_category(category: &str) -> bool {
    let c = category.to_ascii_lowercase();
    GENERATIVE_CATEGORIES.contains(&c.as_str())
}

/// The JSON Schema of the cancellation contract shared with Python
/// (`schema/cancellation.schema.json`). Python's `remotemedia.core.cancel` is
/// checked against it, so the two cannot drift.
pub fn cancellation_contract_schema() -> serde_json::Value {
    #[derive(JsonSchema)]
    #[allow(dead_code)]
    struct CancellationContract {
        cancellation: crate::cancel::Cancellation,
        disposition_override: crate::cancel::DispositionOverride,
        generator_declaration: GeneratorDeclaration,
        generated_modality: GeneratedModality,
        cancel_strategy: CancelStrategy,
        cancel_reason: crate::cancel::CancelReason,
        retention: crate::cancel::Retention,
    }
    serde_json::to_value(schemars::schema_for!(CancellationContract)).expect("schema serializes")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cancel::{CancelReason, Retention};

    /// Regenerate with `RM_UPDATE_SCHEMA=1 cargo test -p remotemedia-traits contract_schema`.
    #[test]
    fn contract_schema_is_checked_in() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("schema")
            .join("cancellation.schema.json");
        let current = serde_json::to_string_pretty(&cancellation_contract_schema()).unwrap() + "\n";
        if std::env::var_os("RM_UPDATE_SCHEMA").is_some() {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, &current).unwrap();
        }
        let checked_in = std::fs::read_to_string(&path)
            .unwrap_or_default()
            .replace("\r\n", "\n");
        assert_eq!(
            checked_in, current,
            "schema/cancellation.schema.json is stale; rerun with RM_UPDATE_SCHEMA=1"
        );
    }

    struct Tts;
    #[async_trait]
    impl Cancellable for Tts {
        fn cancel_strategy(&self) -> CancelStrategy {
            CancelStrategy::ObservesToken
        }
        fn default_disposition(&self, _: &CancelContext) -> Disposition {
            Disposition {
                pending_input: Retention::Discard,
                ..Disposition::INTERRUPT
            }
        }
    }
    impl AudioGenerator for Tts {}

    #[test]
    fn role_carries_modality_and_strategy() {
        let tts = Tts;
        let role = GeneratorRole::audio(&tts).unwrap();
        assert_eq!(role.modality(), GeneratedModality::Audio);
        assert_eq!(
            role.declaration(),
            GeneratorDeclaration {
                generates: Some(GeneratedModality::Audio),
                cancel: Some(CancelStrategy::ObservesToken),
            }
        );
        let d = role
            .cancellable()
            .default_disposition(&CancelContext::new(CancelReason::BargeIn));
        assert_eq!(d.pending_input, Retention::Discard);
        assert_eq!(d.partial_output, Retention::Retain);
    }

    #[test]
    fn admission_rule() {
        let missing = GeneratorDeclaration {
            generates: Some(GeneratedModality::Text),
            cancel: None,
        };
        let err = missing.validate("Chat").unwrap_err();
        assert!(err.to_string().contains("Chat"), "{err}");
        assert!(GeneratorDeclaration::NONE.validate("Resample").is_ok());
        assert!(GeneratorDeclaration {
            generates: None,
            cancel: Some(CancelStrategy::OnCancel)
        }
        .validate("X")
        .is_err());
    }

    #[test]
    fn instance_must_match_factory() {
        let declared = GeneratorDeclaration {
            generates: Some(GeneratedModality::Audio),
            cancel: Some(CancelStrategy::ObservesToken),
        };
        assert!(declared.check_instance("Tts", declared).is_ok());
        assert!(declared
            .check_instance("Tts", GeneratorDeclaration::NONE)
            .is_err());
        assert!(GeneratorDeclaration::NONE
            .check_instance("Tts", declared)
            .is_ok());
    }

    #[test]
    fn declaration_json_shape() {
        let d = GeneratorDeclaration {
            generates: Some(GeneratedModality::Video),
            cancel: Some(CancelStrategy::DropStopsWork),
        };
        assert_eq!(
            serde_json::to_string(&d).unwrap(),
            r#"{"generates":"video","cancel":"drop_stops_work"}"#
        );
        assert_eq!(
            serde_json::to_string(&GeneratorDeclaration::NONE).unwrap(),
            "{}"
        );
    }

    #[test]
    fn generative_categories() {
        assert!(is_generative_category("LLM"));
        assert!(is_generative_category("avatar"));
        assert!(!is_generative_category("stt"));
        assert!(!is_generative_category("audio"));
    }
}

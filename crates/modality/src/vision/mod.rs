//! Vision wire types shared by image-classifying and image-encoding
//! nodes.
//!
//! Consumers: `examples/candle-transformers-loadable/`
//! (`CandleVisionNode` — dinov2 / convmixer / efficientnet / vit;
//! `CandleImageToTextNode` — blip), future OpenAI vision wrappers.
//!
//! # Planned types (land with first consumer)
//!
//! - `Classification { label, score }` — single classification
//!   record; nodes typically emit Vec<Classification> for top-k.
//! - `VisionPreprocess { resize, center_crop, mean, std }` —
//!   imagenet-style preprocessing knobs.
//! - `VisionConfig { model_input_size, preprocess }`.
//!
//! Vision feature extractors (e.g. dinov2 / convmixer / vit
//! backbones whose `forward` returns a hidden-state vector instead
//! of class probabilities) reuse `modality::embedding::FeatureVector`
//! for their tensor output — no parallel `vision::FeatureVector`
//! type. One canonical location for the "model → vector" wire
//! type, regardless of input modality.

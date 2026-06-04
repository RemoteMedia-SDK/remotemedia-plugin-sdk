//! Embedding wire types shared by text-embedding nodes.
//!
//! Consumers: `examples/candle-transformers-loadable/`
//! (`CandleEmbeddingNode` — bert / jina / gte / e5),
//! `examples/foundry-local-loadable/` (`FoundryLocalEmbeddingNode`,
//! planned), future OpenAI `text-embedding-3-*` wrappers.
//!
//! # Planned types (land with first consumer)
//!
//! - `FeatureVector { tensor, dims }` — thin wrapper over a
//!   `RuntimeData::Tensor` with explicit dims; one canonical type
//!   for "model produced a vector" output. Reused by vision feature
//!   extractors (dinov2, convmixer backbones) as well as text
//!   embedders — see the `modality::vision` module note.
//! - `PoolingStrategy` enum — Mean / Cls / Last / MaxToken.
//! - `EmbeddingConfig { max_length, batch_size, normalize, pooling }`.
//! - `l2_normalize(&mut [f32])` — in-place L2 normalization helper.
//!
//! `FeatureVector` is a thin wrapper, not a new wire variant —
//! values transit on `RuntimeData::Tensor` from `remotemedia-types`;
//! this module contributes the metadata + the pooling primitive.

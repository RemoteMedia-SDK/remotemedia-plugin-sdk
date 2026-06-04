//! Text-to-image wire types shared by image-generating nodes.
//!
//! Consumers: `examples/candle-transformers-loadable/`
//! (`CandleT2INode` — stable_diffusion / wuerstchen), future
//! OpenAI / Replicate image-gen wrappers.
//!
//! # Planned types (land with first consumer)
//!
//! - `SchedulerKind` enum — DDIM / DPM / EulerA / PNDM / …
//! - `GenerationParams { width, height, num_steps, guidance_scale,
//!   seed, scheduler }`.
//! - `T2iConfig { generation, negative_prompt, batch_size }`.
//!
//! Optional per-step latent emit helper lands when a node actually
//! needs to stream intermediate steps (e.g. preview-while-generating
//! UX).

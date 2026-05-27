use async_trait::async_trait;
use remotemedia_types::Error;

#[async_trait]
pub trait InterruptableBackend: Send + Sync {
    /// Send an immediate cancel/abort signal to stop in-flight GPU generation or audio synthesis.
    async fn request_cancel(&self, session_id: &str) -> Result<(), Error>;
}

#[async_trait]
pub trait StatefulConversationBackend: Send + Sync {
    /// Clear session history buffers.
    async fn reset_history(&self, session_id: &str) -> Result<(), Error>;

    /// Inject dynamic system prompt or RAG retrieval context.
    async fn set_context(&self, session_id: &str, context: &str) -> Result<(), Error>;

    /// Check barge-in timing and adjust history (early vs. late interrupt rollback).
    async fn finalize_turn(
        &self,
        session_id: &str,
        interrupted: bool,
        elapsed_secs: f32,
    ) -> Result<(), Error>;
}

pub trait VoiceActivityDetectorBackend: Send + Sync {
    /// Reset session audio accumulator buffers.
    fn reset_buffers(&self, session_id: &str);

    /// Pad short audio buffers and run VAD turn-end veto checks.
    fn evaluate_veto(&self, session_id: &str, audio_samples: &[f32]) -> Result<bool, Error>;
}

//! Source nodes: nodes that produce without client input (openspec:
//! add-source-node-lifecycle).
//!
//! A [`SourceNode`] is written as one body. It is self-driven (the session's
//! client input is not delivered to it, and the session stays open while it
//! runs) and its `background` is the body the runtime runs: started when the
//! session starts, cancelled when the instance stops, its return the node
//! having finished, its error or panic a failure of the node. The same
//! meaning as the Python `SourceNode`.
//!
//! ```ignore
//! struct Clock;
//!
//! #[async_trait::async_trait]
//! impl SourceNode for Clock {
//!     fn node_type(&self) -> &str { "Clock" }
//!     async fn background(&self, ctx: &dyn NodeRuntimeContextRead) -> Result<(), Error> {
//!         let emitter = Emitter::for_call(ctx);
//!         loop {
//!             emitter.emit(RuntimeData::Text("tick".into())).await?;
//!             tokio::time::sleep(Duration::from_secs(1)).await;
//!         }
//!     }
//! }
//!
//! registry.register(Arc::new(source_factory(
//!     "Clock",
//!     MediaConstraints::Text(Default::default()),
//!     |_params| Ok(Clock),
//! )));
//! ```
//!
//! A source has no upstream to take its output format from, so it declares
//! its output capabilities: [`source_factory`] requires them, and
//! `#[node(source, output_caps = "...")]` does not compile without them.

use std::sync::Arc;

use remotemedia_types::{Error, RuntimeData};
use serde_json::Value;

use crate::capabilities::{CapabilityBehavior, MediaCapabilities, MediaConstraints};
use crate::runtime_context::{InitializeContextRead, NodeRuntimeContextRead};
use crate::streaming::{
    AsyncNodeWrapper, AsyncStreamingNode, InstanceScope, StreamingNode, StreamingNodeFactory,
};

/// A node that produces on its own. See the module documentation.
#[async_trait::async_trait]
pub trait SourceNode: Send + Sync + 'static {
    /// The node type name.
    fn node_type(&self) -> &str;

    /// One-time setup, before the session starts. The body does not run yet.
    async fn initialize(&self, _ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        Ok(())
    }

    /// The node's body. It produces through the instance's emitter
    /// (`Emitter::for_call(ctx)`). Cancellation drops this future: cleanup
    /// belongs in guards.
    async fn background(&self, ctx: &dyn NodeRuntimeContextRead) -> Result<(), Error>;

    /// Input from an upstream node, when the manifest connects one. Passed
    /// through by default.
    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        Ok(data)
    }

    /// See [`StreamingNode::instance_scope`].
    fn instance_scope(&self) -> Option<InstanceScope> {
        None
    }
}

/// A [`SourceNode`] as a streaming node: self-driven, with a body.
pub struct SourceNodeWrapper<T: SourceNode>(pub Arc<T>);

impl<T: SourceNode> SourceNodeWrapper<T> {
    pub fn new(node: T) -> Self {
        Self(Arc::new(node))
    }

    /// The node as the router runs it.
    pub fn boxed(node: T) -> Box<dyn StreamingNode> {
        Box::new(AsyncNodeWrapper(Arc::new(Self::new(node))))
    }
}

#[async_trait::async_trait]
impl<T: SourceNode> AsyncStreamingNode for SourceNodeWrapper<T> {
    fn node_type(&self) -> &str {
        self.0.node_type()
    }

    async fn initialize(&self, ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        self.0.initialize(ctx).await
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        self.0.process(data).await
    }

    fn instance_scope(&self) -> Option<InstanceScope> {
        self.0.instance_scope()
    }

    fn self_driven(&self) -> Option<bool> {
        Some(true)
    }

    fn has_background(&self) -> bool {
        true
    }

    async fn background(&self, ctx: &dyn NodeRuntimeContextRead) -> Result<(), Error> {
        self.0.background(ctx).await
    }
}

/// A factory for a self-driven node that declares what it puts out.
pub struct SourceFactory<N, F> {
    node_type: String,
    capabilities: MediaCapabilities,
    behavior: CapabilityBehavior,
    make: F,
    _node: std::marker::PhantomData<fn() -> N>,
}

impl<N, F> SourceFactory<N, F>
where
    N: AsyncStreamingNode + 'static,
    F: Fn(&Value) -> Result<N, Error> + Send + Sync,
{
    /// `output` is what the node emits on its default output; `make` builds
    /// a node from its manifest parameters. The node must report itself
    /// self-driven with a body ([`SourceNodeWrapper`] and
    /// `#[node(source, ...)]` do).
    pub fn new(node_type: impl Into<String>, output: MediaConstraints, make: F) -> Self {
        Self::with_capabilities(node_type, MediaCapabilities::with_output(output), make)
    }

    /// [`Self::new`] with a full capability declaration; `capabilities` must
    /// declare an output.
    pub fn with_capabilities(
        node_type: impl Into<String>,
        capabilities: MediaCapabilities,
        make: F,
    ) -> Self {
        Self {
            node_type: node_type.into(),
            capabilities,
            behavior: CapabilityBehavior::Static,
            make,
            _node: std::marker::PhantomData,
        }
    }

    /// How the declared capabilities are determined (`Static` by default;
    /// `Configured` when they depend on the node's parameters).
    pub fn behavior(mut self, behavior: CapabilityBehavior) -> Self {
        self.behavior = behavior;
        self
    }
}

impl<N, F> StreamingNodeFactory for SourceFactory<N, F>
where
    N: AsyncStreamingNode + 'static,
    F: Fn(&Value) -> Result<N, Error> + Send + Sync,
{
    fn create(
        &self,
        _node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        Ok(Box::new(AsyncNodeWrapper(Arc::new((self.make)(params)?))))
    }

    fn node_type(&self) -> &str {
        &self.node_type
    }

    fn self_driven(&self) -> Option<bool> {
        Some(true)
    }

    fn media_capabilities(&self, _params: &Value) -> Option<MediaCapabilities> {
        Some(self.capabilities.clone())
    }

    fn capability_behavior(&self) -> CapabilityBehavior {
        self.behavior
    }
}

/// A factory for a [`SourceNode`]. `output` is required: a source has no
/// upstream to take its output format from.
pub fn source_factory<T, F>(
    node_type: impl Into<String>,
    output: MediaConstraints,
    make: F,
) -> SourceFactory<SourceNodeWrapper<T>, impl Fn(&Value) -> Result<SourceNodeWrapper<T>, Error> + Send + Sync>
where
    T: SourceNode,
    F: Fn(&Value) -> Result<T, Error> + Send + Sync,
{
    SourceFactory::new(node_type, output, move |params: &Value| {
        make(params).map(SourceNodeWrapper::new)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capabilities::TextConstraints;

    struct Once;

    #[async_trait::async_trait]
    impl SourceNode for Once {
        fn node_type(&self) -> &str {
            "Once"
        }
        async fn background(&self, _ctx: &dyn NodeRuntimeContextRead) -> Result<(), Error> {
            Ok(())
        }
    }

    #[test]
    fn a_source_node_is_self_driven_with_a_body_and_declares_its_output() {
        let factory = source_factory(
            "Once",
            MediaConstraints::Text(TextConstraints::default()),
            |_params| Ok(Once),
        );
        assert_eq!(factory.self_driven(), Some(true));
        assert_eq!(factory.capability_behavior(), CapabilityBehavior::Static);
        let capabilities = factory.media_capabilities(&Value::Null).unwrap();
        assert!(capabilities.default_output().is_some());

        let node = factory.create("once".into(), &Value::Null, None).unwrap();
        assert_eq!(node.node_type(), "Once");
        assert_eq!(node.self_driven(), Some(true));
        assert!(node.has_background());
    }
}

//! Node providers: how a crate linked into a host contributes node factories.
//!
//! A crate of nodes can be used two ways:
//!
//! - **Loaded as a plugin.** It exports its factories with
//!   `remotemedia_plugin_sdk::plugin_export!`, and the host loads the
//!   library at runtime. No provider is involved.
//! - **Linked into a host.** The host binary depends on the crate directly.
//!   The crate submits a [`NodeFactoryProvider`], and every registry the host
//!   builds from its defaults registers that provider's factories.
//!
//! The provider names only [`StreamingNodeFactory`], so a crate of nodes can
//! offer both without depending on `remotemedia-sdk-base`. The host's
//! registry, which lives in base, collects these providers next to its own.
//!
//! ```ignore
//! use std::sync::Arc;
//! use remotemedia_traits::provider::NodeFactoryProvider;
//! use remotemedia_traits::StreamingNodeFactory;
//!
//! struct MyNodesProvider;
//!
//! impl NodeFactoryProvider for MyNodesProvider {
//!     fn provider_name(&self) -> &'static str {
//!         "my-nodes"
//!     }
//!     fn factories(&self) -> Vec<Arc<dyn StreamingNodeFactory>> {
//!         vec![Arc::new(MyNodeFactory)]
//!     }
//! }
//!
//! remotemedia_traits::inventory::submit! {
//!     &MyNodesProvider as &'static dyn NodeFactoryProvider
//! }
//! ```
//!
//! A linked crate is only part of the host binary if the host references one
//! of its symbols. A host that uses no item from the crate can force the link
//! with `use my_nodes as _;`, or by naming its provider.

use std::sync::Arc;

use crate::streaming::StreamingNodeFactory;

/// A set of node factories a linked crate contributes to the host.
pub trait NodeFactoryProvider: Send + Sync {
    /// Name used in logs and diagnostics, e.g. `"candle-nodes"`.
    fn provider_name(&self) -> &'static str;

    /// The factories to register. Called each time a host builds a registry
    /// from its defaults.
    fn factories(&self) -> Vec<Arc<dyn StreamingNodeFactory>>;

    /// Order among providers. When two providers register the same node
    /// type, the host decides which wins by priority; see its registry for
    /// the rule. The host's built-in nodes use 1000 and its Python nodes 500.
    /// Default: 100.
    fn priority(&self) -> i32 {
        100
    }
}

inventory::collect!(&'static dyn NodeFactoryProvider);

/// Every submitted provider, highest priority first.
pub fn iter_factory_providers() -> impl Iterator<Item = &'static dyn NodeFactoryProvider> {
    let mut providers: Vec<&'static dyn NodeFactoryProvider> =
        inventory::iter::<&'static dyn NodeFactoryProvider>
            .into_iter()
            .copied()
            .collect();
    providers.sort_by_key(|provider| std::cmp::Reverse(provider.priority()));
    providers.into_iter()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::StreamingNode;
    use remotemedia_types::Error;
    use serde_json::Value;

    struct EchoFactory;

    impl StreamingNodeFactory for EchoFactory {
        fn create(
            &self,
            _node_id: String,
            _params: &Value,
            _session_id: Option<String>,
        ) -> Result<Box<dyn StreamingNode>, Error> {
            Err(Error::Execution("not needed by this test".into()))
        }
        fn node_type(&self) -> &str {
            "ProviderTestEcho"
        }
    }

    struct LowProvider;
    impl NodeFactoryProvider for LowProvider {
        fn provider_name(&self) -> &'static str {
            "provider-test-low"
        }
        fn factories(&self) -> Vec<Arc<dyn StreamingNodeFactory>> {
            vec![Arc::new(EchoFactory)]
        }
        fn priority(&self) -> i32 {
            -5
        }
    }

    struct HighProvider;
    impl NodeFactoryProvider for HighProvider {
        fn provider_name(&self) -> &'static str {
            "provider-test-high"
        }
        fn factories(&self) -> Vec<Arc<dyn StreamingNodeFactory>> {
            Vec::new()
        }
        fn priority(&self) -> i32 {
            5_000
        }
    }

    inventory::submit! { &LowProvider as &'static dyn NodeFactoryProvider }
    inventory::submit! { &HighProvider as &'static dyn NodeFactoryProvider }

    #[test]
    fn submitted_providers_are_collected_highest_priority_first() {
        let names: Vec<_> = iter_factory_providers()
            .map(|provider| provider.provider_name())
            .filter(|name| name.starts_with("provider-test-"))
            .collect();
        assert_eq!(names, ["provider-test-high", "provider-test-low"]);
    }

    #[test]
    fn a_provider_hands_out_its_factories() {
        let low = iter_factory_providers()
            .find(|provider| provider.provider_name() == "provider-test-low")
            .expect("submitted provider");
        let types: Vec<_> = low
            .factories()
            .iter()
            .map(|factory| factory.node_type().to_string())
            .collect();
        assert_eq!(types, ["ProviderTestEcho"]);
    }
}

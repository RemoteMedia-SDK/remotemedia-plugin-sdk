use super::*;
use remotemedia_types::{CopyContext, CopyPolicy, CopyReport};
use std::collections::BTreeMap;

/// Fixed set of host-input ledgers for one session's mapped workers.
///
/// Only the named nodes may use this scope. Clones share counters, including
/// after worker shutdown; a new session must construct a new scope. Reports
/// cover host-to-worker materialization, not copies inside arbitrary node code.
#[derive(Clone)]
pub struct WorkerInputCopies {
    session_id: Arc<str>,
    nodes: Arc<BTreeMap<String, CopyContext>>,
}

impl WorkerInputCopies {
    pub fn new(
        session_id: impl Into<String>,
        node_ids: impl IntoIterator<Item = String>,
        policy: CopyPolicy,
    ) -> Self {
        let session_id = session_id.into();
        let nodes = node_ids
            .into_iter()
            .map(|node| {
                let context = CopyContext::new(
                    policy,
                    format!("session/{session_id}/node/{node}/mapped-input"),
                );
                (node, context)
            })
            .collect();
        Self {
            session_id: session_id.into(),
            nodes: Arc::new(nodes),
        }
    }

    /// Stable node ordering; each ledger is coherent, but concurrent calls may
    /// advance between ledgers. Quiesce workers for a final aggregate report.
    pub fn reports(&self) -> BTreeMap<String, CopyReport> {
        self.nodes
            .iter()
            .map(|(node, context)| (node.clone(), context.report()))
            .collect()
    }

    pub(super) fn context(&self, identity: &CallContext) -> Result<CopyContext, Error> {
        if identity.session_id.as_deref() != Some(self.session_id.as_ref()) {
            return Err(Error::ConfigError(
                "mapped input policy belongs to a different session".into(),
            ));
        }
        self.nodes.get(&identity.node_id).cloned().ok_or_else(|| {
            Error::ConfigError(format!(
                "mapped input policy has no node '{}'",
                identity.node_id
            ))
        })
    }
}

pub(crate) fn input_copy_context(
    copies: Option<&WorkerInputCopies>,
    identity: &CallContext,
) -> Result<CopyContext, Error> {
    match copies {
        Some(copies) => copies.context(identity),
        None => Ok(CopyContext::new(
            CopyPolicy::RequireZeroCopy,
            format!(
                "session/{}/node/{}/mapped-input",
                identity.session_id.as_deref().unwrap_or(""),
                identity.node_id
            ),
        )),
    }
}

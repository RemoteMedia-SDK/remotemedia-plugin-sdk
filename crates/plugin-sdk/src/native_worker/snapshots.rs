//! Native node-output snapshot discovery and live reads over ordinary bounded calls.
use super::{client::Client, *};
use remotemedia_shared_memory::peer::SnapshotInputs;
use remotemedia_traits::ports::{SnapshotPort, TimestampedSnapshot};
use std::{any::Any, sync::Weak};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum Operation {
    Discover,
    Read { port: String },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Stamp {
    pts_us: u64,
    written_at_us: u64,
    seq: u64,
}

type Publication = Arc<TimestampedSnapshot<RuntimeData>>;

pub(super) fn execute(request: &WorkerRequest, readers: &SnapshotInputs) -> Result<(), Error> {
    if !request.supports_snapshot_outputs() {
        return Err(Error::Execution(
            "snapshot outputs were not negotiated".into(),
        ));
    }
    match serde_json::from_value(request.arguments.clone()).map_err(transport_error)? {
        Operation::Discover => {
            let mut names: Vec<_> = readers.keys().cloned().collect();
            names.sort();
            emit_json(request, names)
        }
        Operation::Read { port } => {
            let reader = readers
                .get(&port)
                .ok_or_else(|| Error::Execution(format!("unknown snapshot output {port:?}")))?;
            let Some(snapshot) = reader.runtime_snapshot()? else {
                return emit_json(request, serde_json::Value::Null);
            };
            if matches!(
                &snapshot.value,
                RuntimeData::Audio {
                    samples: remotemedia_types::AudioSamples::Vec(_),
                    ..
                }
            ) {
                return Err(Error::Execution(
                    "snapshot output contains unshared legacy audio".into(),
                ));
            }
            emit_json(
                request,
                Stamp {
                    pts_us: snapshot.pts_us,
                    written_at_us: snapshot.written_at_us,
                    seq: snapshot.seq,
                },
            )?;
            request
                .emit(snapshot.value.clone())
                .map_err(transport_error)
        }
    }
}

pub(super) async fn discover(
    client: &Arc<Client>,
    identity: &CallContext,
    expected: &[String],
) -> Result<SnapshotInputs, Error> {
    let mut call = client
        .call(
            CallKind::Snapshot,
            identity.clone(),
            serde_json::to_value(Operation::Discover).map_err(transport_error)?,
            vec![],
            remotemedia_shared_memory::peer::CallResources::default(),
        )
        .await?;
    let Some(output) = call.next().await? else {
        return Err(Error::Execution("missing snapshot discovery reply".into()));
    };
    let RuntimeData::Json(value) = output.into_data() else {
        return Err(Error::Execution("invalid snapshot discovery reply".into()));
    };
    let names: Vec<String> = serde_json::from_value(value).map_err(transport_error)?;
    if call.next().await?.is_some() {
        return Err(Error::Execution("extra snapshot discovery reply".into()));
    }
    let actual: std::collections::HashSet<_> = names.iter().collect();
    if actual.len() != names.len()
        || actual.len() != expected.len()
        || expected.iter().any(|name| !actual.contains(name))
    {
        return Err(Error::Execution(
            "worker snapshot outputs differ from factory declarations".into(),
        ));
    }
    Ok(names
        .into_iter()
        .map(|port| {
            let reader: Arc<dyn SnapshotPort> = Arc::new(Reader {
                client: Arc::downgrade(client),
                identity: identity.clone(),
                port: port.clone(),
            });
            (port, reader)
        })
        .collect())
}

struct Reader {
    client: Weak<Client>,
    identity: CallContext,
    port: String,
}
impl Reader {
    async fn read(&self, client: &Client) -> Result<Option<Publication>, Error> {
        let operation = Operation::Read {
            port: self.port.clone(),
        };
        let mut call = client
            .call(
                CallKind::Snapshot,
                self.identity.clone(),
                serde_json::to_value(operation).map_err(transport_error)?,
                vec![],
                remotemedia_shared_memory::peer::CallResources::default(),
            )
            .await?;
        let Some(header) = call.next().await? else {
            return Err(Error::Execution("missing snapshot header".into()));
        };
        let RuntimeData::Json(header) = header.into_data() else {
            return Err(Error::Execution("invalid snapshot header".into()));
        };
        let stamp: Option<Stamp> = serde_json::from_value(header).map_err(transport_error)?;
        let publication = if let Some(stamp) = stamp {
            let value = call
                .next()
                .await?
                .ok_or_else(|| Error::Execution("missing snapshot payload".into()))?
                .into_data();
            Some(Arc::new(TimestampedSnapshot {
                value,
                pts_us: stamp.pts_us,
                written_at_us: stamp.written_at_us,
                seq: stamp.seq,
            }))
        } else {
            None
        };
        if call.next().await?.is_some() {
            return Err(Error::Execution("extra snapshot payload".into()));
        }
        Ok(publication)
    }
}
impl SnapshotPort for Reader {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn supports_runtime_data(&self) -> bool {
        true
    }
    fn snapshot_any(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        self.runtime_snapshot()
            .ok()
            .flatten()
            .map(|value| value as Arc<dyn Any + Send + Sync>)
    }
    fn runtime_snapshot(&self) -> Result<Option<Publication>, Error> {
        let client = self
            .client
            .upgrade()
            .ok_or_else(|| Error::Execution("snapshot producer is closed".into()))?;
        // SnapshotPort is synchronous. Its receive threads remain independent;
        // this executor only polls channel futures and does not nest Tokio.
        futures::executor::block_on(self.read(&client))
    }
}

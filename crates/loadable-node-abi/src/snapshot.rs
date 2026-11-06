//! Same-process portable snapshots: latest-wins readers retain immutable media.
#![allow(clippy::unnecessary_cast)]
use abi_stable::{
    sabi_trait,
    std_types::{RArc, ROption, RResult, RString},
    StableAbi,
};

#[repr(C)]
#[derive(Clone, StableAbi)]
pub struct FfiSnapshot {
    pub value: crate::buffer::FfiMessage,
    pub pts_us: u64,
    pub written_at_us: u64,
    pub seq: u64,
}

#[sabi_trait]
pub trait SnapshotReader: Clone + Send + Sync + 'static {
    fn read(&self) -> RResult<ROption<FfiSnapshot>, RString>;
}
pub type SnapshotReaderArc = SnapshotReader_TO<RArc<()>>;

#[repr(C)]
#[derive(Clone, StableAbi)]
pub struct FfiSnapshotPort {
    pub name: RString,
    pub reader: SnapshotReaderArc,
}

#[repr(u8)]
#[derive(Clone, Copy, StableAbi)]
pub enum FfiPortKind {
    Stream,
    Snapshot,
}

#[repr(C)]
#[derive(Clone, StableAbi)]
pub struct FfiPortDeclaration {
    pub name: RString,
    pub kind: FfiPortKind,
}

#[cfg(feature = "runtime")]
pub mod runtime {
    use super::*;
    use abi_stable::{
        sabi_trait::TD_Opaque,
        std_types::{RErr, ROk, RVec},
    };
    use remotemedia_traits::ports::{PortKind, SnapshotPort, TimestampedSnapshot};
    use remotemedia_types::{Error, RuntimeData};
    use std::{any::Any, collections::HashMap, sync::Arc};

    #[derive(Clone)]
    struct Exported(Arc<dyn SnapshotPort>);
    impl SnapshotReader for Exported {
        fn read(&self) -> RResult<ROption<FfiSnapshot>, RString> {
            match self.0.runtime_snapshot() {
                Ok(None) => ROk(ROption::RNone),
                Err(error) => RErr(error.to_string().into()),
                Ok(Some(snapshot)) => match encode_snapshot(&snapshot.value) {
                    Ok(value) => ROk(ROption::RSome(FfiSnapshot {
                        value,
                        pts_us: snapshot.pts_us,
                        written_at_us: snapshot.written_at_us,
                        seq: snapshot.seq,
                    })),
                    Err(error) => RErr(error),
                },
            }
        }
    }

    fn encode_snapshot(value: &RuntimeData) -> Result<crate::buffer::FfiMessage, RString> {
        if matches!(
            value,
            RuntimeData::Audio {
                samples: remotemedia_types::AudioSamples::Vec(_),
                ..
            }
        ) {
            return Err(
                "snapshot contains unshared legacy audio; publish through OutputPort<RuntimeData>"
                    .into(),
            );
        }
        crate::runtime_buffer::encode(value.clone())
    }

    struct Imported(SnapshotReaderArc);
    impl SnapshotPort for Imported {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn supports_runtime_data(&self) -> bool {
            true
        }
        fn runtime_snapshot(&self) -> Result<Option<Arc<TimestampedSnapshot<RuntimeData>>>, Error> {
            self.0
                .read()
                .into_result()
                .map_err(|e| Error::Execution(e.into_string()))?
                .into_option()
                .map(|snapshot| {
                    Ok(Arc::new(TimestampedSnapshot {
                        value: crate::runtime_buffer::decode(snapshot.value)
                            .map_err(|e| Error::Execution(e.into_string()))?,
                        pts_us: snapshot.pts_us,
                        written_at_us: snapshot.written_at_us,
                        seq: snapshot.seq,
                    }))
                })
                .transpose()
        }
        fn snapshot_any(&self) -> Option<Arc<dyn Any + Send + Sync>> {
            self.runtime_snapshot()
                .ok()
                .flatten()
                .map(|s| s as Arc<dyn Any + Send + Sync>)
        }
    }

    pub fn export_ports(
        ports: HashMap<String, Arc<dyn SnapshotPort>>,
    ) -> Result<RVec<FfiSnapshotPort>, Error> {
        ports.into_iter().map(|(name, port)| {
            if !port.supports_runtime_data() {
                return Err(Error::Execution(format!("snapshot port '{name}' requires portable RuntimeData across the plugin ABI")));
            }
            Ok(FfiSnapshotPort {
                name: name.into(), reader: SnapshotReader_TO::from_ptr(RArc::new(Exported(port)), TD_Opaque),
            })
        }).collect::<Result<Vec<_>, _>>().map(Into::into)
    }

    pub fn import_ports(
        ports: RVec<FfiSnapshotPort>,
    ) -> Result<HashMap<String, Arc<dyn SnapshotPort>>, Error> {
        let mut imported = HashMap::new();
        for port in ports {
            if imported
                .insert(
                    port.name.into_string(),
                    Arc::new(Imported(port.reader)) as Arc<dyn SnapshotPort>,
                )
                .is_some()
            {
                return Err(Error::Execution("duplicate plugin snapshot port".into()));
            }
        }
        Ok(imported)
    }

    pub fn import_kinds(ports: RVec<FfiPortDeclaration>) -> HashMap<String, PortKind> {
        ports
            .into_iter()
            .map(|p| {
                (
                    p.name.into_string(),
                    match p.kind {
                        FfiPortKind::Stream => PortKind::Stream,
                        FfiPortKind::Snapshot => PortKind::Snapshot,
                    },
                )
            })
            .collect()
    }
}

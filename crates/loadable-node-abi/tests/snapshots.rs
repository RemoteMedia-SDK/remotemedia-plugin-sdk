#![cfg(feature = "runtime")]
use abi_stable::{
    sabi_trait::TD_Opaque,
    std_types::{RArc, ROk, ROption, RResult, RString},
};
use loadable_node_abi::{
    runtime_buffer,
    snapshot::{
        runtime::{export_ports, import_ports},
        FfiSnapshot, FfiSnapshotPort, SnapshotReader, SnapshotReader_TO,
    },
};
use remotemedia_traits::ports::{OutputPort, SnapshotPort};
use remotemedia_types::{AudioSamples, RuntimeData};
use std::{collections::HashMap, sync::Arc};

#[test]
fn legacy_audio_is_promoted_at_publication_without_moving_samples() {
    let samples = vec![0.25; 16];
    let address = samples.as_ptr();
    let port = OutputPort::empty();
    port.publish(
        RuntimeData::Audio {
            samples: AudioSamples::Vec(samples),
            sample_rate: 48000,
            channels: 2,
            stream_id: None,
            timestamp_us: Some(20),
            arrival_ts_us: None,
            metadata: None,
        },
        20,
    );
    let input = port.input();
    let publication = input.runtime_snapshot().unwrap().unwrap();
    let RuntimeData::Audio { samples, .. } = &publication.value else {
        panic!()
    };
    assert_eq!(samples.variant_name(), "Shared");
    assert_eq!(samples.as_ptr(), address);
    let ports = HashMap::from([("audio".into(), Arc::new(input) as Arc<dyn SnapshotPort>)]);
    let imported = import_ports(export_ports(ports).unwrap()).unwrap();
    let returned = imported["audio"].runtime_snapshot().unwrap().unwrap();
    let RuntimeData::Audio { samples, .. } = &returned.value else {
        panic!()
    };
    assert_eq!(samples.as_ptr(), address);
    assert_eq!(
        (returned.pts_us, returned.seq, returned.written_at_us),
        (
            publication.pts_us,
            publication.seq,
            publication.written_at_us
        )
    );
    port.publish(RuntimeData::Text("replacement".into()), 21);
    drop((port, imported, publication));
    assert_eq!(samples.as_slice(), &[0.25; 16]);
}

#[derive(Clone)]
struct Malformed;
impl SnapshotReader for Malformed {
    fn read(&self) -> RResult<ROption<FfiSnapshot>, RString> {
        let mut value = runtime_buffer::encode(RuntimeData::Text("invalid".into())).unwrap();
        value.version = u16::MAX;
        ROk(ROption::RSome(FfiSnapshot {
            value,
            pts_us: 0,
            written_at_us: 0,
            seq: 0,
        }))
    }
}

#[test]
fn malformed_reads_and_duplicate_ports_are_explicit_errors() {
    let port = FfiSnapshotPort {
        name: "input".into(),
        reader: SnapshotReader_TO::from_ptr(RArc::new(Malformed), TD_Opaque),
    };
    assert!(import_ports(vec![port.clone(), port.clone()].into()).is_err());
    let imported = import_ports(vec![port].into()).unwrap();
    assert!(imported["input"].runtime_snapshot().is_err());
}

#[test]
fn unsupported_types_are_rejected_even_before_first_publication() {
    let port = OutputPort::<u32>::empty();
    assert!(port.input().runtime_snapshot().is_err());
    assert!(export_ports(HashMap::from([(
        "input".into(),
        Arc::new(port.input()) as Arc<dyn SnapshotPort>
    )]))
    .is_err());
}

#[test]
fn custom_port_cannot_materialize_legacy_audio_during_export() {
    use remotemedia_traits::ports::TimestampedSnapshot;
    struct Legacy(Arc<TimestampedSnapshot<RuntimeData>>);
    impl SnapshotPort for Legacy {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn supports_runtime_data(&self) -> bool {
            true
        }
        fn runtime_snapshot(
            &self,
        ) -> Result<Option<Arc<TimestampedSnapshot<RuntimeData>>>, remotemedia_types::Error>
        {
            Ok(Some(self.0.clone()))
        }
        fn snapshot_any(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
            Some(self.0.clone())
        }
    }
    let port = Legacy(Arc::new(TimestampedSnapshot {
        value: RuntimeData::Audio {
            samples: AudioSamples::Vec(vec![0.25; 16]),
            sample_rate: 48000,
            channels: 2,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        },
        pts_us: 0,
        written_at_us: 0,
        seq: 0,
    }));
    let ports = import_ports(
        export_ports(HashMap::from([(
            "audio".into(),
            Arc::new(port) as Arc<dyn SnapshotPort>,
        )]))
        .unwrap(),
    )
    .unwrap();
    assert!(ports["audio"]
        .runtime_snapshot()
        .unwrap_err()
        .to_string()
        .contains("unshared legacy audio"));
}

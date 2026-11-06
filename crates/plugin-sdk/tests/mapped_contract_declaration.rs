#![cfg(all(feature = "native-worker", any(windows, target_os = "linux")))]
use remotemedia_plugin_sdk::{
    native_worker::{
        NativeWorkerFactory, PoolConfig, SessionId, SessionWorkerFactory, SharedPool, SizeClass,
        WorkerInputCopies,
    },
    traits::{buffer_contract::*, StreamingNodeFactory},
    types::{CopyPolicy, Error},
};
use std::collections::BTreeMap;

fn declaration(protocol: &BufferProtocol) -> BufferContract {
    let port = BufferPortContract {
        layouts: vec![BufferLayout::Binary],
        domains: vec![BufferDomain::SessionMapped],
        max_payload_bytes: 4096,
    };
    BufferContract {
        version: BUFFER_CONTRACT_VERSION,
        protocol: protocol.clone(),
        inputs: BTreeMap::from([(String::new(), port.clone())]),
        outputs: BTreeMap::from([(String::new(), port)]),
        production: BufferProduction::OutputAllocator,
        retention: Some(BufferRetention {
            owners: 2,
            bytes: 8192,
        }),
    }
}
fn factory() -> NativeWorkerFactory {
    // SAFETY: these tests never launch this nonexistent executable.
    unsafe {
        NativeWorkerFactory::for_sessions("nonexistent-contract-worker".into(), "Declared".into())
    }
}

#[test]
fn declarations_remain_unknown_unless_authored_and_errors_propagate() {
    let params = serde_json::json!({});
    assert_eq!(factory().try_buffer_contract(&params).unwrap(), None);
    assert_eq!(
        factory()
            .with_buffer_contract(|_, _| Ok(None))
            .try_buffer_contract(&params)
            .unwrap(),
        None
    );
    let f =
        factory().with_buffer_contract(|_, _| Err(Error::ConfigError("authored failure".into())));
    assert!(f
        .create("node".into(), &params, None)
        .err()
        .unwrap()
        .to_string()
        .contains("authored failure"));
}

#[test]
fn concrete_protocol_and_contract_survive_session_binding() {
    let f = factory()
        .with_buffer_contract(|params, protocol| {
            assert_eq!(params["mode"], "direct");
            Ok(Some(declaration(protocol)))
        })
        .with_snapshot_inputs(vec!["state".into()])
        .unwrap()
        .with_snapshot_outputs(vec!["state".into()])
        .unwrap();
    let params = serde_json::json!({"mode":"direct"});
    let contract = f.try_buffer_contract(&params).unwrap().unwrap();
    assert_eq!(
        contract.protocol,
        BufferProtocol::MappedNative {
            wire: 1,
            snapshots: 2
        }
    );
    assert!(f.supports_buffer_protocol(&contract.protocol));
    assert!(!f.supports_buffer_protocol(&BufferProtocol::RustOwners));
    assert!(!f.supports_buffer_protocol(&BufferProtocol::MappedNative {
        wire: 1,
        snapshots: 0
    }));
    let pool = SharedPool::new(
        SessionId([91; 16]),
        PoolConfig {
            classes: vec![SizeClass {
                bytes: 4096,
                slots: 2,
            }],
            max_bytes: 8192,
            max_leases: 8,
        },
    )
    .unwrap();
    let bound = f.bind_session(
        pool,
        WorkerInputCopies::new("session", ["node".into()], CopyPolicy::RequireZeroCopy),
    );
    assert_eq!(bound.try_buffer_contract(&params).unwrap(), Some(contract));
    assert_eq!(bound.input_port_kinds(), f.input_port_kinds());
    assert_eq!(bound.output_port_kinds(), f.output_port_kinds());
}

#[test]
fn malformed_declarations_fail_before_worker_startup() {
    let mutations: Vec<(fn(&mut BufferContract), &str)> = vec![
        (|c| c.version += 1, "transport version"),
        (
            |c| c.protocol = BufferProtocol::RustOwners,
            "transport version",
        ),
        (
            |c| c.production = BufferProduction::PrivateStorage,
            "private output storage",
        ),
        (
            |c| c.inputs.get_mut("").unwrap().domains = vec![BufferDomain::Host],
            "only SessionMapped",
        ),
        (
            |c| {
                c.outputs
                    .get_mut("")
                    .unwrap()
                    .domains
                    .push(BufferDomain::Host)
            },
            "only SessionMapped",
        ),
        (
            |c| c.inputs.get_mut("").unwrap().layouts = vec![BufferLayout::TensorPythonIpc],
            "runtime encoding",
        ),
        (
            |c| c.outputs.get_mut("").unwrap().layouts = vec![BufferLayout::TensorProtobuf],
            "runtime encoding",
        ),
        (
            |c| c.inputs.get_mut("").unwrap().max_payload_bytes = 0,
            "bound",
        ),
    ];
    for (mutate, expected) in mutations {
        let f = factory().with_buffer_contract(move |_, protocol| {
            let mut c = declaration(protocol);
            mutate(&mut c);
            Ok(Some(c))
        });
        let error = f
            .create("node".into(), &serde_json::json!({}), None)
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains(expected), "{expected}: {error}");
    }
}

#[cfg(feature = "mapped-python")]
#[test]
fn python_checks_declarations_before_provisioning_and_uses_its_actual_protocol() {
    use remotemedia_plugin_sdk::mapped_python::{
        MappedPythonConfig, MappedPythonFactory, MappedPythonWheels,
    };
    // SAFETY: no Python is provisioned or started by these declaration checks.
    let factory = unsafe {
        MappedPythonFactory::for_sessions(MappedPythonConfig {
            node_type: "Declared".into(),
            module_root: "missing-module".into(),
            entry_module: "missing".into(),
            requirements: vec![],
            wheels: MappedPythonWheels {
                client: "missing-client".into(),
                buffers: "missing-buffers".into(),
            },
            environment: Default::default(),
        })
    };
    let params = serde_json::json!({});
    assert_eq!(factory.try_buffer_contract(&params).unwrap(), None);
    let valid = factory
        .clone()
        .with_snapshot_inputs(vec!["state".into()])
        .unwrap()
        .with_buffer_contract(|_, protocol| Ok(Some(declaration(protocol))));
    let contract = valid.try_buffer_contract(&params).unwrap().unwrap();
    assert_eq!(
        contract.protocol,
        BufferProtocol::MappedPython {
            wire: 1,
            snapshots: 1
        }
    );
    assert!(valid.supports_buffer_protocol(&contract.protocol));
    let invalid = factory.with_buffer_contract(|_, _| {
        Ok(Some(declaration(&BufferProtocol::MappedNative {
            wire: 1,
            snapshots: 0,
        })))
    });
    let error = invalid
        .create("node".into(), &params, None)
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("transport version"), "{error}");
}

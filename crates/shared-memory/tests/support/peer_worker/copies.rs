use super::*;

pub(super) fn execute(request: &WorkerRequest, fixture: &Fixture) -> io::Result<()> {
    if request.arguments["audio"].as_bool().unwrap_or(false) {
        let samples = request.materialize_audio(4, |output| {
            act(request, fixture)?;
            output.copy_from_slice(&[0.25; 4]);
            Ok::<_, io::Error>(())
        })??;
        request.emit(RuntimeData::Audio {
            samples: samples.into(),
            sample_rate: 48000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        })
    } else {
        let len = request.arguments["len"].as_u64().unwrap_or(16) as usize;
        let source = vec![7; len];
        let bytes = request.materialize_bytes(len, |output| {
            act(request, fixture)?;
            output.copy_from_slice(&source);
            Ok::<_, io::Error>(())
        })??;
        request.emit(RuntimeData::Binary(bytes))
    }
}

fn act(request: &WorkerRequest, fixture: &Fixture) -> io::Result<()> {
    match request.arguments["behavior"].as_str().unwrap_or("ok") {
        "fail" => return Err(io::Error::other("copy callback sentinel")),
        "die" => {
            request.emit(RuntimeData::Text("copy-started".into()))?;
            std::process::exit(73);
        }
        "cancel" => {
            assert_eq!(
                request.complete(Ok(())).unwrap_err().kind(),
                io::ErrorKind::WouldBlock
            );
            request.emit(RuntimeData::Text("copy-started".into()))?;
            let mut proceed = fixture.proceed.lock().unwrap();
            while !*proceed {
                proceed = fixture.changed.wait(proceed).unwrap();
            }
            assert!(request.is_cancelled());
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "copy callback cancelled",
            ));
        }
        "credit" => {
            let error = request
                .materialize_bytes(1, |_| Ok::<_, io::Error>(()))
                .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        }
        _ => {}
    }
    Ok(())
}

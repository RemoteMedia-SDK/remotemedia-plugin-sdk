use std::io::{Read, Write};
use std::net::{Ipv4Addr, Shutdown, TcpListener};
use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

#[tokio::test]
async fn relay_carries_binary_upload_and_flushes_partial_response() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let endpoint = listener.local_addr().unwrap();
    let upload = vec![0xa5; 128 * 1024];
    let length = upload.len();
    let (received, wait_for_response) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut bytes = vec![0; length];
        socket.read_exact(&mut bytes).unwrap();
        assert!(bytes.iter().all(|byte| *byte == 0xa5));
        socket.write_all(b"\x00\xffpartial").unwrap();
        wait_for_response
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        let mut eof = [0];
        assert_eq!(socket.read(&mut eof).unwrap(), 0);
        socket.shutdown(Shutdown::Write).unwrap();
    });
    let mut relay = Command::new(env!("CARGO_BIN_EXE_remotemedia-workload-relay"))
        .arg(endpoint.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut input = relay.stdin.take().unwrap();
    let mut output = relay.stdout.take().unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        input.write_all(&upload).await.unwrap();
        let mut response = [0; 9];
        output.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"\x00\xffpartial");
        received.send(()).unwrap();
        drop(input);
        assert!(relay.wait().await.unwrap().success());
    })
    .await
    .unwrap();
    server.join().unwrap();

    let rejected = Command::new(env!("CARGO_BIN_EXE_remotemedia-workload-relay"))
        .arg("0.0.0.0:24621")
        .output()
        .await
        .unwrap();
    assert!(!rejected.status.success());
}

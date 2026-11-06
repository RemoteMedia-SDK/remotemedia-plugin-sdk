//! Descendants do not inherit any protocol or mapping handle.
use remotemedia_shared_memory::channel::Channel;
use remotemedia_shared_memory::channel::WorkerProcess;

pub(super) fn broker() -> Result<(), Box<dyn std::error::Error>> {
    let mut command = Command::new(std::env::current_exe()?);
    command.arg("process-tree");
    let mut process = WorkerProcess::spawn(&mut command)?;
    let (mut sender, mut receiver) = process.take_channel().unwrap().split()?;
    sender.send(b"spawn", None)?;
    let frame = receiver.receive()?;
    println!("{}", String::from_utf8(frame.metadata)?);
    std::io::stdout().flush()?;
    let mut command = String::new();
    std::io::stdin().read_line(&mut command)?;
    assert_eq!(command.trim(), "exit");
    // Skip all Rust destructors. Only OS closure of the broker's noninherited
    // kill-on-close job handle can terminate the live descendants.
    std::process::exit(37);
}
use std::{
    io::{BufRead, Write},
    process::{Command, Stdio},
    time::Duration,
};

fn spawn_leaf(depth: u32) -> Result<Vec<u32>, Box<dyn std::error::Error>> {
    let mut child = Command::new(std::env::current_exe()?)
        .arg("tree-leaf")
        .arg(depth.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;
    let mut line = String::new();
    std::io::BufReader::new(child.stdout.take().unwrap()).read_line(&mut line)?;
    let ids: Vec<u32> = serde_json::from_str(&line)?;
    assert_eq!(ids[0], child.id());
    // Deliberately drop the Child without killing it: the broker's job owns
    // teardown even after this process exits and loses all descendant handles.
    Ok(ids)
}

pub(super) fn leaf() -> Result<(), Box<dyn std::error::Error>> {
    let depth: u32 = std::env::args().nth(2).unwrap().parse()?;
    let mut ids = vec![std::process::id()];
    if depth > 0 {
        ids.extend(spawn_leaf(depth - 1)?);
    }
    println!("{}", serde_json::to_string(&ids)?);
    std::io::stdout().flush()?;
    // Test-only fail-safe prevents an orphan from living indefinitely.
    std::thread::sleep(Duration::from_secs(30));
    Ok(())
}

pub(super) fn run() -> Result<(), Box<dyn std::error::Error>> {
    // SAFETY: private fixture endpoint; no handle or mapping delegation.
    let (mut sender, mut receiver) = unsafe { Channel::from_inherited_stdio()? }.split()?;
    let request = receiver.receive()?;
    assert_eq!(request.metadata, b"spawn");
    // First host message is received only after job assignment.
    let mut ids = vec![std::process::id()];
    ids.extend(spawn_leaf(1)?);
    sender.send(&serde_json::to_vec(&ids)?, None)?;
    let request = receiver.receive()?;
    assert_eq!(request.metadata, b"exit");
    std::process::exit(23);
}

//! Test-only worker: stdout is a granted mapping handle, stdin/stderr are control.
use remotemedia_shared_memory::{
    import, import_writer, writer_descriptor, BufferDescriptor, ElementType, MappedElement,
    WriteDescriptor,
};
use std::fs::File;
use std::io::{self, BufRead};
#[path = "channel_peer.rs"]
#[cfg(any(windows, target_os = "linux"))]
mod channel_peer;
#[cfg(any(windows, target_os = "linux"))]
mod peer_worker;
#[cfg(windows)]
mod process_tree;

fn backing_handle() -> io::Result<File> {
    #[cfg(windows)]
    {
        use std::os::windows::io::AsHandle;
        Ok(File::from(io::stdout().as_handle().try_clone_to_owned()?))
    }
    #[cfg(unix)]
    {
        use std::os::fd::AsFd;
        Ok(File::from(io::stdout().as_fd().try_clone_to_owned()?))
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(windows)]
    if std::env::args().nth(1).as_deref() == Some("tree-broker") {
        return process_tree::broker();
    }
    #[cfg(windows)]
    if std::env::args().nth(1).as_deref() == Some("process-tree") {
        return process_tree::run();
    }
    #[cfg(windows)]
    if std::env::args().nth(1).as_deref() == Some("tree-leaf") {
        return process_tree::leaf();
    }
    #[cfg(any(windows, target_os = "linux"))]
    if std::env::args().nth(1).as_deref() == Some("peer") {
        return peer_worker::run();
    }
    #[cfg(any(windows, target_os = "linux"))]
    if std::env::args().nth(1).as_deref() == Some("channel") {
        return channel_peer::run();
    }
    let mut commands = io::stdin().lock().lines();
    if std::env::args().nth(1).as_deref() == Some("produce") {
        let descriptor =
            serde_json::from_str(&commands.next().ok_or("missing write descriptor")??)?;
        return produce(descriptor, &mut commands);
    }
    let descriptor: BufferDescriptor =
        serde_json::from_str(&commands.next().ok_or("missing descriptor")??)?;
    match descriptor.element {
        ElementType::F32Le => read::<f32>(descriptor, &mut commands, |i| i as f32 * 0.5),
        ElementType::Bytes => read::<u8>(descriptor, &mut commands, |i| (i % 251) as u8),
    }
}

fn produce(
    descriptor: WriteDescriptor,
    commands: &mut impl Iterator<Item = io::Result<String>>,
) -> Result<(), Box<dyn std::error::Error>> {
    // SAFETY: private inherited grant; the broker holds its reservation until
    // this callback or verified exit. This fixture never writes via stdout.
    let mut loan = unsafe {
        import_writer::<f32>(
            backing_handle()?,
            descriptor.clone(),
            descriptor.session,
            descriptor.producer,
            || eprintln!("released"),
        )
    }?;
    assert!(loan.iter().all(|&value| value == 0.0));
    let address = loan.as_ptr();
    for (index, value) in loan.iter_mut().enumerate() {
        *value = index as f32 * 0.5;
    }
    eprintln!("writing");
    match commands
        .next()
        .ok_or("missing publication command")??
        .as_str()
    {
        "crash" => std::process::exit(23),
        "publish" => (),
        command => return Err(format!("unknown command {command}").into()),
    }
    let frozen = loan.freeze();
    assert_eq!(frozen.as_ptr(), address);
    assert_eq!(writer_descriptor(&frozen), Some(&descriptor));
    let retained = frozen.slice(1..frozen.len() - 1)?;
    drop(frozen);
    eprintln!("published");
    match commands.next().ok_or("missing release command")??.as_str() {
        "crash" => std::process::exit(23),
        "release" => {
            for (index, &value) in retained.iter().enumerate() {
                assert_eq!(value, (index + 1) as f32 * 0.5);
            }
            drop(retained);
        }
        command => return Err(format!("unknown command {command}").into()),
    }
    Ok(())
}

fn read<T: MappedElement + PartialEq + std::fmt::Debug>(
    descriptor: BufferDescriptor,
    commands: &mut impl Iterator<Item = io::Result<String>>,
    expected: fn(usize) -> T,
) -> Result<(), Box<dyn std::error::Error>> {
    let expected_start = descriptor.offset as usize / std::mem::size_of::<T>();
    // SAFETY: the parent grants this handle and keeps the broker lease alive
    // until our release event or verified exit. This worker never writes it.
    let buffer = unsafe {
        import::<T>(
            backing_handle()?,
            descriptor.clone(),
            descriptor.session,
            || {
                eprintln!("released");
            },
        )
    }?;
    let retained = buffer.slice(1..buffer.len() - 1)?;
    drop(buffer);
    eprintln!("ready");
    match commands.next().ok_or("missing command")??.as_str() {
        "release" => {
            // Read only after parent has dropped its producer and forwarded.
            for (i, &value) in retained.iter().enumerate() {
                assert_eq!(value, expected(expected_start + 1 + i));
            }
            drop(retained);
        }
        "crash" => std::process::exit(23), // Bypass Rust destructors deliberately.
        command => return Err(format!("unknown command {command}").into()),
    }
    Ok(())
}

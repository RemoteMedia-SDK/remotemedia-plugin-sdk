#[path = "channel_protocol.rs"]
mod protocol;
use protocol::{Command, Event};
use remotemedia_shared_memory::{
    channel::{Channel, HandleAccess, Sender},
    import_message, import_writer, ElementType,
};
use remotemedia_types::{PayloadStorage, RuntimeData, TensorEncoding};
use std::{
    fs::File,
    io::Write,
    sync::{Arc, Mutex},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
fn send(sender: &Arc<Mutex<Sender>>, event: Event) {
    sender
        .lock()
        .unwrap()
        .send(&serde_json::to_vec(&event).unwrap(), None)
        .unwrap();
}

pub fn run() -> Result<()> {
    // SAFETY: this executable is spawned only through WorkerProcess by the test;
    // it claims standard handles before touching stdin/stdout or starting threads.
    let (sender, mut receiver) = unsafe { Channel::from_inherited_stdio() }?.split()?;
    let sender = Arc::new(Mutex::new(sender));
    let hello = receiver.receive()?;
    assert!(hello.file.is_none());
    let Command::Hello(session, peer) = serde_json::from_slice(&hello.metadata)? else {
        return Err("missing hello".into());
    };
    send(&sender, Event::Hello(session, peer));
    let mut input: Option<RuntimeData> = None;
    let mut output = PayloadStorage::None;
    let mut releases = Vec::new();
    loop {
        let frame = receiver.receive()?;
        match serde_json::from_slice::<Command>(&frame.metadata)? {
            Command::Input(descriptor) => {
                if descriptor.session != session || descriptor.recipient != peer {
                    return Err("wrong input identity".into());
                }
                assert_eq!(frame.access, Some(HandleAccess::Read));
                let mut file = frame.file.unwrap();
                noninheritable(&file);
                assert!(
                    file.write_all(&[0]).is_err(),
                    "reader handle retained write rights"
                );
                let released = descriptor.buffer.clone().unwrap();
                let reply = sender.clone();
                // SAFETY: private parent channel binds this file to this exact
                // prepared read lease. The callback runs after final unmap.
                input = Some(unsafe {
                    import_message(
                        descriptor,
                        Some(file),
                        session,
                        peer,
                        TensorEncoding::Runtime,
                        move || send(&reply, Event::InputReleased(released)),
                    )
                }?);
                send(&sender, Event::Accepted);
            }
            Command::Produce(descriptor) => {
                if descriptor.session != session || descriptor.producer != peer {
                    return Err("wrong producer identity".into());
                }
                assert_eq!(frame.access, Some(HandleAccess::ReadWrite));
                let file = frame.file.unwrap();
                noninheritable(&file);
                let (metadata, data) = input
                    .as_ref()
                    .ok_or("no input")?
                    .clone()
                    .into_shared_parts(TensorEncoding::Runtime)?;
                let reply = sender.clone();
                let released = descriptor.clone();
                // SAFETY: the private broker created this initialized exclusive
                // reservation. Each branch freezes its loan before publishing;
                // final owner destruction sends the only release acknowledgement.
                output = match (descriptor.element, data) {
                    (ElementType::F32Le, PayloadStorage::Audio(samples)) => {
                        let mut loan = unsafe {
                            import_writer::<f32>(
                                file,
                                descriptor.clone(),
                                session,
                                peer,
                                move || send(&reply, Event::OutputReleased(released)),
                            )
                        }?;
                        let address = loan.as_ptr();
                        assert_eq!(loan.len(), samples.len());
                        for (out, sample) in loan.iter_mut().zip(samples.iter()) {
                            *out = sample * 2.0 + 1.0;
                        }
                        let frozen = loan.freeze();
                        assert_eq!(address, frozen.as_ptr());
                        PayloadStorage::Audio(frozen)
                    }
                    (ElementType::Bytes, PayloadStorage::Bytes(bytes)) => {
                        let mut loan = unsafe {
                            import_writer::<u8>(
                                file,
                                descriptor.clone(),
                                session,
                                peer,
                                move || send(&reply, Event::OutputReleased(released)),
                            )
                        }?;
                        let address = loan.as_ptr();
                        assert_eq!(loan.len(), bytes.len());
                        for (out, value) in loan.iter_mut().zip(bytes.iter()) {
                            *out = value.wrapping_add(1);
                        }
                        let frozen = loan.freeze();
                        assert_eq!(address, frozen.as_ptr());
                        PayloadStorage::Bytes(frozen)
                    }
                    _ => return Err("wrong transform layout".into()),
                };
                send(&sender, Event::Published(descriptor, metadata));
            }
            Command::ReleaseInput => {
                input.take();
            }
            Command::ReleaseOutput => {
                drop(std::mem::replace(&mut output, PayloadStorage::None));
            }
            Command::ReleaseOutputAsync => {
                let data = std::mem::replace(&mut output, PayloadStorage::None);
                releases.push(std::thread::spawn(move || drop(data)));
            }
            Command::ForgedGrant(raw) => {
                forge_grant(raw)?;
                break;
            }
            Command::Echo(value) => {
                assert!(frame.file.is_none());
                // Reverse handle grants are rejected locally without poisoning
                // valid metadata traffic or sending an arbitrary numeric handle.
                let file = tempfile::tempfile()?;
                assert!(sender
                    .lock()
                    .unwrap()
                    .send(b"bad", Some((&file, HandleAccess::Read)))
                    .is_err());
                send(&sender, Event::Echo(value));
            }
            Command::Stop => break,
            Command::Crash => std::process::exit(23),
            Command::Hello(..) => return Err("duplicate hello".into()),
        }
    }
    drop(input);
    drop(output);
    for release in releases {
        release.join().unwrap();
    }
    Ok(())
}

fn forge_grant(raw: u64) -> Result<()> {
    // Deliberately violate the wire contract without using the public Sender,
    // which rejects reverse grants before writing any bytes.
    let mut header = [0u8; 28];
    header[..4].copy_from_slice(b"RMZC");
    header[4..6].copy_from_slice(&1u16.to_le_bytes());
    header[6..8].copy_from_slice(&1u16.to_le_bytes());
    header[12..20].copy_from_slice(&2u64.to_le_bytes());
    header[20..28].copy_from_slice(&raw.to_le_bytes());
    #[cfg(windows)]
    {
        std::io::stdout().write_all(&header)?;
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsFd;
        File::from(std::io::stdin().as_fd().try_clone_to_owned()?).write_all(&header)?;
    }
    Ok(())
}

fn noninheritable(file: &File) {
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Foundation::{GetHandleInformation, HANDLE_FLAG_INHERIT};
        let mut flags = 0;
        assert_ne!(
            unsafe { GetHandleInformation(file.as_raw_handle(), &mut flags) },
            0
        );
        assert_eq!(flags & HANDLE_FLAG_INHERIT, 0);
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        assert_ne!(
            unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
            0
        );
    }
}

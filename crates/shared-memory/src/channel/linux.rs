use super::{invalid, HandleAccess, HEADER_BYTES};
#[cfg(feature = "linux-worker-supervisor")]
#[path = "linux/namespace.rs"]
pub mod namespace;
use std::{
    fs::File,
    io::{self, Read, Write},
    mem,
    os::{
        fd::{AsFd, AsRawFd, FromRawFd, OwnedFd},
        unix::net::UnixStream,
    },
    process::{Child, Command, Stdio},
};

pub(super) struct Endpoint {
    stream: Option<UnixStream>,
}
impl Endpoint {
    pub(super) fn split(self) -> io::Result<(Self, Self)> {
        let reader = Self {
            stream: Some(self.stream.as_ref().unwrap().try_clone()?),
        };
        Ok((self, reader))
    }
    pub(super) fn spawn(
        command: &mut Command,
        resources: &mut super::ProcessResources,
    ) -> io::Result<(Child, Self)> {
        let (parent, child) = UnixStream::pair()?;
        command.stdin(Stdio::from(OwnedFd::from(child)));
        let result = command.spawn();
        // Command is reusable and otherwise retains another child-end handle,
        // masking EOF after the child exits. Close it even on spawn failure.
        command.stdin(Stdio::null());
        let child = result?;
        resources.live = true;
        super::finish_spawn(
            child,
            Ok(Self {
                stream: Some(parent),
            }),
            resources,
        )
    }
    pub(super) fn from_stdio() -> io::Result<Self> {
        let input = io::stdin();
        // SAFETY: the caller owns this inherited endpoint before starting threads.
        if unsafe { libc::fcntl(input.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } == -1 {
            return Err(io::Error::last_os_error());
        }
        let stream = UnixStream::from(input.as_fd().try_clone_to_owned()?);
        stream.peer_addr()?; // Reject ordinary files/pipes supplied as stdin.
        Ok(Self {
            stream: Some(stream),
        })
    }

    pub(super) fn send(
        &mut self,
        header: [u8; HEADER_BYTES],
        metadata: &[u8],
        file: Option<(&File, HandleAccess)>,
    ) -> io::Result<()> {
        // A fresh read-only open description narrows rights; dup/SCM_RIGHTS alone
        // would preserve the producer file's write access. No peer supplies this path.
        let readonly = match file {
            Some((file, HandleAccess::Read)) => {
                Some(File::open(format!("/proc/self/fd/{}", file.as_raw_fd()))?)
            }
            _ => None,
        };
        let file = readonly.as_ref().or_else(|| file.map(|(file, _)| file));
        let stream = self.stream.as_mut().unwrap();
        send_first_byte(stream, header[0], file)?;
        stream.write_all(&header[1..])?;
        stream.write_all(metadata)
    }
    pub(super) fn receive_header(&mut self) -> io::Result<([u8; HEADER_BYTES], Vec<File>)> {
        let stream = self.stream.as_mut().unwrap();
        let mut header = [0; HEADER_BYTES];
        let files = receive_first_byte(stream, &mut header[0])?;
        stream.read_exact(&mut header[1..])?;
        Ok((header, files))
    }
    pub(super) fn receive_file(
        &self,
        header: &[u8; HEADER_BYTES],
        mut files: Vec<File>,
        access: Option<HandleAccess>,
        worker: bool,
    ) -> io::Result<Option<File>> {
        if header[20..28] != [0; 8]
            || files.len() != usize::from(access.is_some())
            || (access.is_some() && !worker)
        {
            return Err(invalid("unexpected or missing Unix handle grant"));
        }
        Ok(files.pop())
    }
    pub(super) fn read_metadata(&mut self, metadata: &mut [u8]) -> io::Result<()> {
        self.stream.as_mut().unwrap().read_exact(metadata)
    }
    pub(super) fn shutdown(&mut self) {
        if let Some(stream) = self.stream.take() {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
    }
    pub(super) fn shutdown_write(&mut self) {
        if let Some(stream) = &self.stream {
            let _ = stream.shutdown(std::net::Shutdown::Write);
        }
    }
}

fn send_first_byte(stream: &UnixStream, byte: u8, file: Option<&File>) -> io::Result<()> {
    let mut control = [0usize; 8]; // cmsghdr alignment, capacity >= one descriptor.
    let mut iov = libc::iovec {
        iov_base: (&byte as *const u8).cast_mut().cast(),
        iov_len: 1,
    };
    // SAFETY: zero is a valid empty msghdr; pointers below remain live during sendmsg.
    let mut msg: libc::msghdr = unsafe { mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    if let Some(file) = file {
        msg.msg_control = control.as_mut_ptr().cast();
        // SAFETY: aligned storage has ample space; the header and data stay within it.
        unsafe {
            msg.msg_controllen = libc::CMSG_SPACE(mem::size_of::<libc::c_int>() as u32) as usize;
            let cmsg = libc::CMSG_FIRSTHDR(&msg);
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len = libc::CMSG_LEN(mem::size_of::<libc::c_int>() as u32) as usize;
            std::ptr::write_unaligned(
                libc::CMSG_DATA(cmsg).cast::<libc::c_int>(),
                file.as_raw_fd(),
            );
        }
    }
    loop {
        // SAFETY: borrowed socket and stack buffers are valid for this syscall.
        let result = unsafe { libc::sendmsg(stream.as_raw_fd(), &msg, libc::MSG_NOSIGNAL) };
        if result == 1 {
            return Ok(());
        }
        let error = if result < 0 {
            io::Error::last_os_error()
        } else {
            io::Error::new(io::ErrorKind::WriteZero, "empty Unix control send")
        };
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

fn receive_first_byte(stream: &UnixStream, byte: &mut u8) -> io::Result<Vec<File>> {
    let mut control = [0usize; 8];
    let mut iov = libc::iovec {
        iov_base: (byte as *mut u8).cast(),
        iov_len: 1,
    };
    loop {
        // SAFETY: initialized kernel output structure backed by live aligned storage.
        let mut msg: libc::msghdr = unsafe { mem::zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = control.as_mut_ptr().cast();
        msg.msg_controllen = mem::size_of_val(&control);
        // CLOEXEC is atomic with descriptor receipt; another thread cannot exec
        // a briefly inheritable received grant before a later fcntl call.
        let count = unsafe { libc::recvmsg(stream.as_raw_fd(), &mut msg, libc::MSG_CMSG_CLOEXEC) };
        if count < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "worker channel closed",
            ));
        }
        let mut files = Vec::new();
        let mut unexpected = false;
        // SAFETY: the kernel produced bounded ancillary headers in aligned control
        // storage. Adopt every returned fd immediately so all error paths close it.
        unsafe {
            let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
            while !cmsg.is_null() {
                if (*cmsg).cmsg_level != libc::SOL_SOCKET || (*cmsg).cmsg_type != libc::SCM_RIGHTS {
                    unexpected = true;
                    cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
                    continue;
                }
                let base = libc::CMSG_LEN(0) as usize;
                let bytes = (*cmsg)
                    .cmsg_len
                    .checked_sub(base)
                    .ok_or_else(|| invalid("invalid Unix rights size"))?;
                if !bytes.is_multiple_of(mem::size_of::<libc::c_int>()) {
                    return Err(invalid("invalid Unix rights alignment"));
                }
                for index in 0..bytes / mem::size_of::<libc::c_int>() {
                    let fd = std::ptr::read_unaligned(
                        libc::CMSG_DATA(cmsg).cast::<libc::c_int>().add(index),
                    );
                    files.push(File::from_raw_fd(fd));
                }
                cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
            }
        }
        if unexpected
            || msg.msg_flags & (libc::MSG_CTRUNC | libc::MSG_TRUNC) != 0
            || files.len() > 1
        {
            return Err(invalid("truncated or multiple Unix handle grants"));
        }
        return Ok(files);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::{Channel, Side};
    use std::os::unix::fs::MetadataExt;

    fn references(file: &File) -> usize {
        let identity = file.metadata().unwrap();
        std::fs::read_dir("/proc/self/fd")
            .unwrap()
            .filter_map(|entry| std::fs::metadata(entry.ok()?.path()).ok())
            .filter(|entry| entry.dev() == identity.dev() && entry.ino() == identity.ino())
            .count()
    }

    fn send_rights(stream: &mut UnixStream, file: &File, count: usize, bad_header: bool) {
        let mut header = [0u8; HEADER_BYTES];
        header[..4].copy_from_slice(if bad_header { b"FAIL" } else { b"RMZC" });
        header[4..6].copy_from_slice(&1u16.to_le_bytes());
        header[6..8].copy_from_slice(&1u16.to_le_bytes());
        header[12..20].copy_from_slice(&1u64.to_le_bytes());
        let mut control = [0usize; 64];
        let mut iov = libc::iovec {
            iov_base: header.as_mut_ptr().cast(),
            iov_len: 1,
        };
        // SAFETY: deliberate malformed-protocol fixture, but valid kernel buffers
        // and owned source fds. All test counts fit the aligned control storage.
        unsafe {
            let mut msg: libc::msghdr = mem::zeroed();
            msg.msg_iov = &mut iov;
            msg.msg_iovlen = 1;
            msg.msg_control = control.as_mut_ptr().cast();
            msg.msg_controllen =
                libc::CMSG_SPACE((count * mem::size_of::<libc::c_int>()) as u32) as usize;
            let cmsg = libc::CMSG_FIRSTHDR(&msg);
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len =
                libc::CMSG_LEN((count * mem::size_of::<libc::c_int>()) as u32) as usize;
            for index in 0..count {
                std::ptr::write_unaligned(
                    libc::CMSG_DATA(cmsg).cast::<libc::c_int>().add(index),
                    file.as_raw_fd(),
                );
            }
            assert_eq!(
                libc::sendmsg(stream.as_raw_fd(), &msg, libc::MSG_NOSIGNAL),
                1
            );
        }
        stream.write_all(&header[1..]).unwrap();
    }

    #[test]
    fn rejected_and_truncated_ancillary_frames_close_every_received_descriptor() {
        for (count, bad_header) in [(1, true), (2, false), (64, false)] {
            let file = tempfile::tempfile().unwrap();
            let before = references(&file);
            assert_eq!(before, 1);
            let (mut sender, receiver) = UnixStream::pair().unwrap();
            let mut channel = Channel::new(
                Endpoint {
                    stream: Some(receiver),
                },
                Side::Worker,
            );
            send_rights(&mut sender, &file, count, bad_header);
            assert!(channel.receive().is_err());
            assert!(channel.receive().is_err()); // Faulted channels cannot resynchronize.
            assert_eq!(
                references(&file),
                before,
                "leaked rejected rights: count={count}"
            );
            assert!(channel.split().is_err());
        }
    }
}

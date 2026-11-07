use super::{invalid, HandleAccess, HEADER_BYTES};
#[path = "windows/job.rs"]
mod job;
pub(super) use job::ProcessJob;
use std::{
    fs::File,
    io::{self, Read, Write},
    os::windows::io::{AsHandle, AsRawHandle, FromRawHandle, OwnedHandle},
    process::{Child, Command, Stdio},
};
use windows_sys::Win32::{
    Foundation::{
        DuplicateHandle, SetHandleInformation, DUPLICATE_SAME_ACCESS, HANDLE_FLAG_INHERIT,
    },
    Storage::FileSystem::FILE_GENERIC_READ,
    System::Threading::GetCurrentProcess,
};

pub(super) struct Endpoint {
    reader: Option<File>,
    writer: Option<File>,
    peer_process: Option<OwnedHandle>,
}
impl Endpoint {
    pub(super) fn split(mut self) -> io::Result<(Self, Self)> {
        let reader = Self {
            reader: self.reader.take(),
            writer: None,
            peer_process: None,
        };
        Ok((self, reader))
    }
    pub(super) fn spawn(
        command: &mut Command,
        job: &ProcessJob,
        resources: &mut super::ProcessResources,
    ) -> io::Result<(Child, Self)> {
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        resources.live = true;
        let endpoint = (|| {
            // Before any host protocol or mapping transfer. Trusted workers must
            // not start descendants before receiving their first host message.
            job.assign(&child)?;
            let reader = File::from(
                child
                    .stdout
                    .take()
                    .unwrap()
                    .as_handle()
                    .try_clone_to_owned()?,
            );
            let writer = File::from(
                child
                    .stdin
                    .take()
                    .unwrap()
                    .as_handle()
                    .try_clone_to_owned()?,
            );
            let peer_process = Some(child.as_handle().try_clone_to_owned()?);
            Ok(Self {
                reader: Some(reader),
                writer: Some(writer),
                peer_process,
            })
        })();
        super::finish_spawn(child, endpoint, resources)
    }

    pub(super) fn from_stdio() -> io::Result<Self> {
        let input = io::stdin();
        let output = io::stdout();
        // Stop subsequent exec/spawn inheritance of these protocol handles.
        // The caller guarantees early exclusive ownership of standard handles.
        for handle in [input.as_raw_handle(), output.as_raw_handle()] {
            if unsafe { SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0) } == 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(Self {
            reader: Some(File::from(input.as_handle().try_clone_to_owned()?)),
            writer: Some(File::from(output.as_handle().try_clone_to_owned()?)),
            peer_process: None,
        })
    }

    pub(super) fn send(
        &mut self,
        mut header: [u8; HEADER_BYTES],
        metadata: &[u8],
        file: Option<(&File, HandleAccess)>,
    ) -> io::Result<()> {
        if let Some((file, access)) = file {
            let peer = self
                .peer_process
                .as_ref()
                .ok_or_else(|| invalid("worker cannot grant handles"))?;
            let mut target = std::ptr::null_mut();
            let (rights, options) = match access {
                HandleAccess::Read => (FILE_GENERIC_READ, 0),
                HandleAccess::ReadWrite => (0, DUPLICATE_SAME_ACCESS),
            };
            // SAFETY: source file and original child process handles are retained
            // here. The new handle belongs only to that child and is noninheritable.
            if unsafe {
                DuplicateHandle(
                    GetCurrentProcess(),
                    file.as_raw_handle(),
                    peer.as_raw_handle(),
                    &mut target,
                    rights,
                    0,
                    options,
                )
            } == 0
            {
                return Err(io::Error::last_os_error());
            }
            header[20..28].copy_from_slice(&(target as usize as u64).to_le_bytes());
            // On a partial write error, NEVER close this numeric handle remotely:
            // the child may already have adopted/closed it and reused the number.
            // The channel becomes failed; original-child termination cleans up
            // any undelivered handle. Broker leases remain quarantined separately.
        }
        let writer = self.writer.as_mut().unwrap();
        writer.write_all(&header)?;
        writer.write_all(metadata)?;
        writer.flush()
    }

    pub(super) fn receive_header(&mut self) -> io::Result<([u8; HEADER_BYTES], Vec<File>)> {
        let mut header = [0; HEADER_BYTES];
        self.reader.as_mut().unwrap().read_exact(&mut header)?;
        Ok((header, Vec::new()))
    }

    pub(super) fn receive_file(
        &self,
        header: &[u8; HEADER_BYTES],
        _files: Vec<File>,
        access: Option<HandleAccess>,
        worker: bool,
    ) -> io::Result<Option<File>> {
        let raw = u64::from_le_bytes(header[20..28].try_into().unwrap());
        if access.is_none() {
            return if raw == 0 {
                Ok(None)
            } else {
                Err(invalid("unexpected worker handle value"))
            };
        }
        if !worker || raw == 0 || raw == u64::MAX || usize::try_from(raw).is_err() {
            return Err(invalid("invalid handle grant direction or value"));
        }
        // SAFETY: only a trusted spawning broker writes this private pipe. It
        // duplicated this noninheritable handle into this exact child process;
        // the framing/sequence check permits adoption exactly once.
        Ok(Some(unsafe {
            File::from_raw_handle(raw as usize as *mut _)
        }))
    }
    pub(super) fn read_metadata(&mut self, metadata: &mut [u8]) -> io::Result<()> {
        self.reader.as_mut().unwrap().read_exact(metadata)
    }
    pub(super) fn shutdown(&mut self) {
        self.reader.take();
        self.writer.take();
    }
    pub(super) fn shutdown_write(&mut self) {
        self.writer.take();
    }
}

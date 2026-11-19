//! Byte transport from the trusted runtime's stdio to one guest-loopback service.

use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, Shutdown, SocketAddr, SocketAddrV4, TcpStream};
use std::time::Duration;

fn main() -> io::Result<()> {
    let mut args = std::env::args_os().skip(1);
    let endpoint = args
        .next()
        .and_then(|value| value.into_string().ok())
        .and_then(|value| value.parse::<SocketAddrV4>().ok())
        .filter(|addr| addr.ip() == &Ipv4Addr::LOCALHOST && addr.port() != 0)
        .filter(|_| args.next().is_none())
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "expected a loopback endpoint")
        })?;

    let mut response =
        TcpStream::connect_timeout(&SocketAddr::V4(endpoint), Duration::from_secs(2))?;
    let mut request = response.try_clone()?;
    std::thread::Builder::new()
        .name("request".into())
        .spawn(move || {
            let shutdown = match io::copy(&mut io::stdin().lock(), &mut request) {
                Ok(_) => Shutdown::Write,
                Err(_) => Shutdown::Both,
            };
            let _ = request.shutdown(shutdown);
        })?;

    let mut stdout = io::stdout().lock();
    let mut buffer = [0; 16 * 1024];
    loop {
        let count = response.read(&mut buffer)?;
        if count == 0 {
            return Ok(());
        }
        stdout.write_all(&buffer[..count])?;
        // Forward partial response chunks without waiting for a newline or EOF.
        stdout.flush()?;
    }
}

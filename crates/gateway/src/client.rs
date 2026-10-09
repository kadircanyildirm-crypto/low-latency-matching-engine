//! A blocking client for the gateway, for tests, tools and the load generator.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use protocol::{Inbound, Outbound, VERSION, decode_outbound, encode_inbound};

/// A connection to a gateway.
pub struct Client {
    stream: TcpStream,
    /// Bytes received and not yet decoded, from `start` on.
    input: Vec<u8>,
    start: usize,
    /// Messages queued by [`queue`](Client::queue).
    output: Vec<u8>,
}

impl Client {
    /// Connects to the gateway at `addr`.
    pub fn connect(addr: SocketAddr) -> io::Result<Client> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        Ok(Client {
            stream,
            input: Vec::with_capacity(64 << 10),
            start: 0,
            output: Vec::new(),
        })
    }

    /// Connects and logs in. Returns the client and the sequence number of the last command
    /// the exchange had taken.
    pub fn login(addr: SocketAddr, account: u32, token: u64) -> io::Result<(Client, u64)> {
        let mut client = Client::connect(addr)?;
        client.send(&Inbound::Login {
            version: VERSION,
            account,
            token,
        })?;
        match client.receive()? {
            Outbound::LoginAccepted { last_seq, .. } => Ok((client, last_seq)),
            other => Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("login refused: {other:?}"),
            )),
        }
    }

    /// Sends `message` now.
    pub fn send(&mut self, message: &Inbound) -> io::Result<()> {
        self.queue(message);
        self.flush()
    }

    /// Queues `message`, to be sent with the next [`flush`](Client::flush).
    pub fn queue(&mut self, message: &Inbound) {
        encode_inbound(message, &mut self.output);
    }

    /// Sends the queued messages.
    pub fn flush(&mut self) -> io::Result<()> {
        self.stream.write_all(&self.output)?;
        self.output.clear();
        Ok(())
    }

    /// Waits for the next message. A connection the gateway closed is an
    /// `UnexpectedEof` error; bytes that do not decode, `InvalidData`.
    pub fn receive(&mut self) -> io::Result<Outbound> {
        loop {
            if let Some(message) = self.decoded()? {
                return Ok(message);
            }
            self.fill()?;
        }
    }

    /// The next message if one has arrived, without waiting for one.
    pub fn try_receive(&mut self) -> io::Result<Option<Outbound>> {
        if let Some(message) = self.decoded()? {
            return Ok(Some(message));
        }
        self.stream.set_nonblocking(true)?;
        let filled = self.fill();
        self.stream.set_nonblocking(false)?;
        match filled {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(error),
            Ok(()) => self.decoded(),
        }
    }

    /// Makes [`receive`](Client::receive) fail with `WouldBlock` or `TimedOut` after
    /// `timeout` without a message; `None` waits for ever.
    pub fn set_read_timeout(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        self.stream.set_read_timeout(timeout)
    }

    /// The underlying socket.
    pub fn stream(&self) -> &TcpStream {
        &self.stream
    }

    fn decoded(&mut self) -> io::Result<Option<Outbound>> {
        match decode_outbound(&self.input[self.start..]) {
            Ok(Some((message, len))) => {
                self.start += len;
                Ok(Some(message))
            }
            Ok(None) => Ok(None),
            Err(error) => Err(io::Error::new(io::ErrorKind::InvalidData, error)),
        }
    }

    /// Reads what the socket holds.
    fn fill(&mut self) -> io::Result<()> {
        self.input.drain(..self.start);
        self.start = 0;
        let len = self.input.len();
        self.input.resize(len + (64 << 10), 0);
        let read = self.stream.read(&mut self.input[len..]);
        self.input.truncate(len + *read.as_ref().unwrap_or(&0));
        match read? {
            0 => Err(io::ErrorKind::UnexpectedEof.into()),
            _ => Ok(()),
        }
    }
}

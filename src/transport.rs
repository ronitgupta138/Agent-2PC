//! Multi-Node Cluster Network Transport for Distributed 2PC.
//!
//! Provides TCP socket network transport and remote RPC message dispatching
//! for cross-container and multi-host autonomous AI agent clusters.

use crate::cluster::ClusterMessage;
use crate::domain::TxError;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Remote cluster peer client over TCP socket.
pub struct ClusterTransportClient {
    endpoint: String,
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl ClusterTransportClient {
    /// Connect to remote cluster peer over TCP.
    pub fn connect(endpoint: impl Into<String>) -> Result<Self, TxError> {
        let endpoint = endpoint.into();
        let stream = TcpStream::connect(&endpoint)?;
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        stream.set_write_timeout(Some(Duration::from_secs(10)))?;
        let writer = stream.try_clone()?;
        let reader = BufReader::new(stream);
        Ok(Self { endpoint, reader, writer })
    }

    /// Send a cluster message to the remote peer and await response packet.
    pub fn send_message(&mut self, msg: &ClusterMessage) -> Result<ClusterMessage, TxError> {
        let mut line = serde_json::to_string(msg)?;
        line.push('\n');
        self.writer.write_all(line.as_bytes())?;
        self.writer.flush()?;

        let mut response_line = String::new();
        self.reader.read_line(&mut response_line)?;

        if response_line.trim().is_empty() {
            return Err(TxError::Other("Empty response from remote cluster peer".into()));
        }

        let resp: ClusterMessage = serde_json::from_str(&response_line)?;
        Ok(resp)
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

/// TCP listener server handling distributed 2PC messages for a participant node.
pub struct ClusterNodeServer {
    bind_addr: String,
    running: Arc<AtomicBool>,
}

impl ClusterNodeServer {
    pub fn new(bind_addr: impl Into<String>) -> Self {
        Self {
            bind_addr: bind_addr.into(),
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn bind_addr(&self) -> &str {
        &self.bind_addr
    }

    /// Start cluster node listener with a custom request handler callback.
    pub fn serve<F>(&self, handler: F) -> Result<(), TxError>
    where
        F: Fn(ClusterMessage) -> ClusterMessage + Send + Sync + 'static,
    {
        let listener = TcpListener::bind(&self.bind_addr)?;
        listener.set_nonblocking(true)?;
        self.running.store(true, Ordering::SeqCst);

        let handler = Arc::new(handler);

        while self.running.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let h_clone = Arc::clone(&handler);
                    thread::spawn(move || {
                        let _ = stream.set_read_timeout(Some(Duration::from_secs(15)));
                        let _ = stream.set_write_timeout(Some(Duration::from_secs(15)));

                        let reader = BufReader::new(match stream.try_clone() {
                            Ok(s) => s,
                            Err(_) => return,
                        });

                        for line in reader.lines().map_while(Result::ok) {
                            if line.trim().is_empty() {
                                continue;
                            }
                            if let Ok(msg) = serde_json::from_str::<ClusterMessage>(&line) {
                                let reply = h_clone(msg);
                                if let Ok(mut out) = serde_json::to_string(&reply) {
                                    out.push('\n');
                                    let _ = stream.write_all(out.as_bytes());
                                    let _ = stream.flush();
                                }
                            }
                        }
                    });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(e) => return Err(TxError::Io(e)),
            }
        }

        Ok(())
    }

    /// Signal server to stop listening.
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }
}

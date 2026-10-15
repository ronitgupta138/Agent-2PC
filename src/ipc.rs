//! IPC Protocol and Client for agent-2pc Unix Domain Socket Daemon.

use crate::domain::TxError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Wire request payload sent to the agent-2pc daemon.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpcRequest {
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

/// Wire response payload returned by the agent-2pc daemon.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpcResponse {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Client connection for communicating with the local agent-2pc daemon.
pub struct IpcClient {
    socket_path: PathBuf,
    stream: UnixStream,
}

impl IpcClient {
    /// Attempt to connect to the daemon's Unix domain socket.
    pub fn try_connect(socket_path: impl AsRef<Path>) -> Result<Self, TxError> {
        let socket_path = socket_path.as_ref().to_path_buf();
        let stream = UnixStream::connect(&socket_path)?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        Ok(Self { socket_path, stream })
    }

    /// Check if an active daemon is responding to ping over the specified socket path.
    pub fn is_alive(socket_path: impl AsRef<Path>) -> bool {
        let path = socket_path.as_ref();
        if !path.exists() {
            return false;
        }
        if let Ok(mut client) = Self::try_connect(path) {
            match client.call("ping", Value::Null) {
                Ok(resp) => resp.get("status").and_then(|v| v.as_str()) == Some("ok"),
                Err(_) => false,
            }
        } else {
            false
        }
    }

    /// Issue a synchronous request to the daemon and await response.
    pub fn call(&mut self, method: &str, params: Value) -> Result<Value, TxError> {
        let req = IpcRequest {
            method: method.to_string(),
            params,
        };
        let mut line = serde_json::to_string(&req)?;
        line.push('\n');
        self.stream.write_all(line.as_bytes())?;
        self.stream.flush()?;

        let mut reader = BufReader::new(&mut self.stream);
        let mut response_line = String::new();
        reader.read_line(&mut response_line)?;

        if response_line.trim().is_empty() {
            return Err(TxError::Other("Empty response from daemon".into()));
        }

        let resp: IpcResponse = serde_json::from_str(&response_line)?;
        if resp.success {
            Ok(resp.data.unwrap_or(Value::Null))
        } else {
            Err(TxError::Other(
                resp.error.unwrap_or_else(|| "Unknown IPC daemon error".into()),
            ))
        }
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }
}

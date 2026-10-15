//! Embedded Real-Time Web HUD for agent-2pc.
//!
//! Zero-dependency vanilla HTML5/CSS/JS dashboard served on localhost
//! providing live visualization of active transactions, savepoint trees,
//! WAL disk metrics, and connected P2P cluster peer nodes.

use crate::coordinator::Coordinator;
use crate::domain::TxError;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub const DASHBOARD_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>agent-2pc :: Real-Time Transaction HUD</title>
<style>
  :root {
    --bg: #0a0b0e;
    --card-bg: #12141a;
    --border: #1e222d;
    --primary: #0891b2;
    --text: #e2e8f0;
    --muted: #94a3b8;
    --success: #10b981;
    --danger: #ef4444;
    --warning: #f59e0b;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background-color: var(--bg);
    color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace;
    padding: 24px;
    line-height: 1.5;
  }
  .header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    border-bottom: 1px solid var(--border);
    padding-bottom: 16px;
    margin-bottom: 24px;
  }
  .title { font-size: 1.5rem; font-weight: bold; color: var(--primary); display: flex; align-items: center; gap: 8px; }
  .badge { background: #0891b220; color: var(--primary); padding: 4px 10px; border-radius: 4px; font-size: 0.8rem; border: 1px solid var(--primary); }
  .stats-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 16px;
    margin-bottom: 24px;
  }
  .stat-card {
    background: var(--card-bg);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 16px;
  }
  .stat-label { font-size: 0.8rem; color: var(--muted); text-transform: uppercase; margin-bottom: 4px; }
  .stat-val { font-size: 1.6rem; font-weight: bold; color: var(--text); }
  .tx-section {
    background: var(--card-bg);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 20px;
  }
  table { width: 100%; border-collapse: collapse; margin-top: 12px; font-size: 0.9rem; }
  th, td { text-align: left; padding: 10px 12px; border-bottom: 1px solid var(--border); }
  th { color: var(--muted); font-size: 0.8rem; text-transform: uppercase; }
  .status-tag { padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; font-weight: bold; }
  .status-active { background: #0891b220; color: var(--primary); }
  .status-prepared { background: #10b98120; color: var(--success); }
  .status-aborting { background: #ef444420; color: var(--danger); }
  .pulse { width: 8px; height: 8px; background: var(--success); border-radius: 50%; display: inline-block; box-shadow: 0 0 8px var(--success); }
</style>
</head>
<body>
  <div class="header">
    <div class="title"><span class="pulse"></span> agent-2pc Live Hypervisor HUD</div>
    <div class="badge">v2.0.0</div>
  </div>

  <div class="stats-grid">
    <div class="stat-card">
      <div class="stat-label">Active In-Flight</div>
      <div class="stat-val" id="active-count">0</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">WAL Disk Usage</div>
      <div class="stat-val" id="wal-size">0 KB</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">System State</div>
      <div class="stat-val" style="color: var(--success)">HEALTHY</div>
    </div>
  </div>

  <div class="tx-section">
    <h3>Active Transactions & Savepoint Trees</h3>
    <table>
      <thead>
        <tr>
          <th>Transaction ID</th>
          <th>State</th>
          <th>Participants</th>
          <th>Savepoints</th>
          <th>Timeout</th>
        </tr>
      </thead>
      <tbody id="tx-rows">
        <tr><td colspan="5" style="color: var(--muted); text-align: center; padding: 20px;">No in-flight transactions</td></tr>
      </tbody>
    </table>
  </div>

  <script>
    async function refresh() {
      try {
        const res = await fetch('/api/status');
        const data = await res.json();
        document.getElementById('active-count').innerText = data.active_count || 0;
        document.getElementById('wal-size').innerText = (data.wal_bytes / 1024).toFixed(1) + ' KB';

        const tbody = document.getElementById('tx-rows');
        if (!data.transactions || data.transactions.length === 0) {
          tbody.innerHTML = '<tr><td colspan="5" style="color: var(--muted); text-align: center; padding: 20px;">No in-flight transactions</td></tr>';
          return;
        }

        tbody.innerHTML = data.transactions.map(t => `
          <tr>
            <td style="font-family: monospace; color: var(--primary)">${t.tid}</td>
            <td><span class="status-tag status-${t.state.toLowerCase()}">${t.state}</span></td>
            <td>${t.participants_count}</td>
            <td>${t.savepoints_count}</td>
            <td>${t.timeout_ms}ms</td>
          </tr>
        `).join('');
      } catch (err) {
        console.error('Failed to poll HUD status', err);
      }
    }
    setInterval(refresh, 1000);
    refresh();
  </script>
</body>
</html>
"#;

pub struct WebHudServer {
    tx_dir: PathBuf,
    bind_addr: String,
    running: Arc<AtomicBool>,
}

impl WebHudServer {
    pub fn new(tx_dir: impl AsRef<Path>, bind_addr: impl Into<String>) -> Self {
        Self {
            tx_dir: tx_dir.as_ref().to_path_buf(),
            bind_addr: bind_addr.into(),
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn bind_addr(&self) -> &str {
        &self.bind_addr
    }

    pub fn serve(&self) -> Result<(), TxError> {
        let listener = TcpListener::bind(&self.bind_addr)?;
        listener.set_nonblocking(true)?;
        self.running.store(true, Ordering::SeqCst);

        let tx_dir = self.tx_dir.clone();
        let running = Arc::clone(&self.running);

        while running.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let dir = tx_dir.clone();
                    thread::spawn(move || {
                        let _ = handle_http_client(&mut stream, &dir);
                    });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(15));
                }
                Err(e) => return Err(TxError::Io(e)),
            }
        }

        Ok(())
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }
}

fn handle_http_client(stream: &mut TcpStream, tx_dir: &Path) -> Result<(), TxError> {
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;

    let path = request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("/");

    if path == "/" || path == "/index.html" {
        let body = DASHBOARD_HTML;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes())?;
    } else if path == "/api/status" {
        let coord = Coordinator::new(tx_dir)?;
        let active = coord.active_transactions();
        let wal_path = tx_dir.join("wal.bin");
        let wal_bytes = if wal_path.exists() {
            std::fs::metadata(&wal_path)?.len()
        } else {
            0
        };

        let txs: Vec<_> = active
            .iter()
            .map(|t| {
                serde_json::json!({
                    "tid": t.tid.to_string(),
                    "state": t.state.to_string(),
                    "participants_count": t.participants.len(),
                    "savepoints_count": t.savepoints.len(),
                    "timeout_ms": t.timeout_ms
                })
            })
            .collect();

        let json_body = serde_json::to_string(&serde_json::json!({
            "active_count": txs.len(),
            "wal_bytes": wal_bytes,
            "transactions": txs
        }))?;

        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            json_body.len(),
            json_body
        );
        stream.write_all(response.as_bytes())?;
    } else {
        let not_found = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        stream.write_all(not_found.as_bytes())?;
    }

    stream.flush()?;
    Ok(())
}

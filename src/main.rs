// src/main.rs
// Just a single-node and non-pipelining
use bytes::{Buf, BytesMut};
use fxhash::FxHasher;
use std::{
    collections::HashMap,
    hash::{BuildHasherDefault, Hasher},
    net::SocketAddr,
    str,
    sync::Arc,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};

const SHARDS: usize = 16;
type FastHashMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;

#[derive(Clone)]
struct Db {
    shards: Arc<Vec<Arc<RwLock<FastHashMap<String, String>>>>>,
}

impl Db {
    fn new() -> Self {
        let mut shards = Vec::new();
        for _ in 0..SHARDS {
            shards.push(Arc::new(RwLock::new(FastHashMap::default())));
        }
        Self {
            shards: Arc::new(shards),
        }
    }

    fn shard_for(&self, key: &str) -> usize {
        let mut hasher = FxHasher::default();
        hasher.write(key.as_bytes());
        (hasher.finish() as usize) % SHARDS
    }

    async fn set(&self, key: String, val: String) {
        let idx = self.shard_for(&key);
        let mut map = self.shards[idx].write().await;
        map.insert(key, val);
    }

    async fn get(&self, key: &str) -> Option<String> {
        let idx = self.shard_for(key);
        let map = self.shards[idx].read().await;
        map.get(key).cloned()
    }
}

// ───────────────────────── RESP parser ─────────────────────────
fn parse_resp(buf: &mut BytesMut) -> Option<Vec<String>> {
    if buf.is_empty() || buf[0] != b'*' {
        return None;
    }

    let mut pos = match buf.windows(2).position(|w| w == b"\r\n") {
        Some(p) => p,
        None => return None,
    };
    let n: usize = str::from_utf8(&buf[1..pos]).ok()?.parse().ok()?;
    buf.advance(pos + 2);

    let mut result = Vec::with_capacity(n);

    for _ in 0..n {
        if buf.is_empty() || buf[0] != b'$' {
            return None;
        }

        pos = match buf.windows(2).position(|w| w == b"\r\n") {
            Some(p) => p,
            None => return None,
        };
        let len: usize = str::from_utf8(&buf[1..pos]).ok()?.parse().ok()?;
        buf.advance(pos + 2);

        if buf.len() < len + 2 {
            return None;
        }

        // Copy into String (acceptable for simple test). Later you can avoid this copy.
        let s = String::from_utf8(buf[..len].to_vec()).ok()?;
        buf.advance(len + 2);
        result.push(s);
    }
    Some(result)
}
// ───────────────────────────────────────────────────────────────

async fn handle_conn(stream: TcpStream, db: Db, peer: SocketAddr) {
    // Split into read/write halves
    let (mut reader, writer) = stream.into_split();
    // BufWriter around write half for buffered writes + explicit flush
    let mut writer = BufWriter::new(writer);
    let mut buf = BytesMut::with_capacity(4096);

    loop {
        // Read into buffer
        let n = match reader.read_buf(&mut buf).await {
            Ok(0) => return, // closed
            Ok(n) => n,
            Err(_) => return,
        };

        if n == 0 {
            break;
        }

        while let Some(frame) = parse_resp(&mut buf) {
            // Basic command dispatch
            match frame[0].to_ascii_uppercase().as_str() {
                "PING" => {
                    if writer.write_all(b"+PONG\r\n").await.is_err() {
                        return;
                    }
                    if writer.flush().await.is_err() {
                        return;
                    }
                }
                "SET" if frame.len() >= 3 => {
                    db.set(frame[1].clone(), frame[2].clone()).await;
                    if writer.write_all(b"+OK\r\n").await.is_err() {
                        return;
                    }
                    if writer.flush().await.is_err() {
                        return;
                    }
                }
                "GET" if frame.len() == 2 => {
                    if let Some(val) = db.get(&frame[1]).await {
                        // Proper RESP bulk string: $len\r\n<data>\r\n
                        let resp = format!("${}\r\n{}\r\n", val.len(), val);
                        if writer.write_all(resp.as_bytes()).await.is_err() {
                            return;
                        }
                        if writer.flush().await.is_err() {
                            return;
                        }
                    } else {
                        if writer.write_all(b"$-1\r\n").await.is_err() {
                            return;
                        }
                        if writer.flush().await.is_err() {
                            return;
                        }
                    }
                }
                "CONFIG" => {
                    // Minimal stub so redis-benchmark won't warn (it tries CONFIG GET *)
                    // We return an empty array: *0\r\n
                    if writer.write_all(b"*0\r\n").await.is_err() {
                        return;
                    }
                    if writer.flush().await.is_err() {
                        return;
                    }
                }
                _ => {
                    if writer.write_all(b"-ERR unknown command\r\n").await.is_err() {
                        return;
                    }
                    if writer.flush().await.is_err() {
                        return;
                    }
                }
            }
        }
    }

    println!("Connection from {} closed", peer);
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> anyhow::Result<()> {
    let addr = "127.0.0.1:6379";
    let listener = TcpListener::bind(addr).await?;
    println!("mini async Redis-compatible server running at {}", addr);

    let db = Db::new();

    loop {
        let (stream, peer) = listener.accept().await?;
        let db_clone = db.clone();
        tokio::spawn(async move {
            handle_conn(stream, db_clone, peer).await;
        });
    }
}

// Got some issues with SET command, not properly working.

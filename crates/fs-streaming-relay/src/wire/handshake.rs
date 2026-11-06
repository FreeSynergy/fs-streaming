//! The RTMP handshake over a socket, with a time limit.
//!
//! Pattern: Adapter over `rml_rtmp::handshake::Handshake` — a peer that stalls here is cut off after
//! `timeout`, and bytes that came after the handshake are handed back to the caller.

use std::time::Duration;

use rml_rtmp::handshake::{Handshake, HandshakeProcessResult, PeerType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::Duplex;
use crate::error::WireError;

/// Run the handshake as `peer`; returns the bytes that followed it.
pub async fn handshake<S: Duplex>(
    io: &mut S,
    peer: PeerType,
    timeout: Duration,
) -> Result<Vec<u8>, WireError> {
    tokio::time::timeout(timeout, run(io, peer))
        .await
        .map_err(|_| WireError::Timeout)?
}

async fn run<S: Duplex>(io: &mut S, peer: PeerType) -> Result<Vec<u8>, WireError> {
    let client = peer == PeerType::Client;
    let mut hs = Handshake::new(peer);
    if client {
        let first = hs
            .generate_outbound_p0_and_p1()
            .map_err(|_| WireError::Protocol)?;
        io.write_all(&first).await?;
    }
    let mut buf = [0u8; 4096];
    loop {
        let n = io.read(&mut buf).await?;
        if n == 0 {
            return Err(WireError::Closed);
        }
        match hs
            .process_bytes(&buf[..n])
            .map_err(|_| WireError::Protocol)?
        {
            HandshakeProcessResult::InProgress { response_bytes } => {
                io.write_all(&response_bytes).await?;
            }
            HandshakeProcessResult::Completed {
                response_bytes,
                remaining_bytes,
            } => {
                io.write_all(&response_bytes).await?;
                io.flush().await?;
                return Ok(remaining_bytes);
            }
        }
    }
}

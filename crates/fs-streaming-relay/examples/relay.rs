//! A real relay on the command line: listens for one sender and forwards to the targets given.
//!
//! Pattern: Facade user — builds the core service and the relay, creates one stream, adds the targets.
//!
//! ```text
//! cargo run --example relay -- [--bind 127.0.0.1:1935] [--record DIR] URL [--key KEY | --key-env VAR] [URL ...]
//! ```
//!
//! A `--key` on the command line can be read by other users of the machine; prefer `--key-env`. The
//! publisher key and the address to send to are printed once. State is kept in memory only.

use std::sync::Arc;

use fs_objects::TursoStore;
use fs_objects::traits::UnloggedObjects;
use fs_streaming_core::{StreamService, Streams};
use fs_streaming_relay::{FolderSink, RelayConfig, RtmpRelay};

struct Args {
    bind: Option<std::net::SocketAddr>,
    record: Option<String>,
    targets: Vec<(String, String)>,
}

fn parse() -> Result<Args, String> {
    let mut args = Args {
        bind: None,
        record: None,
        targets: vec![],
    };
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--bind" => {
                args.bind = Some(
                    it.next()
                        .ok_or("--bind needs an address")?
                        .parse()
                        .map_err(|_| "bad address")?,
                );
            }
            "--record" => args.record = Some(it.next().ok_or("--record needs a folder")?),
            "--key" => {
                args.targets.last_mut().ok_or("--key comes after a URL")?.1 =
                    it.next().ok_or("--key needs a value")?;
            }
            "--key-env" => {
                let var = it.next().ok_or("--key-env needs a name")?;
                args.targets
                    .last_mut()
                    .ok_or("--key-env comes after a URL")?
                    .1 = std::env::var(var).map_err(|_| "that variable is not set")?;
            }
            url => args.targets.push((url.to_string(), String::new())),
        }
    }
    Ok(args)
}

#[tokio::main]
async fn main() {
    let args = match parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!(
                "error: {e}\nusage: relay [--bind ADDR] [--record DIR] URL [--key KEY | --key-env VAR] [URL ...]"
            );
            std::process::exit(2);
        }
    };
    let store: Arc<dyn UnloggedObjects> =
        Arc::new(TursoStore::open_in_memory().await.expect("in-memory store"));
    let service = StreamService::new(store);
    let mut cfg = RelayConfig::new();
    if let Some(b) = args.bind {
        cfg = cfg.bind(b);
    }
    if let Some(dir) = &args.record {
        cfg = cfg.sink(Arc::new(FolderSink::new(dir)));
    }
    let relay = RtmpRelay::start(service.clone(), cfg)
        .await
        .expect("listen");
    service.attach(relay.clone());
    let created = service.create("relay").await.expect("create the stream");
    for (i, (url, key)) in args.targets.iter().enumerate() {
        if let Err(e) = service
            .add_target(created.stream.id, &format!("target {}", i + 1), url, key)
            .await
        {
            eprintln!("target {} refused: {e}", i + 1);
            std::process::exit(2);
        }
    }
    if args.record.is_some() {
        service
            .set_recording(created.stream.id, true)
            .await
            .expect("recording");
    }
    println!("send to: {}", created.ingest_url);
    println!(
        "stream key (shown once): {}",
        created.publisher_key.expose()
    );
    println!(
        "for ffmpeg: -f flv {}/{}",
        created.ingest_url,
        created.publisher_key.expose()
    );
    let _ = tokio::signal::ctrl_c().await;
    relay.shutdown().await;
}

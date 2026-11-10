//! Opening the program: store, core, relay.
//!
//! Pattern: Facade — one call builds what the window needs, in this process and with the local store, as the
//! core's DESIGN opens it (streams and targets in a store of their own, never connected to the hub). The relay
//! listens on loopback unless `--bind` says otherwise; the store file is private to the user.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use fs_objects::TursoStore;
use fs_objects::traits::UnloggedObjects;
use fs_streaming_core::{Gate, StreamService, Streams};
use fs_streaming_relay::{FolderSink, RelayConfig, RtmpRelay};
use tokio::runtime::{Handle, Runtime};

use crate::args::Args;

/// The running program: the core, the relay and the runtime that drives them.
pub struct Program {
    runtime: Runtime,
    service: Arc<StreamService>,
    relay: Option<Arc<RtmpRelay>>,
}

impl Program {
    /// The core service itself (for a host or a test that needs more than `Streams`).
    pub fn service(&self) -> Arc<StreamService> {
        self.service.clone()
    }

    /// The core's `Streams`.
    pub fn streams(&self) -> Arc<dyn Streams> {
        self.service.clone()
    }

    /// The runtime's handle.
    pub fn handle(&self) -> Handle {
        self.runtime.handle().clone()
    }

    /// End the relay's tasks.
    pub fn shutdown(&self) {
        if let Some(relay) = &self.relay {
            self.runtime.block_on(relay.shutdown());
        }
    }
}

/// `$XDG_DATA_HOME/freesynergy/streaming`, else `$HOME/.local/share/...`, else (Windows) `%LOCALAPPDATA%\...`.
fn data_dir() -> Result<PathBuf, String> {
    let base = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/share")))
        .or_else(|| std::env::var_os("LOCALAPPDATA").map(PathBuf::from))
        .ok_or("no data folder: give --store or set $XDG_DATA_HOME, $HOME or %LOCALAPPDATA%")?;
    Ok(base.join("freesynergy").join("streaming"))
}

/// A folder only the user can enter: the store holds target keys.
fn private_dir(dir: &Path) -> Result<(), String> {
    let mut b = std::fs::DirBuilder::new();
    b.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut b, 0o700);
    b.create(dir).map_err(|e| format!("{}: {e}", dir.display()))
}

/// Open the store, the core and the relay.
///
/// # Errors
/// A message for the person: no runtime, a store that does not open.
pub fn open_program(args: &Args) -> Result<Program, String> {
    let runtime = Runtime::new().map_err(|e| e.to_string())?;
    let (file, folder) = match (&args.store, args.snapshot.is_some()) {
        (Some(f), _) => (Some(f.clone()), f.with_extension("recordings")),
        // A picture alone keeps nothing.
        (None, true) => (None, std::env::temp_dir().join("fs-streaming-recordings")),
        (None, false) => {
            let dir = data_dir()?;
            private_dir(&dir)?;
            (Some(dir.join("app.db")), dir.join("recordings"))
        }
    };
    let folder = args.record.clone().unwrap_or(folder);
    let store: Arc<dyn UnloggedObjects> = runtime
        .block_on(async {
            match &file {
                Some(f) => TursoStore::open(f)
                    .await
                    .map(|s| Arc::new(s) as Arc<dyn UnloggedObjects>),
                None => TursoStore::open_in_memory()
                    .await
                    .map(|s| Arc::new(s) as Arc<dyn UnloggedObjects>),
            }
        })
        .map_err(|e| format!("cannot open the store: {e}"))?;
    let service = StreamService::new(store);
    let mut config = RelayConfig::new().sink(Arc::new(FolderSink::new(folder)));
    if let Some(addr) = args.bind {
        config = config.bind(addr);
    }
    // A snapshot draws a page and leaves: it opens no port.
    let relay = if args.snapshot.is_some() {
        None
    } else {
        match runtime.block_on(RtmpRelay::start(service.clone(), config)) {
            Ok(relay) => {
                service.attach(relay.clone());
                Some(relay)
            }
            Err(e) => {
                eprintln!("fs-streaming: the relay cannot listen: {e}");
                runtime.block_on(service.listen_failed());
                None
            }
        }
    };
    Ok(Program {
        runtime,
        service,
        relay,
    })
}

//! The words after the program name.
//!
//! Pattern: Value object + parser — plain options, no secret: a stream key is never given on the command line.

use std::net::SocketAddr;
use std::path::PathBuf;

use fs_components_app::parse_size;

/// The help text.
pub const HELP: &str = "fs-streaming [--store FILE] [--record DIR] [--bind ADDR] [--size WxH] [--snapshot FILE.png]\n\n\
  --store FILE      where streams and targets are kept (default: the data folder of the user)\n\
  --record DIR      where recordings go (default: next to the store)\n\
  --bind ADDR       where senders connect (default 127.0.0.1, a port the system picks); ingest is plain RTMP\n\
  --size WxH        window size\n\
  --snapshot FILE   draw the first page into a picture and leave";

/// The options.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Args {
    /// Help was asked for.
    pub help: bool,
    /// The store file.
    pub store: Option<PathBuf>,
    /// The folder for recordings.
    pub record: Option<PathBuf>,
    /// Where the relay listens.
    pub bind: Option<SocketAddr>,
    /// Window size.
    pub size: (u32, u32),
    /// Draw a picture instead of opening a window.
    pub snapshot: Option<PathBuf>,
}

/// Read the words.
///
/// # Errors
/// A message for the person.
pub fn parse(words: &[String]) -> Result<Args, String> {
    let mut args = Args {
        help: false,
        store: None,
        record: None,
        bind: None,
        size: (1200, 800),
        snapshot: None,
    };
    let mut it = words.iter();
    while let Some(word) = it.next() {
        let mut value = |what: &str| it.next().ok_or_else(|| format!("{word} needs {what}"));
        match word.as_str() {
            "--help" | "-h" => args.help = true,
            "--store" => args.store = Some(value("a file")?.into()),
            "--record" => args.record = Some(value("a folder")?.into()),
            "--bind" => {
                args.bind = Some(
                    value("an address")?
                        .parse()
                        .map_err(|_| "--bind wants HOST:PORT, like 127.0.0.1:1935".to_owned())?,
                );
            }
            "--size" => args.size = parse_size(value("WIDTHxHEIGHT")?)?,
            "--snapshot" => args.snapshot = Some(value("a file")?.into()),
            other => return Err(format!("unknown word {other}; see --help")),
        }
    }
    Ok(args)
}

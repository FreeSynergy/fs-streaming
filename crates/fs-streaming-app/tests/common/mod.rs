//! A real core in a temporary store with a relay on loopback, and the two backends over it.
//! A key made by the test is a marker: it must not be found in any record or result.
#![allow(dead_code)]

use fs_admin_frontend::Values;
use fs_streaming_app::{CoreLink, Program, StreamBackend, TargetBackend, open_program, parse};
use fs_streaming_core::Streams;

pub struct Rig {
    pub program: Program,
    pub streams: StreamBackend,
    pub targets: TargetBackend,
    pub key: String,
    pub stream_id: String,
    _dir: tempfile::TempDir,
}

pub fn rig() -> Rig {
    let dir = tempfile::tempdir().unwrap();
    let store = dir.path().join("app.db");
    let args = parse(&["--store".into(), store.display().to_string()]).unwrap();
    let program = open_program(&args).unwrap();
    let created = program
        .handle()
        .block_on(program.service().create("morning show"))
        .unwrap();
    let core = CoreLink::new(program.streams(), program.handle());
    Rig {
        streams: StreamBackend::new(core.clone()),
        targets: TargetBackend::new(core),
        key: created.publisher_key.expose().to_owned(),
        stream_id: format!("urn:uuid:{}", created.stream.id),
        program,
        _dir: dir,
    }
}

pub fn v(pairs: &[(&str, &[&str])]) -> Values {
    pairs
        .iter()
        .map(|(k, vals)| {
            (
                (*k).to_owned(),
                vals.iter().map(|s| (*s).to_owned()).collect(),
            )
        })
        .collect()
}

pub fn dump(values: &Values) -> String {
    format!("{values:?}")
}

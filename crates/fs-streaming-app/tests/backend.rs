//! The backends against the traits `Backend` and `Streams`, over a real core with a real store file and a relay on
//! loopback. A key made by the test is a marker: it must not be found in any record or result.

use fs_admin_frontend::{Backend, BackendError, Values};
use fs_streaming_app::{CoreLink, Program, StreamBackend, TargetBackend, open_program, parse};
use fs_streaming_core::Streams;
use fs_streaming_core::describe::{Owner, table};

struct Rig {
    program: Program,
    streams: StreamBackend,
    targets: TargetBackend,
    key: String,
    stream_id: String,
    _dir: tempfile::TempDir,
}

fn rig() -> Rig {
    let dir = tempfile::tempdir().unwrap();
    let store = dir.path().join("app.db");
    let args = parse(&["--store".into(), store.display().to_string()]).unwrap();
    let program = open_program(&args).unwrap();
    let service = program.service();
    let created = program
        .handle()
        .block_on(service.create("morning show"))
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

fn v(pairs: &[(&str, &[&str])]) -> Values {
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

fn dump(values: &Values) -> String {
    format!("{values:?}")
}

#[test]
fn a_stream_is_listed_without_its_key() {
    let r = rig();
    let list = r.streams.list(&Values::new()).unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].id, r.stream_id);
    assert_eq!(list[0].values["name"], ["morning show"]);
    assert_eq!(list[0].values["state"], ["idle"]);
    assert!(!dump(&list[0].values).contains(&r.key));
    assert!(list[0].values["ingestUrl"][0].starts_with("rtmp://"));
    r.program.shutdown();
}

#[test]
fn targets_are_added_edited_switched_and_removed_and_never_show_a_key() {
    let r = rig();
    let added = r
        .targets
        .run(
            "add",
            &v(&[
                ("stream", &[&r.stream_id]),
                ("name", &["main"]),
                ("url", &["rtmps://live.example.org/app"]),
            ]),
        )
        .unwrap();
    let id = added.id.unwrap();
    let list = r.targets.list(&Values::new()).unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].values["hasKey"], ["no"]);
    assert_eq!(list[0].values["secure"], ["yes"]);
    assert!(
        list[0]
            .values
            .keys()
            .all(|k| !k.to_lowercase().ends_with("key") || k == "hasKey")
    );
    r.targets
        .run(
            "edit",
            &v(&[
                ("id", &[&id]),
                ("name", &["second"]),
                ("url", &["rtmp://127.0.0.1:9/app"]),
            ]),
        )
        .unwrap();
    r.targets
        .run("setEnabled", &v(&[("id", &[&id]), ("enabled", &["off"])]))
        .unwrap();
    let list = r.targets.list(&v(&[("stream", &[&r.stream_id])])).unwrap();
    assert_eq!(list[0].values["name"], ["second"]);
    assert_eq!(list[0].values["enabled"], ["off"]);
    assert_eq!(list[0].values["state"], ["off"]);
    r.targets.run("remove", &v(&[("id", &[&id])])).unwrap();
    assert!(r.targets.list(&Values::new()).unwrap().is_empty());
    r.program.shutdown();
}

#[test]
fn a_bad_address_is_a_finding_on_the_address() {
    let r = rig();
    let e = r
        .targets
        .run(
            "add",
            &v(&[
                ("stream", &[&r.stream_id]),
                ("name", &["x"]),
                ("url", &["http://example.org/a"]),
            ]),
        )
        .unwrap_err();
    match e {
        BackendError::Rejected(f) => assert_eq!(f[0].field, "url"),
        other => panic!("{other:?}"),
    }
    r.program.shutdown();
}

#[test]
fn rename_recording_check_stop_status_and_delete_work() {
    let r = rig();
    let id = &r.stream_id;
    r.streams
        .run("rename", &v(&[("id", &[id]), ("name", &["evening"])]))
        .unwrap();
    r.streams
        .run("setRecording", &v(&[("id", &[id]), ("recording", &["on"])]))
        .unwrap();
    let s = &r.streams.list(&Values::new()).unwrap()[0];
    assert_eq!(
        (
            s.values["name"][0].as_str(),
            s.values["recording"][0].as_str()
        ),
        ("evening", "on")
    );
    r.targets
        .run(
            "add",
            &v(&[
                ("stream", &[id]),
                ("name", &["dead"]),
                ("url", &["rtmp://127.0.0.1:1/app"]),
            ]),
        )
        .unwrap();
    let checked = r.streams.run("check", &v(&[("id", &[id])])).unwrap();
    assert_eq!(checked.values["reached"], ["no"]);
    assert_eq!(checked.values["target"].len(), 1);
    assert!(!dump(&checked.values).contains(&r.key));
    r.streams.run("stop", &v(&[("id", &[id])])).unwrap();
    let st = r.streams.run("status", &v(&[("id", &[id])])).unwrap();
    assert_eq!(st.values["state"], ["idle"]);
    assert!(!dump(&st.values).contains(&r.key));
    r.streams.run("delete", &v(&[("id", &[id])])).unwrap();
    assert!(r.streams.list(&Values::new()).unwrap().is_empty());
    r.program.shutdown();
}

#[test]
fn a_command_that_must_not_run_answers_unsupported_with_its_name() {
    let r = rig();
    for (owner, name) in [
        (Owner::Stream, "create"),
        (Owner::Stream, "rotateKey"),
        (Owner::Stream, "transcode"),
        (Owner::Target, "setKey"),
    ] {
        let backend: &dyn Backend = if owner == Owner::Stream {
            &r.streams
        } else {
            &r.targets
        };
        match backend.run(name, &Values::new()) {
            Err(BackendError::Unavailable(m)) => assert_eq!(m, format!("unsupported:{name}")),
            other => panic!("{name}: {other:?}"),
        }
    }
    assert!(matches!(
        r.streams.run("nonsense", &Values::new()),
        Err(BackendError::NotFound)
    ));
    r.program.shutdown();
}

#[test]
fn every_supported_row_has_a_handler() {
    let r = rig();
    for row in table().into_iter().filter(|row| row.supported) {
        let backend: &dyn Backend = if row.owner == Owner::Stream {
            &r.streams
        } else {
            &r.targets
        };
        if let Err(BackendError::Unavailable(m)) = backend.run(&row.command.name, &Values::new()) {
            assert!(!m.starts_with("no handler"), "{}", row.command.name);
        }
    }
    r.program.shutdown();
}

#[test]
fn supported_follows_the_table() {
    let r = rig();
    assert!(r.streams.supported("stop"));
    assert!(!r.streams.supported("create"));
    assert!(!r.streams.supported("nonsense"));
    assert!(r.targets.supported("add"));
    assert!(!r.targets.supported("setKey"));
    r.program.shutdown();
}

#[test]
fn the_core_still_has_the_streams_trait_objects() {
    let r = rig();
    let s: std::sync::Arc<dyn Streams> = r.program.streams();
    let page = r.program.handle().block_on(s.list(None)).unwrap();
    assert_eq!(page.items.len(), 1);
    r.program.shutdown();
}

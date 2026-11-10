//! The stream backend against the traits `Backend` and `Streams`, over a real core.

mod common;

use common::{dump, rig, v};
use fs_admin_frontend::{Backend, BackendError, Values};
use fs_streaming_core::describe::{Owner, table};
use fs_types::{BasicType, Edit};

#[test]
fn a_stream_is_listed_without_its_key_and_found_by_its_name() {
    let r = rig();
    let list = r.streams.list(&Values::new()).unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].id, r.stream_id);
    assert_eq!(list[0].values["name"], ["morning show"]);
    assert_eq!(list[0].values["state"], ["idle"]);
    assert!(!dump(&list[0].values).contains(&r.key));
    assert!(list[0].values["ingestUrl"][0].starts_with("rtmp://"));
    assert_eq!(
        r.streams.list(&v(&[("term", &["MORNING"])])).unwrap().len(),
        1
    );
    assert!(
        r.streams
            .list(&v(&[("term", &["evening"])]))
            .unwrap()
            .is_empty()
    );
    r.program.shutdown();
}

#[test]
fn change_is_typed_field_by_field_and_missing_means_unchanged() {
    let r = rig();
    let edit = Edit::new().set(&["name"], BasicType::Text, &["evening"]);
    r.streams.change(&r.stream_id, &edit).unwrap();
    let s = &r.streams.list(&Values::new()).unwrap()[0];
    assert_eq!(
        (
            s.values["name"][0].as_str(),
            s.values["recording"][0].as_str()
        ),
        ("evening", "off")
    );
    let edit = Edit::new().set(&["recording"], BasicType::Choice, &["on"]);
    r.streams.change(&r.stream_id, &edit).unwrap();
    let s = &r.streams.list(&Values::new()).unwrap()[0];
    assert_eq!(
        (
            s.values["name"][0].as_str(),
            s.values["recording"][0].as_str()
        ),
        ("evening", "on")
    );
    r.program.shutdown();
}

#[test]
fn change_refuses_a_cleared_name_and_a_field_the_core_owns_and_changes_nothing() {
    let r = rig();
    for edit in [
        Edit::new().clear(&["name"], BasicType::Text),
        Edit::new().set(&["name"], BasicType::Text, &["x"]).set(
            &["state"],
            BasicType::Choice,
            &["live"],
        ),
    ] {
        assert!(
            matches!(r.streams.change(&r.stream_id, &edit), Err(BackendError::Rejected(f)) if !f.is_empty())
        );
    }
    assert_eq!(
        r.streams.list(&Values::new()).unwrap()[0].values["name"],
        ["morning show"]
    );
    assert!(matches!(
        r.streams.change(
            "urn:uuid:00000000-0000-7000-8000-000000000000",
            &Edit::new()
        ),
        Err(BackendError::NotFound)
    ));
    r.program.shutdown();
}

#[test]
fn recording_check_stop_status_and_delete_work_on_the_record() {
    let r = rig();
    let id = r.stream_id.as_str();
    r.streams
        .run("setRecording", Some(id), &v(&[("recording", &["on"])]))
        .unwrap();
    r.targets
        .run(
            "add",
            None,
            &v(&[
                ("stream", &[id]),
                ("name", &["dead"]),
                ("url", &["rtmp://127.0.0.1:1/app"]),
            ]),
        )
        .unwrap();
    let checked = r.streams.run("check", Some(id), &Values::new()).unwrap();
    assert_eq!(checked.values["reached"], ["no"]);
    assert_eq!(checked.values["target"].len(), 1);
    assert!(!dump(&checked.values).contains(&r.key));
    r.streams.run("stop", Some(id), &Values::new()).unwrap();
    let st = r.streams.run("status", Some(id), &Values::new()).unwrap();
    assert_eq!(st.values["state"], ["idle"]);
    assert!(!dump(&st.values).contains(&r.key));
    r.streams.run("delete", Some(id), &Values::new()).unwrap();
    assert!(r.streams.list(&Values::new()).unwrap().is_empty());
    r.program.shutdown();
}

#[test]
fn a_command_that_must_not_run_answers_unsupported_with_its_name() {
    let r = rig();
    for name in ["create", "rotateKey", "transcode"] {
        match r.streams.run(name, None, &Values::new()) {
            Err(BackendError::Unavailable(m)) => assert_eq!(m, format!("unsupported:{name}")),
            other => panic!("{name}: {other:?}"),
        }
    }
    assert!(matches!(
        r.streams.run("nonsense", None, &Values::new()),
        Err(BackendError::NotFound)
    ));
    r.program.shutdown();
}

#[test]
fn every_supported_row_has_a_handler_and_supported_follows_the_table() {
    let r = rig();
    for row in table().into_iter().filter(|row| row.owner == Owner::Stream) {
        assert_eq!(r.streams.supported(&row.command.name), row.supported);
        if row.supported
            && let Err(BackendError::Unavailable(m)) =
                r.streams.run(&row.command.name, None, &Values::new())
        {
            assert!(!m.starts_with("no handler"), "{}", row.command.name);
        }
    }
    assert!(!r.streams.supported("nonsense"));
    r.program.shutdown();
}

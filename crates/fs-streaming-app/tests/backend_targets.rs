//! The target backend against the traits `Backend` and `Streams`, over a real core. No key is ever in a record.

mod common;

use common::{rig, v};
use fs_admin_frontend::{Backend, BackendError, Values};
use fs_streaming_core::describe::{Owner, table};
use fs_types::{BasicType, Edit};

fn add(r: &common::Rig, name: &str, url: &str) -> Result<String, BackendError> {
    r.targets
        .run(
            "add",
            None,
            &v(&[
                ("stream", &[&r.stream_id]),
                ("name", &[name]),
                ("url", &[url]),
            ]),
        )
        .map(|o| o.id.unwrap())
}

#[test]
fn targets_are_added_changed_switched_and_removed_and_never_show_a_key() {
    let r = rig();
    let id = add(&r, "main", "rtmps://live.example.org/app").unwrap();
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
    // only the name is in the edit: the address stays
    r.targets
        .change(
            &id,
            &Edit::new().set(&["name"], BasicType::Text, &["second"]),
        )
        .unwrap();
    // only the flag is in the edit
    r.targets
        .change(
            &id,
            &Edit::new().set(&["enabled"], BasicType::Choice, &["off"]),
        )
        .unwrap();
    let list = r.targets.list(&v(&[("stream", &[&r.stream_id])])).unwrap();
    assert_eq!(list[0].values["name"], ["second"]);
    assert_eq!(list[0].values["url"], ["rtmps://live.example.org/app"]);
    assert_eq!(list[0].values["enabled"], ["off"]);
    assert_eq!(list[0].values["state"], ["off"]);
    r.targets
        .run("setEnabled", Some(&id), &v(&[("enabled", &["on"])]))
        .unwrap();
    assert_eq!(r.targets.list(&v(&[("term", &["SEC"])])).unwrap().len(), 1);
    r.targets.run("remove", Some(&id), &Values::new()).unwrap();
    assert!(r.targets.list(&Values::new()).unwrap().is_empty());
    r.program.shutdown();
}

#[test]
fn a_bad_address_is_a_finding_on_the_address_and_changes_nothing() {
    let r = rig();
    match add(&r, "x", "http://example.org/a") {
        Err(BackendError::Rejected(f)) => assert_eq!(f[0].field, "url"),
        other => panic!("{other:?}"),
    }
    let id = add(&r, "ok", "rtmp://127.0.0.1:9/app").unwrap();
    let e = r.targets.change(
        &id,
        &Edit::new().set(&["url"], BasicType::Link, &["http://example.org/a"]),
    );
    assert!(matches!(e, Err(BackendError::Rejected(f)) if f[0].field == "url"));
    assert!(matches!(
        r.targets.change(
            &id,
            &Edit::new().set(&["hasKey"], BasicType::Choice, &["yes"])
        ),
        Err(BackendError::Rejected(_))
    ));
    assert_eq!(
        r.targets.list(&Values::new()).unwrap()[0].values["url"],
        ["rtmp://127.0.0.1:9/app"]
    );
    r.program.shutdown();
}

#[test]
fn setting_a_key_is_not_run_and_supported_follows_the_table() {
    let r = rig();
    match r.targets.run("setKey", None, &Values::new()) {
        Err(BackendError::Unavailable(m)) => assert_eq!(m, "unsupported:setKey"),
        other => panic!("{other:?}"),
    }
    for row in table().into_iter().filter(|row| row.owner == Owner::Target) {
        assert_eq!(r.targets.supported(&row.command.name), row.supported);
        if row.supported
            && let Err(BackendError::Unavailable(m)) =
                r.targets.run(&row.command.name, None, &Values::new())
        {
            assert!(!m.starts_with("no handler"), "{}", row.command.name);
        }
    }
    r.program.shutdown();
}

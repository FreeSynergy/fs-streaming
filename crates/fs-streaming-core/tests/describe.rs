//! The type description of `Stream` and `Target`: against the trait `DescribesType` and the command table.

use std::collections::BTreeSet;

use fs_streaming_core::describe::{Owner, table};
use fs_streaming_core::{Stream, Target};
use fs_types::{DescribesType, TypeDescription, TypeRegistry};

/// The capability ids of `requirements.md`, as of 2026-11-10 (first column of its three tables).
const IDS: &str = include_str!("data/capability-ids.txt");

fn ids() -> BTreeSet<&'static str> {
    IDS.lines().filter(|l| !l.trim().is_empty()).collect()
}

/// The ids in the first cell of each table row of a `requirements.md`.
fn ids_of(markdown: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for line in markdown.lines().filter(|l| l.starts_with('|')) {
        let cell = line.split('|').nth(1).unwrap_or_default();
        for (i, part) in cell.split('`').enumerate() {
            let known_shape = part.split_once('.').is_some_and(|(a, b)| {
                !a.is_empty()
                    && !b.is_empty()
                    && a.bytes().chain(b.bytes()).all(|c| c.is_ascii_lowercase())
            });
            if i % 2 == 1 && known_shape {
                out.insert(part.to_owned());
            }
        }
    }
    out
}

#[test]
fn the_table_has_every_capability_id_of_the_requirements() {
    let have: BTreeSet<&str> = table()
        .iter()
        .flat_map(|r| r.capabilities.iter().copied())
        .collect();
    let missing: Vec<_> = ids().difference(&have).copied().collect();
    assert!(
        missing.is_empty(),
        "capability ids missing from the table: {missing:?}"
    );
    let unknown: Vec<_> = have.difference(&ids()).copied().collect();
    assert!(
        unknown.is_empty(),
        "ids in the table that requirements.md does not have: {unknown:?}"
    );
}

#[test]
fn the_list_of_ids_is_the_one_in_requirements_md_when_that_file_is_at_hand() {
    // `FS_REQUIREMENTS_MD` names the file; without it there is nothing to compare (the committed list is used).
    let Ok(path) = std::env::var("FS_REQUIREMENTS_MD") else {
        return;
    };
    let text = std::fs::read_to_string(path).expect("the file named by FS_REQUIREMENTS_MD");
    let live = ids_of(&text);
    let committed: BTreeSet<String> = ids().into_iter().map(str::to_owned).collect();
    assert_eq!(live, committed);
}

#[test]
fn the_id_extraction_reads_several_ids_in_one_cell() {
    let md = "| Control (`stream.list`, `stream.create`) | x |\n| Not planned | `no.thing` y |\n";
    let got = ids_of(md);
    assert_eq!(got.len(), 2, "{got:?}");
}

#[test]
fn a_capability_is_in_exactly_one_row() {
    let mut seen = BTreeSet::new();
    for r in table() {
        for c in r.capabilities {
            assert!(seen.insert(*c), "{c} is in two rows");
        }
    }
}

#[test]
fn every_row_says_why_when_it_is_not_supported_and_only_then() {
    for r in table() {
        assert_eq!(r.supported, r.reason.is_empty(), "{}", r.command.name);
    }
}

#[test]
fn both_types_enter_the_registry() {
    let mut reg = TypeRegistry::new();
    reg.register::<Stream>().expect("Stream is sound");
    reg.register::<Target>().expect("Target is sound");
}

#[test]
fn the_description_carries_every_row_of_its_type() {
    for (d, owner) in [
        (Stream::describe(), Owner::Stream),
        (Target::describe(), Owner::Target),
    ] {
        let rows: Vec<_> = table().into_iter().filter(|r| r.owner == owner).collect();
        assert_eq!(d.commands.len(), rows.len());
        for r in rows {
            let c = d.find_command(&r.command.name).expect("described");
            assert_eq!(
                (&c.kind, &c.input, &c.result),
                (&r.command.kind, &r.command.input, &r.command.result)
            );
        }
    }
}

#[test]
fn the_standard_is_named() {
    assert!(!Stream::describe().standard.is_empty());
    assert!(!Target::describe().standard.is_empty());
}

fn every_name(d: &TypeDescription) -> Vec<String> {
    let mut names: Vec<String> = d.fields.iter().map(|f| f.name.to_lowercase()).collect();
    for c in &d.commands {
        for f in c.input.iter().chain(&c.result) {
            names.push(f.name.to_lowercase());
        }
    }
    names
}

#[test]
fn no_field_or_argument_holds_a_key() {
    for d in [Stream::describe(), Target::describe()] {
        for n in every_name(&d) {
            assert!(
                n != "key" && !n.contains("secret") && !n.contains("password"),
                "{}: {n}",
                d.name
            );
            assert_ne!(n, "publisherkey");
            assert_ne!(n, "streamkey");
        }
    }
    // The only field that mentions a key is the yes/no flag; `check` reports the far end's verdict as `keyCheck`.
    let target = Target::describe();
    assert!(target.find_field("hasKey").is_some());
    assert!(target.find_field("key").is_none());
}

#[test]
fn a_command_on_one_record_refers_to_its_own_type() {
    for (d, own) in [
        (Stream::describe(), "Stream"),
        (Target::describe(), "Target"),
    ] {
        for name in ["delete", "stop", "remove", "setEnabled"] {
            if let Some(c) = d.find_command(name) {
                let id = c.input.first().expect("an input");
                assert_eq!(id.name, "id", "{name}");
                assert_eq!(id.target, own);
                assert!(id.required);
            }
        }
    }
}

#[test]
fn the_description_is_sound_and_flags_each_command_from_its_row() {
    for (d, owner) in [
        (Stream::describe(), Owner::Stream),
        (Target::describe(), Owner::Target),
    ] {
        d.validate().expect("sound");
        for r in table().into_iter().filter(|r| r.owner == owner) {
            let c = d
                .find_command(&r.command.name)
                .expect("every row is described");
            assert_eq!(c.supported, r.supported, "{}", r.command.name);
        }
        assert!(
            d.commands.iter().any(|c| !c.supported),
            "planned commands are carried, locked"
        );
    }
}

#[test]
fn change_has_no_input_and_find_is_the_standard_one() {
    for d in [Stream::describe(), Target::describe()] {
        let change = d.find_command("change").expect("change");
        assert!(change.input.is_empty() && change.supported);
        let find = d.find_command("find").expect("find");
        assert_eq!(find.input[0].name, "term");
        assert!(find.supported);
    }
}

#[test]
fn record_commands_are_the_ones_on_one_record() {
    let d = Stream::describe();
    let names: Vec<&str> = d
        .record_commands()
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    for n in [
        "change",
        "delete",
        "stop",
        "setRecording",
        "check",
        "status",
    ] {
        assert!(names.contains(&n), "{n}");
    }
    assert!(!names.contains(&"find") && !names.contains(&"create"));
}

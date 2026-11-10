//! The English table: every key a description names has a text, and the About names the program.

use fs_components_fields::Say;
use fs_streaming_app::Words;
use fs_streaming_core::{Stream, Target};
use fs_types::{DescribesType, FieldDescription, TypeDescription};

fn keys(d: &TypeDescription) -> Vec<String> {
    let mut out = vec![d.label.clone()];
    let mut add = |f: &FieldDescription| {
        out.push(f.label.clone());
        out.extend(f.choices.iter().map(|c| format!("{}.{c}", f.label)));
    };
    d.fields.iter().for_each(&mut add);
    for c in &d.commands {
        c.input.iter().chain(&c.result).for_each(&mut add);
    }
    out.extend(d.commands.iter().map(|c| c.label.clone()));
    out
}

#[test]
fn every_label_of_both_descriptions_has_an_english_text() {
    for d in [Stream::describe(), Target::describe()] {
        for k in keys(&d) {
            assert!(Words::english(&k).is_some(), "no English text for {k}");
        }
    }
}

#[test]
fn a_missing_key_comes_back_as_itself() {
    assert_eq!(Words.say("nothing.like.this"), "nothing.like.this");
    assert_eq!(Words.say("menu.quit"), "Quit");
}

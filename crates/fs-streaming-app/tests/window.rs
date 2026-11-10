//! The window: the standard menus with one view per data type, and the About of the program.

use fs_streaming_app::{open_program, parse, program_window};

#[test]
fn the_window_has_a_view_per_data_type_and_the_standard_menus() {
    let dir = tempfile::tempdir().unwrap();
    let args = parse(&[
        "--store".into(),
        dir.path().join("app.db").display().to_string(),
        "--bind".into(),
        "127.0.0.1:0".into(),
    ])
    .unwrap();
    let program = open_program(&args).unwrap();
    let app = program_window(&program).unwrap();
    app.with_shell(|s| {
        let ids: Vec<&str> = s
            .menus()
            .iter()
            .flat_map(|m| m.entries.iter().map(|e| e.id.as_str()))
            .collect();
        assert_eq!(
            ids,
            [
                "program-quit",
                "view-Stream",
                "view-Target",
                "help-help",
                "help-about"
            ]
        );
    });
    program.shutdown();
}

#[test]
fn the_about_page_names_the_program_and_nothing_secret() {
    let about = fs_streaming_app::about();
    assert_eq!(about.name, "FreeSynergy.Streaming");
    assert!(about.version.starts_with('v'));
    assert_eq!(about.licence, "MIT OR Apache-2.0");
    assert_eq!(
        about.repository,
        "https://github.com/FreeSynergy/fs-streaming"
    );
}

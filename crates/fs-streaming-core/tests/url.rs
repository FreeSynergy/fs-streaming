//! Target addresses are parsed strictly.

use fs_streaming_core::{TargetUrl, UrlProblem};

fn problem(text: &str) -> UrlProblem {
    match TargetUrl::parse(text) {
        Err(fs_streaming_core::Error::InvalidUrl(p)) => p,
        other => panic!("{text}: expected a refusal, got {other:?}"),
    }
}

#[test]
fn good_addresses_are_read_and_written_back_the_same_way() {
    let u = TargetUrl::parse("RTMPS://A.Example.ORG/live2/").unwrap();
    assert!(u.is_secure());
    assert_eq!(
        (u.host(), u.port(), u.app()),
        ("a.example.org", 443, "live2")
    );
    assert_eq!(u.to_string(), "rtmps://a.example.org/live2");
    let u = TargetUrl::parse("rtmp://127.0.0.1:1936/live/abc").unwrap();
    assert_eq!((u.port(), u.app()), (1936, "live/abc"));
    assert_eq!(TargetUrl::parse("rtmp://host/app").unwrap().port(), 1935);
    let v6 = TargetUrl::parse("rtmp://[::1]:1935/app").unwrap();
    assert_eq!(
        (v6.host(), v6.to_string().as_str()),
        ("::1", "rtmp://[::1]:1935/app")
    );
}

#[test]
fn user_query_fragment_scheme_port_and_path_are_refused() {
    assert_eq!(problem("rtmp://user@host/app"), UrlProblem::User);
    assert_eq!(problem("rtmp://user:pw@host/app"), UrlProblem::User);
    assert_eq!(
        problem("rtmp://host/app?key=1"),
        UrlProblem::QueryOrFragment
    );
    assert_eq!(problem("rtmp://host/app#x"), UrlProblem::QueryOrFragment);
    assert_eq!(problem("http://host/app"), UrlProblem::Scheme);
    assert_eq!(problem("host/app"), UrlProblem::Scheme);
    assert_eq!(problem("rtmp://host:abc/app"), UrlProblem::Port);
    assert_eq!(problem("rtmp://host:0/app"), UrlProblem::Port);
    assert_eq!(problem("rtmp://host"), UrlProblem::Path);
    assert_eq!(problem("rtmp://host//"), UrlProblem::Path);
    assert_eq!(problem("rtmp:///app"), UrlProblem::Host);
    assert_eq!(problem("rtmp://ho st/app"), UrlProblem::Host);
}

#[test]
fn a_key_stays_only_for_the_same_place_and_not_from_rtmps_to_rtmp() {
    let base = TargetUrl::parse("rtmps://a.example.org/live").unwrap();
    assert!(base.keeps_key_for(&TargetUrl::parse("rtmps://A.example.org/other").unwrap()));
    assert!(!base.keeps_key_for(&TargetUrl::parse("rtmps://b.example.org/live").unwrap()));
    assert!(!base.keeps_key_for(&TargetUrl::parse("rtmp://a.example.org/live").unwrap()));
    assert!(!base.keeps_key_for(&TargetUrl::parse("rtmps://a.example.org:444/live").unwrap()));
    let plain = TargetUrl::parse("rtmp://a.example.org/live").unwrap();
    assert!(
        plain.keeps_key_for(&TargetUrl::parse("rtmps://a.example.org/live").unwrap()),
        "going to rtmps is safer, not worse"
    );
}

#[test]
fn brackets_that_are_not_one_exact_ipv6_literal_percent_signs_backslashes_and_size_are_refused() {
    for bad in [
        "rtmp://[::1]evil.com/app",
        "rtmp://[evil.com]/app",
        "rtmp://[::1/app",
        "rtmp://[::1]:/app",
        "rtmp://[::1]x:1935/app",
        "rtmp://h/a%3Fkey=X",
        "rtmp://h/a%2e",
        "rtmp://h\\@evil/app",
        "rtmp://h/a\\b",
    ] {
        assert!(TargetUrl::parse(bad).is_err(), "{bad}");
    }
    let long = format!("rtmp://h/{}", "a".repeat(70_000));
    assert_eq!(problem(&long), UrlProblem::TooLong);
    assert_eq!(
        problem(&format!("rtmp://h/{}", "a".repeat(2100))),
        UrlProblem::TooLong
    );
    assert!(TargetUrl::parse(&format!("rtmp://h/{}", "a".repeat(2000))).is_ok());
    assert_eq!(
        TargetUrl::parse("rtmp://[::1]:1936/app")
            .unwrap()
            .to_string(),
        "rtmp://[::1]:1936/app"
    );
}

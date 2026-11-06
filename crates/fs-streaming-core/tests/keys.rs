//! Publisher keys: random, shown once, compared by hash.

use fs_streaming_core::{Secret, keys};

#[test]
fn keys_are_256_random_bits_and_never_printed() {
    let (a, b) = (keys::generate().unwrap(), keys::generate().unwrap());
    assert_eq!(a.expose().len(), 64);
    assert!(a.expose().bytes().all(|c| c.is_ascii_hexdigit()));
    assert_ne!(a.expose(), b.expose());
    assert_eq!(format!("{a:?}"), "[hidden]");
    assert_eq!(format!("{:?}", Some(Secret::new("x"))), "Some([hidden])");
}

#[test]
fn only_a_hash_is_kept_and_a_wrong_key_does_not_match() {
    assert_eq!(
        keys::hash("abc"),
        "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    let key = keys::generate().unwrap();
    let kept = keys::hash(key.expose());
    assert!(!kept.contains(key.expose()));
    assert!(keys::matches(Some(&kept), key.expose()));
    assert!(!keys::matches(Some(&kept), "wrong"));
    assert!(!keys::matches(Some(&kept), ""));
    assert!(
        !keys::matches(None, key.expose()),
        "no such stream: the same answer as a wrong key"
    );
}

#[test]
fn sendable_keys() {
    assert!(keys::is_sendable("live_abc-123.x"));
    assert!(!keys::is_sendable(""));
    assert!(!keys::is_sendable("a/b"));
    assert!(!keys::is_sendable("a\nb"));
    assert!(!keys::is_sendable(&"k".repeat(2000)));
}

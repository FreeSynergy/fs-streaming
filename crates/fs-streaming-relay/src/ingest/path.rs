//! Reading the stream id and the key out of what a sender connected to.
//!
//! Pattern: Strategy-free parser — senders split `live/<id>/<key>` between the application and the
//! stream name in different ways, so both are joined and cut again.

use fs_streaming_core::ObjectId;

/// `(stream id, key)` from the application name and the stream name; `None` if it does not fit.
pub fn parse(app: &str, name: &str) -> Option<(ObjectId, String)> {
    let joined = format!("{app}/{name}");
    let segs: Vec<&str> = joined.split('/').filter(|s| !s.is_empty()).collect();
    match segs.as_slice() {
        ["live", id, key] => Some((id.parse().ok()?, (*key).to_string())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn any_split_of_the_same_path_gives_the_same_answer() {
        let id = ObjectId::new();
        let want = Some((id, "k".to_string()));
        assert_eq!(parse(&format!("live/{id}"), "k"), want);
        assert_eq!(parse("live", &format!("{id}/k")), want);
        assert_eq!(parse(&format!("live/{id}/k"), ""), want);
    }

    #[test]
    fn other_shapes_do_not_fit() {
        let id = ObjectId::new();
        assert_eq!(parse("live", "k"), None);
        assert_eq!(parse(&format!("other/{id}"), "k"), None);
        assert_eq!(parse(&format!("live/{id}"), ""), None);
        assert_eq!(parse("live/not-an-id", "k"), None);
    }
}

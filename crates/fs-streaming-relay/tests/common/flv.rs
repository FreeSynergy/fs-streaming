//! Reading an FLV file the way the tests need.

/// `(type, timestamp, first payload byte)` of every tag of an FLV file.
pub fn tags(flv: &[u8]) -> Vec<(u8, u32, u8)> {
    let (mut at, mut out) = (13, vec![]);
    while at + 12 <= flv.len() {
        let len = u32::from_be_bytes([0, flv[at + 1], flv[at + 2], flv[at + 3]]) as usize;
        let ts = u32::from_be_bytes([flv[at + 7], flv[at + 4], flv[at + 5], flv[at + 6]]);
        out.push((flv[at], ts, flv[at + 11]));
        at += 11 + len + 4;
    }
    out
}

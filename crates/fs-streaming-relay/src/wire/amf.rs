//! A flat check of AMF0 nesting, before anything recursive sees the bytes.
//!
//! Pattern: Guard — `rml_amf0` reads nested values by recursion, so a message of nested arrays can
//! overflow the stack even within the size limit. This walks the structure with a plain stack and no
//! recursion, and says no to anything deeper than `MAX_DEPTH` or not well formed. No values are built.

/// The deepest nesting accepted. Real commands are two or three levels deep.
pub const MAX_DEPTH: usize = 16;

/// What a container still expects.
enum Open {
    /// Object or typed object: key, value, ... until the end marker.
    Object,
    /// Array with this many values to go.
    Array(u64),
}

fn take<'a>(d: &'a [u8], at: &mut usize, n: usize) -> Option<&'a [u8]> {
    let end = at.checked_add(n)?;
    let s = d.get(*at..end)?;
    *at = end;
    Some(s)
}

fn u16_at(d: &[u8], at: &mut usize) -> Option<usize> {
    let b = take(d, at, 2)?;
    Some(usize::from(u16::from_be_bytes([b[0], b[1]])))
}

fn u32_at(d: &[u8], at: &mut usize) -> Option<usize> {
    let b = take(d, at, 4)?;
    usize::try_from(u32::from_be_bytes([b[0], b[1], b[2], b[3]])).ok()
}

/// Whether `d` is a sequence of well-formed AMF0 values nested at most [`MAX_DEPTH`] deep.
pub fn is_shallow(d: &[u8]) -> bool {
    walk(d).is_some()
}

fn walk(d: &[u8]) -> Option<()> {
    let (mut at, mut stack): (usize, Vec<Open>) = (0, Vec::new());
    while at < d.len() || !stack.is_empty() {
        // Inside an object a key comes first; an empty key and the end marker close it.
        if matches!(stack.last(), Some(Open::Object)) {
            let key = u16_at(d, &mut at)?;
            take(d, &mut at, key)?;
            if key == 0 && d.get(at) == Some(&9) {
                at += 1;
                stack.pop();
                continue;
            }
        }
        if let Some(Open::Array(0)) = stack.last() {
            stack.pop();
            continue;
        }
        let marker = *take(d, &mut at, 1)?.first()?;
        if let Some(Open::Array(n)) = stack.last_mut() {
            *n -= 1;
        }
        match marker {
            0 => {
                take(d, &mut at, 8)?;
            }
            1 => {
                take(d, &mut at, 1)?;
            }
            2 => {
                let n = u16_at(d, &mut at)?;
                take(d, &mut at, n)?;
            }
            5..=7 => {
                if marker == 7 {
                    take(d, &mut at, 2)?;
                }
            }
            0x0b => {
                take(d, &mut at, 10)?;
            }
            0x0c | 0x0f => {
                let n = u32_at(d, &mut at)?;
                take(d, &mut at, n)?;
            }
            3 => stack.push(Open::Object),
            0x10 => {
                let n = u16_at(d, &mut at)?;
                take(d, &mut at, n)?;
                stack.push(Open::Object);
            }
            8 => {
                u32_at(d, &mut at)?;
                stack.push(Open::Object);
            }
            0x0a => {
                let n = u32_at(d, &mut at)?;
                stack.push(Open::Array(n as u64));
            }
            _ => return None,
        }
        if stack.len() > MAX_DEPTH {
            return None;
        }
        while let Some(Open::Array(0)) = stack.last() {
            stack.pop();
        }
    }
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_normal_command_passes() {
        let mut d = vec![2, 0, 7];
        d.extend_from_slice(b"connect");
        d.extend_from_slice(&[0, 0x3f, 0xf0, 0, 0, 0, 0, 0, 0]);
        d.extend_from_slice(&[3, 0, 3]);
        d.extend_from_slice(b"app");
        d.extend_from_slice(&[2, 0, 4]);
        d.extend_from_slice(b"live");
        d.extend_from_slice(&[0, 0, 9, 5]);
        assert!(is_shallow(&d));
    }

    #[test]
    fn deep_nesting_and_broken_bytes_are_refused() {
        let deep: Vec<u8> = (0..1500).flat_map(|_| [0x0a, 0, 0, 0, 1]).collect();
        assert!(!is_shallow(&deep));
        let fine: Vec<u8> = (0..10)
            .flat_map(|_| [0x0a, 0, 0, 0, 1])
            .chain([5])
            .collect();
        assert!(is_shallow(&fine));
        assert!(!is_shallow(&[0x0a, 0, 0, 0, 5, 5]));
        assert!(!is_shallow(&[2, 0, 9, b'x']));
        assert!(!is_shallow(&[0x77]));
    }
}

//! Reading the far end's commands.
//!
//! Pattern: Adapter over `rml_rtmp`'s AMF0 messages — only the code, the level and the kind of reply
//! are kept. The far end's own words are dropped here, so they can reach no note and no log line.

use rml_rtmp::messages::{MessagePayload, RtmpMessage};
use rml_rtmp::rml_amf0::Amf0Value;

/// What a command from the far end means to us.
pub enum Reply {
    /// `_result` with its arguments.
    Result(Vec<Amf0Value>),
    /// `_error`.
    Error,
    /// `onStatus`.
    Status { code: String, error: bool },
}

/// Classify a message; `None` if it is not a reply we care about.
pub fn parse(m: &MessagePayload) -> Option<Reply> {
    if m.type_id != 20 && m.type_id != 17 {
        return None;
    }
    let RtmpMessage::Amf0Command {
        command_name,
        additional_arguments,
        ..
    } = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| m.to_rtmp_message()))
        .ok()?
        .ok()?
    else {
        return None;
    };
    match command_name.as_str() {
        "_result" => Some(Reply::Result(additional_arguments)),
        "_error" => Some(Reply::Error),
        "onStatus" => {
            let info = additional_arguments
                .into_iter()
                .next()?
                .get_object_properties()?;
            let text = |k: &str| match info.get(k) {
                Some(Amf0Value::Utf8String(s)) => s.clone(),
                _ => String::new(),
            };
            let (code, level) = (text("code"), text("level"));
            let error = level == "error"
                || ["BadName", "Rejected", "Failed", "Denied", "Unauthorized"]
                    .iter()
                    .any(|w| code.ends_with(w));
            Some(Reply::Status { code, error })
        }
        _ => None,
    }
}

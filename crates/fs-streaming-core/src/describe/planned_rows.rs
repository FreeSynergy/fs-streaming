//! The capabilities of `requirements.md` that the core cannot do (yet), as commands that are named, not hidden.
//!
//! Pattern: Registry entry — rows of the table, each with its reason. They are described so a frontend can show
//! them locked ("coming"); the app never runs them. When the core gets one, its row moves to the supported
//! rows and loses `.planned(..)`; nothing else changes.

use fs_types::{BasicType, CommandKind};

use super::fields::{arg, flag, id};
use super::row::{Owner, Row};

const S: Owner = Owner::Stream;

const NOT_BUILT: &str = "not built in stage 1 (DESIGN.md, Not built): the core forwards RTMP and records, nothing else yet";
const DECODES: &str =
    "stage 2: needs decoding or encoding; no codec provider exists yet (ADR 0048)";
const FILES: &str = "kept by fs-files (ADR 0050); streaming only hands a recording over, and that handing over is not built";
const OTHER_PROGRAM: &str =
    "owned by another program (ADR 0050), reached through its interface once that exists";

/// A command on one stream, with only its id as input.
fn on_stream(
    name: &str,
    cap: &'static [&'static str],
    kind: CommandKind,
    why: &'static str,
) -> Row {
    Row::new(S, name, kind, cap).input(id(S, name)).planned(why)
}

/// A command with an address as input (a source, a file, a page).
fn with_link(
    name: &str,
    cap: &'static [&'static str],
    kind: CommandKind,
    why: &'static str,
) -> Row {
    Row::new(S, name, kind, cap)
        .input(arg(S, name, "name", BasicType::Text))
        .input(arg(S, name, "url", BasicType::Link).required())
        .planned(why)
}

/// Rows for the capabilities the core cannot do.
pub(super) fn rows() -> Vec<Row> {
    let k = CommandKind::Other;
    vec![
        Row::new(S, "publish", k, &["stream.publish"])
            .input(id(S, "publish"))
            .input(
                arg(S, "publish", "protocol", BasicType::Choice)
                    .required()
                    .choices(&["rtmp", "rtmps", "srt", "rtsp", "whip"]),
            )
            .planned("only plain RTMP ingest is built; RTMPS, SRT, RTSP and WHIP are not (put your own TLS in front of RTMP)"),
        Row::new(S, "play", k, &["stream.play"])
            .input(id(S, "play"))
            .input(
                arg(S, "play", "protocol", BasicType::Choice)
                    .required()
                    .choices(&["hls", "ll-hls", "dash", "whep", "moq", "http"]),
            )
            .planned(NOT_BUILT),
        on_stream("remux", &["stream.remux"], k, NOT_BUILT),
        with_link("pullSource", &["source.pull"], CommandKind::Create, NOT_BUILT),
        on_stream("authorizeViewer", &["viewer.authorize"], k, NOT_BUILT),
        on_stream("secureEndpoint", &["endpoint.secure"], k, "ingest is plain RTMP; RTMPS is used only towards targets"),
        on_stream("listRecordings", &["recording.list"], CommandKind::Find, FILES),
        on_stream("playRecording", &["recording.play"], k, FILES),
        Row::new(S, "sendHook", CommandKind::Create, &["hook.send"])
            .input(id(S, "sendHook"))
            .input(arg(S, "sendHook", "url", BasicType::Link).required())
            .planned(NOT_BUILT),
        on_stream("transcode", &["stream.transcode"], k, DECODES),
        on_stream("createRendition", &["rendition.create"], CommandKind::Create, DECODES),
        on_stream("overlay", &["stream.overlay"], k, DECODES),
        on_stream("composeScene", &["scene.compose"], k, DECODES),
        on_stream("mixAudio", &["audio.mix"], k, DECODES),
        on_stream("selectEncoder", &["encoder.select"], k, DECODES),
        on_stream("preview", &["stream.preview"], k, DECODES),
        on_stream("timeshift", &["stream.timeshift"], k, NOT_BUILT),
        Row::new(S, "createPlaylist", CommandKind::Create, &["playlist.create"])
            .input(arg(S, "createPlaylist", "name", BasicType::Text).required())
            .input(arg(S, "createPlaylist", "items", BasicType::Link).multiple())
            .planned(NOT_BUILT),
        Row::new(S, "scheduleBroadcast", k, &["broadcast.schedule"])
            .input(id(S, "scheduleBroadcast"))
            .input(arg(S, "scheduleBroadcast", "start", BasicType::DateTime).required())
            .planned("the event belongs to fs-calendar (JSCalendar); the call through its interface is not built"),
        on_stream("linkRelay", &["relay.link"], k, NOT_BUILT),
        with_link("importMedia", &["media.import"], CommandKind::Create, FILES),
        with_link("probeMedia", &["media.probe"], k, NOT_BUILT),
        Row::new(S, "expireRecordings", k, &["recording.expire"])
            .input(id(S, "expireRecordings"))
            .input(arg(S, "expireRecordings", "days", BasicType::Text).required())
            .planned(FILES),
        on_stream("disconnectPublisher", &["publisher.disconnect"], k, "the core can end a sender (`stop`) but cannot block one publisher apart from rotating the key"),
        on_stream("aggregateChat", &["chat.aggregate"], k, OTHER_PROGRAM),
        on_stream("embedPlayer", &["player.embed"], k, NOT_BUILT),
        on_stream("monitor", &["stream.monitor"], k, "live numbers exist as `status` (bytes, state); viewers and health indicators are not built"),
        on_stream("report", &["stream.report"], k, "`Status` is live only, nothing of it is stored"),
        Row::new(S, "inviteGuest", k, &["guest.invite"])
            .input(id(S, "inviteGuest"))
            .planned(NOT_BUILT),
        Row::new(S, "limitGuests", k, &["guest.limit"])
            .input(id(S, "limitGuests"))
            .input(arg(S, "limitGuests", "limit", BasicType::Text).required())
            .planned(NOT_BUILT),
        Row::new(S, "publishChannel", k, &["channel.publish"])
            .input(id(S, "publishChannel"))
            .input(flag(arg(S, "publishChannel", "public", BasicType::Choice).required(), "on", "off"))
            .planned(NOT_BUILT),
        on_stream("follow", &["stream.follow"], k, NOT_BUILT),
        on_stream("peer", &["stream.peer"], k, NOT_BUILT),
        with_link("publishVideo", &["video.publish"], k, "publishing a stored video through a platform is not built; the video comes from fs-files"),
    ]
}

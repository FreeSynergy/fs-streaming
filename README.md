# fs-streaming

FreeSynergy program for live streams: record once, and bring it everywhere, with no provider in between. This repo holds stage 1: receive one stream, send it to many targets without decoding it, and record it.

* `crates/fs-streaming-core`: streams, targets, keys as secrets, state; no media.
* `crates/fs-streaming-relay`: the forwarding itself.
* The contract is `streaming.capnp` in [fs-interfaces](https://github.com/FreeSynergy/fs-interfaces) (tag v0.29.0).

Licence: MIT OR Apache-2.0.

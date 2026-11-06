//! The sender announces a window; the relay acknowledges what it reads, as often as the window asks.

mod common;

use common::raw::{Answer, Raw};
use common::raw_ext::payload;
use common::rig::Rig;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn media_is_acknowledged_per_the_window_the_sender_announced() {
    let rig = Rig::new().await;
    let made = rig.stream("s").await;
    let (app, name) = made.app_and_name();
    let mut raw = Raw::connect(rig.addr).await;
    assert_eq!(raw.publish(&app, &name).await, Answer::Accepted);
    raw.write_payload(&payload(5, 0, 0, &5000u32.to_be_bytes()))
        .await;
    raw.headers().await;
    // A sender that waits for an acknowledgement before it sends more, as some do.
    let mut acked = 0u32;
    for round in 0..5u32 {
        for n in 0..2 {
            raw.tick(round * 2 + n, 3_000).await;
        }
        let next = loop {
            let m = raw
                .next_message()
                .await
                .expect("the relay kept the connection and acknowledged");
            if m.type_id == 3 {
                break u32::from_be_bytes([m.data[0], m.data[1], m.data[2], m.data[3]]);
            }
        };
        assert!(
            next > acked,
            "round {round}: the count grows ({acked} -> {next})"
        );
        assert!(
            next >= (round + 1) * 6_000,
            "and follows what was read: {next}"
        );
        acked = next;
    }
}

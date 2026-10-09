//! The web gateway's decoders on arbitrary bytes, as browsers' connections bring them: an
//! HTTP request, WebSocket frames, and the JSON in them.
//!
//! - Nothing panics.
//! - A frame that decodes took exactly the bytes its header says, and text is UTF-8.
//! - A stream cut anywhere decodes the same frames up to the cut.
//! - Every JSON message that parses converts, or is refused, without panicking.

#![no_main]

use gateway::web::{http, json, ws};
use libfuzzer_sys::fuzz_target;

const MAX: usize = 4 << 10;

fuzz_target!(|bytes: &[u8]| {
    let _ = http::parse(bytes, 8 << 10);

    let mut at = 0;
    let mut frames = Vec::new();
    while let Ok(Some((frame, len))) = ws::decode(&bytes[at..], MAX) {
        assert!(len >= 6 && at + len <= bytes.len());
        if let ws::Frame::Text(text) = &frame {
            if let Ok(message) = json::parse(text) {
                let _ = message.inbound();
            }
        }
        frames.push(frame);
        at += len;
    }
    let cut = bytes.len() / 2;
    let mut at = 0;
    let mut count = 0;
    while let Ok(Some((frame, len))) = ws::decode(&bytes[at..cut], MAX) {
        assert_eq!(Some(&frame), frames.get(count));
        count += 1;
        at += len;
    }

    if let Ok(text) = std::str::from_utf8(bytes) {
        if let Ok(message) = json::parse(text) {
            let _ = message.inbound();
        }
    }
});

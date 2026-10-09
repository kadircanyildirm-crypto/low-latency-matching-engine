//! The wire protocol's decoders on arbitrary byte streams, read as a gateway reads a socket:
//! message after message, until the bytes run out or one does not decode.
//!
//! - Decoding never panics.
//! - Every message that decodes re-encodes to exactly the bytes it came from.
//! - A stream cut anywhere decodes the same messages up to the cut.

#![no_main]

use libfuzzer_sys::fuzz_target;
use protocol::{decode_inbound, decode_outbound, encode_inbound, encode_outbound};

fuzz_target!(|bytes: &[u8]| {
    // As the gateway reads a client.
    let mut at = 0;
    let mut inbound = Vec::new();
    while let Ok(Some((message, len))) = decode_inbound(&bytes[at..]) {
        let mut again = Vec::new();
        encode_inbound(&message, &mut again);
        assert_eq!(again, &bytes[at..at + len]);
        inbound.push(message);
        at += len;
    }
    // A prefix of the stream decodes to a prefix of the messages.
    let cut = bytes.len() / 2;
    let mut at = 0;
    let mut count = 0;
    while let Ok(Some((message, len))) = decode_inbound(&bytes[at..cut]) {
        assert_eq!(Some(&message), inbound.get(count));
        count += 1;
        at += len;
    }
    // As a client reads the gateway.
    let mut at = 0;
    while let Ok(Some((message, len))) = decode_outbound(&bytes[at..]) {
        let mut again = Vec::new();
        encode_outbound(&message, &mut again);
        assert_eq!(again, &bytes[at..at + len]);
        at += len;
    }
});

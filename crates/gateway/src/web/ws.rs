//! WebSocket frames (RFC 6455), as a server reads and writes them.
//!
//! Only what a browser sends is accepted: masked frames, each a whole message (no
//! fragmentation), text up to a size limit, and the control frames. Anything else is an
//! error, and the connection is closed: a server that guessed at malformed frames would be
//! the easiest part of the exchange to confuse.

use std::fmt;

/// A frame from a client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    /// A text message.
    Text(String),
    /// A ping, whose payload the pong must echo.
    Ping(Vec<u8>),
    /// A pong.
    Pong,
    /// The client is closing the connection.
    Close,
}

/// Why bytes are not a frame this server accepts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WsError {
    /// Frames from a client must be masked.
    Unmasked,
    /// A reserved bit is set.
    Reserved,
    /// A message in several frames, or a continuation frame.
    Fragmented,
    /// An opcode this server does not take: binary, or a reserved one.
    Opcode(u8),
    /// A control frame longer than 125 bytes.
    LongControl,
    /// A payload over the limit.
    TooLong,
    /// A length in more bytes than it needs.
    NonMinimalLength,
    /// Text that is not UTF-8.
    NotUtf8,
}

impl fmt::Display for WsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WsError::Unmasked => write!(f, "an unmasked frame from a client"),
            WsError::Reserved => write!(f, "a reserved bit is set"),
            WsError::Fragmented => write!(f, "a fragmented message"),
            WsError::Opcode(op) => write!(f, "opcode {op} is not accepted"),
            WsError::LongControl => write!(f, "a control frame over 125 bytes"),
            WsError::TooLong => write!(f, "a payload over the limit"),
            WsError::NonMinimalLength => write!(f, "a length not in its shortest form"),
            WsError::NotUtf8 => write!(f, "text that is not UTF-8"),
        }
    }
}

impl std::error::Error for WsError {}

const CONTINUATION: u8 = 0x0;
const TEXT: u8 = 0x1;
const CLOSE: u8 = 0x8;
const PING: u8 = 0x9;
const PONG: u8 = 0xA;

/// Decodes the frame at the start of `buf`: `Ok(None)` if `buf` does not hold all of it
/// yet, otherwise the frame and its length. Payloads over `max_payload` bytes are refused
/// as soon as their length is read.
pub fn decode(buf: &[u8], max_payload: usize) -> Result<Option<(Frame, usize)>, WsError> {
    if buf.len() < 2 {
        return Ok(None);
    }
    let (first, second) = (buf[0], buf[1]);
    let fin = first & 0x80 != 0;
    if first & 0x70 != 0 {
        return Err(WsError::Reserved);
    }
    let opcode = first & 0x0F;
    if second & 0x80 == 0 {
        return Err(WsError::Unmasked);
    }
    let control = opcode & 0x08 != 0;
    match opcode {
        CONTINUATION => return Err(WsError::Fragmented),
        TEXT | CLOSE | PING | PONG => {}
        other => return Err(WsError::Opcode(other)),
    }
    if !fin {
        return Err(WsError::Fragmented);
    }
    let (len, mut at) = match second & 0x7F {
        126 => {
            let Some(bytes) = buf.get(2..4) else {
                return Ok(None);
            };
            let len = u64::from(u16::from_be_bytes([bytes[0], bytes[1]]));
            if len < 126 {
                return Err(WsError::NonMinimalLength);
            }
            (len, 4)
        }
        127 => {
            let Some(bytes) = buf.get(2..10) else {
                return Ok(None);
            };
            let len = u64::from_be_bytes(bytes.try_into().unwrap());
            if len <= u64::from(u16::MAX) {
                return Err(WsError::NonMinimalLength);
            }
            (len, 10)
        }
        short => (u64::from(short), 2),
    };
    if control && len > 125 {
        return Err(WsError::LongControl);
    }
    if len > max_payload as u64 {
        return Err(WsError::TooLong);
    }
    let len = len as usize;
    let Some(mask) = buf.get(at..at + 4) else {
        return Ok(None);
    };
    let mask: [u8; 4] = mask.try_into().unwrap();
    at += 4;
    let Some(masked) = buf.get(at..at + len) else {
        return Ok(None);
    };
    let payload: Vec<u8> = masked
        .iter()
        .enumerate()
        .map(|(i, byte)| byte ^ mask[i % 4])
        .collect();
    let frame = match opcode {
        TEXT => Frame::Text(String::from_utf8(payload).map_err(|_| WsError::NotUtf8)?),
        CLOSE => Frame::Close,
        PING => Frame::Ping(payload),
        _ => Frame::Pong,
    };
    Ok(Some((frame, at + len)))
}

/// Appends a frame of `opcode` with `payload`, unmasked, as a server sends it.
fn encode(opcode: u8, payload: &[u8], out: &mut Vec<u8>) {
    out.push(0x80 | opcode);
    match payload.len() {
        len @ 0..=125 => out.push(len as u8),
        len @ 126..=0xFFFF => {
            out.push(126);
            out.extend_from_slice(&(len as u16).to_be_bytes());
        }
        len => {
            out.push(127);
            out.extend_from_slice(&(len as u64).to_be_bytes());
        }
    }
    out.extend_from_slice(payload);
}

/// Appends a text message.
pub fn encode_text(text: &str, out: &mut Vec<u8>) {
    encode(TEXT, text.as_bytes(), out);
}

/// Appends the pong answering a ping with `payload`.
pub fn encode_pong(payload: &[u8], out: &mut Vec<u8>) {
    encode(PONG, payload, out);
}

/// Appends a close frame.
pub fn encode_close(out: &mut Vec<u8>) {
    encode(CLOSE, &[], out);
}

/// Masks a frame's payload as a client does, for tests and for clients of the web gateway.
pub fn encode_client(opcode_text: bool, payload: &[u8], mask: [u8; 4], out: &mut Vec<u8>) {
    let start = out.len();
    encode(if opcode_text { TEXT } else { PING }, payload, out);
    // The mask bit, and the key after the length.
    out[start + 1] |= 0x80;
    let header = out.len() - payload.len();
    let body: Vec<u8> = payload
        .iter()
        .enumerate()
        .map(|(i, byte)| byte ^ mask[i % 4])
        .collect();
    out.truncate(header);
    out.extend_from_slice(&mask);
    out.extend_from_slice(&body);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(text: &str) -> Vec<u8> {
        let mut out = Vec::new();
        encode_client(true, text.as_bytes(), [1, 2, 3, 4], &mut out);
        out
    }

    #[test]
    fn client_frames_decode_whatever_their_length() {
        for len in [0, 1, 125, 126, 65_535, 65_536, 70_000] {
            let text = "x".repeat(len);
            let bytes = client(&text);
            assert_eq!(
                decode(&bytes, 1 << 20),
                Ok(Some((Frame::Text(text.clone()), bytes.len())))
            );
            // Every prefix is incomplete.
            for cut in [0, 1, 2, bytes.len() / 2, bytes.len() - 1] {
                if cut < bytes.len() {
                    assert_eq!(
                        decode(&bytes[..cut], 1 << 20),
                        Ok(None),
                        "{len} cut at {cut}"
                    );
                }
            }
        }
        let mut ping = Vec::new();
        encode_client(false, b"hi", [9, 8, 7, 6], &mut ping);
        assert_eq!(
            decode(&ping, 10),
            Ok(Some((Frame::Ping(b"hi".to_vec()), ping.len())))
        );
    }

    #[test]
    fn what_a_browser_would_not_send_is_refused() {
        let good = client("hello");
        let mut unmasked = good.clone();
        unmasked[1] &= 0x7F;
        assert_eq!(decode(&unmasked, 100), Err(WsError::Unmasked));
        let mut reserved = good.clone();
        reserved[0] |= 0x40;
        assert_eq!(decode(&reserved, 100), Err(WsError::Reserved));
        let mut fragment = good.clone();
        fragment[0] &= 0x7F;
        assert_eq!(decode(&fragment, 100), Err(WsError::Fragmented));
        let mut binary = good.clone();
        binary[0] = 0x82;
        assert_eq!(decode(&binary, 100), Err(WsError::Opcode(2)));
        assert_eq!(decode(&good, 4), Err(WsError::TooLong));
        // A short length written long.
        let long_form = [0x81, 0x80 | 126, 0, 5, 1, 2, 3, 4];
        assert_eq!(decode(&long_form, 100), Err(WsError::NonMinimalLength));
        // A ping over 125 bytes.
        let mut ping = Vec::new();
        encode_client(false, &[0; 126], [0; 4], &mut ping);
        assert_eq!(decode(&ping, 1_000), Err(WsError::LongControl));
        // Invalid UTF-8.
        let mut bad = Vec::new();
        encode_client(true, &[0xFF, 0xFE], [0; 4], &mut bad);
        assert_eq!(decode(&bad, 100), Err(WsError::NotUtf8));
        for error in [WsError::Unmasked, WsError::Opcode(3), WsError::TooLong] {
            assert!(!error.to_string().is_empty());
        }
    }

    #[test]
    fn server_frames_carry_their_length() {
        for len in [0usize, 125, 126, 65_535, 65_536] {
            let mut out = Vec::new();
            encode_text(&"y".repeat(len), &mut out);
            let header = match len {
                0..=125 => 2,
                126..=65_535 => 4,
                _ => 10,
            };
            assert_eq!(out.len(), header + len);
            assert_eq!(out[0], 0x81);
        }
        let mut out = Vec::new();
        encode_pong(b"ab", &mut out);
        encode_close(&mut out);
        assert_eq!(out, [0x8A, 2, b'a', b'b', 0x88, 0]);
    }
}

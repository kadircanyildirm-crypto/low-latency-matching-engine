//! The little HTTP the web gateway speaks: `GET` for its own static files, and the upgrade
//! of a connection to a WebSocket (RFC 6455 §4). Requests with a body, other methods, and
//! anything larger than a limit are refused.

use std::fmt;

use base64::Engine as _;
use sha1::{Digest, Sha1};

/// A request the web gateway serves.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    /// A file.
    Get {
        /// The path, without the query.
        path: String,
    },
    /// A WebSocket handshake.
    Upgrade {
        /// The client's `Sec-WebSocket-Key`.
        key: String,
    },
}

/// Why a request is refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpError {
    /// Not HTTP/1.1, or not parseable.
    Malformed,
    /// Longer than the limit before its headers end.
    TooLarge,
    /// Not `GET`, or with a body.
    Method,
    /// An upgrade without the headers a WebSocket handshake needs.
    BadUpgrade,
}

impl fmt::Display for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HttpError::Malformed => write!(f, "a malformed request"),
            HttpError::TooLarge => write!(f, "a request over the size limit"),
            HttpError::Method => write!(f, "only GET without a body is served"),
            HttpError::BadUpgrade => write!(f, "an incomplete WebSocket handshake"),
        }
    }
}

impl std::error::Error for HttpError {}

/// Headers read at most.
const MAX_HEADERS: usize = 32;

/// Parses the request at the start of `buf`: `Ok(None)` if its headers have not all
/// arrived, otherwise the request and its length. A request whose headers take more than
/// `max` bytes is refused.
pub fn parse(buf: &[u8], max: usize) -> Result<Option<(Request, usize)>, HttpError> {
    let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
    let mut request = httparse::Request::new(&mut headers);
    let len = match request.parse(buf) {
        Ok(httparse::Status::Complete(len)) if len <= max => len,
        Ok(httparse::Status::Complete(_)) => return Err(HttpError::TooLarge),
        Ok(httparse::Status::Partial) if buf.len() > max => return Err(HttpError::TooLarge),
        Ok(httparse::Status::Partial) => return Ok(None),
        Err(_) => return Err(HttpError::Malformed),
    };
    if request.version != Some(1) {
        return Err(HttpError::Malformed);
    }
    if request.method != Some("GET") {
        return Err(HttpError::Method);
    }
    let header = |name: &str| {
        request
            .headers
            .iter()
            .find(|h| h.name.eq_ignore_ascii_case(name))
            .and_then(|h| std::str::from_utf8(h.value).ok())
            .map(str::trim)
    };
    if header("content-length").is_some_and(|v| v != "0") || header("transfer-encoding").is_some() {
        return Err(HttpError::Method);
    }
    let path = request.path.unwrap_or("/");
    let path = path.split('?').next().unwrap_or("/").to_owned();
    let wants_upgrade = header("upgrade").is_some_and(|v| v.eq_ignore_ascii_case("websocket"));
    if !wants_upgrade {
        return Ok(Some((Request::Get { path }, len)));
    }
    let connection = header("connection").unwrap_or("");
    let upgrades = connection
        .split(',')
        .any(|token| token.trim().eq_ignore_ascii_case("upgrade"));
    let key = header("sec-websocket-key").filter(|key| {
        base64::engine::general_purpose::STANDARD
            .decode(key)
            .is_ok_and(|bytes| bytes.len() == 16)
    });
    match (upgrades, header("sec-websocket-version"), key) {
        (true, Some("13"), Some(key)) => Ok(Some((
            Request::Upgrade {
                key: key.to_owned(),
            },
            len,
        ))),
        _ => Err(HttpError::BadUpgrade),
    }
}

/// The `Sec-WebSocket-Accept` answering `key` (RFC 6455 §1.3).
pub fn accept_key(key: &str) -> String {
    let mut sha = Sha1::new();
    sha.update(key.as_bytes());
    sha.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    base64::engine::general_purpose::STANDARD.encode(sha.finalize())
}

/// The response accepting a WebSocket handshake.
pub fn upgrade_response(key: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Accept: {}\r\n\r\n",
        accept_key(key)
    )
    .into_bytes()
}

/// A complete response, after which the connection closes.
pub fn response(status: &str, content_type: &str, body: &[u8]) -> Vec<u8> {
    let mut out = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\
         Connection: close\r\nCache-Control: no-cache\r\nX-Content-Type-Options: nosniff\r\n\
         Content-Security-Policy: default-src 'self'; connect-src 'self' ws: wss:\r\n\r\n",
        body.len()
    )
    .into_bytes();
    out.extend_from_slice(body);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_rfc_example_key_is_accepted() {
        // RFC 6455 §1.3.
        assert_eq!(
            accept_key("dGhlIHNhbXBsZSBub25jZQ=="),
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        );
    }

    #[test]
    fn requests_are_read_strictly() {
        let get = b"GET /app.js?v=2 HTTP/1.1\r\nHost: x\r\n\r\n";
        assert_eq!(
            parse(get, 1_024),
            Ok(Some((
                Request::Get {
                    path: "/app.js".into()
                },
                get.len()
            )))
        );
        for cut in 0..get.len() {
            assert_eq!(parse(&get[..cut], 1_024), Ok(None), "cut at {cut}");
        }
        let upgrade = b"GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: WebSocket\r\n\
            Connection: keep-alive, Upgrade\r\nSec-WebSocket-Version: 13\r\n\
            Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n";
        assert_eq!(
            parse(upgrade, 1_024),
            Ok(Some((
                Request::Upgrade {
                    key: "dGhlIHNhbXBsZSBub25jZQ==".into()
                },
                upgrade.len()
            )))
        );
        let refused: [(&[u8], HttpError); 6] = [
            (b"POST / HTTP/1.1\r\n\r\n", HttpError::Method),
            (
                b"GET / HTTP/1.1\r\nContent-Length: 3\r\n\r\nabc",
                HttpError::Method,
            ),
            (b"GET / HTTP/1.0\r\n\r\n", HttpError::Malformed),
            (
                b"GET / HTTP/1.1\r\nUpgrade: websocket\r\n\r\n",
                HttpError::BadUpgrade,
            ),
            (b"\x00\x01 garbage\r\n\r\n", HttpError::Malformed),
            (
                b"GET / HTTP/1.1\r\nUpgrade: websocket\r\nConnection: upgrade\r\n\
                  Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: short\r\n\r\n",
                HttpError::BadUpgrade,
            ),
        ];
        for (bytes, error) in refused {
            assert_eq!(
                parse(bytes, 1_024),
                Err(error),
                "{}",
                String::from_utf8_lossy(bytes)
            );
            assert!(!error.to_string().is_empty());
        }
        assert_eq!(parse(&[b'G'; 2_000], 1_024), Err(HttpError::TooLarge));
        assert_eq!(parse(get, 10), Err(HttpError::TooLarge));
    }

    #[test]
    fn responses_say_how_long_they_are() {
        let response = response("200 OK", "text/plain", b"hello");
        let text = String::from_utf8(response).unwrap();
        assert!(text.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(text.contains("Content-Length: 5\r\n"));
        assert!(text.ends_with("\r\n\r\nhello"));
        let upgrade = String::from_utf8(upgrade_response("dGhlIHNhbXBsZSBub25jZQ==")).unwrap();
        assert!(upgrade.contains("Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n"));
    }
}

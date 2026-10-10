//! What Bitstamp's WebSocket sends, as the mirror needs it: `detail_order_book_<pair>`,
//! the best hundred orders of each side with their ids, best first and in time order at a
//! price, sent again whenever it changes; and `live_trades_<pair>`, each trade with its
//! taker's side. Prices and quantities are kept as integer ticks and lots.

use orderbook::Side;
use serde_json::Value;

use crate::decimal;

/// An order resting on the venue's book.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Resting {
    /// The venue's id for it.
    pub id: u64,
    /// Its price, in ticks.
    pub price: i64,
    /// Its quantity, in lots.
    pub lots: u64,
}

/// The best orders of each side of the venue's book, best first.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Book {
    /// Buy orders, highest price first, then oldest first.
    pub bids: Vec<Resting>,
    /// Sell orders, lowest price first, then oldest first.
    pub asks: Vec<Resting>,
}

/// A trade on the venue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Trade {
    /// The venue's id for it.
    pub id: u64,
    /// Its price, in ticks.
    pub price: i64,
    /// Its quantity, in lots.
    pub lots: u64,
    /// The side of the order that took liquidity.
    pub taker: Side,
}

/// One message from the venue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// The best orders of each side, now.
    Book(Book),
    /// A trade.
    Trade(Trade),
    /// The venue asks its clients to reconnect, before it goes down for maintenance.
    Reconnect,
    /// Anything else: subscriptions confirmed, heartbeats.
    Other,
}

/// The channels for `pair`, such as `ethusd`.
pub fn channels(pair: &str) -> [String; 2] {
    [
        format!("detail_order_book_{pair}"),
        format!("live_trades_{pair}"),
    ]
}

/// The subscription to `channel`.
pub fn subscribe(channel: &str) -> String {
    serde_json::json!({"event": "bts:subscribe", "data": {"channel": channel}}).to_string()
}

/// A heartbeat, which keeps the connection open while the market is quiet.
pub fn heartbeat() -> String {
    serde_json::json!({"event": "bts:heartbeat"}).to_string()
}

/// `text`, with prices of `price_decimals` and quantities of `lot_decimals`. `None` if it is
/// not what Bitstamp sends, or has a number the exchange cannot hold exactly.
pub fn parse(text: &str, price_decimals: u32, lot_decimals: u32) -> Option<Message> {
    let value: Value = serde_json::from_str(text).ok()?;
    let event = value.get("event")?.as_str()?;
    let channel = value.get("channel").and_then(Value::as_str).unwrap_or("");
    let data = value.get("data");
    let price = |v: &Value| -> Option<i64> {
        i64::try_from(decimal::parse(v.as_str()?, price_decimals)?).ok()
    };
    let lots = |v: &Value| decimal::parse(v.as_str()?, lot_decimals);
    Some(match event {
        "bts:request_reconnect" => Message::Reconnect,
        "data" if channel.starts_with("detail_order_book_") => {
            let side = |key: &str| -> Option<Vec<Resting>> {
                data?
                    .get(key)?
                    .as_array()?
                    .iter()
                    .map(|entry| {
                        let entry = entry.as_array()?;
                        Some(Resting {
                            price: price(entry.first()?)?,
                            lots: lots(entry.get(1)?)?,
                            id: entry.get(2)?.as_str()?.parse().ok()?,
                        })
                    })
                    .collect()
            };
            Message::Book(Book {
                bids: side("bids")?,
                asks: side("asks")?,
            })
        }
        "trade" if channel.starts_with("live_trades_") => {
            let data = data?;
            Message::Trade(Trade {
                id: data.get("id")?.as_u64()?,
                price: price(data.get("price_str")?)?,
                lots: lots(data.get("amount_str")?)?,
                // Type 0 is a buy: the buyer took the seller's resting order.
                taker: match data.get("type")?.as_u64()? {
                    0 => Side::Buy,
                    1 => Side::Sell,
                    _ => return None,
                },
            })
        }
        _ => Message::Other,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Messages as Bitstamp sent them, shortened.
    #[test]
    fn the_venues_messages_are_read() {
        let book = r#"{"data": {"timestamp": "1791655062", "microtimestamp": "1791655062522252",
            "bids": [["2505.43", "0.200000", "2059551069351936"], ["2505.31", "4.784466", "2059551016316928"]],
            "asks": [["2505.45", "1.000000", "2059551068528640"]]},
            "channel": "detail_order_book_ethusd", "event": "data"}"#;
        assert_eq!(
            parse(book, 2, 6),
            Some(Message::Book(Book {
                bids: vec![
                    Resting {
                        id: 2_059_551_069_351_936,
                        price: 250_543,
                        lots: 200_000
                    },
                    Resting {
                        id: 2_059_551_016_316_928,
                        price: 250_531,
                        lots: 4_784_466
                    },
                ],
                asks: vec![Resting {
                    id: 2_059_551_068_528_640,
                    price: 250_545,
                    lots: 1_000_000
                }],
            }))
        );
        let trade = r#"{"data": {"id": 652644637, "timestamp": "1791655080", "amount": 0.00459,
            "amount_str": "0.004590", "price": 2505.43, "price_str": "2505.43", "type": 1,
            "microtimestamp": "1791655080352000", "buy_order_id": 2059551069351936,
            "sell_order_id": 2059551152926720}, "channel": "live_trades_ethusd", "event": "trade"}"#;
        assert_eq!(
            parse(trade, 2, 6),
            Some(Message::Trade(Trade {
                id: 652_644_637,
                price: 250_543,
                lots: 4_590,
                taker: Side::Sell,
            }))
        );
        assert_eq!(
            parse(
                r#"{"event": "bts:request_reconnect", "channel": "", "data": ""}"#,
                2,
                6
            ),
            Some(Message::Reconnect)
        );
        assert_eq!(
            parse(
                r#"{"event": "bts:subscription_succeeded", "channel": "live_trades_ethusd", "data": {}}"#,
                2,
                6
            ),
            Some(Message::Other)
        );
        // More decimals than the exchange holds, or what is not JSON, is not read.
        let finer = book.replace("2505.43", "2505.431");
        assert_eq!(parse(&finer, 2, 6), None);
        assert_eq!(parse("not json", 2, 6), None);
        assert_eq!(
            subscribe("live_trades_ethusd"),
            r#"{"data":{"channel":"live_trades_ethusd"},"event":"bts:subscribe"}"#
        );
        assert_eq!(
            channels("ethusd"),
            [
                "detail_order_book_ethusd".to_owned(),
                "live_trades_ethusd".to_owned()
            ]
        );
    }
}

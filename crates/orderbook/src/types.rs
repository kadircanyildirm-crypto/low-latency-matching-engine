//! Value types shared by the matching engine and everything that talks to it.

/// Exchange-assigned order identifier, unique among resting orders.
pub type OrderId = u64;

/// Price as an integer number of ticks. Floating point never touches the book.
pub type Price = i64;

/// Quantity in lots.
pub type Qty = u64;

/// Side of an order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Side {
    /// Bid: buys at the limit price or lower.
    Buy,
    /// Ask: sells at the limit price or higher.
    Sell,
}

impl Side {
    /// The side this side trades against.
    #[inline]
    pub const fn opposite(self) -> Side {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

/// An instruction to the matching engine.
///
/// Commands are processed strictly one at a time, in arrival order, and the resulting
/// [`Event`] stream is a pure function of the command stream. That determinism is what makes
/// event sourcing and replay possible.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Command {
    /// Good-till-cancelled limit order. Trades against the opposite side at `price` or
    /// better; any remainder rests on the book at `price`.
    Limit {
        id: OrderId,
        side: Side,
        price: Price,
        qty: Qty,
    },
    /// Trades against the opposite side at any price. Whatever cannot be filled is
    /// cancelled; market orders never rest.
    Market { id: OrderId, side: Side, qty: Qty },
    /// Removes a resting order.
    Cancel { id: OrderId },
    /// Sets a resting order's price and open quantity.
    ///
    /// Reducing the quantity at the same price keeps queue priority. Any other change
    /// re-enters the order at the back of the queue and trades if the new price crosses.
    Modify { id: OrderId, price: Price, qty: Qty },
}

impl Command {
    /// The order this command creates or refers to.
    #[inline]
    pub const fn id(&self) -> OrderId {
        match *self {
            Command::Limit { id, .. }
            | Command::Market { id, .. }
            | Command::Cancel { id }
            | Command::Modify { id, .. } => id,
        }
    }
}

/// Output of the matching engine.
///
/// Per command the engine emits:
/// - `Limit` / `Market`: either a single `Rejected`, or `Accepted` followed by zero or more
///   `Trade`s and then `Rested` (limit remainder) or `Cancelled { reason: NoLiquidity }`
///   (market remainder) if any quantity is left.
/// - `Cancel`: `Cancelled { reason: Requested }` or `Rejected`.
/// - `Modify`: `Rejected`, or `Modified`. If the order lost priority, `Modified` is followed
///   by the same `Trade`s / `Rested` a new limit order would produce.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Event {
    /// A new order passed validation.
    Accepted { id: OrderId },
    /// A command was refused; the book is unchanged.
    Rejected { id: OrderId, reason: RejectReason },
    /// `taker` (the incoming order) traded `qty` with resting order `maker` at the maker's
    /// price.
    Trade {
        taker: OrderId,
        maker: OrderId,
        taker_side: Side,
        price: Price,
        qty: Qty,
    },
    /// The order, or what was left of it after trading, now rests on the book.
    Rested {
        id: OrderId,
        side: Side,
        price: Price,
        qty: Qty,
    },
    /// `qty` open quantity of the order was removed from the book.
    Cancelled {
        id: OrderId,
        qty: Qty,
        reason: CancelReason,
    },
    /// The order now has this price and open quantity.
    Modified { id: OrderId, price: Price, qty: Qty },
}

/// Why a command was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RejectReason {
    /// Quantity was zero.
    InvalidQuantity,
    /// Price is outside the book's configured band.
    PriceOutOfRange,
    /// A resting order already uses this id.
    DuplicateOrderId,
    /// No resting order has this id (never existed, already filled, or already cancelled).
    UnknownOrder,
    /// The book holds its maximum number of resting orders.
    BookFull,
}

/// Why open quantity left the book without trading.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CancelReason {
    /// A `Cancel` command.
    Requested,
    /// A market order ran out of opposite-side liquidity.
    NoLiquidity,
}

/// Receives the engine's output.
///
/// The book is generic over the sink, so the call is monomorphized and inlined: no dynamic
/// dispatch and no intermediate buffer on the hot path.
pub trait EventSink {
    /// Called once per event, in order.
    fn on_event(&mut self, event: Event);
}

impl EventSink for Vec<Event> {
    #[inline]
    fn on_event(&mut self, event: Event) {
        self.push(event);
    }
}

impl<S: EventSink + ?Sized> EventSink for &mut S {
    #[inline]
    fn on_event(&mut self, event: Event) {
        (**self).on_event(event);
    }
}

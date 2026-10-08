//! Value types shared by the matching engine and everything that talks to it.

/// Exchange-assigned order identifier, unique among resting orders.
pub type OrderId = u64;

/// The participant (account) an order belongs to. Used for self-trade prevention and to
/// make sure only the owner can cancel or modify an order.
pub type OwnerId = u32;

/// Identifier of one execution, assigned by the book from a counter: 1, 2, 3, ...
pub type TradeId = u64;

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
        /// New order's id; must not belong to a resting order.
        id: OrderId,
        /// Participant placing the order.
        owner: OwnerId,
        /// Buy or sell.
        side: Side,
        /// Limit price in ticks.
        price: Price,
        /// Order quantity, `1..=max_order_qty`.
        qty: Qty,
    },
    /// Trades against the opposite side at any price within the book's price protection.
    /// Whatever cannot be filled is cancelled; market orders never rest.
    Market {
        /// New order's id; must not belong to a resting order.
        id: OrderId,
        /// Participant placing the order.
        owner: OwnerId,
        /// Buy or sell.
        side: Side,
        /// Order quantity, `1..=max_order_qty`.
        qty: Qty,
    },
    /// Removes a resting order. Only its owner may cancel it.
    Cancel {
        /// Order to cancel.
        id: OrderId,
        /// Must match the order's owner.
        owner: OwnerId,
    },
    /// Cancel/replace with FIX semantics: `qty` is the new *total* order quantity, including
    /// what has already been filled, so a modify that races a fill can never over-fill.
    ///
    /// - `qty <= filled`: nothing is left to work and the order is removed.
    /// - Same price and `qty` not above the current total: the open quantity shrinks in place
    ///   and the order keeps its queue priority.
    /// - Anything else: the order re-enters at the back of the queue at the new price and
    ///   trades if that price crosses.
    Modify {
        /// Order to modify.
        id: OrderId,
        /// Must match the order's owner.
        owner: OwnerId,
        /// New limit price in ticks.
        price: Price,
        /// New total quantity (filled + open), `1..=max_order_qty`.
        qty: Qty,
    },
}

impl Command {
    /// The order this command creates or refers to.
    #[inline]
    pub const fn id(&self) -> OrderId {
        match *self {
            Command::Limit { id, .. }
            | Command::Market { id, .. }
            | Command::Cancel { id, .. }
            | Command::Modify { id, .. } => id,
        }
    }

    /// The participant who sent the command.
    #[inline]
    pub const fn owner(&self) -> OwnerId {
        match *self {
            Command::Limit { owner, .. }
            | Command::Market { owner, .. }
            | Command::Cancel { owner, .. }
            | Command::Modify { owner, .. } => owner,
        }
    }
}

/// Output of the matching engine.
///
/// Per command the engine emits:
/// - `Limit` / `Market`: either a single `Rejected`, or `Accepted` followed by any number
///   of `Trade`s (and `Cancelled { reason: SelfTrade }` for resting orders removed by
///   self-trade prevention), then at most one of `Rested` (limit remainder) or `Cancelled`
///   (remainder that may not rest).
/// - `Cancel`: `Cancelled { reason: Requested }` or `Rejected`.
/// - `Modify`: `Rejected`, or `Modified`. If the order lost priority, `Modified` is followed
///   by the same events a new limit order would produce after `Accepted`.
///
/// "Leaves" quantity is the open quantity still working on the book; zero means the order
/// is done.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Event {
    /// A new order passed validation.
    Accepted {
        /// The new order.
        id: OrderId,
    },
    /// A command was refused; the book is unchanged.
    Rejected {
        /// Order the command created or referred to.
        id: OrderId,
        /// Why.
        reason: RejectReason,
    },
    /// The incoming order (`taker`) traded with a resting order (`maker`) at the maker's
    /// price.
    Trade {
        /// Unique, gap-free execution id.
        trade_id: TradeId,
        /// Incoming order.
        taker: OrderId,
        /// Resting order.
        maker: OrderId,
        /// Side of the incoming order; the maker is on the opposite side.
        taker_side: Side,
        /// Execution price: the maker's price.
        price: Price,
        /// Executed quantity.
        qty: Qty,
        /// Taker's open quantity after this execution.
        taker_leaves: Qty,
        /// Maker's open quantity after this execution; zero means the maker is filled.
        maker_leaves: Qty,
    },
    /// The order, or what was left of it after trading, now rests on the book.
    Rested {
        /// The resting order.
        id: OrderId,
        /// Its side.
        side: Side,
        /// Its price.
        price: Price,
        /// Its open quantity.
        qty: Qty,
    },
    /// Open quantity left the book without trading. The order is done.
    Cancelled {
        /// The order.
        id: OrderId,
        /// Open quantity removed.
        qty: Qty,
        /// Why.
        reason: CancelReason,
    },
    /// A modify was applied.
    Modified {
        /// The order.
        id: OrderId,
        /// New price.
        price: Price,
        /// New total quantity.
        qty: Qty,
        /// Open quantity after the modify (before any trading it causes). Zero means the
        /// new total did not exceed what was already filled, and the order is done.
        leaves: Qty,
    },
}

/// Why a command was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RejectReason {
    /// Quantity was zero or above the book's `max_order_qty`.
    InvalidQuantity,
    /// Price is outside the book's static price band.
    PriceOutOfRange,
    /// A limit price lies further through the opposite best price than the book's price
    /// protection allows (a likely fat-finger order).
    PriceOutsideProtection,
    /// A resting order already uses this id.
    DuplicateOrderId,
    /// No resting order with this id belongs to the sender. Orders of other participants
    /// are reported the same way, so their existence does not leak.
    UnknownOrder,
    /// The book holds its maximum number of resting orders and this order could only rest.
    BookFull,
}

/// Why open quantity left the book without trading.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CancelReason {
    /// A `Cancel` command.
    Requested,
    /// A market order ran out of opposite-side liquidity.
    NoLiquidity,
    /// A market order reached the edge of the book's price protection.
    PriceProtection,
    /// Self-trade prevention: the order would have traded with an order of the same owner.
    SelfTrade,
}

/// How the book prevents an owner from trading with themselves.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SelfTradePolicy {
    /// Cancel the resting order and keep matching the incoming one.
    CancelResting,
    /// Cancel the rest of the incoming order; resting orders are untouched.
    CancelIncoming,
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

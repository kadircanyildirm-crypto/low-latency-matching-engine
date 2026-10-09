// liquibook adapter for the comparison harness; src/main.rs drives it.
//
// The replay loops live here, so the engine is called directly from C++ and no FFI call
// sits inside a timed region. The per-command timer is the same TSC sequence the Rust
// harness uses (compare/harness/src/clock.rs).
//
// Orders are plain structs owned by the adapter, indexed by order id in a vector sized
// from the stream header, and passed to the book as raw pointers: liquibook identifies an
// order by its pointer, and a direct index is the cheapest possible way for a caller to
// find it again. liquibook's own examples use std::shared_ptr, which costs more.

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <vector>

#if defined(_M_X64) || defined(__x86_64__)
#define SHIM_TSC 1
#if defined(_MSC_VER)
#include <intrin.h>
#else
#include <x86intrin.h>
#endif
#else
#include <chrono>
#endif

#include "book/order_book.h"

namespace lb = liquibook::book;

// Same layout as harness::stream::Record.
struct Record {
    uint8_t kind;
    uint8_t side;
    uint16_t reserved;
    uint32_t qty;
    uint64_t id;
    int64_t price;
};
static_assert(sizeof(Record) == 24, "record layout");

// Same layout as harness::stream::Summary.
struct Summary {
    uint64_t trades;
    uint64_t traded_qty;
    uint64_t resting_orders;
    uint64_t resting_qty;
    int64_t best_bid;
    int64_t best_ask;
};

enum : uint8_t { LIMIT = 0, MARKET = 1, CANCEL = 2, MOVE = 3 };

// The interface liquibook's OrderBook template requires of an order.
struct Order {
    lb::Price price_ = 0;
    lb::Quantity qty_ = 0;
    bool buy_ = false;
    bool ioc_ = false;

    bool is_buy() const { return buy_; }
    lb::Price price() const { return price_; }
    lb::Price stop_price() const { return 0; }
    lb::Quantity order_qty() const { return qty_; }
    bool all_or_none() const { return false; }
    bool immediate_or_cancel() const { return ioc_; }
};

// Consumes the engine's output: every fill reaches on_fill through liquibook's callback
// queue, which is how an application receives executions.
class Book : public lb::OrderBook<Order*> {
public:
    uint64_t trades = 0;
    uint64_t traded_qty = 0;

protected:
    void on_fill(Order* const&, Order* const&, lb::Quantity qty, lb::Price, bool,
                 bool) override {
        ++trades;
        traded_qty += qty;
    }
};

struct Ctx {
    Book book;
    std::vector<Order> orders;
};

static inline void apply(Ctx* c, const Record& r) {
    switch (r.kind) {
    case LIMIT: {
        Order& o = c->orders[r.id];
        o.price_ = static_cast<lb::Price>(r.price);
        o.qty_ = r.qty;
        o.buy_ = r.side == 0;
        o.ioc_ = false;
        c->book.add(&o);
        break;
    }
    case MARKET: {
        // Price 0 is liquibook's market price; immediate-or-cancel keeps the remainder
        // off the book, as a market order of the other engines. The condition must be
        // passed to add(): OrderTracker reads the order's own immediate_or_cancel() only
        // under LIQUIBOOK_ORDER_KNOWS_CONDITIONS, and even then ORs it into its
        // constructor parameter instead of the member, so a market order's unfilled
        // remainder would rest on the book at the market price.
        Order& o = c->orders[r.id];
        o.price_ = lb::MARKET_ORDER_PRICE;
        o.qty_ = r.qty;
        o.buy_ = r.side == 0;
        o.ioc_ = true;
        c->book.add(&o, lb::oc_immediate_or_cancel);
        break;
    }
    case CANCEL:
        c->book.cancel(&c->orders[r.id]);
        break;
    case MOVE: {
        // replace() finds the order at its current price, then re-adds it with its open
        // quantity at the new one. The order object must then carry the new price, or
        // the next lookup would search the old level.
        Order& o = c->orders[r.id];
        const lb::Price price = static_cast<lb::Price>(r.price);
        c->book.replace(&o, lb::SIZE_UNCHANGED, price);
        o.price_ = price;
        break;
    }
    default:
        std::fprintf(stderr, "liquibook shim: bad record kind %u\n", unsigned(r.kind));
        std::abort();
    }
}

#if SHIM_TSC
static inline uint64_t tsc_start() {
    _mm_lfence();
    const uint64_t t = __rdtsc();
    _mm_lfence();
    return t;
}

static inline uint64_t tsc_stop() {
    unsigned int aux;
    const uint64_t t = __rdtscp(&aux);
    _mm_lfence();
    return t;
}
#else
// Other targets: nanoseconds, as the Rust harness's fallback clock.
static inline uint64_t tsc_start() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                     std::chrono::steady_clock::now().time_since_epoch())
                                     .count());
}
static inline uint64_t tsc_stop() { return tsc_start(); }
#endif

// No C++ exception may unwind into Rust.
template <class F> static void guarded(F f) {
    try {
        f();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "liquibook threw: %s\n", e.what());
        std::abort();
    } catch (...) {
        std::fprintf(stderr, "liquibook threw an unknown exception\n");
        std::abort();
    }
}

extern "C" {

Ctx* lb_new(uint64_t max_id) {
    Ctx* c = nullptr;
    guarded([&] {
        c = new Ctx();
        c->orders.resize(static_cast<size_t>(max_id) + 1);
    });
    return c;
}

void lb_free(Ctx* c) { delete c; }

void lb_replay(Ctx* c, const Record* records, size_t n) {
    guarded([&] {
        for (size_t i = 0; i < n; ++i) {
            apply(c, records[i]);
        }
    });
}

void lb_replay_timed(Ctx* c, const Record* records, size_t n, uint64_t* ticks) {
    guarded([&] {
        for (size_t i = 0; i < n; ++i) {
            const uint64_t start = tsc_start();
            apply(c, records[i]);
            ticks[i] = tsc_stop() - start;
        }
    });
}

void lb_summary(const Ctx* c, Summary* out) {
    Summary s{};
    s.trades = c->book.trades;
    s.traded_qty = c->book.traded_qty;
    for (const auto* side : {&c->book.bids(), &c->book.asks()}) {
        for (const auto& entry : *side) {
            ++s.resting_orders;
            s.resting_qty += entry.second.open_qty();
        }
    }
    s.best_bid = c->book.bids().empty()
                     ? INT64_MIN
                     : static_cast<int64_t>(c->book.bids().begin()->first.price());
    s.best_ask = c->book.asks().empty()
                     ? INT64_MAX
                     : static_cast<int64_t>(c->book.asks().begin()->first.price());
    *out = s;
}

} // extern "C"

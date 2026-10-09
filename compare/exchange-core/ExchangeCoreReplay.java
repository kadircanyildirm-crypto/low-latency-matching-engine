// exchange-core's matching engine core replaying the comparison streams; see
// docs/COMPARISON.md. compare/run.sh exchange-core builds and runs it.
//
// What is measured: OrderBookDirectImpl, exchange-core's performance order book, driven
// through IOrderBook.processCommand exactly as its MatchingEngineRouter drives it, with the
// router's object-pool sizes and its event helper (EVENTS_POOLING is off in exchange-core,
// so trade events are plain allocations there too). The LMAX pipeline around it (ring
// buffer, risk engine, journaling, result handlers) is not: it would add inter-thread
// hand-offs that the other engines do not have. exchange-core's own order book
// benchmark (ITOrderBookBase) measures the same layer.
//
// Mapping: limit = PLACE_ORDER GTC; market = PLACE_ORDER IOC at the extreme price
// (exchange-core has no market order type); cancel = CANCEL_ORDER; move = MOVE_ORDER,
// which re-inserts the order with its remaining size at the new price, matching first.
// Every order's uid is its side, as in the stream. Trades are counted by walking each
// command's matcher event chain, as the risk and result stages would.
//
// The protocol mirrors compare/harness/src/run.rs: an untimed warm-up pass over the whole
// stream (which also lets the JIT compile everything), then per run a throughput pass and
// a latency pass on fresh books, each after a System.gc() as in exchange-core's own
// benchmarks. Latency uses System.nanoTime(), the finest clock Java offers; on Windows it
// ticks in 100 ns steps (QueryPerformanceCounter at 10 MHz), so per-command percentiles
// are quantised to 100 ns there. Rows go to the same CSV with the same percentile
// definition (nearest rank, integer arithmetic).

import exchange.core2.collections.objpool.ObjectsPool;
import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.core.common.L2MarketData;
import exchange.core2.core.common.MatcherEventType;
import exchange.core2.core.common.MatcherTradeEvent;
import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.SymbolType;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.common.config.LoggingConfiguration;
import exchange.core2.core.orderbook.IOrderBook;
import exchange.core2.core.orderbook.OrderBookDirectImpl;
import exchange.core2.core.orderbook.OrderBookEventsHelper;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import net.openhft.affinity.Affinity;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;

public final class ExchangeCoreReplay {

    static final String NAME = "exchange-core";
    static final String[] SCENARIOS = {"baseline", "sweep", "deep", "modify"};
    static final String CSV_HEADER = "engine,scenario,round,run,commands,throughput_mcmd_s,"
            + "chunk_median_mcmd_s,mean_ns,p50_ns,p90_ns,p99_ns,p99_9_ns,p99_99_ns,max_ns,timer_overhead_ns,verified,core";

    static final byte LIMIT = 0, MARKET = 1, CANCEL = 2, MOVE = 3;

    /** Commands per timed chunk of the throughput pass, as in the Rust harness. */
    static final int CHUNK = 100_000;

    /** A stream file, decoded into columns before anything is timed. */
    static final class Stream {
        int count;
        int warmup;
        long maxId;
        long[] expect = new long[6];
        byte[] kind;
        byte[] side;
        int[] qty;
        long[] id;
        long[] price;
    }

    static Stream read(Path path) throws IOException {
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer head = ByteBuffer.allocate(128).order(ByteOrder.LITTLE_ENDIAN);
            readFully(ch, head);
            byte[] magic = new byte[8];
            head.get(0, magic);
            if (!"LOBCMDS1".equals(new String(magic, StandardCharsets.US_ASCII))
                    || head.getInt(8) != 1 || head.getInt(12) != 24) {
                throw new IOException("not a version 1 command stream: " + path);
            }
            Stream s = new Stream();
            s.count = Math.toIntExact(head.getLong(16));
            s.warmup = Math.toIntExact(head.getLong(24));
            s.maxId = head.getLong(56);
            for (int i = 0; i < 6; i++) {
                s.expect[i] = head.getLong(64 + 8 * i);
            }
            s.kind = new byte[s.count];
            s.side = new byte[s.count];
            s.qty = new int[s.count];
            s.id = new long[s.count];
            s.price = new long[s.count];
            ByteBuffer buf = ByteBuffer.allocate(24 * 65536).order(ByteOrder.LITTLE_ENDIAN);
            int i = 0;
            while (i < s.count) {
                int n = Math.min(65536, s.count - i);
                buf.clear().limit(24 * n);
                readFully(ch, buf);
                buf.flip();
                for (int k = 0; k < n; k++, i++) {
                    s.kind[i] = buf.get();
                    s.side[i] = buf.get();
                    buf.getShort();
                    s.qty[i] = buf.getInt();
                    s.id[i] = buf.getLong();
                    s.price[i] = buf.getLong();
                }
            }
            return s;
        }
    }

    static void readFully(FileChannel ch, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            if (ch.read(buf) < 0) {
                throw new IOException("stream truncated");
            }
        }
    }

    /** One fresh order book and the command object reused for every call. */
    static final class Engine {
        final IOrderBook book;
        final OrderCommand cmd = new OrderCommand();
        long trades;
        long tradedQty;

        Engine() {
            // MatchingEngineRouter's pool sizes.
            HashMap<Integer, Integer> pool = new HashMap<>();
            pool.put(ObjectsPool.DIRECT_ORDER, 1024 * 1024);
            pool.put(ObjectsPool.DIRECT_BUCKET, 1024 * 64);
            pool.put(ObjectsPool.ART_NODE_4, 1024 * 32);
            pool.put(ObjectsPool.ART_NODE_16, 1024 * 16);
            pool.put(ObjectsPool.ART_NODE_48, 1024 * 8);
            pool.put(ObjectsPool.ART_NODE_256, 1024 * 4);
            // A futures contract: no fees, and moves skip the exchange-pair risk check on the
            // bid's reserved price, which belongs to the risk engine.
            CoreSymbolSpecification spec = CoreSymbolSpecification.builder()
                    .symbolId(1)
                    .type(SymbolType.FUTURES_CONTRACT)
                    .baseCurrency(1)
                    .quoteCurrency(2)
                    .baseScaleK(1)
                    .quoteScaleK(1)
                    .takerFee(0)
                    .makerFee(0)
                    .marginBuy(0)
                    .marginSell(0)
                    .build();
            book = new OrderBookDirectImpl(spec, new ObjectsPool(pool),
                    OrderBookEventsHelper.NON_POOLED_EVENTS_HELPER, LoggingConfiguration.DEFAULT);
            cmd.symbol = 1;
        }

        void apply(Stream s, int i) {
            final OrderCommand c = cmd;
            final boolean buy = s.side[i] == 0;
            c.orderId = s.id[i];
            c.uid = s.side[i];
            c.matcherEvent = null;
            c.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
            switch (s.kind[i]) {
                case LIMIT:
                    c.command = OrderCommandType.PLACE_ORDER;
                    c.orderType = OrderType.GTC;
                    c.action = buy ? OrderAction.BID : OrderAction.ASK;
                    c.price = s.price[i];
                    c.reserveBidPrice = c.price;
                    c.size = s.qty[i];
                    break;
                case MARKET:
                    c.command = OrderCommandType.PLACE_ORDER;
                    c.orderType = OrderType.IOC;
                    c.action = buy ? OrderAction.BID : OrderAction.ASK;
                    c.price = buy ? Long.MAX_VALUE : 0L;
                    c.reserveBidPrice = c.price;
                    c.size = s.qty[i];
                    break;
                case CANCEL:
                    c.command = OrderCommandType.CANCEL_ORDER;
                    break;
                case MOVE:
                    c.command = OrderCommandType.MOVE_ORDER;
                    c.price = s.price[i];
                    break;
                default:
                    throw new IllegalStateException("bad record kind " + s.kind[i]);
            }
            IOrderBook.processCommand(book, c);
            for (MatcherTradeEvent e = c.matcherEvent; e != null; e = e.nextEvent) {
                if (e.eventType == MatcherEventType.TRADE) {
                    trades++;
                    tradedQty += e.size;
                }
            }
        }

        void replay(Stream s, int from, int to) {
            for (int i = from; i < to; i++) {
                apply(s, i);
            }
        }

        void replayTimed(Stream s, int from, int to, long[] ns) {
            for (int i = from; i < to; i++) {
                final long start = System.nanoTime();
                apply(s, i);
                ns[i - from] = System.nanoTime() - start;
            }
        }

        long[] summary() {
            L2MarketData top = book.getL2MarketDataSnapshot(1);
            return new long[] {
                trades,
                tradedQty,
                book.getOrdersNum(OrderAction.BID) + book.getOrdersNum(OrderAction.ASK),
                book.getTotalOrdersVolume(OrderAction.BID) + book.getTotalOrdersVolume(OrderAction.ASK),
                top.bidSize > 0 ? top.bidPrices[0] : Long.MIN_VALUE,
                top.askSize > 0 ? top.askPrices[0] : Long.MAX_VALUE,
            };
        }
    }

    static boolean check(String scenario, String pass, Stream s, long[] got) {
        if (Arrays.equals(got, s.expect)) {
            return true;
        }
        System.out.printf("  %-9s %s: outcome differs from the recorded one%n      expected %s%n      got      %s%n",
                scenario, pass, Arrays.toString(s.expect), Arrays.toString(got));
        return false;
    }

    /** Same definition as harness::run::nearest_rank. */
    static int nearestRank(int n, long ppm) {
        long rank = (ppm * n + 999_999L) / 1_000_000L;
        return (int) Math.max(1, Math.min(rank, Math.max(n, 1))) - 1;
    }

    static double timerOverheadNs() {
        long[] samples = new long[10_001];
        for (int i = 0; i < samples.length; i++) {
            long t = System.nanoTime();
            samples[i] = System.nanoTime() - t;
        }
        Arrays.sort(samples);
        return samples[samples.length / 2];
    }

    static String env(String name, String fallback) {
        String v = System.getenv(name);
        return v == null || v.trim().isEmpty() ? fallback : v.trim();
    }

    static boolean selected(String scenario) {
        String only = System.getenv("CMP_SCENARIOS");
        if (only == null || only.trim().isEmpty()) {
            return true;
        }
        for (String n : only.split(",")) {
            if (n.trim().equals(scenario)) {
                return true;
            }
        }
        return false;
    }

    public static void main(String[] args) throws IOException {
        Path data = Paths.get(env("CMP_DATA", "compare/data"));
        Path results = Paths.get(env("CMP_RESULTS", "compare/results/results.csv"));
        int runs = Math.max(1, Integer.parseInt(env("CMP_RUNS", "1")));
        String round = env("CMP_ROUND", "0");
        int core = Integer.parseInt(env("CMP_CORE",
                Integer.toString(Runtime.getRuntime().availableProcessors() - 1)));
        String pinned = pin(core);
        double overhead = timerOverheadNs();
        System.out.printf("== %s (exchange-core @ 2f85487, OrderBookDirectImpl via IOrderBook.processCommand; %s %s)%n",
                NAME, System.getProperty("java.vm.name"), System.getProperty("java.version"));
        System.out.printf(Locale.ROOT, "  core %s | System.nanoTime | timer overhead ~%.0f ns (included)%n",
                pinned, overhead);

        for (String scenario : SCENARIOS) {
            if (!selected(scenario)) {
                continue;
            }
            Path path = data.resolve(scenario + ".bin");
            if (!Files.exists(path)) {
                System.out.printf("  %-9s skipped: no stream at %s (run the exporter)%n", scenario, path);
                continue;
            }
            Stream s = read(path);
            int measured = s.count - s.warmup;

            Engine warm = new Engine();
            warm.replay(s, 0, s.count);
            check(scenario, "warm-up pass", s, warm.summary());
            warm = null;

            long[] ns = new long[measured];
            for (int run = 1; run <= runs; run++) {
                Engine e = new Engine();
                e.replay(s, 0, s.warmup);
                System.gc();
                int chunks = (measured + CHUNK - 1) / CHUNK;
                double[] rates = new double[chunks];
                long elapsed = 0;
                for (int c = 0; c < chunks; c++) {
                    int from = s.warmup + c * CHUNK;
                    int to = Math.min(s.count, from + CHUNK);
                    long started = System.nanoTime();
                    e.replay(s, from, to);
                    long took = System.nanoTime() - started;
                    elapsed += took;
                    rates[c] = (to - from) / (took / 1e9) / 1e6;
                }
                Arrays.sort(rates);
                double chunkMedian = chunks % 2 == 1 ? rates[chunks / 2]
                        : (rates[chunks / 2 - 1] + rates[chunks / 2]) / 2;
                boolean verified = check(scenario, "throughput pass", s, e.summary());
                e = null;

                e = new Engine();
                e.replay(s, 0, s.warmup);
                System.gc();
                e.replayTimed(s, s.warmup, s.count, ns);
                verified &= check(scenario, "latency pass", s, e.summary());
                e = null;

                Arrays.sort(ns);
                double sum = 0;
                for (long v : ns) {
                    sum += v;
                }
                double mean = sum / measured;
                long p50 = ns[nearestRank(measured, 500_000)];
                long p90 = ns[nearestRank(measured, 900_000)];
                long p99 = ns[nearestRank(measured, 990_000)];
                long p999 = ns[nearestRank(measured, 999_000)];
                long p9999 = ns[nearestRank(measured, 999_900)];
                long max = ns[measured - 1];
                double throughput = measured / (elapsed / 1e9) / 1e6;
                System.out.printf(Locale.ROOT,
                        "  %-9s run %d: %6.2f M cmd/s (%6.2f median chunk) | p50 %5d | p90 %5d | p99 %6d | p99.9 %6d | p99.99 %7d | max %8d ns | %s%n",
                        scenario, run, throughput, chunkMedian, p50, p90, p99, p999, p9999, max,
                        verified ? "verified" : "MISMATCH");
                String row = String.format(Locale.ROOT,
                        "%s,%s,%s,%d,%d,%.4f,%.4f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%s,%s",
                        NAME, scenario, round, run, measured, throughput, chunkMedian, mean, (double) p50, (double) p90,
                        (double) p99, (double) p999, (double) p9999, (double) max, overhead,
                        verified ? "yes" : "no", pinned);
                appendRow(results, row);
            }
        }
    }

    /** kernel32's thread affinity calls, through JNA (on exchange-core's classpath). */
    public interface WinThread extends Library {
        WinThread INSTANCE = Native.load("kernel32", WinThread.class);

        Pointer GetCurrentThread();

        long SetThreadAffinityMask(Pointer thread, long mask);
    }

    /**
     * Pins the main thread, like the Rust harness; the JIT and GC threads stay free. On
     * Windows OpenHFT Affinity 3.2.2 fails (GetProcessAffinityMask: invalid handle), so the
     * thread mask is set directly there.
     */
    static String pin(int core) {
        try {
            if (System.getProperty("os.name").startsWith("Windows")) {
                long previous = WinThread.INSTANCE.SetThreadAffinityMask(
                        WinThread.INSTANCE.GetCurrentThread(), 1L << core);
                return previous != 0 ? Integer.toString(core) : "none";
            }
            Affinity.setAffinity(core);
            return Integer.toString(core);
        } catch (Throwable e) {
            return "none";
        }
    }

    static void appendRow(Path results, String row) throws IOException {
        if (results.getParent() != null) {
            Files.createDirectories(results.getParent());
        }
        boolean fresh = !Files.exists(results) || Files.size(results) == 0;
        String text = (fresh ? CSV_HEADER + "\n" : "") + row + "\n";
        Files.write(results, text.getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}

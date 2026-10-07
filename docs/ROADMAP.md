# Yol Haritası

Her faz kendi başına gösterilebilir bir sonuç üretir. Bir faz "bitti" sayılmadan önce
kabul kriterlerinin hepsi sağlanmış olmalı. Yarım kalmış beş faz yerine bitmiş, ölçülmüş
ve belgelenmiş iki faz daha değerlidir.

| Faz | Konu | Durum |
|-----|------|-------|
| 1 | Çekirdek order book + test altyapısı + benchmark | ✅ Tamamlandı |
| 2 | Event sourcing: journal + replay | ⏳ Sırada |
| 3 | Binary protokol + TCP gateway | — |
| 4 | Pipeline: gateway → sequencer → matcher → publisher | — |
| 5 | Linux'ta uçtan uca ölçüm ve optimizasyon | — |
| 6 | Hot standby replikasyon ve failover | — |
| 7 | Genişletmeler (opsiyonel) | — |

---

## Faz 1 — Çekirdek order book ✅

**Amaç:** Tek thread'li, deterministik, hot path'te heap allocation yapmayan, fiyat-zaman
öncelikli bir order book.

**Yapılanlar**
- Emir tipleri: limit (GTC), market, cancel, modify (aynı fiyatta miktar azaltma önceliği
  korur, diğer her değişiklik kuyruğun sonuna atar).
- Veri yapıları: fiyat basamağı dizisi (ladder) + iki seviyeli bitset ile en iyi fiyat
  araması, önceden ayrılmış slab içinde intrusive FIFO kuyruklar, ön-rezerve edilmiş
  `FxHashMap` ile id → slot eşlemesi.
- Testler:
  - 19 senaryo testi (her kural için birebir olay dizisi)
  - Referans implementasyona karşı differential property test (1000 rastgele dizi)
  - Her komuttan sonra iç tutarlılık kontrolü (`validate()`)
  - Bitset için `BTreeSet`'e karşı property test
  - 200 bin komutluk soak testi
  - Determinizm testi
  - Sayan global allocator ile **sıfır allocation kanıtı** (1 milyon komut)
- Ölçüm: TSC tabanlı saat + HdrHistogram ile komut başına gecikme histogramı
  (`.hgrm` çıktısı), Criterion ile throughput regresyon takibi.

**Kabul kriterleri:** tüm testler yeşil, clippy uyarısız, hot path sıfır allocation,
latency raporu README'de.

---

## Faz 2 — Event sourcing: journal + replay

**Amaç:** Sistemin durumunu yalnızca komut log'undan birebir yeniden kurabilmek.

- Her komuta artan bir sıra numarası (sequence number) veren sequencer.
- Append-only journal dosyası: sabit başlık + uzunluk + CRC32 ile her kayıt.
- Çökme sonrası kurtarma: yarım yazılmış son kaydı tespit edip kesme.
- Snapshot: belirli aralıklarla defter durumunu yaz; açılışta snapshot + log kuyruğunu
  replay et.
- Durum hash'i: replay sonrası defter hash'i, canlı çalışmadaki hash ile aynı olmalı.
- `fsync` politikaları (her komut / grup commit / OS'e bırak) ve her birinin gecikmeye
  etkisini ölçen benchmark.

**Kabul kriterleri:** rastgele noktada öldürülen sürecin replay ile birebir aynı duruma
dönmesini kanıtlayan test; journal yazma maliyeti ölçülmüş.

---

## Faz 3 — Binary protokol + TCP gateway

**Amaç:** Dış dünyadan emir kabul etmek.

- Sabit uzunluklu, little-endian mesajlar: `NewOrder`, `Cancel`, `Modify`,
  `ExecutionReport`, `Reject`, `Heartbeat`.
- Framing, oturum (login / heartbeat / logout), istemci emir id'si → borsa emir id'si
  eşlemesi, hesap kavramı.
- `cargo-fuzz` ile protokol çözücüsüne fuzzing.
- Yük üreten basit bir istemci (load generator).

**Kabul kriterleri:** fuzzing'de çökme yok; load generator ile uçtan uca emir akışı.

---

## Faz 4 — Pipeline

**Amaç:** LMAX tarzı aşamalı mimari: her aşama kendi çekirdeğinde.

- Kendi SPSC ring buffer'ımız: cache-line padding, false sharing'den kaçınma, batch okuma.
- Aşamalar: gateway → sequencer/journal → matcher → publisher.
- Market data publisher: L2 snapshot + artımlı güncellemeler.
- CPU pinning, busy-spin ile bekleme, backpressure.

**Kabul kriterleri:** ring buffer için ayrı testler ve benchmark; pipeline uçtan uca
çalışıyor.

---

## Faz 5 — Ölçüm ve optimizasyon (Linux)

**Amaç:** Gerçek ve savunulabilir gecikme rakamları.

- Open-loop yük testi (sabit varış hızında) ve coordinated omission düzeltmesi.
- `isolcpus`, `nohz_full`, IRQ affinity ayarları.
- `perf` + flamegraph, cache-miss analizi; her optimizasyonun önce/sonra grafiği.
- **Araştırma sorusu:** order book veri yapısı karşılaştırması: ladder + bitset /
  `BTreeMap` / sıralı `Vec`. Cache miss ve tail latency üzerinden (akademik rapor için).

**Kabul kriterleri:** donanım ve ayarları belgelenmiş, tekrarlanabilir bir benchmark raporu.

---

## Faz 6 — Hot standby

**Amaç:** Ana node çökerse yedek node aynı durumdan devam eder.

- Primary journal'ı standby'a akıtır; standby aynı komutları replay eder.
- Heartbeat ile çökme tespiti, epoch/fencing ile split-brain önleme.
- Failover süresinin ölçümü.

**Kabul kriterleri:** primary'yi öldüren test; standby'ın durum hash'i primary ile aynı.

---

## Faz 7 — Genişletmeler (opsiyonel)

- IOC / FOK / post-only emir tipleri, self-trade prevention.
- Çoklu enstrüman (enstrüman başına shard / thread).
- `io_uring` ile ağ katmanı.
- Demo için web tabanlı canlı derinlik (depth) arayüzü.

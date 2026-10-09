//! Per-command timestamps: the TSC, read exactly as `crates/orderbook/benches/latency.rs`
//! reads it, so the numbers here and in the README come from the same instrument.

use std::time::Instant;

/// Ticks to nanoseconds, calibrated against the OS monotonic clock.
#[derive(Clone, Copy, Debug)]
pub struct Clock {
    ns_per_tick: f64,
}

impl Clock {
    /// Measures the tick rate over 250 ms.
    pub fn calibrate() -> Self {
        let wall = Instant::now();
        let t0 = start();
        while wall.elapsed().as_millis() < 250 {
            std::hint::spin_loop();
        }
        let t1 = stop();
        let ns = wall.elapsed().as_nanos() as f64;
        Self {
            ns_per_tick: ns / t1.wrapping_sub(t0).max(1) as f64,
        }
    }

    /// Nanoseconds in `ticks`.
    pub fn to_ns(&self, ticks: u64) -> f64 {
        ticks as f64 * self.ns_per_tick
    }

    /// What the ticks are.
    pub fn describe(&self) -> String {
        if cfg!(target_arch = "x86_64") {
            format!("TSC @ {:.3} GHz", 1.0 / self.ns_per_tick)
        } else {
            "std::time::Instant".to_string()
        }
    }

    /// Median cost of an empty start/stop pair, in nanoseconds; included in every sample.
    pub fn overhead_ns(&self) -> f64 {
        let mut samples: Vec<u64> = (0..10_001)
            .map(|_| {
                let s = start();
                stop().wrapping_sub(s)
            })
            .collect();
        samples.sort_unstable();
        self.to_ns(samples[samples.len() / 2])
    }
}

/// Timestamp before the measured work: `lfence` keeps earlier instructions from drifting
/// past it.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
#[allow(unused_unsafe)]
pub fn start() -> u64 {
    use std::arch::x86_64::{_mm_lfence, _rdtsc};
    // SAFETY: both intrinsics only read the time-stamp counter or order instructions;
    // every x86_64 processor has them.
    unsafe {
        _mm_lfence();
        let t = _rdtsc();
        _mm_lfence();
        t
    }
}

/// Timestamp after the measured work: `rdtscp` waits for it to retire, and `lfence` keeps
/// later work out.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
#[allow(unused_unsafe)]
pub fn stop() -> u64 {
    use std::arch::x86_64::{__rdtscp, _mm_lfence};
    let mut aux = 0u32;
    // SAFETY: as in `start`; `aux` is a valid place for the processor id.
    unsafe {
        let t = __rdtscp(&mut aux);
        _mm_lfence();
        t
    }
}

#[cfg(not(target_arch = "x86_64"))]
fn origin() -> Instant {
    static ORIGIN: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    *ORIGIN.get_or_init(Instant::now)
}

/// Fallback for other targets: monotonic nanoseconds.
#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
pub fn start() -> u64 {
    origin().elapsed().as_nanos() as u64
}

/// Fallback for other targets: monotonic nanoseconds.
#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
pub fn stop() -> u64 {
    origin().elapsed().as_nanos() as u64
}

/// The processor brand string from CPUID.
#[cfg(target_arch = "x86_64")]
#[allow(unused_unsafe)]
pub fn cpu_brand() -> String {
    use std::arch::x86_64::__cpuid;
    let mut bytes = Vec::with_capacity(48);
    for leaf in 0x8000_0002u32..=0x8000_0004 {
        // SAFETY: the extended brand-string leaves exist on every x86_64 processor.
        let r = unsafe { __cpuid(leaf) };
        for reg in [r.eax, r.ebx, r.ecx, r.edx] {
            bytes.extend_from_slice(&reg.to_le_bytes());
        }
    }
    String::from_utf8_lossy(&bytes)
        .trim_matches(char::from(0))
        .trim()
        .to_string()
}

/// The processor brand string from CPUID.
#[cfg(not(target_arch = "x86_64"))]
pub fn cpu_brand() -> String {
    std::env::consts::ARCH.to_string()
}

/// On a hybrid Intel processor, the type of the core this thread runs on: CPUID leaf 0x1A
/// says `P-core` or `E-core`. `None` elsewhere.
#[cfg(target_arch = "x86_64")]
#[allow(unused_unsafe)]
pub fn core_type() -> Option<&'static str> {
    use std::arch::x86_64::{__cpuid, __cpuid_count};
    // SAFETY: leaf 0 exists everywhere; the others are read only when leaf 0 says they exist.
    unsafe {
        if __cpuid(0).eax < 0x1A || __cpuid_count(7, 0).edx & (1 << 15) == 0 {
            return None;
        }
        match __cpuid_count(0x1A, 0).eax >> 24 {
            0x40 => Some("P-core"),
            0x20 => Some("E-core"),
            _ => None,
        }
    }
}

/// On a hybrid Intel processor, the type of the core this thread runs on.
#[cfg(not(target_arch = "x86_64"))]
pub fn core_type() -> Option<&'static str> {
    None
}

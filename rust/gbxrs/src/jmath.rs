//! Java's `Math.hypot`, bit for bit: a port of fdlibm `__ieee754_hypot` (`FdLibm.Hypot` in the JDK), the same port as
//! `pyvx/_jmath.py`. Rust's `f64::hypot` is the platform libm's, which rounds differently now and then, so it cannot
//! stand in: the conforming triangulation compares these distances and must make the JVM's choices.

fn hi(x: f64) -> i64 {
    (x.to_bits() >> 32) as i64
}

fn lo(x: f64) -> i64 {
    (x.to_bits() & 0xFFFF_FFFF) as i64
}

fn with_hi(x: f64, h: i64) -> f64 {
    f64::from_bits((((h as u64) & 0xFFFF_FFFF) << 32) | (x.to_bits() & 0xFFFF_FFFF))
}

fn from_hi(h: i64) -> f64 {
    f64::from_bits(((h as u64) & 0xFFFF_FFFF) << 32)
}

/// `sqrt(x*x + y*y)` without undue overflow or underflow, rounded exactly as Java's `Math.hypot`.
pub fn hypot(x: f64, y: f64) -> f64 {
    let mut ha = hi(x) & 0x7FFF_FFFF;
    let mut hb = hi(y) & 0x7FFF_FFFF;
    let (mut a, mut b);
    if hb > ha {
        a = y;
        b = x;
        std::mem::swap(&mut ha, &mut hb);
    } else {
        a = x;
        b = y;
    }
    a = with_hi(a, ha); // |a|
    b = with_hi(b, hb); // |b|
    if (ha - hb) > 0x3C0_0000 {
        // a/b > 2**60
        return a + b;
    }
    let mut k: i64 = 0;
    if ha > 0x5F30_0000 {
        // a > 2**500
        if ha >= 0x7FF0_0000 {
            // Inf or NaN
            let mut w = a + b; // for sNaN
            if ((ha & 0xF_FFFF) | lo(a)) == 0 {
                w = a;
            }
            if ((hb ^ 0x7FF0_0000) | lo(b)) == 0 {
                w = b;
            }
            return w;
        }
        // scale a and b by 2**-600
        ha -= 0x2580_0000;
        hb -= 0x2580_0000;
        k += 600;
        a = with_hi(a, ha);
        b = with_hi(b, hb);
    }
    if hb < 0x20B0_0000 {
        // b < 2**-500
        if hb <= 0x000F_FFFF {
            // subnormal b or 0
            if (hb | lo(b)) == 0 {
                return a;
            }
            let t1 = from_hi(0x7FD0_0000); // 2**1022
            b *= t1;
            a *= t1;
            k -= 1022;
            // fdlibm (and the JDK's port) keep the words read before this scaling: so does this port
        } else {
            // scale a and b by 2**600
            ha += 0x2580_0000;
            hb += 0x2580_0000;
            k -= 600;
            a = with_hi(a, ha);
            b = with_hi(b, hb);
        }
    }
    // medium size a and b
    let mut w = a - b;
    if w > b {
        let t1 = from_hi(ha);
        let t2 = a - t1;
        w = (t1 * t1 - (b * (-b) - t2 * (a + t1))).sqrt();
    } else {
        a = a + a;
        let y1 = from_hi(hb);
        let y2 = b - y1;
        let t1 = from_hi(ha + 0x0010_0000);
        let t2 = a - t1;
        w = (t1 * y1 - (w * (-w) - (t1 * y2 + t2 * b))).sqrt();
    }
    if k != 0 {
        return from_hi(hi(1.0) + (k << 20)) * w;
    }
    w
}

#[cfg(test)]
mod tests {
    use super::hypot;

    /// The JDK 17 vectors the Python port is checked against (`python/geobrix/test/pyvx/data/java_hypot.txt`; a copy
    /// lives in the crate so the check runs on the crate alone): every result bit for bit.
    #[test]
    fn matches_java_math_hypot_bit_for_bit() {
        let text = include_str!("../testdata/java_hypot.txt");
        let (mut n, mut bad) = (0, Vec::new());
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let w: Vec<u64> = line
                .split_whitespace()
                .map(|t| u64::from_str_radix(t, 16).unwrap())
                .collect();
            let (x, y, want) = (f64::from_bits(w[0]), f64::from_bits(w[1]), w[2]);
            let got = hypot(x, y).to_bits();
            n += 1;
            if got != want && !(f64::from_bits(want).is_nan() && f64::from_bits(got).is_nan()) {
                bad.push(format!("{x:e} {y:e}: {got:016x} != {want:016x}"));
            }
        }
        assert!(n > 1000, "{n} vectors");
        assert!(
            bad.is_empty(),
            "{} mismatches: {:?}",
            bad.len(),
            &bad[..bad.len().min(5)]
        );
    }

    #[test]
    fn special_values() {
        assert_eq!(hypot(3.0, 4.0), 5.0);
        assert_eq!(hypot(-3.0, 4.0), 5.0);
        assert_eq!(hypot(0.0, 0.0), 0.0);
        assert_eq!(hypot(f64::INFINITY, f64::NAN), f64::INFINITY);
        assert_eq!(hypot(f64::NAN, f64::NEG_INFINITY), f64::INFINITY);
        assert!(hypot(f64::NAN, 1.0).is_nan());
        assert_eq!(hypot(1e300, 1e300), 1.414_213_562_373_095_2e300);
        assert_eq!(hypot(5e-324, 0.0), 5e-324);
    }
}

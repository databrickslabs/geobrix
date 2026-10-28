use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

const QUADRANTS: [&str; 5] = ["", "SW", "NW", "NE", "SE"];
const LETTER_MAP: [[&str; 8]; 14] = [
    ["SV", "SW", "SX", "SY", "SZ", "TV", "TW", "TX"],
    ["SQ", "SR", "SS", "ST", "SU", "TQ", "TR", "TS"],
    ["SL", "SM", "SN", "SO", "SP", "TL", "TM", "TN"],
    ["SF", "SG", "SH", "SJ", "SK", "TF", "TG", "TH"],
    ["SA", "SB", "SC", "SD", "SE", "TA", "TB", "TC"],
    ["NV", "NW", "NX", "NY", "NZ", "OV", "OW", "OX"],
    ["NQ", "NR", "NS", "NT", "NU", "OQ", "OR", "OS"],
    ["NL", "NM", "NN", "NO", "NP", "OL", "OM", "ON"],
    ["NF", "NG", "NH", "NJ", "NK", "OF", "OG", "OH"],
    ["NA", "NB", "NC", "ND", "NE", "OA", "OB", "OC"],
    ["HV", "HW", "HX", "HY", "HZ", "JV", "JW", "JX"],
    ["HQ", "HR", "HS", "HT", "HU", "JQ", "JR", "JS"],
    ["HL", "HM", "HN", "HO", "HP", "JL", "JM", "JN"],
    ["HF", "HG", "HH", "HJ", "HK", "JF", "JG", "JH"],
];

fn pow10(e: i32) -> i64 {
    10i64.pow(e as u32)
}

/// The int64 arithmetic wraps, as numpy's does: a finite coordinate far outside GB (1e300) gives a
/// meaningless id in a debug build as in a release one, never an overflow panic.
fn encode(el: i64, nl: i64, eb: i64, nb: i64, q: i64, np: i32, res: i32) -> i64 {
    let id = pow10(5 + 2 * np - 2);
    let els = pow10(3 + 2 * np - 2);
    let nls = pow10(1 + 2 * np - 2);
    let base = id.wrapping_add(el.wrapping_mul(els));
    if res == -1 {
        base.div_euclid(100).wrapping_add(q)
    } else {
        base.wrapping_add(nl.wrapping_mul(nls))
            .wrapping_add(eb.wrapping_mul(pow10(np)))
            .wrapping_add(nb.wrapping_mul(10))
            .wrapping_add(q)
    }
}

fn point_to_cell_id(e: f64, n: f64, res: i32) -> i64 {
    let (ei, ni) = (e.trunc() as i64, n.trunc() as i64);
    let (el, nl) = (
        (ei as f64 / 100000.0).trunc() as i64,
        (ni as f64 / 100000.0).trunc() as i64,
    );
    let div = (if res < 0 {
        pow10(6 - res.abs() + 1)
    } else {
        pow10(6 - res)
    }) as f64;
    let q = if res >= -1 {
        0
    } else {
        let (eq, nq) = (ei as f64 / div, ni as f64 / div);
        let (ed, nd) = (eq - eq.floor(), nq - nq.floor());
        if ed < 0.5 && nd < 0.5 {
            1
        } else if ed < 0.5 {
            2
        } else if nd < 0.5 {
            4
        } else {
            3
        }
    };
    let np = if res >= -1 { res.abs() } else { res.abs() - 1 };
    let eb = (ei.rem_euclid(100000) as f64 / div).floor() as i64;
    let nb = (ni.rem_euclid(100000) as f64 / div).floor() as i64;
    encode(el, nl, eb, nb, q, np, res)
}

fn idx(d: &[u8], a: usize, b: usize, max: usize) -> usize {
    let a = a.min(d.len());
    let b = b.min(d.len()).max(a);
    d[a..b]
        .iter()
        .fold(0usize, |v, x| v * 10 + *x as usize)
        .min(max)
}

fn format_cell(id: i64, out: &mut String) {
    let d: Vec<u8> = id
        .unsigned_abs()
        .to_string()
        .bytes()
        .map(|b| b - b'0')
        .collect();
    let (x, y) = (
        idx(&d, 3, 5, LETTER_MAP.len() - 1),
        idx(&d, 1, 3, LETTER_MAP[0].len() - 1),
    );
    if d.len() < 6 {
        out.push_str(&LETTER_MAP[x][y][..1]);
        return;
    }
    let q = (d[d.len() - 1] as usize).min(QUADRANTS.len() - 1);
    out.push_str(LETTER_MAP[x][y]);
    let coords = &d[5..d.len() - 1];
    let k = coords.len() / 2;
    for c in coords[..k].iter().chain(coords[k..2 * k].iter()) {
        out.push((b'0' + c) as char);
    }
    out.push_str(QUADRANTS[q]);
}

/// (data, offsets, validity, null_count): the buffers of an Arrow string array.
type ArrowStrings<'py> = (
    Bound<'py, PyBytes>,
    Bound<'py, PyBytes>,
    Bound<'py, PyBytes>,
    usize,
);

/// Appends the cell of (e, n) and says so. A coordinate that is not a finite number (NaN or an
/// infinity) is NULL on every level (AP-21): nothing is appended and the answer is false.
fn push_cell(e: f64, n: f64, res: i32, out: &mut String) -> bool {
    if !(e.is_finite() && n.is_finite()) {
        return false;
    }
    format_cell(point_to_cell_id(e, n, res), out);
    true
}

/// BNG cell ids for float64 east/north buffers (NaN and infinities = NULL), returned as Arrow string buffers.
#[pyfunction]
fn bng_eastnorth_cells<'py>(
    py: Python<'py>,
    east: &Bound<'py, PyBytes>,
    north: &Bound<'py, PyBytes>,
    resolution: i32,
) -> PyResult<ArrowStrings<'py>> {
    let (e, n) = (east.as_bytes(), north.as_bytes());
    if e.len() != n.len() || e.len() % 8 != 0 {
        return Err(PyValueError::new_err(
            "east/north must be equal-length float64 buffers",
        ));
    }
    if !matches!(resolution, -6..=-1 | 1..=6) {
        return Err(PyValueError::new_err(format!(
            "BNG resolution index must be one of -6..-1 or 1..6; got {resolution}"
        )));
    }
    let len = e.len() / 8;
    let mut data = String::with_capacity(len * 12);
    let mut offsets: Vec<u8> = Vec::with_capacity((len + 1) * 4);
    let mut validity = vec![0u8; len.div_ceil(8)];
    let mut nulls = 0usize;
    offsets.extend_from_slice(&0i32.to_le_bytes());
    for i in 0..len {
        let ev = f64::from_le_bytes(e[i * 8..i * 8 + 8].try_into().unwrap());
        let nv = f64::from_le_bytes(n[i * 8..i * 8 + 8].try_into().unwrap());
        if push_cell(ev, nv, resolution, &mut data) {
            validity[i >> 3] |= 1 << (i & 7);
        } else {
            nulls += 1;
        }
        offsets.extend_from_slice(&(data.len() as i32).to_le_bytes());
    }
    Ok((
        PyBytes::new(py, data.as_bytes()),
        PyBytes::new(py, &offsets),
        PyBytes::new(py, &validity),
        nulls,
    ))
}

#[pymodule]
fn _gbxrs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(bng_eastnorth_cells, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{format_cell, point_to_cell_id, push_cell};

    /// The cases the oracle table does not hold: the 500km special case, quadrants, the 1 m
    /// corner and out-of-GB coordinates (floor-mod). Expected strings are pygx/_bng.py's.
    #[test]
    fn edge_cases_match_the_light_tier() {
        let cases = [
            (530000.0, 180000.0, -1, "T"),
            (530000.0, 180000.0, -3, "TQ38SW"),
            (325000.0, 673000.0, -2, "NTNW"),
            (0.0, 0.0, -6, "SV00000000SW"),
            (699999.9, 1299999.9, 6, "JM9999999999"),
            (700000.0, 1300000.0, 1, "JH"),
            (-1500.5, -250.7, 3, "SV9899"),
            (-1500.5, -250.7, -3, "SV99NE"),
        ];
        for (e, n, res, want) in cases {
            let mut got = String::new();
            format_cell(point_to_cell_id(e, n, res), &mut got);
            assert_eq!(got, want, "({e}, {n}) at {res}");
        }
    }

    /// The light tier does int64 arithmetic that wraps, so a finite coordinate far outside GB gives a
    /// cell id (garbage in, garbage out) and never a panic: a debug build must agree with release.
    #[test]
    fn huge_finite_coordinates_wrap_instead_of_panicking() {
        let values = [1e300, -1e300, 9.3e18, 5e12, -1e13];
        for res in [-6, -5, -4, -3, -2, -1, 1, 2, 3, 4, 5, 6] {
            for e in values {
                for n in [180000.0, 1e300, -1e13] {
                    let mut cell = String::new();
                    assert!(push_cell(e, n, res, &mut cell), "({e}, {n}) at {res}");
                    assert!(!cell.is_empty(), "({e}, {n}) at {res}");
                }
            }
        }
    }

    /// NaN and the infinities are NULL (AP-21): no cell is written, whichever coordinate they are in.
    #[test]
    fn a_coordinate_that_is_not_finite_is_null() {
        let bad = [f64::NAN, f64::INFINITY, f64::NEG_INFINITY];
        for res in [-6, -1, 3, 6] {
            for x in bad {
                for (e, n) in [(x, 180000.0), (530000.0, x), (x, x)] {
                    let mut cell = String::new();
                    assert!(!push_cell(e, n, res, &mut cell), "({e}, {n}) at {res}");
                    assert!(cell.is_empty());
                }
            }
        }
        let mut cell = String::new();
        assert!(push_cell(530000.0, 180000.0, 3, &mut cell) && cell == "TQ3080");
    }
}

//! The bounds check of a window cut from a raster (`decode_window`): arithmetic that cannot overflow.

/// True when the window `[col, row, w, h]` lies inside a `width` x `height` raster: no negative offset, a positive size,
/// and each far edge (`col + w`, `row + h`) within the raster. The sums are checked: a window comes from data, and an
/// offset and size near `i64::MAX` would wrap in a release build and pass a plain comparison.
#[cfg_attr(not(feature = "gdal"), allow(dead_code))] // only the `gdal` decode calls it
pub fn inside(window: [i64; 4], width: i64, height: i64) -> bool {
    let [col, row, w, h] = window;
    col >= 0
        && row >= 0
        && w > 0
        && h > 0
        && col.checked_add(w).is_some_and(|edge| edge <= width)
        && row.checked_add(h).is_some_and(|edge| edge <= height)
}

#[cfg(test)]
mod tests {
    use super::inside;

    #[test]
    fn a_window_is_inside_or_it_is_not() {
        assert!(inside([0, 0, 16, 16], 16, 16));
        assert!(inside([8, 8, 8, 8], 16, 16));
        for bad in [
            [8, 8, 9, 8],        // past the right edge
            [0, 8, 16, 9],       // past the bottom edge
            [-1, 0, 4, 4],       // negative offset
            [0, 0, 0, 4],        // empty
            [0, 0, 4, -1],       // negative size
            [i64::MAX, 0, 1, 1], // the sum would overflow
            [1, 0, i64::MAX, 1], // the sum would overflow
        ] {
            assert!(!inside(bad, 16, 16), "{bad:?}");
        }
    }
}

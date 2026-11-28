// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The prometheus-scrape-parser-rs Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Byte-search helper functions.

const CHUNK: usize = 16;
type Bitmap = u16;

/// Index of the first `needle` byte, scanning full chunks at a time.
#[inline]
pub fn find_byte(needle: u8, haystack: &[u8]) -> Option<usize> {
    let (chunks, rest) = haystack.as_chunks::<CHUNK>();
    for (i, chunk) in chunks.iter().enumerate() {
        let mask = chunk_mask(chunk, needle);
        if mask != 0 {
            return Some(i * CHUNK + mask.trailing_zeros() as usize);
        }
    }
    rest.iter()
        .position(|&b| b == needle)
        .map(|i| chunks.len() * CHUNK + i)
}

/// Index of the first byte equal to either needle, chunked like
/// [`find_byte`].
#[inline]
pub fn find_byte2(n1: u8, n2: u8, haystack: &[u8]) -> Option<usize> {
    let (chunks, rest) = haystack.as_chunks::<CHUNK>();
    for (i, chunk) in chunks.iter().enumerate() {
        let mask = chunk_mask2(chunk, n1, n2);
        if mask != 0 {
            return Some(i * CHUNK + mask.trailing_zeros() as usize);
        }
    }
    rest.iter()
        .position(|&b| b == n1 || b == n2)
        .map(|i| chunks.len() * CHUNK + i)
}

/// Index of the first byte outside the plain (unquoted) name class
/// `[a-zA-Z0-9_]` plus `:` when `colon`; a `"` stops it as well.
#[inline]
pub fn find_non_name_byte(colon: bool, haystack: &[u8]) -> usize {
    let (chunks, rest) = haystack.as_chunks::<CHUNK>();
    for (i, chunk) in chunks.iter().enumerate() {
        let mask = name_mask(chunk, colon);
        if mask != 0 {
            return i * CHUNK + mask.trailing_zeros() as usize;
        }
    }
    rest.iter()
        .position(|&b| !is_plain_name_byte(b, colon))
        .map(|i| chunks.len() * CHUNK + i)
        .unwrap_or(haystack.len())
}

/// Hit bitmap for one chunk: bit i is set when `chunk[i] == needle`.
#[inline]
fn chunk_mask(chunk: &[u8; CHUNK], needle: u8) -> Bitmap {
    let mut bitmap = 0;
    for (i, &b) in chunk.iter().enumerate() {
        bitmap |= ((b == needle) as Bitmap) << i;
    }
    bitmap
}

/// Hit bitmap for either needle: bit i is set when `chunk[i]` equals
/// `n1` or `n2`.
#[inline]
fn chunk_mask2(chunk: &[u8; CHUNK], n1: u8, n2: u8) -> Bitmap {
    let mut bitmap  = 0;
    for (i, &b) in chunk.iter().enumerate() {
        bitmap |= (((b == n1) | (b == n2)) as Bitmap) << i;
    }
    bitmap
}

#[inline]
fn is_plain_name_byte(b: u8, colon: bool) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || (colon && b == b':')
}

/// Bitmap of the positions holding a byte outside the plain name class.
#[inline]
fn name_mask(chunk: &[u8; CHUNK], colon: bool) -> Bitmap {
    let mut bitmap  = 0;
    for (i, &b) in chunk.iter().enumerate() {
        bitmap |= ((!is_plain_name_byte(b, colon)) as Bitmap) << i;
    }
    bitmap
}

#[cfg(test)]
mod tests {
    use super::{find_byte, find_byte2, find_non_name_byte};

    #[test]
    fn find_byte_hits_at_every_offset_across_chunk_boundaries() {
        for len in 0..=33 {
            let plain = vec![b'x'; len];
            for offset in 0..len {
                let mut hit = plain.clone();
                hit[offset] = b'#';
                assert_eq!(
                    find_byte(b'#', &hit),
                    Some(offset),
                    "len {len} offset {offset}"
                );
            }
        }
    }

    #[test]
    fn find_byte_returns_leftmost_hit() {
        assert_eq!(find_byte(b'#', b"xx#xx#xx"), Some(2));
    }

    #[test]
    fn find_byte_misses_when_needle_absent() {
        for len in [0, 1, 7, 8, 15, 16, 17, 31, 32, 33] {
            assert_eq!(find_byte(b'#', &vec![b'x'; len]), None, "len {len}");
        }
    }

    #[test]
    fn find_byte2_hits_at_every_offset_for_either_needle() {
        for len in 0..=33 {
            let plain = vec![b'x'; len];
            for offset in 0..len {
                for needle in *b" \t" {
                    let mut hit = plain.clone();
                    hit[offset] = needle;
                    assert_eq!(
                        find_byte2(b' ', b'\t', &hit),
                        Some(offset),
                        "len {len} offset {offset} needle {}",
                        needle as char
                    );
                }
            }
        }
    }

    #[test]
    fn find_byte2_returns_leftmost_hit_either_needle() {
        assert_eq!(find_byte2(b'#', b'*', b"xx*xx#xx"), Some(2));
        assert_eq!(find_byte2(b'#', b'*', b"xx#xx*xx"), Some(2));
        // Adjacent needles across the chunk boundary (offsets 15 and 16).
        assert_eq!(find_byte2(b'#', b'*', b"xxxxxxxxxxxxxxx#*"), Some(15));
    }

    #[test]
    fn find_byte2_misses_when_both_absent() {
        for len in [0, 1, 7, 8, 15, 16, 17, 31, 32, 33] {
            assert_eq!(find_byte2(b' ', b'\t', &vec![b'x'; len]), None, "len {len}");
        }
    }

    #[test]
    fn find_non_name_byte_hits_at_every_offset_across_chunk_boundaries() {
        for len in 0..=33 {
            let plain = vec![b'x'; len];
            for offset in 0..len {
                let mut hit = plain.clone();
                hit[offset] = b'#';
                assert_eq!(
                    find_non_name_byte(true, &hit),
                    offset,
                    "len {len} offset {offset}"
                );
            }
        }
    }

    #[test]
    fn find_non_name_byte_treats_colon_by_the_flag() {
        assert_eq!(find_non_name_byte(true, b"m:"), 2);
        assert_eq!(find_non_name_byte(false, b"a:"), 1);
    }

    #[test]
    fn find_non_name_byte_returns_leftmost_hit() {
        assert_eq!(find_non_name_byte(true, b"xx#xx#xx"), 2);
    }

    #[test]
    fn find_non_name_byte_scans_to_end_when_all_plain() {
        for len in [0, 1, 15, 16, 17, 31, 32, 33] {
            assert_eq!(find_non_name_byte(true, &vec![b'x'; len]), len, "len {len}");
        }
    }

    #[test]
    fn find_non_name_byte_hits_at_the_first_byte() {
        assert_eq!(find_non_name_byte(true, b"#x"), 0);
    }
}

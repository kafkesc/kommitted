use chrono::{DateTime, Utc};
use std::cmp::Ordering;

/// An Offset in a Topic Partition, and the date-time at which it is known.
///
/// This is used to represent concepts like
/// "the timestamp at which a Topic Partition offset was produced".
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct KnownOffset {
    pub offset: u64,
    pub at: DateTime<Utc>,
}

/// Result of a call to [`search`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum KnownOffsetSearchRes {
    /// Offset found, it was part of the search input.
    Exact(KnownOffset),

    /// Offset not found, but 2 [`KnownOffset`] that contain it were found.
    Range(KnownOffset, KnownOffset),

    /// Offset not found.
    None,
}

/// Search an offset (`needle`) inside an slice of [`KnownOffset`] (`haystack`).
///
/// If found, returns a [`KnownOffsetSearchRes::Exact`]. Alternatively, if the offset is
/// _not_ found _but_ the offsets in the haystack contain it, return the 2 closest
/// [`KnownOffset`] in a [`KnownOffsetSearchRes::Range`].
/// Otherwise, it returns [`KnownOffsetSearchRes::None`].
pub fn search(needle: u64, haystack: &[KnownOffset]) -> KnownOffsetSearchRes {
    // Base case: empty
    if haystack.is_empty() {
        return KnownOffsetSearchRes::None;
    }

    // Base case: single element
    if haystack.len() == 1 {
        return if haystack[0].offset == needle {
            // `haystack` contains 1 element, and it is `needle`
            KnownOffsetSearchRes::Exact(haystack[0].clone())
        } else {
            KnownOffsetSearchRes::None
        };
    }

    // Base case: 2 elements
    if haystack.len() == 2 {
        return if haystack[0].offset == needle {
            KnownOffsetSearchRes::Exact(haystack[0].clone())
        } else if haystack[1].offset == needle {
            KnownOffsetSearchRes::Exact(haystack[1].clone())
        } else if haystack[0].offset < needle && needle < haystack[1].offset {
            KnownOffsetSearchRes::Range(
                haystack[0].clone(),
                haystack[1].clone(),
            )
        } else {
            KnownOffsetSearchRes::None
        };
    }

    // Select left `l`, right `r` and pivot `p`: boundaries of the search
    let l: usize = 0;
    let r: usize = haystack.len() - 1;
    let p = (r - l) / 2;

    match needle.cmp(&haystack[p].offset) {
        Ordering::Equal => {
            // Found `needle`
            KnownOffsetSearchRes::Exact(haystack[p].clone())
        },
        Ordering::Less => {
            if haystack[p - 1].offset < needle {
                // `needle` not found: return the approximate range `[p-1, p]`
                KnownOffsetSearchRes::Range(
                    haystack[p - 1].clone(),
                    haystack[p].clone(),
                )
            } else {
                // Keep searching to the left of pivot
                search(needle, &haystack[0..=p - 1])
            }
        },
        Ordering::Greater => {
            if needle < haystack[p + 1].offset {
                // `needle` not found: return the approximate range `[p, p+1]`
                KnownOffsetSearchRes::Range(
                    haystack[p].clone(),
                    haystack[p + 1].clone(),
                )
            } else {
                // Keep searching to the right of pivot
                search(needle, &haystack[p + 1..=r])
            }
        },
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::{FixedOffset, Utc};
    use std::ops::Add;

    fn build_even_input() -> Vec<KnownOffset> {
        vec![
            KnownOffset {
                offset: 1,
                at: Utc::now().add(FixedOffset::east_opt(0).unwrap()),
            },
            KnownOffset {
                offset: 3,
                at: Utc::now().add(FixedOffset::east_opt(100).unwrap()),
            },
            KnownOffset {
                offset: 7,
                at: Utc::now().add(FixedOffset::east_opt(500).unwrap()),
            },
            KnownOffset {
                offset: 10,
                at: Utc::now().add(FixedOffset::east_opt(700).unwrap()),
            },
            KnownOffset {
                offset: 17,
                at: Utc::now().add(FixedOffset::east_opt(1200).unwrap()),
            },
            KnownOffset {
                offset: 27,
                at: Utc::now().add(FixedOffset::east_opt(2100).unwrap()),
            },
            KnownOffset {
                offset: 44,
                at: Utc::now().add(FixedOffset::east_opt(3900).unwrap()),
            },
            KnownOffset {
                offset: 45,
                at: Utc::now().add(FixedOffset::east_opt(3950).unwrap()),
            },
            KnownOffset {
                offset: 89,
                at: Utc::now().add(FixedOffset::east_opt(6000).unwrap()),
            },
            KnownOffset {
                offset: 123,
                at: Utc::now().add(FixedOffset::east_opt(11111).unwrap()),
            },
        ]
    }

    fn build_odd_input() -> Vec<KnownOffset> {
        let res = build_even_input();
        res[0..res.len() - 1].to_vec()
    }

    #[test]
    fn search_even_input() {
        let input = build_even_input();

        assert!(matches!(search(0, &input), KnownOffsetSearchRes::None));

        assert!(matches!(
            search(1, &input),
            KnownOffsetSearchRes::Exact(KnownOffset {
                offset: 1,
                ..
            })
        ));

        assert!(matches!(
            search(11, &input),
            KnownOffsetSearchRes::Range(
                KnownOffset {
                    offset: 10,
                    ..
                },
                KnownOffset {
                    offset: 17,
                    ..
                }
            )
        ));

        assert!(matches!(
            search(45, &input),
            KnownOffsetSearchRes::Exact(KnownOffset {
                offset: 45,
                ..
            })
        ));

        assert!(matches!(
            search(70, &input),
            KnownOffsetSearchRes::Range(
                KnownOffset {
                    offset: 45,
                    ..
                },
                KnownOffset {
                    offset: 89,
                    ..
                }
            )
        ));

        assert!(matches!(
            search(123, &input),
            KnownOffsetSearchRes::Exact(KnownOffset {
                offset: 123,
                ..
            })
        ));

        assert!(matches!(search(124, &input), KnownOffsetSearchRes::None));
        assert!(matches!(search(200, &input), KnownOffsetSearchRes::None));
        assert!(matches!(search(400, &input), KnownOffsetSearchRes::None));
    }

    #[test]
    fn search_odd_input() {
        let input = build_odd_input();

        assert!(matches!(search(0, &input), KnownOffsetSearchRes::None));

        assert!(matches!(
            search(1, &input),
            KnownOffsetSearchRes::Exact(KnownOffset {
                offset: 1,
                ..
            })
        ));

        assert!(matches!(
            search(11, &input),
            KnownOffsetSearchRes::Range(
                KnownOffset {
                    offset: 10,
                    ..
                },
                KnownOffset {
                    offset: 17,
                    ..
                }
            )
        ));

        assert!(matches!(
            search(45, &input),
            KnownOffsetSearchRes::Exact(KnownOffset {
                offset: 45,
                ..
            })
        ));

        assert!(matches!(
            search(70, &input),
            KnownOffsetSearchRes::Range(
                KnownOffset {
                    offset: 45,
                    ..
                },
                KnownOffset {
                    offset: 89,
                    ..
                }
            )
        ));

        assert!(matches!(
            search(89, &input),
            KnownOffsetSearchRes::Exact(KnownOffset {
                offset: 89,
                ..
            })
        ));

        assert!(matches!(search(123, &input), KnownOffsetSearchRes::None));
        assert!(matches!(search(200, &input), KnownOffsetSearchRes::None));
        assert!(matches!(search(400, &input), KnownOffsetSearchRes::None));
    }
}

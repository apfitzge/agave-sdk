//! Producer-local FIFO allocation of byte ranges in a fixed shared-memory region.
//!
//! This module owns no memory mapping and exposes no payload references. Handles
//! are relative to the region, byte-aligned, and never straddle its physical end.
//! The integration layer owns copying, publication, and proof that a committed
//! allocation is no longer readable before reclaiming through its logical end.

pub(super) use crate::payload::PayloadHandle;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ReserveError {
    TooLarge,
    Full,
    PositionExhausted,
}

#[derive(Debug)]
pub(super) struct PayloadRing {
    capacity: u64,
    head: u64,
    tail: u64,
}

impl PayloadRing {
    /// Capacity must be nonzero. All allocator state is producer-local.
    pub(super) fn new(capacity: u64) -> Option<Self> {
        (capacity != 0).then_some(Self {
            capacity,
            head: 0,
            tail: 0,
        })
    }

    /// Reserve a contiguous range without changing committed state. A mutable
    /// borrow permits only one outstanding reservation. Dropping it, including
    /// during unwinding, cancels it without consuming bytes or wrap padding.
    pub(super) fn reserve(&mut self, len: u64) -> Result<Reservation<'_>, ReserveError> {
        if len > self.capacity {
            return Err(ReserveError::TooLarge);
        }
        let offset = self.head.checked_rem(self.capacity).unwrap();
        let remaining = self.capacity.checked_sub(offset).unwrap();
        let padding = if len > remaining { remaining } else { 0 };
        let start = self
            .head
            .checked_add(padding)
            .ok_or(ReserveError::PositionExhausted)?;
        let end = start
            .checked_add(len)
            .ok_or(ReserveError::PositionExhausted)?;
        // An empty ring has no readers: skip otherwise unusable wrap padding.
        // Commit the adjustment only with the reservation, preserving rollback.
        let tail = if self.head == self.tail {
            start
        } else {
            self.tail
        };
        if end.checked_sub(tail).unwrap() > self.capacity {
            return Err(ReserveError::Full);
        }
        let handle = PayloadHandle {
            offset: if len == 0 {
                0
            } else {
                start.checked_rem(self.capacity).unwrap()
            },
            len,
        };
        Ok(Reservation {
            ring: self,
            handle,
            end,
            tail,
        })
    }

    /// Reclaim a FIFO prefix ending at a previously committed logical end from
    /// THIS ring. The caller must prove every allocation in that prefix is no
    /// longer accessible (e.g. via its lane's broadcast reclamation watermark).
    ///
    /// Bounds are checked, but allocation boundaries and reader liveness belong
    /// to the integration layer. Invalid/stale positions leave state unchanged.
    pub(super) fn reclaim_through(&mut self, end: u64) -> bool {
        if end < self.tail || end > self.head {
            return false;
        }
        self.tail = end;
        true
    }
}

#[must_use = "commit the reservation or drop it to cancel"]
pub(super) struct Reservation<'a> {
    ring: &'a mut PayloadRing,
    handle: PayloadHandle,
    end: u64,
    tail: u64,
}

impl Reservation<'_> {
    pub(super) fn handle(&self) -> PayloadHandle {
        self.handle
    }

    /// Infallibly commit and return the logical end to retain in FIFO bookkeeping.
    /// Publication must not leave a committed allocation without its record.
    pub(super) fn commit(self) -> u64 {
        self.ring.head = self.end;
        self.ring.tail = self.tail;
        self.end
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allocate(ring: &mut PayloadRing, len: u64) -> (PayloadHandle, u64) {
        let reservation = ring.reserve(len).unwrap();
        (reservation.handle(), reservation.commit())
    }

    #[test]
    fn exact_fit_and_fifo_reclamation() {
        assert!(PayloadRing::new(0).is_none());
        let mut ring = PayloadRing::new(8).unwrap();
        let (_, first) = allocate(&mut ring, 3);
        let (_, second) = allocate(&mut ring, 5);
        assert_eq!(ring.reserve(1).err(), Some(ReserveError::Full));
        assert!(ring.reclaim_through(first));
        assert_eq!(
            allocate(&mut ring, 3).0,
            PayloadHandle { offset: 0, len: 3 }
        );
        assert_eq!(ring.reserve(1).err(), Some(ReserveError::Full));
        assert!(ring.reclaim_through(second));
        assert!(!ring.reclaim_through(first));
        assert!(!ring.reclaim_through(12));
        assert_eq!(allocate(&mut ring, 5).0.offset, 3);
    }

    #[test]
    fn wrap_padding_counts_until_its_allocation_is_reclaimed() {
        let mut ring = PayloadRing::new(10).unwrap();
        let (_, first) = allocate(&mut ring, 6);
        let (_, second) = allocate(&mut ring, 2);
        assert!(ring.reclaim_through(first));
        let (wrapped, third) = allocate(&mut ring, 5);
        assert_eq!(wrapped, PayloadHandle { offset: 0, len: 5 });
        assert_eq!(third, 15); // Includes the two-byte padding at offsets 8..10.
        assert_eq!(ring.reserve(2).err(), Some(ReserveError::Full));
        assert!(ring.reclaim_through(second));
        allocate(&mut ring, 3);
        assert_eq!(ring.reserve(1).err(), Some(ReserveError::Full));
        assert!(ring.reclaim_through(third));
        assert_eq!(allocate(&mut ring, 5).0.offset, 0);
    }

    #[test]
    fn empty_ring_can_allocate_its_entire_capacity_after_wrap() {
        let mut ring = PayloadRing::new(10).unwrap();
        let (_, end) = allocate(&mut ring, 7);
        assert!(ring.reclaim_through(end));
        {
            let reservation = ring.reserve(10).unwrap();
            assert_eq!(reservation.handle().offset, 0);
        }
        assert_eq!((ring.head, ring.tail), (7, 7));
        assert_eq!(
            allocate(&mut ring, 10),
            (PayloadHandle { offset: 0, len: 10 }, 20)
        );
        assert_eq!(ring.reserve(1).err(), Some(ReserveError::Full));
    }

    #[test]
    fn panic_cancels_wrapping_reservation() {
        let mut ring = PayloadRing::new(10).unwrap();
        let (_, first) = allocate(&mut ring, 6);
        allocate(&mut ring, 2);
        ring.reclaim_through(first);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let reservation = ring.reserve(5).unwrap();
            assert_eq!(reservation.handle().offset, 0);
            panic!("payload copy failed");
        }));
        assert!(result.is_err());
        assert_eq!((ring.head, ring.tail), (8, 6));
        assert_eq!(allocate(&mut ring, 2).0.offset, 8);
    }

    #[test]
    fn empty_payload_is_canonical_and_consumes_no_space() {
        let mut ring = PayloadRing::new(1).unwrap();
        allocate(&mut ring, 1);
        assert_eq!(
            allocate(&mut ring, 0),
            (PayloadHandle { offset: 0, len: 0 }, 1)
        );
        assert_eq!((ring.head, ring.tail), (1, 0));
    }

    #[test]
    fn oversized_and_exhausted_positions_do_not_change_state() {
        let mut ring = PayloadRing::new(10).unwrap();
        assert_eq!(ring.reserve(11).err(), Some(ReserveError::TooLarge));
        assert_eq!((ring.head, ring.tail), (0, 0));
        ring.head = u64::MAX;
        ring.tail = u64::MAX;
        assert_eq!(ring.reserve(1).err(), Some(ReserveError::PositionExhausted));
        assert_eq!(
            ring.reserve(10).err(),
            Some(ReserveError::PositionExhausted)
        );
        assert_eq!((ring.head, ring.tail), (u64::MAX, u64::MAX));
    }

    #[test]
    fn ten_mebibytes_and_independent_rings() {
        const SIZE: u64 = 10 * 1024 * 1024;
        let mut first = PayloadRing::new(SIZE).unwrap();
        let mut second = PayloadRing::new(SIZE).unwrap();
        assert_eq!(allocate(&mut first, SIZE).0.len, SIZE);
        assert_eq!(first.reserve(1).err(), Some(ReserveError::Full));
        assert_eq!(allocate(&mut second, SIZE).0.len, SIZE);
    }
}

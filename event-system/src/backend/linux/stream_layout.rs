//! The enclosing stream file format, independent of shaq's queue layout.
//!
//! Version 2 begins with sixteen little-endian u64 words (128 bytes):
//! magic, format version, header length, file length, identifier, queue offset,
//! queue length, shaq format version, queue ABI, payload offset, payload capacity
//! per lane, payload stride, payload lane count, and three reserved zero words.
//! The queue ABI records pointer width in the low 32 bits and native byte order
//! (0 = little, 1 = big) in the high 32 bits: shaq's shared layout is native.
//!
//! The queue and each payload lane start on page boundaries. Payload descriptors
//! are all zero when disabled. Stride includes page padding; capacity does not.
//! Payload bytes follow the queue in the same file. Older formats are rejected.
//!
//! The header is immutable after creation. Initialize the queue first, write
//! this header, seal the file against resizing, then publish the stream directory.
//! Directory publication prevents subscribers from observing partial setup; no
//! concurrently updated readiness word is needed in this header.

use std::{alloc::Layout, fs::File, io, os::unix::fs::FileExt};

const MAGIC: u64 = u64::from_le_bytes(*b"agaveevt");
const VERSION: u64 = 2;
const HEADER_LEN: usize = 128;
const QUEUE_ABI: u64 = (usize::BITS as u64) | ((cfg!(target_endian = "big") as u64) << 32);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct StreamLayout {
    pub(super) file_len: u64,
    pub(super) identifier: u64,
    pub(super) queue_offset: u64,
    pub(super) queue_len: u64,
    pub(super) payload_offset: u64,
    pub(super) payload_capacity: u64,
    pub(super) payload_stride: u64,
    pub(super) payload_lanes: u64,
}

impl StreamLayout {
    pub(super) fn new(queue: Layout, identifier: u64) -> io::Result<Self> {
        let alignment = page_size()?.max(queue.align());
        let queue_offset = HEADER_LEN
            .checked_next_multiple_of(alignment)
            .ok_or_else(|| invalid_layout("stream queue offset overflow"))?;
        let file_len = queue_offset
            .checked_add(queue.size())
            .filter(|&len| len <= isize::MAX as usize)
            .ok_or_else(|| invalid_layout("stream file length overflow"))?;
        Ok(Self {
            file_len: file_len as u64,
            identifier,
            queue_offset: queue_offset as u64,
            queue_len: queue.size() as u64,
            payload_offset: 0,
            payload_capacity: 0,
            payload_stride: 0,
            payload_lanes: 0,
        })
    }

    /// Reserve optional payload storage. A zero capacity keeps the queue-only
    /// extent; otherwise each producer lane gets exactly `capacity` usable bytes.
    pub(super) fn with_payloads(mut self, capacity: u64, lanes: usize) -> io::Result<Self> {
        self.file_len = self
            .queue_offset
            .checked_add(self.queue_len)
            .ok_or_else(|| invalid_layout("queue extent overflow"))?;
        self.payload_offset = 0;
        self.payload_capacity = 0;
        self.payload_stride = 0;
        self.payload_lanes = 0;
        if capacity == 0 {
            return Ok(self);
        }
        if lanes == 0 {
            return Err(invalid_layout("payload storage requires producer lanes"));
        }
        let page = page_size()? as u64;
        self.payload_offset = self
            .file_len
            .checked_next_multiple_of(page)
            .ok_or_else(|| invalid_layout("payload offset overflow"))?;
        self.payload_capacity = capacity;
        self.payload_stride = capacity
            .checked_next_multiple_of(page)
            .ok_or_else(|| invalid_layout("payload stride overflow"))?;
        self.payload_lanes = lanes as u64;
        self.file_len = self
            .payload_stride
            .checked_mul(self.payload_lanes)
            .and_then(|len| self.payload_offset.checked_add(len))
            .filter(|&len| len <= isize::MAX as u64)
            .ok_or_else(|| invalid_layout("payload extent overflow"))?;
        Ok(self)
    }

    /// File offset and usable byte length for one lane. Mapping/copying follows
    /// in the publication integration; padding is never part of an allocation.
    #[allow(dead_code)]
    pub(super) fn payload_region(&self, lane: usize) -> Option<(u64, u64)> {
        let lane = u64::try_from(lane).ok()?;
        if lane >= self.payload_lanes {
            return None;
        }
        let offset = lane
            .checked_mul(self.payload_stride)?
            .checked_add(self.payload_offset)?;
        Some((offset, self.payload_capacity))
    }

    /// Writes the header after the enclosing file has been sized and its queue
    /// initialized, but before the stream directory is published.
    pub(super) fn write(&self, file: &File) -> io::Result<()> {
        let mut words = [0u64; HEADER_LEN / 8];
        words[..13].copy_from_slice(&[
            MAGIC,
            VERSION,
            HEADER_LEN as u64,
            self.file_len,
            self.identifier,
            self.queue_offset,
            self.queue_len,
            u64::from(shaq::VERSION),
            QUEUE_ABI,
            self.payload_offset,
            self.payload_capacity,
            self.payload_stride,
            self.payload_lanes,
        ]);
        let mut bytes = [0u8; HEADER_LEN];
        for (chunk, word) in bytes.as_chunks_mut::<8>().0.iter_mut().zip(words) {
            chunk.copy_from_slice(&word.to_le_bytes());
        }
        file.write_all_at(&bytes, 0)
    }

    /// Reads and validates the outer layout before the queue is mapped. Callers
    /// must first verify the file is sealed against resizing.
    pub(super) fn read(file: &File) -> io::Result<Self> {
        let file_len = file.metadata()?.len();
        if file_len < HEADER_LEN as u64 || file_len > isize::MAX as u64 {
            return Err(invalid_layout("invalid stream file length"));
        }
        let mut bytes = [0u8; HEADER_LEN];
        file.read_exact_at(&mut bytes, 0)?;
        let mut words = [0u64; HEADER_LEN / 8];
        for (word, chunk) in words.iter_mut().zip(bytes.as_chunks::<8>().0) {
            *word = u64::from_le_bytes(*chunk);
        }
        if words[0] != MAGIC {
            return Err(invalid_layout("invalid stream magic"));
        }
        if words[1] != VERSION || words[2] != HEADER_LEN as u64 {
            return Err(invalid_layout(
                "unsupported stream header version or length",
            ));
        }
        if words[3] != file_len {
            return Err(invalid_layout("stream file length mismatch"));
        }
        if words[7] != u64::from(shaq::VERSION) || words[8] != QUEUE_ABI {
            return Err(invalid_layout("unsupported broadcast version or ABI"));
        }
        if words[13..].iter().any(|&word| word != 0) {
            return Err(invalid_layout("unsupported stream header extensions"));
        }
        let layout = Self {
            file_len,
            identifier: words[4],
            queue_offset: words[5],
            queue_len: words[6],
            payload_offset: words[9],
            payload_capacity: words[10],
            payload_stride: words[11],
            payload_lanes: words[12],
        };
        if layout.queue_offset < HEADER_LEN as u64
            || !layout.queue_offset.is_multiple_of(page_size()? as u64)
            || layout.queue_len == 0
            || layout
                .queue_offset
                .checked_add(layout.queue_len)
                .is_none_or(|end| end > file_len)
        {
            return Err(invalid_layout("invalid broadcast region"));
        }
        let queue_end = layout.queue_offset.checked_add(layout.queue_len).unwrap();
        if layout.payload_capacity == 0 {
            if words[9..13].iter().any(|&word| word != 0) {
                return Err(invalid_layout("invalid disabled payload region"));
            }
        } else {
            let page = page_size()? as u64;
            if layout.payload_lanes == 0
                || !layout.payload_offset.is_multiple_of(page)
                || layout.payload_offset < queue_end
                || layout.payload_capacity.checked_next_multiple_of(page)
                    != Some(layout.payload_stride)
                || layout
                    .payload_stride
                    .checked_mul(layout.payload_lanes)
                    .and_then(|len| layout.payload_offset.checked_add(len))
                    != Some(file_len)
            {
                return Err(invalid_layout("invalid payload region"));
            }
        }
        Ok(layout)
    }
}

fn page_size() -> io::Result<usize> {
    // SAFETY: sysconf accepts _SC_PAGESIZE and does not access caller memory.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    usize::try_from(page_size)
        .ok()
        .filter(|size| size.is_power_of_two())
        .ok_or_else(|| io::Error::other("failed to determine page size"))
}

fn invalid_layout(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use {super::*, rstest::rstest};

    fn file() -> File {
        let file = tempfile::tempfile().unwrap();
        let queue = Layout::from_size_align(256, 64).unwrap();
        let layout = StreamLayout::new(queue, 42).unwrap();
        file.set_len(layout.file_len).unwrap();
        layout.write(&file).unwrap();
        file
    }

    #[test]
    fn header_roundtrip_and_encoding() {
        let file = file();
        let layout = StreamLayout::read(&file).unwrap();
        assert_eq!(layout.identifier, 42);
        assert_eq!(layout.queue_len, 256);
        assert_eq!(layout.queue_offset, page_size().unwrap() as u64);
        let mut bytes = [0; 16];
        file.read_exact_at(&mut bytes, 0).unwrap();
        assert_eq!(&bytes[..8], b"agaveevt");
        assert_eq!(&bytes[8..], &2u64.to_le_bytes());
    }

    #[rstest]
    #[case::legacy_magic(0, u64::from_be_bytes(*b"shaqcast"))]
    #[case::old_version(1, 1)]
    #[case::future_version(1, 3)]
    #[case::header_len(2, 0)]
    #[case::file_len(3, u64::MAX)]
    #[case::queue_overlaps_header(5, 0)]
    #[case::queue_misaligned(5, 129)]
    #[case::queue_outside_file(5, 1 << 60)]
    #[case::empty_queue(6, 0)]
    #[case::queue_end_overflow(6, u64::MAX)]
    #[case::queue_outside_extent(6, 1 << 60)]
    #[case::queue_version(7, u64::MAX)]
    #[case::queue_abi(8, u64::MAX)]
    #[case::disabled_offset(9, 1)]
    #[case::disabled_capacity(10, 1)]
    #[case::disabled_stride(11, 4096)]
    #[case::disabled_lanes(12, 1)]
    #[case::reserved(13, 1)]
    fn rejects_invalid_header(#[case] word: u64, #[case] value: u64) {
        let file = file();
        file.write_all_at(&value.to_le_bytes(), word.checked_mul(8).unwrap())
            .unwrap();
        assert_eq!(
            StreamLayout::read(&file).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[rstest]
    fn rejects_truncated_header(#[values(0, 8, 127)] len: u64) {
        let file = file();
        file.set_len(len).unwrap();
        assert_eq!(
            StreamLayout::read(&file).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn rejects_total_size_overflow() {
        let queue = Layout::from_size_align(isize::MAX as usize, 1).unwrap();
        assert!(StreamLayout::new(queue, 0).is_err());
    }
    fn payload_file() -> (File, StreamLayout) {
        let file = tempfile::tempfile().unwrap();
        let layout = StreamLayout::new(Layout::from_size_align(256, 64).unwrap(), 42)
            .unwrap()
            .with_payloads(5001, 3)
            .unwrap();
        file.set_len(layout.file_len).unwrap();
        layout.write(&file).unwrap();
        (file, layout)
    }

    #[test]
    fn payload_regions_roundtrip_without_changing_usable_capacity() {
        let (file, layout) = payload_file();
        assert_eq!(StreamLayout::read(&file).unwrap(), layout);
        let page = page_size().unwrap() as u64;
        let mut previous_end = layout.queue_offset + layout.queue_len;
        for lane in 0..3 {
            let (offset, capacity) = layout.payload_region(lane).unwrap();
            assert_eq!(capacity, 5001);
            assert!(offset.is_multiple_of(page));
            assert!(offset >= previous_end);
            previous_end = offset + capacity;
        }
        assert!(previous_end <= layout.file_len);
        assert!(layout.payload_region(3).is_none());
        let disabled = layout.with_payloads(0, 3).unwrap();
        assert_eq!(
            disabled.file_len,
            disabled.queue_offset + disabled.queue_len
        );
        assert!(disabled.payload_region(0).is_none());
    }

    #[rstest]
    #[case::overlap(9, 0)]
    #[case::misaligned(9, 129)]
    #[case::outside_file(9, 1 << 60)]
    #[case::zero_capacity(10, 0)]
    #[case::capacity_overflow(10, u64::MAX)]
    #[case::short_stride(11, 1)]
    #[case::stride_overflow(11, u64::MAX)]
    #[case::zero_lanes(12, 0)]
    #[case::count_overflow(12, u64::MAX)]
    fn rejects_invalid_payload_regions(#[case] word: u64, #[case] value: u64) {
        let (file, _) = payload_file();
        file.write_all_at(&value.to_le_bytes(), word.checked_mul(8).unwrap())
            .unwrap();
        assert!(StreamLayout::read(&file).is_err());
    }

    #[test]
    fn rejects_payload_size_overflow_and_missing_lanes() {
        let layout = StreamLayout::new(Layout::from_size_align(256, 64).unwrap(), 0).unwrap();
        assert!(layout.with_payloads(u64::MAX, 1).is_err());
        assert!(layout.with_payloads(isize::MAX as u64, 2).is_err());
        assert!(layout.with_payloads(4096, usize::MAX).is_err());
        assert!(layout.with_payloads(1, 0).is_err());
    }
}

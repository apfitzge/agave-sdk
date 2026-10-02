//! Shared payload bytes. Allocation and broadcast lifetimes remain external.
use {
    super::{REQUIRED_SEALS, payload_ring::PayloadHandle, stream_layout::StreamLayout},
    std::{fs::File, io, os::fd::AsRawFd, ptr::NonNull},
};

#[derive(Debug)]
pub(super) struct PayloadStorage {
    base: NonNull<u8>,
    len: usize,
    layout: StreamLayout,
    writable: bool,
}

// SAFETY: moving the owner does not move its mapping. Payload references/writes
// require external lifetime/exclusion proofs, documented on the unsafe methods.
unsafe impl Send for PayloadStorage {}
// SAFETY: immutable descriptors; all byte access requires caller synchronization.
unsafe impl Sync for PayloadStorage {}

impl PayloadStorage {
    /// Map just the payload region. Consumers can request a read-only mapping;
    /// disabled payload storage returns None without creating a mapping.
    pub(super) fn map(file: &File, writable: bool) -> io::Result<Option<Self>> {
        // SAFETY: live descriptor; F_GET_SEALS takes no pointer argument.
        let seals = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GET_SEALS) };
        if seals == -1 {
            return Err(io::Error::last_os_error());
        }
        if seals & REQUIRED_SEALS != REQUIRED_SEALS {
            return Err(invalid("payload file must be sealed against resizing"));
        }
        // Validate the extent after checking seals, so it cannot be truncated
        // between validation and mapping. The published header is immutable.
        let layout = StreamLayout::read(file)?;
        if layout.payload_capacity == 0 {
            return Ok(None);
        }
        let len = layout
            .file_len
            .checked_sub(layout.payload_offset)
            .and_then(|len| usize::try_from(len).ok())
            .ok_or_else(|| invalid("invalid payload mapping length"))?;
        let offset = libc::off_t::try_from(layout.payload_offset)
            .map_err(|_| invalid("payload mapping offset overflow"))?;
        let protection = libc::PROT_READ | if writable { libc::PROT_WRITE } else { 0 };
        // SAFETY: validated, nonempty, page-aligned extent in a sealed file.
        let address = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                protection,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                offset,
            )
        };
        if address == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let Some(base) = NonNull::new(address.cast()) else {
            // SAFETY: mmap succeeded, but Rust references cannot address null.
            unsafe { libc::munmap(address, len) };
            return Err(io::Error::other("payload mapping at null address"));
        };
        Ok(Some(Self {
            base,
            len,
            layout,
            writable,
        }))
    }

    fn range(&self, lane: usize, handle: PayloadHandle) -> io::Result<(usize, usize)> {
        let (offset, capacity) = self
            .layout
            .payload_region(lane)
            .ok_or_else(|| invalid("invalid payload lane"))?;
        if handle
            .offset
            .checked_add(handle.len)
            .is_none_or(|end| end > capacity)
        {
            return Err(invalid("payload handle outside its lane"));
        }
        let start = offset
            .checked_sub(self.layout.payload_offset)
            .and_then(|start| start.checked_add(handle.offset))
            .and_then(|start| usize::try_from(start).ok())
            .ok_or_else(|| invalid("payload offset overflow"))?;
        let len = usize::try_from(handle.len).map_err(|_| invalid("payload length overflow"))?;
        if start.checked_add(len).is_none_or(|end| end > self.len) {
            return Err(invalid("payload handle outside mapping"));
        }
        Ok((start, len))
    }

    /// Copy into an unpublished allocation. Does not publish or allocate.
    ///
    /// # Safety
    /// The caller owns this complete range exclusively, with no concurrent reads
    /// or writes through any mapping. The source must not alias the destination,
    /// including through a different mapping of the same file.
    pub(super) unsafe fn write(
        &self,
        lane: usize,
        handle: PayloadHandle,
        bytes: &[u8],
    ) -> io::Result<()> {
        if !self.writable {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "read-only payload mapping",
            ));
        }
        let (start, len) = self.range(lane, handle)?;
        if len != bytes.len() {
            return Err(invalid("payload copy length mismatch"));
        }
        // SAFETY: validated bounds, initialized source, caller guarantees exclusive
        // destination ownership and no aliasing through this or another mapping.
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), self.base.as_ptr().add(start), len)
        };
        Ok(())
    }

    /// Borrow previously published bytes, without granting reclamation rights.
    ///
    /// # Safety
    /// The allocation must be initialized and immutable for the returned borrow.
    /// For broadcast payloads, acquire publication and keep the corresponding
    /// cell held for the entire borrow. Bounds alone do not establish lifetime.
    pub(super) unsafe fn read(&self, lane: usize, handle: PayloadHandle) -> io::Result<&[u8]> {
        let (start, len) = self.range(lane, handle)?;
        // SAFETY: validated bounds; caller proves initialization, synchronization,
        // and absence of mutation/reclamation throughout the returned borrow.
        Ok(unsafe { std::slice::from_raw_parts(self.base.as_ptr().add(start), len) })
    }
}

impl Drop for PayloadStorage {
    fn drop(&mut self) {
        // SAFETY: this owner holds exactly this mapping; all borrows have ended.
        unsafe { libc::munmap(self.base.as_ptr().cast(), self.len) };
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use {
        super::{super::create_sealed_queue_with_payloads, *},
        crate::{StreamConfig, event},
    };

    #[event]
    struct TestEvent {
        value: u64,
    }

    fn storage(capacity: u64) -> File {
        let (_, file) = create_sealed_queue_with_payloads::<TestEvent>(
            StreamConfig {
                capacity: 2,
                publisher_slots: 2,
                subscriber_slots: 1,
            },
            42,
            capacity,
        )
        .unwrap();
        file
    }

    #[test]
    fn independent_mappings_share_bytes_and_outlive_the_file() {
        let file = storage(10 * 1024 * 1024);
        let writer = PayloadStorage::map(&file, true).unwrap().unwrap();
        let reader = PayloadStorage::map(&file, false).unwrap().unwrap();
        assert_ne!(writer.base, reader.base);
        drop(file);
        let bytes = vec![37; 10 * 1024 * 1024];
        let handle = PayloadHandle {
            offset: 0,
            len: bytes.len() as u64,
        };
        // SAFETY: this test exclusively owns the file, writes before reading, and
        // keeps the mappings live. No concurrent access or reclamation occurs.
        unsafe {
            writer.write(0, handle, &bytes).unwrap();
            assert_eq!(reader.read(0, handle).unwrap(), bytes);
            assert!(
                reader
                    .read(1, handle)
                    .unwrap()
                    .iter()
                    .all(|&byte| byte == 0)
            );
            writer
                .write(1, PayloadHandle { offset: 1, len: 3 }, b"abc")
                .unwrap();
            assert_eq!(
                reader.read(1, PayloadHandle { offset: 1, len: 3 }).unwrap(),
                b"abc"
            );
            assert_eq!(
                reader.write(0, handle, &bytes).unwrap_err().kind(),
                io::ErrorKind::PermissionDenied
            );
        }
        drop(writer);
        // SAFETY: initialized, immutable bytes remain mapped by reader.
        assert_eq!(unsafe { reader.read(0, handle) }.unwrap(), bytes);
    }

    #[test]
    fn rejects_handles_crossing_lanes_or_addressing_page_padding() {
        let file = storage(5);
        let mapping = PayloadStorage::map(&file, true).unwrap().unwrap();
        for (lane, handle) in [
            (2, PayloadHandle { offset: 0, len: 1 }),
            (0, PayloadHandle { offset: 4, len: 2 }),
            (0, PayloadHandle { offset: 5, len: 1 }),
            (
                0,
                PayloadHandle {
                    offset: u64::MAX,
                    len: 1,
                },
            ),
            (
                0,
                PayloadHandle {
                    offset: 1,
                    len: u64::MAX,
                },
            ),
        ] {
            assert!(mapping.range(lane, handle).is_err());
        }
        // SAFETY: exclusive test mapping; mismatched lengths are rejected before
        // any copy. The valid zero-byte read observes no mutable payload bytes.
        unsafe {
            assert!(
                mapping
                    .write(0, PayloadHandle { offset: 0, len: 2 }, b"x")
                    .is_err()
            );
            assert!(
                mapping
                    .read(0, PayloadHandle { offset: 0, len: 0 })
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[test]
    fn disabled_storage_and_unsealed_files() {
        assert!(PayloadStorage::map(&storage(0), true).unwrap().is_none());
        assert!(PayloadStorage::map(&tempfile::tempfile().unwrap(), false).is_err());
    }
}

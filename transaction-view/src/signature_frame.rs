use {
    crate::{
        bytes::{advance_offset_for_array, read_byte},
        result::{Result, TransactionViewError},
    },
    solana_message::MESSAGE_VERSION_PREFIX,
    solana_signature::Signature,
};

/// Metadata for accessing transaction-level signatures in a transaction view.
#[derive(Debug, Clone)]
pub(crate) struct SignatureFrame {
    /// The number of signatures in the transaction.
    pub(crate) num_signatures: u8,
    /// Offset to the first signature in the transaction packet.
    pub(crate) offset: u16,
}

impl SignatureFrame {
    /// Get the number of signatures and the offset to the first signature in
    /// the transaction packet, starting at the given `offset`.
    #[inline(always)]
    pub(crate) fn try_new(bytes: &[u8], offset: &mut usize) -> Result<Self> {
        // Transaction version dispatch reserves first bytes with the MSB set
        // for versioned transactions. Legacy/v0 counts must fit in one byte.
        let num_signatures = read_byte(bytes, offset)?;
        if num_signatures == 0 || num_signatures & MESSAGE_VERSION_PREFIX != 0 {
            return Err(TransactionViewError::ParseError);
        }

        let signature_offset = *offset as u16;
        advance_offset_for_array::<Signature>(bytes, offset, u16::from(num_signatures))?;

        Ok(Self {
            num_signatures,
            offset: signature_offset,
        })
    }
}

#[cfg(test)]
mod tests {
    use {super::*, solana_short_vec::ShortU16, wincode::Serialize};

    fn serialize_short_vec(vec: &Vec<Signature>) -> Vec<u8> {
        wincode::containers::Vec::<Signature, ShortU16>::serialize(vec).unwrap()
    }

    #[test]
    fn test_zero_signatures() {
        let bytes = serialize_short_vec(&vec![]);
        let mut offset = 0;
        assert!(SignatureFrame::try_new(&bytes, &mut offset).is_err());
    }

    #[test]
    fn test_one_signature() {
        let bytes = serialize_short_vec(&vec![Signature::default()]);
        let mut offset = 0;
        let frame = SignatureFrame::try_new(&bytes, &mut offset).unwrap();
        assert_eq!(frame.num_signatures, 1);
        assert_eq!(frame.offset, 1);
        assert_eq!(offset, 1 + core::mem::size_of::<Signature>());
    }

    #[test]
    fn test_max_one_byte_signature_count() {
        let signatures = vec![Signature::default(); 127];
        let bytes = serialize_short_vec(&signatures);
        let mut offset = 0;
        let frame = SignatureFrame::try_new(&bytes, &mut offset).unwrap();
        assert_eq!(frame.num_signatures, 127);
        assert_eq!(frame.offset, 1);
        assert_eq!(offset, 1 + 127 * core::mem::size_of::<Signature>());
    }

    #[test]
    fn test_non_zero_offset() {
        let mut bytes = serialize_short_vec(&vec![Signature::default()]);
        bytes.insert(0, 0); // Insert a byte at the beginning of the packet.
        let mut offset = 1; // Start at the second byte.
        let frame = SignatureFrame::try_new(&bytes, &mut offset).unwrap();
        assert_eq!(frame.num_signatures, 1);
        assert_eq!(frame.offset, 2);
        assert_eq!(offset, 2 + core::mem::size_of::<Signature>());
    }

    #[test]
    fn test_multibyte_signature_count() {
        let signatures = vec![Signature::default(); 128];
        let bytes = serialize_short_vec(&signatures);
        let mut offset = 0;
        assert!(SignatureFrame::try_new(&bytes, &mut offset).is_err());
    }

    #[test]
    fn test_u16_max_signatures() {
        let signatures = vec![Signature::default(); u16::MAX as usize];
        let bytes = serialize_short_vec(&signatures);
        let mut offset = 0;
        assert!(SignatureFrame::try_new(&bytes, &mut offset).is_err());
    }
}

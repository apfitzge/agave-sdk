//! Numeric handles and schema metadata for variable-sized payloads.
#[cfg(target_os = "linux")]
use wincode::SchemaRead;
use wincode::SchemaWrite;
#[cfg(target_os = "linux")]
use {
    std::{io, ops::Range},
    wincode_dynamic::{Decoder, PrimitiveTy, RootSchema, Ty},
};

/// Internal lane-relative allocation handle. No public API resolves raw handles.
#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub(crate) struct PayloadHandle {
    pub offset: u64,
    pub len: u64,
}

/// Macro adapter: reserve two numeric fields without serializing the slice.
#[doc(hidden)]
pub struct PayloadSlice<'a>(std::marker::PhantomData<&'a [u8]>);

// SAFETY: every write emits exactly two u64s. This is not a zero-copy encoding
// of the source slice; TYPE_META deliberately retains its conservative default.
unsafe impl<'a, C: wincode::config::ConfigCore> SchemaWrite<C> for PayloadSlice<'a> {
    type Src = &'a [u8];
    fn size_of(_: &Self::Src) -> wincode::WriteResult<usize> {
        <[u64; 2] as SchemaWrite<C>>::size_of(&[0, 0])
    }
    fn write(writer: impl wincode::io::Writer, _: &Self::Src) -> wincode::WriteResult<()> {
        <[u64; 2] as SchemaWrite<C>>::write(writer, &[0, 0])
    }
}

/// Persist the producer's markers alongside its ordinary event schema. A typed
/// consumer's choice of Rust type cannot redirect access to an unmarked field.
#[cfg(target_os = "linux")]
#[derive(Debug, SchemaRead, SchemaWrite)]
pub(crate) struct StreamSchema {
    pub(crate) event: RootSchema,
    fields: Vec<(Option<String>, String)>,
}

#[cfg(target_os = "linux")]
impl StreamSchema {
    pub(crate) fn new<E: crate::Event>() -> Self {
        Self {
            event: E::schema(),
            fields: E::PAYLOAD_FIELDS
                .iter()
                .map(|&(variant, field)| (variant.map(str::to_owned), field.to_owned()))
                .collect(),
        }
    }

    /// Locate the marked handle in the *serialized* active variant. Decoding
    /// preceding fields supports dynamic offsets and wincode's enum tag encoding.
    pub(crate) fn payload_range(&self, bytes: &[u8]) -> io::Result<Option<Range<usize>>> {
        if self.fields.is_empty() {
            return Ok(None);
        }
        let mut remaining = bytes;
        {
            let (variant, mut fields) = match Decoder::new(&self.event) {
                Decoder::Struct(decoder) => (None, decoder.fields(&mut remaining)),
                Decoder::Enum(decoder) => {
                    let variant = decoder.decode_variant(&mut remaining).map_err(invalid)?;
                    (Some(variant.variant_name()), variant.fields())
                }
            };
            let Some((_, name)) = self
                .fields
                .iter()
                .find(|(name, _)| name.as_deref() == variant)
            else {
                return Ok(None);
            };
            let mut found = false;
            for field in fields.by_ref() {
                let field = field.map_err(invalid)?;
                if field.name() == name {
                    if field.ty() != Ty::PrimitiveTy(PrimitiveTy::U64) {
                        return Err(invalid("invalid payload field type"));
                    }
                    let length = fields
                        .next()
                        .ok_or_else(|| invalid("missing payload length"))?
                        .map_err(invalid)?;
                    if length.ty() != Ty::PrimitiveTy(PrimitiveTy::U64) {
                        return Err(invalid("invalid payload length type"));
                    }
                    found = true;
                    break;
                }
            }
            if !found {
                return Err(invalid("missing payload field"));
            }
        }
        let end = bytes.len().checked_sub(remaining.len()).unwrap();
        let start = end
            .checked_sub(16)
            .ok_or_else(|| invalid("invalid payload field size"))?;
        Ok(Some(start..end))
    }
}

#[cfg(target_os = "linux")]
fn invalid(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use solana_address::Address;
use solana_hash::Hash;
use solana_sdk::message::MESSAGE_VERSION_PREFIX;
use solana_sdk::signature::Signature;
use std::mem;
use std::mem::MaybeUninit;
use wincode::containers::{Elem, Pod};
use wincode::error::invalid_tag_encoding;
use wincode::io::Reader;
use wincode::len::ShortU16Len;
use wincode::{containers, ReadResult, SchemaRead, SchemaWrite};

#[derive(Debug, Default, PartialEq, Eq, Clone, SchemaRead)]
pub struct EntryProxy {
    /// The number of hashes since the previous Entry ID.
    pub num_hashes: u64,

    #[wincode(with = "Pod<Hash>")]
    /// The SHA-256 hash `num_hashes` after the previous Entry ID.
    pub hash: Hash,

    /// An unordered list of transactions that were observed before the Entry ID was
    /// generated. They may have been observed before a previous Entry ID but were
    /// pushed back into this list to ensure deterministic interpretation of the ledger.
    #[wincode(with = "Vec<crate::wincode::VersionedTransaction>")]
    pub transactions: Vec<solana_sdk::transaction::VersionedTransaction>,
}

impl EntryProxy {
    pub fn to_entry(self) -> solana_entry::entry::Entry {
        unsafe { mem::transmute(self) }
    }
}

#[derive(SchemaRead)]
#[wincode(from = "solana_sdk::message::compiled_instruction::CompiledInstruction")]
struct CompiledInstruction {
    program_id_index: u8,
    accounts: containers::Vec<Pod<u8>, ShortU16Len>,
    data: containers::Vec<Pod<u8>, ShortU16Len>,
}

#[derive(SchemaRead)]
#[wincode(from = "solana_sdk::message::v0::MessageAddressTableLookup")]
struct MessageAddressTableLookup {
    account_key: Pod<Address>,
    writable_indexes: containers::Vec<Pod<u8>, ShortU16Len>,
    readonly_indexes: containers::Vec<Pod<u8>, ShortU16Len>,
}

#[derive(SchemaRead, Copy, Clone)]
#[wincode(from = "solana_sdk::message::MessageHeader", struct_extensions)]
struct MessageHeader {
    num_required_signatures: u8,
    num_readonly_signed_accounts: u8,
    num_readonly_unsigned_accounts: u8,
}

#[derive(SchemaRead)]
#[wincode(from = "solana_sdk::message::v0::Message")]
struct V0Message {
    #[wincode(with = "Pod<_>")]
    header: MessageHeader,
    account_keys: containers::Vec<Pod<Address>, ShortU16Len>,
    recent_blockhash: Pod<Hash>,
    instructions: containers::Vec<Elem<CompiledInstruction>, ShortU16Len>,
    address_table_lookups: containers::Vec<Elem<MessageAddressTableLookup>, ShortU16Len>,
}

#[derive(SchemaRead)]
#[wincode(from = "solana_sdk::message::legacy::Message", struct_extensions)]
struct LegacyMessage {
    header: MessageHeader,
    account_keys: containers::Vec<Pod<Address>, ShortU16Len>,
    recent_blockhash: Pod<Hash>,
    instructions: containers::Vec<Elem<CompiledInstruction>, ShortU16Len>,
}

struct VersionedMsg;
impl<'a> SchemaRead<'a> for VersionedMsg {
    type Dst = solana_sdk::message::VersionedMessage;

    fn read(reader: &mut impl Reader<'a>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        // From `solana_message`:
        //
        // If the first bit is set, the remaining 7 bits will be used to determine
        // which message version is serialized starting from version `0`. If the first
        // is bit is not set, all bytes are used to encode the legacy `Message`
        // format.
        let variant = u8::get(reader)?;

        if variant & MESSAGE_VERSION_PREFIX != 0 {
            let version = variant & !MESSAGE_VERSION_PREFIX;
            return match version {
                0 => {
                    let msg = V0Message::get(reader)?;
                    dst.write(solana_sdk::message::VersionedMessage::V0(msg));
                    Ok(())
                }
                _ => Err(invalid_tag_encoding(version as usize)),
            };
        }

        /// A guard that ensures the [`legacy::Message`] is properly dropped on error or panic.
        ///
        /// Fields will be dropped in reverse initialization order.
        ///
        /// This is necessary in particular for [`legacy::Message`] as it contains heap allocated fields.
        /// Namely, `account_keys` and `instructions`, which are `Vec<Address>` and `Vec<CompiledInstruction>`,
        /// respectively. These will leak if not dropped on error or panic.
        struct LegacyMessageDropGuard<'a> {
            inner: &'a mut MaybeUninit<solana_sdk::message::legacy::Message>,
            field_init_count: u8,
        }

        impl<'a> LegacyMessageDropGuard<'a> {
            const fn new(inner: &'a mut MaybeUninit<solana_sdk::message::legacy::Message>) -> Self {
                Self {
                    inner,
                    field_init_count: 0,
                }
            }

            const fn inc_init_count(&mut self) {
                self.field_init_count += 1;
            }
        }

        impl Drop for LegacyMessageDropGuard<'_> {
            // Fields are initialized in order, matching the serialized format.
            //
            // 0 -> header
            // 1 -> account_keys
            // 2 -> recent_blockhash
            // 3 -> instructions
            //
            // We drop in reverse order to match Rust's drop semantics.
            fn drop(&mut self) {
                use core::ptr;

                // No fields have been initialized.
                if self.field_init_count == 0 {
                    return;
                }

                if self.field_init_count == 4 {
                    // SAFETY: All fields have been initialized, safe to drop the entire message.
                    unsafe { self.inner.assume_init_drop() };
                    return;
                }

                let msg_ptr = self.inner.as_mut_ptr();

                // We don't technically have to worry about recent_blockhash, since it's on the stack,
                // but we do it for completeness.
                if self.field_init_count == 3 {
                    // SAFETY: Recent blockhash is initialized, safe to drop it.
                    unsafe {
                        ptr::drop_in_place(&raw mut (*msg_ptr).recent_blockhash);
                    }
                }
                if self.field_init_count >= 2 {
                    // SAFETY: Account keys are initialized, safe to drop.
                    unsafe {
                        ptr::drop_in_place(&raw mut (*msg_ptr).account_keys);
                    }
                }
                // Similarly to recent_blockhash, we don't technically have to worry about header,
                // but we do it for completeness.
                if self.field_init_count >= 1 {
                    // SAFETY: Header is initialized, safe to drop it.
                    unsafe {
                        ptr::drop_in_place(&raw mut (*msg_ptr).header);
                    }
                }
            }
        }

        let mut msg = MaybeUninit::<solana_sdk::message::legacy::Message>::uninit();
        // We've already read the variant byte which, in the legacy case, represents
        // the `num_required_signatures` field.
        // As such, we need to write the remaining fields into the message manually,
        // as calling `LegacyMessage::read` will miss the first field.
        let header_uninit = LegacyMessage::uninit_header_mut(&mut msg);

        // We don't need to worry about a drop guard for the header,
        // as it's comprised entirely of `u8`s on the stack.
        MessageHeader::write_uninit_num_required_signatures(variant, header_uninit);
        MessageHeader::read_num_readonly_signed_accounts(reader, header_uninit)?;
        MessageHeader::read_num_readonly_unsigned_accounts(reader, header_uninit)?;

        let mut guard = LegacyMessageDropGuard::new(&mut msg);
        // 1. Header is initialized.
        guard.inc_init_count();
        LegacyMessage::read_account_keys(reader, guard.inner)?;
        // 2. Account keys are initialized.
        guard.inc_init_count();
        LegacyMessage::read_recent_blockhash(reader, guard.inner)?;
        // 3. Recent blockhash is initialized.
        guard.inc_init_count();
        LegacyMessage::read_instructions(reader, guard.inner)?;
        // 4. Instructions are initialized.
        guard.inc_init_count();

        // All fields are initialized, safe to drop the the guard.
        mem::forget(guard);

        // SAFETY: All fields are initialized, safe to assume initialized.
        let msg = unsafe { msg.assume_init() };
        dst.write(solana_sdk::message::VersionedMessage::Legacy(msg));

        Ok(())
    }
}

#[derive(SchemaRead)]
#[wincode(from = "solana_sdk::transaction::VersionedTransaction")]
pub struct VersionedTransaction {
    signatures: containers::Vec<Pod<Signature>, ShortU16Len>,
    message: VersionedMsg,
}

#[derive(Clone)]
pub struct SkipBytesReader {
    chunks: Vec<Bytes>,
    cross_chunk: Vec<u8>,
    chunk_idx: usize,
    offset: usize,
}

impl SkipBytesReader {
    pub fn new(chunks: Vec<Bytes>) -> Self {
        Self {
            cross_chunk: Vec::new(),
            chunk_idx: 0,
            offset: 0,
            chunks,
        }
    }

    pub fn total_bytes(&self) -> usize {
        self.chunks.iter().map(|x| x.len()).sum()
    }

    pub fn set_position(&mut self, pos: usize) {
        let mut advanced = 0;
        for (idx, chunk) in self.chunks.iter().enumerate() {
            if advanced + chunk.len() > pos {
                self.chunk_idx = idx;
                self.offset = pos - advanced;
                return;
            }
            advanced += chunk.len();
        }
        self.chunk_idx = self.chunks.len();
        self.offset = 0;
    }
}

impl Reader<'_> for SkipBytesReader {
    type Trusted<'b>
    where
        Self: 'b,
    = Self;

    fn fill_buf(&mut self, n_bytes: usize) -> wincode::io::ReadResult<&[u8]> {
        let current = match self.chunks.get(self.chunk_idx) {
            None => return Err(wincode::io::ReadError::ReadSizeLimit(n_bytes)),
            Some(current) => current,
        };

        Ok(&current[self.offset..current.len().min(self.offset + n_bytes)])
    }

    fn fill_exact(&mut self, n_bytes: usize) -> wincode::io::ReadResult<&[u8]> {
        if let Some(current) = self.chunks.get(self.chunk_idx) {
            let remain = current.len() - self.offset;
            if remain >= n_bytes {
                // 在本chunk内
                return Ok(&current[self.offset..current.len().min(self.offset + n_bytes)]);
            }
        }
        // 需要跨越chunk,需要使用拷贝创建临时缓冲区
        if self.cross_chunk.len() > 0 {
            // 清空缓冲区
            self.cross_chunk.clear();
        }
        let mut iter = self.chunks.iter().enumerate().skip(self.chunk_idx);
        while self.cross_chunk.len() < n_bytes {
            if let Some((idx, chunk)) = iter.next() {
                if idx == self.chunk_idx {
                    self.cross_chunk
                        .extend_from_slice(&chunk[self.offset..chunk.len()]);
                } else {
                    let remain_size = n_bytes - self.cross_chunk.len();
                    self.cross_chunk
                        .extend_from_slice(&chunk[..chunk.len().min(remain_size)]);
                }
            } else {
                return Err(wincode::io::ReadError::ReadSizeLimit(n_bytes));
            }
        }
        Ok(&self.cross_chunk)
    }

    unsafe fn consume_unchecked(&mut self, amt: usize) {
        let _ = self.consume(amt);
    }

    fn consume(&mut self, amt: usize) -> wincode::io::ReadResult<()> {
        if amt == 0 {
            return Ok(());
        }
        let mut unadvanced = amt;
        let mut iter = self.chunks.iter().skip(self.chunk_idx);
        while unadvanced > 0 {
            if let Some(chunk) = iter.next() {
                let remain_bytes = chunk.len() - self.offset;
                let advance = remain_bytes.min(amt);
                self.offset += advance;
                unadvanced -= advance;
                // next chunk
                if self.offset == chunk.len() {
                    self.offset = 0;
                    self.chunk_idx += 1;
                    iter = self.chunks.iter().skip(self.chunk_idx);
                }
            } else {
                return Err(wincode::io::ReadError::ReadSizeLimit(unadvanced));
            }
        }
        Ok(())
    }

    unsafe fn as_trusted_for(
        &mut self,
        n_bytes: usize,
    ) -> wincode::io::ReadResult<Self::Trusted<'_>> {
        let mut new_reader = self.clone();
        // 消费当前reader
        self.consume(n_bytes)?;
        let last_chunk = self
            .chunks
            .get(self.chunk_idx)
            .map(|before_chunk| before_chunk.slice(..self.offset));
        if let Some(last_chunk) = last_chunk {
            new_reader.chunks.insert(self.chunk_idx, last_chunk);
            new_reader.chunks.truncate(self.chunk_idx + 1);
        }
        Ok(new_reader)
    }
}

#[cfg(test)]
mod test {
    use crate::wincode::SkipBytesReader;
    use bytes::Bytes;
    use libc::read;
    use wincode::io::{ReadError, Reader};

    #[test]
    fn skip_bytes_reader_fill_buf_test() {
        let mut reader = SkipBytesReader::new(vec![
            Bytes::from_static(&[0u8, 1u8, 2u8]),
            Bytes::from_static(&[3u8, 4u8, 5u8]),
        ]);
        assert_eq!(reader.fill_buf(0).ok(), Some(&[] as &[u8]));
        assert_eq!(reader.fill_buf(1).ok(), Some(&[0u8] as &[u8]));
        assert_eq!(reader.fill_buf(2).ok(), Some(&[0u8, 1u8] as &[u8]));
        assert_eq!(reader.fill_buf(3).ok(), Some(&[0u8, 1u8, 2u8] as &[u8]));

        assert_eq!(reader.fill_buf(4).ok(), Some(&[0u8, 1u8, 2u8] as &[u8]));
    }

    #[test]
    fn skip_bytes_reader_fill_exact_test() {
        let mut reader = SkipBytesReader::new(vec![
            Bytes::from_static(&[0u8, 1u8, 2u8]),
            Bytes::from_static(&[3u8, 4u8, 5u8]),
        ]);
        assert_eq!(reader.fill_exact(0).ok(), Some(&[] as &[u8]));
        assert_eq!(reader.fill_exact(1).ok(), Some(&[0u8] as &[u8]));
        assert_eq!(reader.fill_exact(2).ok(), Some(&[0u8, 1u8] as &[u8]));
        assert_eq!(reader.fill_exact(3).ok(), Some(&[0u8, 1u8, 2u8] as &[u8]));

        assert_eq!(
            reader.fill_exact(6).ok(),
            Some(&[0u8, 1u8, 2u8, 3u8, 4u8, 5u8] as &[u8])
        );
        assert!(matches!(reader.fill_exact(7), Err(ReadError::ReadSizeLimit(_))));
    }

    #[test]
    fn skip_bytes_reader_consume_test() {
        let new_reader = || {
            let reader = SkipBytesReader::new(vec![
                Bytes::from_static(&[0u8, 1u8, 2u8]),
                Bytes::from_static(&[3u8, 4u8, 5u8]),
            ]);
            reader
        };
        // let mut reader = new_reader();
        // reader.consume(0).unwrap();
        // assert_eq!(reader.chunk_idx, 0);
        // assert_eq!(reader.offset, 0);

        let mut reader = new_reader();
        reader.consume(3).unwrap();
        assert_eq!(reader.chunk_idx, 1);
        assert_eq!(reader.offset, 0);

        reader.consume(1).unwrap();
        assert_eq!(reader.chunk_idx, 1);
        assert_eq!(reader.offset, 1);

        let mut reader = new_reader();
        reader.consume(6).unwrap();
        assert!(matches!(reader.consume(1), Err(ReadError::ReadSizeLimit(_))))

    }

    #[test]
    fn skip_bytes_reader_set_position_test() {
        let mut reader = SkipBytesReader::new(vec![
            Bytes::from_static(&[0u8, 1u8, 2u8]),
            Bytes::from_static(&[3u8, 4u8, 5u8]),
        ]);
        assert_eq!(reader.total_bytes(), 6);

        reader.set_position(0);
        assert_eq!(reader.chunk_idx, 0);
        assert_eq!(reader.offset, 0);

        reader.set_position(1);
        assert_eq!(reader.chunk_idx, 0);
        assert_eq!(reader.offset, 1);

        reader.set_position(2);
        assert_eq!(reader.chunk_idx, 0);
        assert_eq!(reader.offset, 2);

        reader.set_position(3);
        assert_eq!(reader.chunk_idx, 1);
        assert_eq!(reader.offset, 0);

        reader.set_position(5);
        assert_eq!(reader.chunk_idx, 1);
        assert_eq!(reader.offset, 2);

        reader.set_position(6);
        assert_eq!(reader.chunk_idx, 2);
        assert_eq!(reader.offset, 0);
    }
}

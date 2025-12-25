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

// #[derive(Debug, Default, PartialEq, Eq, Clone, SchemaRead)]
// pub struct ProxyEntries {
//     #[wincode(with = "Vec<crate::wincode::Entry>")]
//     pub vec: Vec<solana_entry::entry::Entry>
// }

#[derive(Debug, Default, PartialEq, Eq, Clone, SchemaRead)]
pub struct Entry {
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

impl Entry {
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

#[cfg(test)]
mod test {
    fn t() {}
}

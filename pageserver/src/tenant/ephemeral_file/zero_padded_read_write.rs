//! The heart of how [`super::EphemeralFile`] does its reads and writes.
//!
//! # Writes
//!
//! [`super::EphemeralFile`] writes small, borrowed buffers using [`RW::write_all_borrowed`].
//! The [`RW`] batches these into [`RW::TAIL_SZ`] bigger writes, using [`owned_buffers_io::write::BufferedWriter`].
//!
//! # Reads
//!
//! [`super::EphemeralFile`] always reads full [`PAGE_SZ`]ed blocks using [`RW::read_blk`].
//!
//! The [`RW`] serves these reads either from the buffered writer's in-memory buffer
//! or redirects the caller to read from the underlying [`VirtualFile`]` if they have already
//! been flushed.
//!
//! The current caller is [`super::page_caching::RW`]. In case it gets redirected to read from
//! [`VirtualFile`], it consults the [`crate::page_cache`] first.

mod zero_padded_buffer;

use crate::{
    page_cache::PAGE_SZ,
    tenant::ephemeral_file::zero_padded_buffer,
    virtual_file::{
        owned_buffers_io::{self, write::Buffer},
        VirtualFile,
    },
};

/// See module-level comment.
pub struct RW {
    buffered_writer: owned_buffers_io::write::BufferedWriter<
        zero_padded_buffer::Buf<{ Self::TAIL_SZ }>,
        owned_buffers_io::util::size_tracking_writer::Writer<VirtualFile>,
    >,
}

pub enum ReadResult<'a> {
    NeedsReadFromVirtualFile { virtual_file: &'a VirtualFile },
    ServedFromZeroPaddedMutableTail { buffer: &'a [u8; PAGE_SZ] },
}

impl RW {
    const TAIL_SZ: usize = 64 * 1024;

    pub fn new(file: VirtualFile) -> Self {
        let bytes_flushed_tracker = owned_buffers_io::util::size_tracking_writer::Writer::new(file);
        let buffered_writer = owned_buffers_io::write::BufferedWriter::new(
            bytes_flushed_tracker,
            zero_padded_buffer::Buf::default(),
        );
        Self { buffered_writer }
    }

    pub(crate) fn as_inner_virtual_file(&self) -> &VirtualFile {
        self.buffered_writer.as_inner().as_inner()
    }

    pub async fn write_all_borrowed(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffered_writer.write_buffered_borrowed(buf).await
    }

    pub fn bytes_written(&self) -> u64 {
        let flushed_offset = self.buffered_writer.as_inner().bytes_written();
        let buffer: &zero_padded_buffer::Buf<{ Self::TAIL_SZ }> =
            self.buffered_writer.inspect_buffer();
        flushed_offset + u64::try_from(buffer.pending()).unwrap()
    }

    pub(crate) async fn read_blk(&self, blknum: u32) -> Result<ReadResult, std::io::Error> {
        let flushed_offset = self.buffered_writer.as_inner().bytes_written();
        let buffer: &zero_padded_buffer::Buf<{ Self::TAIL_SZ }> =
            self.buffered_writer.inspect_buffer();
        let buffered_offset = flushed_offset + u64::try_from(buffer.pending()).unwrap();
        let read_offset = (blknum as u64) * (PAGE_SZ as u64);

        assert_eq!(
            flushed_offset % (Self::TAIL_SZ as u64), 0,
            "we only use write_buffered_borrowed to write to the buffered writer, so it's guaranteed that flushes happen buffer.cap()-sized chunks"
        );
        assert_eq!(
            flushed_offset % (PAGE_SZ as u64),
            0,
            "the logic below can't handle if the page is spread across the flushed part and the buffer"
        );

        if read_offset < flushed_offset {
            assert!(
                read_offset + (PAGE_SZ as u64) <= flushed_offset,
                "this impl can't deal with pages spread across flushed & buffered part"
            );
            Ok(ReadResult::NeedsReadFromVirtualFile {
                virtual_file: self.as_inner_virtual_file(),
            })
        } else {
            let read_until_offset = read_offset + (PAGE_SZ as u64);
            if !(0..buffered_offset).contains(&read_until_offset) {
                // The blob_io code relies on the reader allowing reads past
                // the end of what was written, up to end of the current PAGE_SZ chunk.
                // This is a relict of the past where we would get a pre-zeroed page from the page cache.
                //
                // DeltaLayer probably has the same issue, not sure why it needs no special treatment.
                let nbytes_past_end = read_until_offset.checked_sub(buffered_offset).unwrap();
                if nbytes_past_end >= (PAGE_SZ as u64) {
                    // TODO: treat this as error. Pre-existing issue before this patch.
                    panic!(
                        "return IO error: read past end of file: read=0x{read_offset:x} buffered=0x{buffered_offset:x} flushed=0x{flushed_offset}"
                    )
                }
            }
            let read_offset_in_buffer = read_offset
                .checked_sub(flushed_offset)
                .expect("would have taken `if` branch instead of this one");
            let read_offset_in_buffer = usize::try_from(read_offset_in_buffer).unwrap();
            let zero_padded_slice = buffer.as_zero_padded_slice();
            let page = &zero_padded_slice[read_offset_in_buffer..(read_offset_in_buffer + PAGE_SZ)];
            Ok(ReadResult::ServedFromZeroPaddedMutableTail {
                buffer: page
                    .try_into()
                    .expect("the slice above got it as page-size slice"),
            })
        }
    }
}

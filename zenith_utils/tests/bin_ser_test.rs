use bytes::{Buf, BytesMut};
use hex_literal::hex;
use serde::Deserialize;
use std::io::Read;
use zenith_utils::bin_ser::LeSer;

#[derive(Debug, PartialEq, Deserialize)]
pub struct HeaderData {
    magic: u16,
    info: u16,
    tli: u32,
    pageaddr: u64,
    len: u32,
}

// A manual implementation using BytesMut, just so we can
// verify that we decode the same way.
pub fn decode_header_data(buf: &mut BytesMut) -> HeaderData {
    HeaderData {
        magic: buf.get_u16_le(),
        info: buf.get_u16_le(),
        tli: buf.get_u32_le(),
        pageaddr: buf.get_u64_le(),
        len: buf.get_u32_le(),
    }
}

pub fn decode2<R: Read>(reader: &mut R) -> HeaderData {
    HeaderData::des_from(reader).unwrap()
}

#[test]
fn test1() {
    let raw1 = hex!("8940 7890 5534 7890  1289 5379 8378 7893  4207 8923 4712 3218");
    let mut buf1 = BytesMut::from(&raw1[..]);
    let mut buf2 = &raw1[..];
    let dec1 = decode_header_data(&mut buf1);
    let dec2 = decode2(&mut buf2);
    assert_eq!(dec1, dec2);
    assert_eq!(buf1, buf2);
}

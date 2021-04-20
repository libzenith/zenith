use std::net::SocketAddr;
use std::str::FromStr;
use std::fmt;

pub mod page_cache;
pub mod page_service;
pub mod pg_constants;
pub mod restore_local_repo;
pub mod tui;
pub mod tui_event;
mod tui_logger;
pub mod waldecoder;
pub mod walreceiver;
pub mod walredo;
pub mod basebackup;

#[derive(Debug, Clone)]
pub struct PageServerConf {
    pub daemonize: bool,
    pub interactive: bool,
    pub listen_addr: SocketAddr,
}

// Zenith Timeline ID is a 32-byte random ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ZTimelineId([u8; 16]);

impl FromStr for ZTimelineId {
    type Err = hex::FromHexError;

    fn from_str(s: &str) -> Result<ZTimelineId, Self::Err> {
        let timelineid = hex::decode(s)?;

        let mut buf: [u8; 16] = [0u8; 16];
        buf.copy_from_slice(timelineid.as_slice());
        Ok(ZTimelineId(buf))
    }

}

impl ZTimelineId {
    pub fn from(b: [u8; 16]) -> ZTimelineId {
        ZTimelineId(b)
    }

    pub fn get_from_buf(buf: &mut dyn bytes::Buf) -> ZTimelineId {
        let mut arr = [0u8; 16];
        buf.copy_to_slice(&mut arr);
        ZTimelineId::from(arr)
    }

    pub fn as_arr(&self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Display for ZTimelineId {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
         f.write_str(&hex::encode(self.0))
    }
}

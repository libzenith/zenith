//! [`super::VirtualFile`] supports different IO engines.
//!
//! The [`IoEngineKind`] enum identifies them.
//!
//! The choice of IO engine is global.
//! Initialize using [`init`].
//!
//! Then use [`get`] and  [`super::OpenOptions`].

use tokio_epoll_uring::{IoBuf, Slice};
use tracing::Instrument;

pub(crate) use super::api::IoEngineKind;
#[derive(Clone, Copy)]
#[repr(u8)]
pub(crate) enum IoEngine {
    NotSet,
    StdFs,
    #[cfg(target_os = "linux")]
    TokioEpollUring,
}

impl From<IoEngineKind> for IoEngine {
    fn from(value: IoEngineKind) -> Self {
        match value {
            IoEngineKind::StdFs => IoEngine::StdFs,
            #[cfg(target_os = "linux")]
            IoEngineKind::TokioEpollUring => IoEngine::TokioEpollUring,
        }
    }
}

impl TryFrom<u8> for IoEngine {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            v if v == (IoEngine::NotSet as u8) => IoEngine::NotSet,
            v if v == (IoEngine::StdFs as u8) => IoEngine::StdFs,
            #[cfg(target_os = "linux")]
            v if v == (IoEngine::TokioEpollUring as u8) => IoEngine::TokioEpollUring,
            x => return Err(x),
        })
    }
}

static IO_ENGINE: AtomicU8 = AtomicU8::new(IoEngine::NotSet as u8);

pub(crate) fn set(engine_kind: IoEngineKind) {
    let engine: IoEngine = engine_kind.into();
    IO_ENGINE.store(engine as u8, std::sync::atomic::Ordering::Relaxed);
    #[cfg(not(test))]
    {
        let metric = &crate::metrics::virtual_file_io_engine::KIND;
        metric.reset();
        metric
            .with_label_values(&[&format!("{engine_kind}")])
            .set(1);
    }
}

#[cfg(not(test))]
pub(super) fn init(engine_kind: IoEngineKind) {
    set(engine_kind);
}

/// Longer-term, this API should only be used by [`super::VirtualFile`].
pub(crate) fn get() -> IoEngine {
    let cur = IoEngine::try_from(IO_ENGINE.load(Ordering::Relaxed)).unwrap();
    if cfg!(test) {
        let env_var_name = "NEON_PAGESERVER_UNIT_TEST_VIRTUAL_FILE_IOENGINE";
        match cur {
            IoEngine::NotSet => {
                let kind = match std::env::var(env_var_name) {
                    Ok(v) => match v.parse::<IoEngineKind>() {
                        Ok(engine_kind) => engine_kind,
                        Err(e) => {
                            panic!("invalid VirtualFile io engine for env var {env_var_name}: {e:#}: {v:?}")
                        }
                    },
                    Err(std::env::VarError::NotPresent) => {
                        crate::config::defaults::DEFAULT_VIRTUAL_FILE_IO_ENGINE
                            .parse()
                            .unwrap()
                    }
                    Err(std::env::VarError::NotUnicode(_)) => {
                        panic!("env var {env_var_name} is not unicode");
                    }
                };
                self::set(kind);
                self::get()
            }
            x => x,
        }
    } else {
        cur
    }
}

use std::{
    os::unix::prelude::FileExt,
    sync::atomic::{AtomicU8, Ordering},
};

use super::{FileGuard, Metadata};

#[cfg(target_os = "linux")]
fn epoll_uring_error_to_std(e: tokio_epoll_uring::Error<std::io::Error>) -> std::io::Error {
    match e {
        tokio_epoll_uring::Error::Op(e) => e,
        tokio_epoll_uring::Error::System(system) => {
            std::io::Error::new(std::io::ErrorKind::Other, system)
        }
    }
}

impl IoEngine {
    pub(super) async fn read_at<B>(
        &self,
        file_guard: FileGuard,
        offset: u64,
        mut buf: B,
    ) -> ((FileGuard, B), std::io::Result<usize>)
    where
        B: tokio_epoll_uring::BoundedBufMut + Send,
    {
        match self {
            IoEngine::NotSet => panic!("not initialized"),
            IoEngine::StdFs => {
                // SAFETY: `dst` only lives at most as long as this match arm, during which buf remains valid memory.
                let dst = unsafe {
                    std::slice::from_raw_parts_mut(buf.stable_mut_ptr(), buf.bytes_total())
                };
                let res = file_guard.with_std_file(|std_file| std_file.read_at(dst, offset));
                if let Ok(nbytes) = &res {
                    assert!(*nbytes <= buf.bytes_total());
                    // SAFETY: see above assertion
                    unsafe {
                        buf.set_init(*nbytes);
                    }
                }
                #[allow(dropping_references)]
                drop(dst);
                ((file_guard, buf), res)
            }
            #[cfg(target_os = "linux")]
            IoEngine::TokioEpollUring => {
                let system = tokio_epoll_uring::thread_local_system().await;
                let (resources, res) = system.read(file_guard, offset, buf).await;
                (resources, res.map_err(epoll_uring_error_to_std))
            }
        }
    }
    pub(super) async fn sync_all(&self, file_guard: FileGuard) -> (FileGuard, std::io::Result<()>) {
        match self {
            IoEngine::NotSet => panic!("not initialized"),
            IoEngine::StdFs => {
                let res = file_guard.with_std_file(|std_file| std_file.sync_all());
                (file_guard, res)
            }
            #[cfg(target_os = "linux")]
            IoEngine::TokioEpollUring => {
                let system = tokio_epoll_uring::thread_local_system().await;
                let (resources, res) = system.fsync(file_guard).await;
                (resources, res.map_err(epoll_uring_error_to_std))
            }
        }
    }
    pub(super) async fn sync_data(
        &self,
        file_guard: FileGuard,
    ) -> (FileGuard, std::io::Result<()>) {
        match self {
            IoEngine::NotSet => panic!("not initialized"),
            IoEngine::StdFs => {
                let res = file_guard.with_std_file(|std_file| std_file.sync_data());
                (file_guard, res)
            }
            #[cfg(target_os = "linux")]
            IoEngine::TokioEpollUring => {
                let system = tokio_epoll_uring::thread_local_system().await;
                let (resources, res) = system.fdatasync(file_guard).await;
                (resources, res.map_err(epoll_uring_error_to_std))
            }
        }
    }
    pub(super) async fn metadata(
        &self,
        file_guard: FileGuard,
    ) -> (FileGuard, std::io::Result<Metadata>) {
        match self {
            IoEngine::NotSet => panic!("not initialized"),
            IoEngine::StdFs => {
                let res =
                    file_guard.with_std_file(|std_file| std_file.metadata().map(Metadata::from));
                (file_guard, res)
            }
            #[cfg(target_os = "linux")]
            IoEngine::TokioEpollUring => {
                let system = tokio_epoll_uring::thread_local_system().await;
                let (resources, res) = system.statx(file_guard).await;
                (
                    resources,
                    res.map_err(epoll_uring_error_to_std).map(Metadata::from),
                )
            }
        }
    }
    pub(super) async fn write_at<B: IoBuf + Send>(
        &self,
        file_guard: FileGuard,
        offset: u64,
        buf: Slice<B>,
    ) -> ((FileGuard, Slice<B>), std::io::Result<usize>) {
        match self {
            IoEngine::NotSet => panic!("not initialized"),
            IoEngine::StdFs => {
                let result = file_guard.with_std_file(|std_file| std_file.write_at(&buf, offset));
                ((file_guard, buf), result)
            }
            #[cfg(target_os = "linux")]
            IoEngine::TokioEpollUring => {
                let system = tokio_epoll_uring::thread_local_system().await;
                let (resources, res) = system.write(file_guard, offset, buf).await;
                (resources, res.map_err(epoll_uring_error_to_std))
            }
        }
    }

    /// If we switch a user of [`tokio::fs`] to use [`super::io_engine`],
    /// they'd start blocking the executor thread if [`IoEngine::StdFs`] is configured
    /// whereas before the switch to [`super::io_engine`], that wasn't the case.
    /// This method helps avoid such a regression.
    ///
    /// Panics if the `spawn_blocking` fails, see [`tokio::task::JoinError`] for reasons why that can happen.
    pub(crate) async fn spawn_blocking_and_block_on_if_std<Fut, R>(&self, work: Fut) -> R
    where
        Fut: 'static + Send + std::future::Future<Output = R>,
        R: 'static + Send,
    {
        match self {
            IoEngine::NotSet => panic!("not initialized"),
            IoEngine::StdFs => {
                let span = tracing::info_span!("spawn_blocking_block_on_if_std");
                tokio::task::spawn_blocking({
                    move || tokio::runtime::Handle::current().block_on(work.instrument(span))
                })
                .await
                .expect("failed to join blocking code most likely it panicked, panicking as well")
            }
            #[cfg(target_os = "linux")]
            IoEngine::TokioEpollUring => work.await,
        }
    }
}

//! Crutch module to work around tracing infrastructure deficiencies
//!
//! We wish to collect granular request spans without impacting performance
//! by much. Ideally, we should have zero overhead for a sampling rate of 0.
//!
//! The approach taken by the pageserver crate is to use a completely different
//! span hierarchy for the performance spans. Spans are explicitly stored in
//! the request context and use a different [`tracing::Subscriber`] in order
//! to avoid expensive filtering.
//!
//! [`tracing::Span`] instances record their [`tracing::Dispatch`] and, implcitly,
//! their [`tracing::Subscriber`] at creation time. However, upon exiting the span,
//! the global default [`tracing::Dispatch`] is used. This is problematic if one
//! wishes to juggle different subscribers.
//!
//! In order to work around this, this module provides a [`PerfSpan`] type which
//! wraps a [`Span`] and sets the default subscriber when exiting the span. This
//! achieves the correct routing.
//!
//! There's also a modified version of [`tracing::Instrument`] which works with
//! [`PerfSpan`].

use core::{
    future::Future,
    marker::Sized,
    mem::ManuallyDrop,
    pin::Pin,
    task::{Context, Poll},
};
use pin_project_lite::pin_project;
use tracing::{field, span::Span, Dispatch};

#[derive(Debug, Clone)]
pub struct PerfSpan {
    inner: ManuallyDrop<Span>,
    dispatch: Dispatch,
}

#[must_use = "once a span has been entered, it should be exited"]
pub struct PerfSpanEntered<'a> {
    span: &'a PerfSpan,
}

impl PerfSpan {
    pub fn new(span: Span, dispatch: Dispatch) -> Self {
        Self {
            inner: ManuallyDrop::new(span),
            dispatch,
        }
    }

    pub fn record<Q: field::AsField + ?Sized, V: field::Value>(
        &self,
        field: &Q,
        value: V,
    ) -> &Self {
        self.inner.record(field, value);
        self
    }

    pub fn enter(&self) -> PerfSpanEntered {
        PerfSpanEntered { span: self }
    }

    pub fn inner(&self) -> &Span {
        &self.inner
    }
}

impl Drop for PerfSpan {
    fn drop(&mut self) {
        // Bring the desired dispatch into scope before explicitly calling
        // the span destructor. This routes the span exit to the correct
        // [`tracing::Subscriber`].
        let _dispatch_guard = tracing::dispatcher::set_default(&self.dispatch);
        // SAFETY: ManuallyDrop in Drop implementation
        unsafe { ManuallyDrop::drop(&mut self.inner) }
    }
}

impl Drop for PerfSpanEntered<'_> {
    fn drop(&mut self) {
        assert!(self.span.inner.id().is_some());

        let _dispatch_guard = tracing::dispatcher::set_default(&self.span.dispatch);
        self.span.dispatch.exit(&self.span.inner.id().unwrap());
    }
}

pub trait PerfInstrument: Sized {
    fn instrument(self, span: PerfSpan) -> PerfInstrumented<Self> {
        PerfInstrumented {
            inner: ManuallyDrop::new(self),
            span,
        }
    }
}

pin_project! {
    #[project = PerfInstrumentedProj]
    #[derive(Debug, Clone)]
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct PerfInstrumented<T> {
        // `ManuallyDrop` is used here to to enter instrument `Drop` by entering
        // `Span` and executing `ManuallyDrop::drop`.
        #[pin]
        inner: ManuallyDrop<T>,
        span: PerfSpan,
    }

    impl<T> PinnedDrop for PerfInstrumented<T> {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            let _enter = this.span.enter();
            // SAFETY: 1. `Pin::get_unchecked_mut()` is safe, because this isn't
            //             different from wrapping `T` in `Option` and calling
            //             `Pin::set(&mut this.inner, None)`, except avoiding
            //             additional memory overhead.
            //         2. `ManuallyDrop::drop()` is safe, because
            //            `PinnedDrop::drop()` is guaranteed to be called only
            //            once.
            unsafe { ManuallyDrop::drop(this.inner.get_unchecked_mut()) }
        }
    }
}

impl<'a, T> PerfInstrumentedProj<'a, T> {
    /// Get a mutable reference to the [`Span`] a pinned mutable reference to
    /// the wrapped type.
    fn span_and_inner_pin_mut(self) -> (&'a mut PerfSpan, Pin<&'a mut T>) {
        // SAFETY: As long as `ManuallyDrop<T>` does not move, `T` won't move
        //         and `inner` is valid, because `ManuallyDrop::drop` is called
        //         only inside `Drop` of the `Instrumented`.
        let inner = unsafe { self.inner.map_unchecked_mut(|v| &mut **v) };
        (self.span, inner)
    }
}

impl<T: Future> Future for PerfInstrumented<T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (span, inner) = self.project().span_and_inner_pin_mut();
        let _enter = span.enter();
        inner.poll(cx)
    }
}

impl<T: Sized> PerfInstrument for T {}

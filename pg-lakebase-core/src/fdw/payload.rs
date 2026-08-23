//! Ownership of provider private data and executor state.

use core::mem::MaybeUninit;
use core::ptr;

/// Owns the two values whose lifetimes are controlled by an executor wrapper.
///
/// Scan and modify wrappers have different PostgreSQL callback contracts and
/// therefore remain separate types. This small object only centralizes the
/// unsafe initialization and destruction bookkeeping shared by both.
pub(crate) struct ProviderPayload<D, S> {
    private_data: MaybeUninit<D>,
    provider_state: MaybeUninit<S>,
    private_initialized: bool,
    provider_state_initialized: bool,
}

impl<D, S> ProviderPayload<D, S> {
    pub(crate) fn with_private(private_data: D) -> Self {
        Self {
            private_data: MaybeUninit::new(private_data),
            provider_state: MaybeUninit::uninit(),
            private_initialized: true,
            provider_state_initialized: false,
        }
    }

    pub(crate) fn empty() -> Self {
        Self {
            private_data: MaybeUninit::uninit(),
            provider_state: MaybeUninit::uninit(),
            private_initialized: false,
            provider_state_initialized: false,
        }
    }

    #[inline]
    pub(crate) fn private_data(&self) -> &D {
        debug_assert!(self.private_initialized);
        // SAFETY: the initialization flag is maintained with the value.
        unsafe { self.private_data.assume_init_ref() }
    }

    pub(crate) fn install_provider_state(&mut self, state: S) {
        debug_assert!(!self.provider_state_initialized);
        // SAFETY: the flag proves this location is uninitialized.
        unsafe { self.provider_state.as_mut_ptr().write(state) };
        self.provider_state_initialized = true;
    }

    #[inline]
    pub(crate) fn provider_state_initialized(&self) -> bool {
        self.provider_state_initialized
    }

    #[inline]
    pub(crate) fn provider_state_ptr(&mut self) -> Option<*mut S> {
        self.provider_state_initialized
            .then_some(self.provider_state.as_mut_ptr())
    }

    /// Return the initialized provider-state address when the callback
    /// lifecycle already establishes that the state is present.
    ///
    /// # Safety
    ///
    /// `install_provider_state` must have completed and `cleanup` must not
    /// have started.
    pub(crate) unsafe fn provider_state_ptr_unchecked(&mut self) -> *mut S {
        debug_assert!(self.provider_state_initialized);
        self.provider_state.as_mut_ptr()
    }

    /// Borrow the initialized provider state without repeating a lifecycle
    /// branch at a caller that already owns that invariant.
    ///
    /// # Safety
    ///
    /// `install_provider_state` must have completed and `cleanup` must not
    /// have started.
    pub(crate) unsafe fn provider_state_unchecked(&self) -> &S {
        debug_assert!(self.provider_state_initialized);
        unsafe { self.provider_state.assume_init_ref() }
    }

    pub(crate) fn cleanup(&mut self) {
        if self.provider_state_initialized {
            // SAFETY: the flag proves the state is initialized exactly once.
            unsafe { ptr::drop_in_place(self.provider_state.as_mut_ptr()) };
            self.provider_state_initialized = false;
        }
        if self.private_initialized {
            // SAFETY: the flag proves private data is initialized exactly once.
            unsafe { ptr::drop_in_place(self.private_data.as_mut_ptr()) };
            self.private_initialized = false;
        }
    }
}

impl<D, S> Drop for ProviderPayload<D, S> {
    fn drop(&mut self) {
        self.cleanup();
    }
}

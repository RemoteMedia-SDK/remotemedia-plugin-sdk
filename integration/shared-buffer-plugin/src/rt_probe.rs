//! Opt-in instrumentation for the real-library callback ownership test.
use remotemedia_plugin_sdk::{
    buffer::OutputAllocatorArc,
    runtime_buffer::{export_allocator, import_allocator},
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    static COUNTS: Cell<Option<(u32, u32)>> = const { Cell::new(None) };
}
struct ProbeAllocator;
fn record(allocation: bool) {
    let _ = COUNTS.try_with(|counts| {
        if let Some((allocs, frees)) = counts.get() {
            counts.set(Some((
                allocs.saturating_add(u32::from(allocation)),
                frees.saturating_add(u32::from(!allocation)),
            )));
        }
    });
}
// Delegate exact layouts and pointers; the counter is local to this library/thread.
unsafe impl GlobalAlloc for ProbeAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(true);
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(true);
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record(true);
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        record(false);
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: ProbeAllocator = ProbeAllocator;

/// Both adapters execute in this DLL/SO; no host Rust trait object crosses FFI.
#[no_mangle]
pub extern "C" fn shared_buffer_fixture_allocator(
    provider: OutputAllocatorArc,
) -> OutputAllocatorArc {
    export_allocator(import_allocator(provider))
}

#[no_mangle]
pub extern "C" fn shared_buffer_fixture_probe_begin() {
    COUNTS.with(|counts| counts.set(Some((0, 0))));
}

/// Upper 32 bits are allocations/reallocations; lower 32 bits are frees.
#[no_mangle]
pub extern "C" fn shared_buffer_fixture_probe_end() -> u64 {
    let (allocs, frees) = COUNTS.with(|counts| counts.replace(None).unwrap_or_default());
    (u64::from(allocs) << 32) | u64::from(frees)
}

/// Positive control: the host must observe both events in the foreign module.
#[no_mangle]
pub extern "C" fn shared_buffer_fixture_probe_allocate() {
    let allocation = Box::new([0u8; 64]);
    std::hint::black_box(&allocation);
    drop(allocation);
}

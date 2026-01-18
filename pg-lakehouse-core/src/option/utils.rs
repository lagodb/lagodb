use std::ffi::CStr;

/// Helper to append a string to the data buffer and return its offset.
///
/// Returns 0 if the string is empty.
/// Note: This adds a null terminator to make it C-string compatible if needed,
/// though Rust usually prefers length+bytes. Here we just store raw bytes + null.
pub fn append_string(data: &mut Vec<u8>, header_size: usize, s: &str) -> u32 {
    if s.is_empty() {
        return 0;
    }
    let offset = (header_size + data.len()) as u32;
    data.extend_from_slice(s.as_bytes());
    data.push(0); // Null terminator for safety
    offset
}

/// Helper to get a string from the cache using offset.
///
/// # Safety
/// The `base_ptr` must be valid, and `offset` must be within the allocated block.
pub unsafe fn get_string_at_offset<'a>(base_ptr: *const u8, offset: u32) -> &'a str {
    unsafe {
        if offset == 0 {
            return "";
        }
        let ptr = base_ptr.add(offset as usize);
        let c_str = CStr::from_ptr(ptr as *const i8);
        c_str.to_str().unwrap_or("")
    }
}

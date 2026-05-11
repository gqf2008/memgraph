use crate::types::Gid;

use std::fmt;

/// Equivalent to C++ EdgeRef: either a Gid or a pointer to Edge.
#[derive(Clone, Copy)]
#[repr(C)]
pub union EdgeRefInner {
    pub gid: Gid,
    pub ptr: *mut std::ffi::c_void,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct EdgeRef {
    inner: EdgeRefInner,
}

impl EdgeRef {
    pub fn from_gid(gid: Gid) -> Self {
        Self {
            inner: EdgeRefInner { gid },
        }
    }

    pub fn from_ptr(ptr: *mut std::ffi::c_void) -> Self {
        Self {
            inner: EdgeRefInner { ptr },
        }
    }

    pub fn gid(&self) -> Gid {
        unsafe { self.inner.gid }
    }

    pub fn as_ptr(&self) -> *mut std::ffi::c_void {
        unsafe { self.inner.ptr }
    }

    pub fn is_null(&self) -> bool {
        self.gid() == Gid::default()
    }
}

impl PartialEq for EdgeRef {
    fn eq(&self, other: &Self) -> bool {
        self.gid() == other.gid()
    }
}

impl Eq for EdgeRef {}

impl PartialOrd for EdgeRef {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.gid().cmp(&other.gid()))
    }
}

impl Ord for EdgeRef {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.gid().cmp(&other.gid())
    }
}

impl fmt::Debug for EdgeRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EdgeRef({})", self.gid())
    }
}

impl fmt::Display for EdgeRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.gid())
    }
}

// Safety: EdgeRef is a u64-sized union. The pointer variant is only
// dereferenced under proper synchronization (vertex lock / GC epoch).
unsafe impl Send for EdgeRef {}
unsafe impl Sync for EdgeRef {}

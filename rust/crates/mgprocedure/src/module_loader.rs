//! Dynamic module loading for MAGE query modules via dlopen.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_void;
use std::ptr;
use std::sync::{LazyLock, Mutex};

use crate::{mgp_memory, mgp_module, MgpError, MgpProcCb};

/// Wrapper to make dlopen handle Send+Sync (safe because handles are thread-safe in POSIX).
struct DlHandle(*mut c_void);
unsafe impl Send for DlHandle {}
unsafe impl Sync for DlHandle {}

/// A loaded MAGE query module.
pub struct LoadedModule {
    name: String,
    handle: DlHandle,
    procs: Vec<LoadedProc>,
}

/// A registered procedure from a loaded module.
pub struct LoadedProc {
    name: String,
    callback: MgpProcCb,
    is_write: bool,
}

/// Global registry of loaded modules and their procedures.
static MODULE_REGISTRY: LazyLock<Mutex<Vec<LoadedModule>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

/// Load a MAGE query module from a shared library file.
///
/// The module must export `mgp_module_init(MgpModuleInitCallback)` following
/// the mg_procedure.h ABI. On success, all procedures registered by the
/// module's init function are available in the global registry.
pub fn load_module(path: &str) -> Result<String, String> {
    let c_path = CString::new(path).map_err(|e| format!("invalid path: {}", e))?;

    // dlopen the shared library
    let handle = DlHandle(unsafe { libc::dlopen(c_path.as_ptr(), libc::RTLD_NOW) });
    if handle.0.is_null() {
        let err = unsafe { CStr::from_ptr(libc::dlerror()).to_string_lossy().into_owned() };
        return Err(format!("dlopen failed for {}: {}", path, err));
    }

    // Look up mgp_module_init
    let init_sym = CString::new("mgp_module_init").unwrap();
    let init_fn: Option<unsafe extern "C" fn(*mut mgp_module, *mut mgp_memory) -> MgpError> =
        unsafe { std::mem::transmute(libc::dlsym(handle.0, init_sym.as_ptr())) };

    if init_fn.is_none() {
        let err = unsafe { CStr::from_ptr(libc::dlerror()).to_string_lossy().into_owned() };
        unsafe { libc::dlclose(handle.0) };
        return Err(format!("module has no mgp_module_init: {}", err));
    }

    // Derive module name from file name
    let name = std::path::Path::new(path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown")
        .to_string();

    // Create mgp_module and call init
    let module = Box::into_raw(Box::new(mgp_module {
        procs: Vec::new(),
        funcs: Vec::new(),
    }));
    let memory = Box::into_raw(Box::new(mgp_memory {
        tracker: ptr::null(),
    }));

    let init_result = unsafe { init_fn.unwrap()(module, memory) };
    if init_result != MgpError::NoError {
        unsafe {
            drop(Box::from_raw(module));
            drop(Box::from_raw(memory));
            libc::dlclose(handle.0);
        }
        return Err(format!("mgp_module_init returned error {:?}", init_result));
    }

    // Collect registered procedures
    let mut loaded_procs = Vec::new();
    unsafe {
        for proc_ptr in &(*module).procs {
            if !proc_ptr.is_null() {
                let proc = &**proc_ptr;
                let proc_name = CStr::from_ptr(proc.name.as_ptr())
                    .to_string_lossy()
                    .into_owned();
                loaded_procs.push(LoadedProc {
                    name: proc_name,
                    callback: proc.callback,
                    is_write: proc.is_write,
                });
            }
        }
        drop(Box::from_raw(module));
        drop(Box::from_raw(memory));
    }

    let loaded = LoadedModule {
        name: name.clone(),
        handle,
        procs: loaded_procs,
    };

    MODULE_REGISTRY.lock().unwrap().push(loaded);
    Ok(name)
}

/// Unload a module by name (dlclose).
pub fn unload_module(name: &str) -> Result<(), String> {
    let mut registry = MODULE_REGISTRY.lock().unwrap();
    let idx = registry.iter().position(|m| m.name == name);
    if let Some(i) = idx {
        let module = registry.remove(i);
        unsafe { libc::dlclose(module.handle.0) };
        Ok(())
    } else {
        Err(format!("module {} not found", name))
    }
}

/// Get all loaded module names.
pub fn loaded_module_names() -> Vec<String> {
    MODULE_REGISTRY.lock().unwrap().iter().map(|m| m.name.clone()).collect()
}

/// Find a loaded procedure by full name (module.proc).
pub fn find_proc(full_name: &str) -> Option<MgpProcCb> {
    let registry = MODULE_REGISTRY.lock().unwrap();
    for module in &*registry {
        for proc in &module.procs {
            let qualified = format!("{}.{}", module.name, proc.name);
            if qualified == full_name {
                return Some(proc.callback);
            }
        }
    }
    None
}

/// Scan a directory for .so/.dylib files and load all found modules.
pub fn scan_and_load_directory(dir: &str) -> Vec<Result<String, String>> {
    let mut results = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
            if ext == "so" || ext == "dylib" {
                let path_str = path.to_string_lossy().into_owned();
                results.push(load_module(&path_str));
            }
        }
    }
    results
}
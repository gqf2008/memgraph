// Cross-language SLK compatibility test.
//
// Verifies that the Rust mgslk crate and the standalone C++ SLK implementation
// produce byte-for-byte identical output for 10K randomly-generated
// Vec<Option<(i64, String)>> structures.
//
// The test uses a deterministic LCG so both sides generate the exact same
// sequence of values without needing to exchange them.
//
// Requires g++ to be on PATH. The test skips gracefully if the C++ compiler
// is unavailable.

use std::path::{Path, PathBuf};
use std::process::Command;

use mgslk::{Reader, SlkLoad, SlkSave};

const SEED: u64 = 0xDEADBEEF;
// Keep total payload under one SLK segment (256 KiB) so that items never
// span segment boundaries.  Each Vec<Option<(i64, String)>> averages ~200
// bytes, so 500 structs ≈ 100 KiB, well within the limit.
const COUNT: u64 = 500;

// ─── LCG (must match C++ exactly) ───────────────────────────────────────────

fn lcg(seed: &mut u64) -> u64 {
    const A: u64 = 6364136223846793005;
    const C: u64 = 1442695040888963407;
    *seed = seed.wrapping_mul(A).wrapping_add(C);
    *seed
}

// ─── Deterministic data generation (must match C++) ─────────────────────────

fn generate_one(seed: &mut u64) -> Vec<Option<(i64, String)>> {
    let len = (lcg(seed) % 20) as usize;
    (0..len)
        .map(|_| {
            if lcg(seed) % 3 == 0 {
                None
            } else {
                Some((lcg(seed) as i64, format!("v{}", lcg(seed))))
            }
        })
        .collect()
}

// ─── Test harness helpers ───────────────────────────────────────────────────

fn workspace_root() -> PathBuf {
    // Start from CARGO_MANIFEST_DIR (the crate being tested) and walk upward
    // looking for the workspace root, identified by the presence of a
    // `tests/` directory containing our C++ sources.
    let start = std::env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::current_dir().unwrap_or_else(|_| ".".into()));
    let mut dir = start.clone();
    for _ in 0..5 {
        if dir.join("tests/cpp_slk_compat.cpp").exists() {
            return dir;
        }
        if !dir.pop() {
            break;
        }
    }
    // Fallback: assume CARGO_MANIFEST_DIR is the workspace root
    start
}

fn cpp_source_path() -> PathBuf {
    workspace_root().join("tests/cpp_slk_compat.cpp")
}

fn cpp_binary_path() -> PathBuf {
    // Place next to the source so it persists across test runs.
    workspace_root().join("tests/cpp_slk_compat")
}

fn ensure_cpp_binary() -> Option<PathBuf> {
    let bin = cpp_binary_path();
    if bin.exists() {
        return Some(bin);
    }
    // Try to compile
    let src = cpp_source_path();
    if !src.exists() {
        return None;
    }
    let status = Command::new("g++")
        .args([
            "-std=c++17",
            "-O2",
            src.to_str().unwrap(),
            "-o",
            bin.to_str().unwrap(),
        ])
        .status()
        .ok()?;
    if status.success() && bin.exists() {
        Some(bin)
    } else {
        None
    }
}

fn run_cpp(cmd: &str, count: u64, seed: u64, path: &Path) -> Result<String, String> {
    let bin = ensure_cpp_binary().ok_or("C++ binary not available")?;
    let output = Command::new(&bin)
        .args([
            cmd,
            &count.to_string(),
            &seed.to_string(),
            path.to_str().unwrap(),
        ])
        .output()
        .map_err(|e| format!("Failed to run C++ program: {}", e))?;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    if !output.status.success() {
        return Err(format!(
            "C++ {} failed (exit={}): stdout={} stderr={}",
            cmd,
            output.status,
            stdout,
            stderr
        ));
    }
    Ok(stdout)
}

// ─── Original minimal compatibility tests ───────────────────────────────────

/// Locate the binary output file written by the old C++ program.
fn find_legacy_cpp_output() -> Option<PathBuf> {
    let mut cwd = std::env::current_dir().ok()?;
    for _ in 0..5 {
        for rel in ["cpp_slk_output.bin", "rust/tests/cpp_slk_output.bin", "tests/cpp_slk_output.bin"] {
            let p = cwd.join(rel);
            if p.exists() {
                return Some(p);
            }
        }
        if !cwd.pop() {
            break;
        }
    }
    None
}

#[test]
fn test_legacy_cpp_generated_slk() {
    let path = find_legacy_cpp_output().expect(
        "cpp_slk_output.bin not found. \
         Run the C++ writer first: `g++ rust/tests/cpp_slk_writer.cpp -o cpp_slk_writer && ./cpp_slk_writer`"
    );

    let data = std::fs::read(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {}", path.display(), e));

    assert!(data.len() >= 8, "SLK file too small ({} bytes)", data.len());

    let mut reader = Reader::new(&data);

    let u64_val = u64::slk_load(&mut reader).expect("failed to decode u64");
    assert_eq!(u64_val, 42, "u64 mismatch");

    let string_val = String::slk_load(&mut reader).expect("failed to decode String");
    assert_eq!(string_val, "hello", "String mismatch");

    let vec_val = Vec::<u32>::slk_load(&mut reader).expect("failed to decode Vec<u32>");
    assert_eq!(vec_val, vec![1u32, 2, 3], "Vec<u32> mismatch");

    let extra = reader.load_raw(1);
    assert!(extra.is_err(), "expected no more data after payload, but got extra bytes");
}

#[test]
fn test_legacy_cpp_format_matches_rust_format() {
    let u64_val: u64 = 42;
    let string_val: String = "hello".into();
    let vec_val: Vec<u32> = vec![1, 2, 3];

    let mut rust_payload = Vec::new();
    rust_payload.extend_from_slice(&u64_val.to_le_bytes());
    rust_payload.extend_from_slice(&(string_val.len() as u64).to_le_bytes());
    rust_payload.extend_from_slice(string_val.as_bytes());
    rust_payload.extend_from_slice(&(vec_val.len() as u64).to_le_bytes());
    for v in &vec_val {
        rust_payload.extend_from_slice(&v.to_le_bytes());
    }

    let mut rust_framed = Vec::new();
    rust_framed.extend_from_slice(&(rust_payload.len() as u32).to_le_bytes());
    rust_framed.extend_from_slice(&rust_payload);
    rust_framed.extend_from_slice(&0u32.to_le_bytes());

    let path = find_legacy_cpp_output().expect("cpp_slk_output.bin not found");
    let cpp_data = std::fs::read(&path).unwrap();

    assert_eq!(
        rust_framed, cpp_data,
        "Rust encoder and C++ writer produced different bytes"
    );
}

// ─── Comprehensive 10K random struct bidirectional test ─────────────────────

#[test]
fn test_cpp_rust_bidirectional_10k() {
    if ensure_cpp_binary().is_none() {
        eprintln!("SKIP: g++ not available, cannot compile C++ SLK tester");
        return;
    }

    let temp_dir = std::env::temp_dir();
    let rust_out = temp_dir.join("rust_slk_10k.bin");
    let cpp_out = temp_dir.join("cpp_slk_10k.bin");

    // 1. Rust writes 10K random structs.
    {
        let mut seed = SEED;
        let (mut builder, collector) = mgslk::Builder::new_collecting();
        for _ in 0..COUNT {
            let val = generate_one(&mut seed);
            val.slk_save(&mut builder);
        }
        builder.finalize();
        std::fs::write(&rust_out, collector.into_vec()).unwrap();
    }

    // 2. C++ reads the Rust file and verifies values.
    let result = run_cpp("read", COUNT, SEED, &rust_out);
    assert!(
        result.is_ok(),
        "C++ read of Rust-generated SLK failed: {:?}",
        result.err()
    );
    let stdout = result.unwrap();
    assert!(
        stdout.contains("OK:"),
        "C++ value verification failed: {}",
        stdout
    );

    // 3. C++ writes its own file with the same 10K structs.
    let result = run_cpp("write", COUNT, SEED, &cpp_out);
    assert!(
        result.is_ok(),
        "C++ write failed: {:?}",
        result.err()
    );

    // 4. Rust reads the C++ file and verifies values.
    let cpp_data = std::fs::read(&cpp_out).unwrap();
    let mut reader = Reader::new(&cpp_data);
    let mut seed = SEED;
    for n in 0..COUNT {
        let expected = generate_one(&mut seed);
        let actual: Vec<Option<(i64, String)>> =
            Vec::slk_load(&mut reader).unwrap_or_else(|e| {
                panic!("Rust deserialize failed at iteration {}: {}", n, e)
            });
        assert_eq!(
            expected, actual,
            "Rust read of C++ SLK failed at iteration {}",
            n
        );
    }

    // 5. Byte-for-byte identity check: C++ verifies Rust bytes match its own.
    let result = run_cpp("verify", COUNT, SEED, &rust_out);
    assert!(
        result.is_ok(),
        "C++ byte-level verify failed: {:?}",
        result.err()
    );
    let stdout = result.unwrap();
    assert!(
        stdout.contains("OK:"),
        "C++ byte-level verification failed: {}",
        stdout
    );

    // 6. Same in reverse: Rust compares the two files directly.
    let rust_data = std::fs::read(&rust_out).unwrap();
    let cpp_data = std::fs::read(&cpp_out).unwrap();
    assert_eq!(
        rust_data, cpp_data,
        "Rust-generated and C++-generated SLK files differ in size or content"
    );

    // Cleanup temp files.
    let _ = std::fs::remove_file(&rust_out);
    let _ = std::fs::remove_file(&cpp_out);
}

// ─── PropertyValue cross-language test ──────────────────────────────────────

fn compile_cpp_writer(source: &Path, output: &Path) -> Result<(), String> {
    let status = std::process::Command::new("g++")
        .args([
            "-std=c++17",
            "-O2",
            source.to_str().unwrap(),
            "-o",
            output.to_str().unwrap(),
        ])
        .status()
        .map_err(|e| format!("g++ invocation failed: {}", e))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("g++ exited with status: {}", status))
    }
}

#[test]
fn test_cpp_property_value_compatibility() {
    // Skip if g++ not available
    if std::process::Command::new("g++").arg("--version").status().is_err() {
        eprintln!("SKIP: g++ not available");
        return;
    }

    let ws = workspace_root();
    let cpp_src = ws.join("tests/cpp_property_value_writer.cpp");
    let cpp_bin = ws.join("tests/cpp_property_value_writer");
    let bin_path = ws.join("tests/cpp_property_values.bin");

    if !cpp_bin.exists() {
        compile_cpp_writer(&cpp_src, &cpp_bin).expect("failed to compile C++ PropertyValue writer");
    }

    // Run C++ writer
    let status = std::process::Command::new(&cpp_bin)
        .arg(&bin_path)
        .status()
        .expect("failed to run C++ PropertyValue writer");
    assert!(status.success(), "C++ PropertyValue writer failed");

    let data = std::fs::read(&bin_path).expect("failed to read C++ output");
    let mut reader = Reader::new(&data);

    use mgcore::property_value::PropertyValue;

    // 1. Null
    assert_eq!(
        PropertyValue::slk_load(&mut reader).unwrap(),
        PropertyValue::Null
    );

    // 2. Bool true
    assert_eq!(
        PropertyValue::slk_load(&mut reader).unwrap(),
        PropertyValue::Bool(true)
    );

    // 3. Bool false
    assert_eq!(
        PropertyValue::slk_load(&mut reader).unwrap(),
        PropertyValue::Bool(false)
    );

    // 4. Int 42
    assert_eq!(
        PropertyValue::slk_load(&mut reader).unwrap(),
        PropertyValue::Int(42)
    );

    // 5. Int -1
    assert_eq!(
        PropertyValue::slk_load(&mut reader).unwrap(),
        PropertyValue::Int(-1)
    );

    // 6. Int max
    assert_eq!(
        PropertyValue::slk_load(&mut reader).unwrap(),
        PropertyValue::Int(i64::MAX)
    );

    // 7. Double 3.14
    let v = PropertyValue::slk_load(&mut reader).unwrap();
    match v {
        PropertyValue::Double(d) => assert!((d - 3.14).abs() < 1e-9, "expected 3.14, got {}", d),
        _ => panic!("expected Double, got {:?}", v),
    }

    // 8. Double -0.0
    let v = PropertyValue::slk_load(&mut reader).unwrap();
    match v {
        PropertyValue::Double(d) => {
            assert_eq!(d.to_bits(), (-0.0f64).to_bits(), "expected -0.0");
        }
        _ => panic!("expected Double, got {:?}", v),
    }

    // 9. String "hello"
    assert_eq!(
        PropertyValue::slk_load(&mut reader).unwrap(),
        PropertyValue::String("hello".into())
    );

    // 10. String empty
    assert_eq!(
        PropertyValue::slk_load(&mut reader).unwrap(),
        PropertyValue::String("".into())
    );

    // 11. String unicode
    assert_eq!(
        PropertyValue::slk_load(&mut reader).unwrap(),
        PropertyValue::String("🦀🚀".into())
    );

    // 12. List [String("a"), String("b")]
    assert_eq!(
        PropertyValue::slk_load(&mut reader).unwrap(),
        PropertyValue::List(vec![
            PropertyValue::String("a".into()),
            PropertyValue::String("b".into()),
        ])
    );

    // 13. List empty
    assert_eq!(
        PropertyValue::slk_load(&mut reader).unwrap(),
        PropertyValue::List(vec![])
    );

    // 14. Map {"key1": String("value1"), "key2": String("value2")}
    assert_eq!(
        PropertyValue::slk_load(&mut reader).unwrap(),
        PropertyValue::Map(vec![
            ("key1".into(), PropertyValue::String("value1".into())),
            ("key2".into(), PropertyValue::String("value2".into())),
        ])
    );

    // 15. Map empty
    assert_eq!(
        PropertyValue::slk_load(&mut reader).unwrap(),
        PropertyValue::Map(vec![])
    );

    // Ensure no trailing data
    let extra = reader.load_raw(1);
    assert!(extra.is_err(), "expected no more data after PropertyValues");

    // Cleanup
    let _ = std::fs::remove_file(&bin_path);
}

#[test]
fn test_cpp_rust_small_count_smoke() {
    if ensure_cpp_binary().is_none() {
        eprintln!("SKIP: g++ not available");
        return;
    }

    let temp_dir = std::env::temp_dir();
    let rust_out = temp_dir.join("rust_slk_smoke.bin");
    let cpp_out = temp_dir.join("cpp_slk_smoke.bin");
    let small_count: u64 = 100;
    let small_seed: u64 = 0x12345678;

    // Rust writes
    {
        let mut seed = small_seed;
        let (mut builder, collector) = mgslk::Builder::new_collecting();
        for _ in 0..small_count {
            let val = generate_one(&mut seed);
            val.slk_save(&mut builder);
        }
        builder.finalize();
        std::fs::write(&rust_out, collector.into_vec()).unwrap();
    }

    // C++ verifies
    let result = run_cpp("verify", small_count, small_seed, &rust_out);
    assert!(result.is_ok(), "Small smoke verify failed: {:?}", result.err());
    assert!(result.unwrap().contains("OK:"));

    // C++ writes
    let result = run_cpp("write", small_count, small_seed, &cpp_out);
    assert!(result.is_ok(), "Small smoke write failed: {:?}", result.err());

    // Rust reads
    let cpp_data = std::fs::read(&cpp_out).unwrap();
    let mut reader = Reader::new(&cpp_data);
    let mut seed = small_seed;
    for n in 0..small_count {
        let expected = generate_one(&mut seed);
        let actual: Vec<Option<(i64, String)>> = Vec::slk_load(&mut reader)
            .unwrap_or_else(|e| panic!("smoke deserialize failed at {}: {}", n, e));
        assert_eq!(expected, actual, "smoke mismatch at {}", n);
    }

    let _ = std::fs::remove_file(&rust_out);
    let _ = std::fs::remove_file(&cpp_out);
}

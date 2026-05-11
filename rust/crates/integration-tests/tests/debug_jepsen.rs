use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

#[test]
fn debug_replication2() {
    let mut path = std::env::current_exe().unwrap();
    path.pop(); path.pop();
    path.push("memgraph-rs");
    if !path.exists() {
        eprintln!("binary not found at {:?}", path);
        return;
    }
    let main_data = tempfile::tempdir().unwrap();
    let replica_data = tempfile::tempdir().unwrap();

    // Write via CLI (no --bolt-port 0)
    for i in 1..=3 {
        let out = Command::new(&path)
            .args(["--data-directory", main_data.path().to_str().unwrap(), "--query", &format!("CREATE (:Counter {{id: 0, val: {}}})", i)])
            .output().unwrap();
        println!("WRITE {} stdout: {:?}", i, String::from_utf8_lossy(&out.stdout));
        println!("WRITE {} stderr: {:?}", i, String::from_utf8_lossy(&out.stderr));
    }

    // Read back via CLI before daemon
    let out = Command::new(&path)
        .args(["--data-directory", main_data.path().to_str().unwrap(), "--query", "MATCH (n:Counter {id: 0}) RETURN n.val AS v"])
        .output().unwrap();
    println!("PRE-DAEMON stdout: {:?}", String::from_utf8_lossy(&out.stdout));

    // Check if snapshot file exists
    let snap = main_data.path().join("snapshot.mgsnap");
    println!("snapshot exists: {}  size: {:?}", snap.exists(), snap.metadata().map(|m| m.len()));

    // Start main daemon
    let mut main = Command::new(&path)
        .args(["--data-directory", main_data.path().to_str().unwrap(), "--replication-role", "main", "--replication-port", "10198", "--bolt-port", "0"])
        .stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    thread::sleep(Duration::from_millis(1000));

    // Read from main daemon via CLI on SAME data dir (THIS IS THE PROBLEM - conflict!)
    let out = Command::new(&path)
        .args(["--data-directory", main_data.path().to_str().unwrap(), "--query", "MATCH (n:Counter {id: 0}) RETURN n.val AS v"])
        .output().unwrap();
    println!("MAIN-DAEMON-CONFLICT stdout: {:?}", String::from_utf8_lossy(&out.stdout));

    // Start replica
    let mut replica = Command::new(&path)
        .args(["--data-directory", replica_data.path().to_str().unwrap(), "--replication-role", "replica", "--replica-of", "127.0.0.1:10198", "--bolt-port", "0"])
        .stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    thread::sleep(Duration::from_millis(3000));

    // Read from replica
    let out = Command::new(&path)
        .args(["--data-directory", replica_data.path().to_str().unwrap(), "--query", "MATCH (n:Counter {id: 0}) RETURN n.val AS v"])
        .output().unwrap();
    println!("REPLICA stdout: {:?}", String::from_utf8_lossy(&out.stdout));

    main.kill().unwrap();
    replica.kill().unwrap();
    let _ = main.wait();
    let _ = replica.wait();
}

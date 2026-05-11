//! Data import from JSONL, CSV, and Parquet files.

use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;

use serde_json::Value as JsonValue;
use mginterp::execute;
use mgstorage::storage::Storage;

/// Import JSONL file: each line is a JSON object with `type`, `labels`, `properties`.
pub fn import_jsonl(storage: &Storage, path: &Path) -> Result<usize, String> {
    let file = fs::File::open(path).map_err(|e| format!("{}", e))?;
    let reader = BufReader::new(file);
    let mut count = 0;
    for line in reader.lines() {
        let line = line.map_err(|e| format!("{}", e))?;
        if line.trim().is_empty() { continue; }
        let obj: JsonValue = serde_json::from_str(&line).map_err(|e| format!("{}", e))?;
        if let Some(query) = json_to_create(&obj) {
            execute(storage, &query).map_err(|e| format!("{}", e))?;
            count += 1;
        }
    }
    Ok(count)
}

/// Import CSV file with header. Columns map to properties.
pub fn import_csv(storage: &Storage, path: &Path, label: &str) -> Result<usize, String> {
    let file = fs::File::open(path).map_err(|e| format!("{}", e))?;
    let mut reader = csv_reader(file);
    let mut count = 0;

    // Read header
    let header_line = reader.next().ok_or("empty CSV")?.map_err(|e| format!("{}", e))?;
    let headers: Vec<String> = header_line.split(',').map(|s| s.trim().to_string()).collect();

    for line in reader {
        let line = line.map_err(|e| format!("{}", e))?;
        let values: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
        let mut props = String::new();
        for (i, val) in values.iter().enumerate() {
            if i >= headers.len() { break; }
            if !props.is_empty() { props.push_str(", "); }
            let parsed = if val.parse::<f64>().is_ok() {
                val.clone()
            } else {
                format!("\"{}\"", val)
            };
            props.push_str(&format!("{}: {}", headers[i], parsed));
        }
        let query = if props.is_empty() {
            format!("CREATE (n:{})", label)
        } else {
            format!("CREATE (n:{} {{{}}})", label, props)
        };
        execute(storage, &query).map_err(|e| format!("{}", e))?;
        count += 1;
    }
    Ok(count)
}

/// Import edges from a CSV with columns: from_id, to_id, edge_type, [properties...]
pub fn import_edge_csv(storage: &Storage, path: &Path) -> Result<usize, String> {
    let file = fs::File::open(path).map_err(|e| format!("{}", e))?;
    let mut reader = csv_reader(file);
    let mut count = 0;

    let header_line = reader.next().ok_or("empty CSV")?.map_err(|e| format!("{}", e))?;
    let headers: Vec<String> = header_line.split(',').map(|s| s.trim().to_string()).collect();

    for line in reader {
        let line = line.map_err(|e| format!("{}", e))?;
        let values: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
        if values.len() < 3 { continue; }
        let from_id = &values[0];
        let to_id = &values[1];
        let edge_type = &values[2];

        let mut props = String::new();
        for (i, val) in values.iter().enumerate().skip(3) {
            if i >= headers.len() { break; }
            if !props.is_empty() { props.push_str(", "); }
            let parsed = if val.parse::<f64>().is_ok() {
                val.clone()
            } else {
                format!("\"{}\"", val)
            };
            props.push_str(&format!("{}: {}", headers[i], parsed));
        }

        let query = if props.is_empty() {
            format!("MATCH (a {{id: \"{}\"}}), (b {{id: \"{}\"}}) CREATE (a)-[:{}]->(b)", from_id, to_id, edge_type)
        } else {
            format!("MATCH (a {{id: \"{}\"}}), (b {{id: \"{}\"}}) CREATE (a)-[:{} {{{}}}]->(b)", from_id, to_id, edge_type, props)
        };
        execute(storage, &query).map_err(|e| format!("{}", e))?;
        count += 1;
    }
    Ok(count)
}

/// Import progress callback type.
pub type ImportProgressCallback = Box<dyn Fn(usize) + Send>;

/// Batch-import vertices from CSV with periodic progress callbacks.
pub fn import_csv_with_progress(
    storage: &Storage,
    path: &Path,
    label: &str,
    batch_size: usize,
    progress: Option<ImportProgressCallback>,
) -> Result<usize, String> {
    let file = fs::File::open(path).map_err(|e| format!("{}", e))?;
    let mut reader = csv_reader(file);
    let mut count = 0;

    let header_line = reader.next().ok_or("empty CSV")?.map_err(|e| format!("{}", e))?;
    let headers: Vec<String> = header_line.split(',').map(|s| s.trim().to_string()).collect();

    for line in reader {
        let line = line.map_err(|e| format!("{}", e))?;
        if line.trim().is_empty() { continue; }
        let values: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
        let mut props = String::new();
        for (i, val) in values.iter().enumerate() {
            if i >= headers.len() { break; }
            if !props.is_empty() { props.push_str(", "); }
            let parsed = if val.parse::<f64>().is_ok() {
                val.clone()
            } else {
                format!("\"{}\"", val)
            };
            props.push_str(&format!("{}: {}", headers[i], parsed));
        }
        let query = if props.is_empty() {
            format!("CREATE (n:{})", label)
        } else {
            format!("CREATE (n:{} {{{}}})", label, props)
        };
        execute(storage, &query).map_err(|e| format!("{}", e))?;
        count += 1;

        if batch_size > 0 && count % batch_size == 0 {
            if let Some(ref cb) = progress {
                cb(count);
            }
        }
    }
    Ok(count)
}

fn csv_reader(file: fs::File) -> impl Iterator<Item = Result<String, std::io::Error>> {
    BufReader::new(file).lines()
}

/// Try Parquet import if arrow feature is enabled.
#[cfg(feature = "parquet")]
pub fn import_parquet(storage: &Storage, path: &Path, label: &str) -> Result<usize, String> {
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let file = fs::File::open(path).map_err(|e| format!("{}", e))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("{}", e))?;
    let reader = builder.build().map_err(|e| format!("{}", e))?;
    let mut count = 0;
    for batch in reader {
        let batch = batch.map_err(|e| format!("{}", e))?;
        let schema = batch.schema();
        for row in 0..batch.num_rows() {
            let mut props = String::new();
            for (i, field) in schema.fields().iter().enumerate() {
                let col = batch.column(i);
                let val = format_arrow_value(col, row);
                if !props.is_empty() { props.push_str(", "); }
                props.push_str(&format!("{}: {}", field.name(), val));
            }
            let query = format!("CREATE (n:{} {{{}}})", label, props);
            execute(storage, &query).map_err(|e| format!("{}", e))?;
            count += 1;
        }
    }
    Ok(count)
}

#[cfg(feature = "parquet")]
fn format_arrow_value(col: &dyn arrow::array::Array, row: usize) -> String {
    use arrow::array::*;
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        return format!("\"{}\"", a.value(row));
    }
    "null".to_string()
}

fn json_to_create(obj: &JsonValue) -> Option<String> {
    let typ = obj.get("type").and_then(|v| v.as_str())?;
    match typ {
        "node" | "vertex" => {
            let label = obj.get("label").and_then(|v| v.as_str()).unwrap_or("Node");
            let props = obj.get("properties").and_then(|v| v.as_object())
                .map(|m| {
                    let parts: Vec<String> = m.iter().map(|(k, v)| {
                        match v {
                            JsonValue::String(s) => format!("{}: \"{}\"", k, s),
                            JsonValue::Number(n) => format!("{}: {}", k, n),
                            JsonValue::Bool(b) => format!("{}: {}", k, b),
                            _ => format!("{}: null", k),
                        }
                    }).collect();
                    format!("{{{}}}", parts.join(", "))
                })
                .unwrap_or_default();
            Some(format!("CREATE (n:{} {})", label, props))
        }
        "edge" | "relationship" => {
            let from = obj.get("from").and_then(|v| v.as_str())?;
            let to = obj.get("to").and_then(|v| v.as_str())?;
            let etype = obj.get("edge_type").and_then(|v| v.as_str()).unwrap_or("REL");
            Some(format!("MATCH (a {{key: \"{}\"}}), (b {{key: \"{}\"}}) CREATE (a)-[:{}]->(b)", from, to, etype))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_import_jsonl() {
        let storage = Storage::new();
        let tmp = "/tmp/mg_import_test.jsonl";
        let mut f = fs::File::create(tmp).unwrap();
        writeln!(f, r#"{{"type":"node","label":"Person","properties":{{"name":"Alice","age":30}}}}"#).unwrap();
        let count = import_jsonl(&storage, Path::new(tmp)).unwrap();
        assert!(count > 0);
        fs::remove_file(tmp).ok();
    }

    #[test]
    fn test_json_to_create_node() {
        let obj: JsonValue = serde_json::from_str(r#"{"type":"node","label":"Person","properties":{"name":"Alice"}}"#).unwrap();
        let q = json_to_create(&obj).unwrap();
        assert!(q.contains("CREATE"));
        assert!(q.contains("Person"));
    }

    #[test]
    fn test_json_to_create_edge() {
        let obj: JsonValue = serde_json::from_str(r#"{"type":"edge","from":"a","to":"b","edge_type":"KNOWS"}"#).unwrap();
        let q = json_to_create(&obj).unwrap();
        assert!(q.contains("MATCH"));
        assert!(q.contains("KNOWS"));
    }

    #[test]
    fn test_import_csv() {
        let storage = Storage::new();
        let tmp = "/tmp/mg_import_test.csv";
        let mut f = fs::File::create(tmp).unwrap();
        writeln!(f, "name,age").unwrap();
        writeln!(f, "Alice,30").unwrap();
        writeln!(f, "Bob,25").unwrap();
        let count = import_csv(&storage, Path::new(tmp), "Person").unwrap();
        assert_eq!(count, 2);
        fs::remove_file(tmp).ok();
    }

    #[test]
    fn test_import_edge_csv() {
        let storage = Storage::new();
        let tmp = "/tmp/mg_import_edge_test.csv";
        let mut f = fs::File::create(tmp).unwrap();
        writeln!(f, "from_id,to_id,edge_type,since").unwrap();
        writeln!(f, "a,b,KNOWS,2020").unwrap();
        let count = import_edge_csv(&storage, Path::new(tmp)).unwrap();
        assert_eq!(count, 1);
        fs::remove_file(tmp).ok();
    }

    #[test]
    fn test_import_csv_with_progress() {
        let storage = Storage::new();
        let tmp = "/tmp/mg_import_progress_test.csv";
        let mut f = fs::File::create(tmp).unwrap();
        writeln!(f, "name,age").unwrap();
        writeln!(f, "Alice,30").unwrap();
        writeln!(f, "Bob,25").unwrap();
        writeln!(f, "Charlie,35").unwrap();

        let progress_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let cb_progress = progress_calls.clone();
        let cb: ImportProgressCallback = Box::new(move |_count| {
            cb_progress.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        });
        let count = import_csv_with_progress(&storage, Path::new(tmp), "Person", 2, Some(cb)).unwrap();
        assert_eq!(count, 3);
        assert!(progress_calls.load(std::sync::atomic::Ordering::Relaxed) >= 1);
        fs::remove_file(tmp).ok();
    }
}

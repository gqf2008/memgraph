//! Bulk import utilities for high-throughput graph ingestion.
//!
//! Supports line-delimited edge lists and simple CSV-like formats.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read};

use mgcore::delta::IsolationLevel;
use mgcore::property_value::PropertyValue;
use mgcore::types::{EdgeTypeId, Gid, LabelId, PropertyId};

use crate::storage::{Storage, StorageError};

/// Parse a string into the best-fitting PropertyValue.
pub fn parse_property_value(s: &str) -> PropertyValue {
    let trimmed = s.trim();
    if trimmed.eq_ignore_ascii_case("null") || trimmed.is_empty() {
        return PropertyValue::Null;
    }
    if let Ok(n) = trimmed.parse::<i64>() {
        return PropertyValue::Int(n);
    }
    if let Ok(f) = trimmed.parse::<f64>() {
        return PropertyValue::Double(f);
    }
    if trimmed.eq_ignore_ascii_case("true") {
        return PropertyValue::Bool(true);
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return PropertyValue::Bool(false);
    }
    PropertyValue::String(trimmed.to_string())
}

/// Parse a simple CSV line (no quoted fields, comma-separated).
fn parse_csv_line(line: &str) -> Vec<&str> {
    line.split(',').map(|s| s.trim()).collect()
}

/// Bulk import vertices from a simple CSV reader.
/// First row is headers; each subsequent row becomes one vertex with the given label.
/// Column values are auto-detected as PropertyValues.
/// Returns the number of vertices created.
pub fn import_vertices_csv(
    storage: &Storage,
    label: LabelId,
    reader: &mut dyn Read,
) -> Result<usize, StorageError> {
    let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    let buf = BufReader::new(reader);
    let mut lines = buf.lines();

    let header_line = lines
        .next()
        .transpose()
        .map_err(|e| StorageError::ConstraintViolation(format!("csv header: {}", e)))?;
    let headers = header_line
        .as_deref()
        .map(parse_csv_line)
        .unwrap_or_default();

    let mut count = 0usize;
    for line in lines {
        let line =
            line.map_err(|e| StorageError::ConstraintViolation(format!("csv record: {}", e)))?;
        let fields = parse_csv_line(&line);
        let gid = storage.allocate_gid();
        storage.create_vertex(&tx, gid)?;
        storage.vertex_add_label(&tx, gid, label)?;

        for (i, field) in fields.iter().enumerate() {
            if i < headers.len() {
                let prop_id = PropertyId::from(i as u32);
                let value = parse_property_value(field);
                storage.vertex_set_property(&tx, gid, prop_id, value)?;
            }
        }
        count += 1;
    }

    storage.commit_transaction(&tx);
    Ok(count)
}

/// Bulk import edges from a CSV with `from`, `to`, and optional `type` columns.
/// First row is headers.
pub fn import_edges_csv(
    storage: &Storage,
    reader: &mut dyn Read,
    edge_type: EdgeTypeId,
) -> Result<usize, StorageError> {
    let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    let buf = BufReader::new(reader);
    let mut lines = buf.lines();

    let header_line = lines
        .next()
        .transpose()
        .map_err(|e| StorageError::ConstraintViolation(format!("csv header: {}", e)))?;
    let headers = header_line
        .as_deref()
        .map(parse_csv_line)
        .unwrap_or_default();

    let from_col = headers
        .iter()
        .position(|h| h.eq_ignore_ascii_case("from"))
        .unwrap_or(0);
    let to_col = headers
        .iter()
        .position(|h| h.eq_ignore_ascii_case("to"))
        .unwrap_or(1);

    let mut count = 0usize;
    for line in lines {
        let line =
            line.map_err(|e| StorageError::ConstraintViolation(format!("csv record: {}", e)))?;
        let fields = parse_csv_line(&line);
        if fields.len() < 2 {
            continue;
        }
        let from_str = fields.get(from_col).unwrap_or(&"0");
        let to_str = fields.get(to_col).unwrap_or(&"0");
        let from_gid = Gid::from(from_str.parse::<u64>().unwrap_or(0));
        let to_gid = Gid::from(to_str.parse::<u64>().unwrap_or(0));

        let edge_gid = storage.allocate_gid();
        storage.create_edge(&tx, edge_gid, from_gid, to_gid, edge_type)?;
        count += 1;
    }

    storage.commit_transaction(&tx);
    Ok(count)
}

/// Import vertices from a simple JSONL-like format:
/// Each line: `gid=<n> label=<n> prop0=val0 prop1=val1 ...`
pub fn import_vertices_jsonl(
    storage: &Storage,
    reader: &mut dyn Read,
) -> Result<usize, StorageError> {
    let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    let buf = BufReader::new(reader);
    let mut count = 0usize;

    for line in buf.lines() {
        let line = line.map_err(|e| StorageError::ConstraintViolation(format!("io: {}", e)))?;
        if line.trim().is_empty() {
            continue;
        }

        let mut gid = None;
        let mut label = None;
        let mut props = HashMap::new();

        for token in line.split_whitespace() {
            if let Some(rest) = token.strip_prefix("gid=") {
                gid = rest.parse::<u64>().ok().map(Gid::from);
            } else if let Some(rest) = token.strip_prefix("label=") {
                label = rest.parse::<u64>().ok().map(|n| LabelId::from(n as u32));
            } else if let Some(eq_pos) = token.find('=') {
                let key = &token[..eq_pos];
                let val = &token[eq_pos + 1..];
                if let Ok(prop_idx) = key.parse::<u32>() {
                    let prop_id = PropertyId::from(prop_idx);
                    props.insert(prop_id, parse_property_value(val));
                }
            }
        }

        let gid = gid.unwrap_or_else(|| storage.allocate_gid());
        storage.create_vertex(&tx, gid)?;

        if let Some(l) = label {
            storage.vertex_add_label(&tx, gid, l)?;
        }

        for (prop_id, value) in props {
            storage.vertex_set_property(&tx, gid, prop_id, value)?;
        }

        count += 1;
    }

    storage.commit_transaction(&tx);
    Ok(count)
}

/// Edge list format: `from_gid to_gid` per line (space-separated).
pub fn import_edge_list(
    storage: &Storage,
    reader: &mut dyn Read,
    edge_type: EdgeTypeId,
) -> Result<usize, StorageError> {
    let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    let buf = BufReader::new(reader);
    let mut count = 0usize;

    for line in buf.lines() {
        let line = line.map_err(|e| StorageError::ConstraintViolation(format!("io: {}", e)))?;
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 2 {
            continue;
        }
        let from_gid = Gid::from(parts[0].parse::<u64>().unwrap_or(0));
        let to_gid = Gid::from(parts[1].parse::<u64>().unwrap_or(0));
        // Auto-create missing vertices
        if storage.get_vertex(from_gid, &tx).is_none() {
            storage.create_vertex(&tx, from_gid)?;
        }
        if storage.get_vertex(to_gid, &tx).is_none() {
            storage.create_vertex(&tx, to_gid)?;
        }
        let edge_gid = storage.allocate_gid();
        storage.create_edge(&tx, edge_gid, from_gid, to_gid, edge_type)?;
        count += 1;
    }

    storage.commit_transaction(&tx);
    Ok(count)
}

/// Streaming bulk loader that batches inserts for maximum throughput.
pub struct StreamingBulkLoader {
    batch_size: usize,
    vertex_buffer: Vec<(Gid, LabelId, HashMap<PropertyId, PropertyValue>)>,
    edge_buffer: Vec<(Gid, Gid, Gid, EdgeTypeId)>,
}

impl StreamingBulkLoader {
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            vertex_buffer: Vec::with_capacity(batch_size),
            edge_buffer: Vec::with_capacity(batch_size),
        }
    }

    pub fn queue_vertex(
        &mut self,
        gid: Gid,
        label: LabelId,
        props: HashMap<PropertyId, PropertyValue>,
    ) {
        self.vertex_buffer.push((gid, label, props));
    }

    pub fn queue_edge(&mut self, gid: Gid, from: Gid, to: Gid, edge_type: EdgeTypeId) {
        self.edge_buffer.push((gid, from, to, edge_type));
    }

    pub fn flush(&mut self, storage: &Storage) -> Result<(usize, usize), StorageError> {
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let mut v_count = 0usize;
        let mut e_count = 0usize;

        for (gid, label, props) in self.vertex_buffer.drain(..) {
            storage.create_vertex(&tx, gid)?;
            storage.vertex_add_label(&tx, gid, label)?;
            for (prop_id, value) in props {
                storage.vertex_set_property(&tx, gid, prop_id, value)?;
            }
            v_count += 1;
        }

        for (gid, from, to, etype) in self.edge_buffer.drain(..) {
            storage.create_edge(&tx, gid, from, to, etype)?;
            e_count += 1;
        }

        storage.commit_transaction(&tx);
        Ok((v_count, e_count))
    }

    pub fn should_flush(&self) -> bool {
        self.vertex_buffer.len() >= self.batch_size || self.edge_buffer.len() >= self.batch_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mgcore::types::{EdgeTypeId, Gid, LabelId};

    #[test]
    fn test_parse_property_value() {
        assert_eq!(parse_property_value("42"), PropertyValue::Int(42));
        assert_eq!(parse_property_value("3.14"), PropertyValue::Double(3.14));
        assert_eq!(parse_property_value("true"), PropertyValue::Bool(true));
        assert_eq!(
            parse_property_value("hello"),
            PropertyValue::String("hello".into())
        );
        assert_eq!(parse_property_value("null"), PropertyValue::Null);
    }

    #[test]
    fn test_parse_csv_line() {
        let line = "a, b, c";
        let fields = parse_csv_line(line);
        assert_eq!(fields, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_streaming_bulk_loader() {
        let storage = Storage::new();
        let mut loader = StreamingBulkLoader::new(10);
        let mut props = HashMap::new();
        props.insert(
            PropertyId::from(0u32),
            PropertyValue::String("Alice".into()),
        );
        loader.queue_vertex(Gid::from(1u64), LabelId::from(0u32), props);
        let mut props2 = HashMap::new();
        props2.insert(PropertyId::from(0u32), PropertyValue::String("Bob".into()));
        loader.queue_vertex(Gid::from(2u64), LabelId::from(0u32), props2);
        loader.queue_edge(
            Gid::from(100u64),
            Gid::from(1u64),
            Gid::from(2u64),
            EdgeTypeId::from(0u32),
        );
        let (v, e) = loader.flush(&storage).unwrap();
        assert_eq!(v, 2);
        assert_eq!(e, 1);
    }

    #[test]
    fn test_import_edge_list() {
        let storage = Storage::new();
        let data = b"1 2\n2 3\n3 1\n";
        let count = import_edge_list(&storage, &mut &data[..], EdgeTypeId::from(0u32)).unwrap();
        assert_eq!(count, 3);
    }
}

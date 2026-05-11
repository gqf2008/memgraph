//! CSV file parser and writer for Memgraph graph imports/exports.
//! Equivalent to C++ `src/csv/`.

use std::collections::HashMap;
use std::io::{Read, Write};

use mgcore::property_value::PropertyValue;

/// Parsed CSV header info.
#[derive(Clone, Debug)]
pub struct CsvHeader {
    pub columns: Vec<String>,
    pub has_headers: bool,
}

/// CSV row iterator that yields rows as property maps.
pub struct CsvReader<R: Read> {
    reader: csv::Reader<R>,
    headers: Option<Vec<String>>,
}

impl<R: Read> CsvReader<R> {
    pub fn new(reader: R, has_headers: bool) -> Self {
        Self {
            reader: csv::ReaderBuilder::new()
                .has_headers(false)
                .flexible(true)
                .from_reader(reader),
            headers: None,
        }
    }

    pub fn with_delimiter(reader: R, has_headers: bool, delimiter: u8) -> Self {
        Self {
            reader: csv::ReaderBuilder::new()
                .has_headers(false)
                .flexible(true)
                .delimiter(delimiter)
                .from_reader(reader),
            headers: None,
        }
    }

    /// Read and store the header row.
    pub fn read_header(&mut self) -> Result<CsvHeader, String> {
        let mut record = csv::StringRecord::new();
        if self.reader.read_record(&mut record).map_err(|e| e.to_string())? {
            let cols: Vec<String> = record.iter().map(|s| s.to_string()).collect();
            self.headers = Some(cols.clone());
            Ok(CsvHeader { columns: cols, has_headers: true })
        } else {
            Ok(CsvHeader { columns: vec![], has_headers: false })
        }
    }

    /// Read next row as a PropertyValue::Map.
    pub fn next_row(&mut self) -> Result<Option<HashMap<String, PropertyValue>>, String> {
        let mut record = csv::StringRecord::new();
        if self.reader.read_record(&mut record).map_err(|e| e.to_string())? {
            let headers = self.headers.as_ref();
            let map: HashMap<String, PropertyValue> = record
                .iter()
                .enumerate()
                .map(|(i, val)| {
                    let key = headers
                        .and_then(|h| h.get(i).cloned())
                        .unwrap_or_else(|| format!("column_{}", i));
                    (key, PropertyValue::String(val.to_string()))
                })
                .collect();
            Ok(Some(map))
        } else {
            Ok(None)
        }
    }

    /// Read next row and attempt type inference for each cell.
    pub fn next_row_typed(&mut self) -> Result<Option<HashMap<String, PropertyValue>>, String> {
        let mut record = csv::StringRecord::new();
        if self.reader.read_record(&mut record).map_err(|e| e.to_string())? {
            let headers = self.headers.as_ref();
            let map: HashMap<String, PropertyValue> = record
                .iter()
                .enumerate()
                .map(|(i, val)| {
                    let key = headers
                        .and_then(|h| h.get(i).cloned())
                        .unwrap_or_else(|| format!("column_{}", i));
                    (key, infer_type(val))
                })
                .collect();
            Ok(Some(map))
        } else {
            Ok(None)
        }
    }

    /// Read all rows and return as Vec of PropertyValue::Map.
    pub fn read_all(&mut self) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
        let mut rows = Vec::new();
        while let Some(row) = self.next_row()? {
            rows.push(row);
        }
        Ok(rows)
    }

    /// Read all rows with type inference.
    pub fn read_all_typed(&mut self) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
        let mut rows = Vec::new();
        while let Some(row) = self.next_row_typed()? {
            rows.push(row);
        }
        Ok(rows)
    }

    /// Process rows in chunks, calling `f` for each chunk.
    pub fn read_chunks<F>(
        &mut self,
        chunk_size: usize,
        mut f: F,
    ) -> Result<usize, String>
    where
        F: FnMut(Vec<HashMap<String, PropertyValue>>) -> Result<(), String>,
    {
        let mut total = 0usize;
        let mut chunk = Vec::with_capacity(chunk_size);
        while let Some(row) = self.next_row()? {
            chunk.push(row);
            if chunk.len() >= chunk_size {
                total += chunk.len();
                f(chunk)?;
                chunk = Vec::with_capacity(chunk_size);
            }
        }
        if !chunk.is_empty() {
            total += chunk.len();
            f(chunk)?;
        }
        Ok(total)
    }
}

/// Infer the best PropertyValue type for a string.
/// Supports: null, int, double, bool, date, datetime, time, list, string.
fn infer_type(s: &str) -> PropertyValue {
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
    // Try date/time formats
    if let Some(dt) = try_parse_date(trimmed) {
        return dt;
    }
    // Try list format: [1, 2, 3] or ["a", "b"]
    if trimmed.starts_with('[') && trimmed.ends_with(']') {
        if let Some(list) = try_parse_list(trimmed) {
            return PropertyValue::List(list);
        }
    }
    PropertyValue::String(trimmed.to_string())
}

/// Try to parse a date/datetime/time string.
fn try_parse_date(s: &str) -> Option<PropertyValue> {
    // ISO 8601 date: 2024-01-15
    if s.len() == 10 && s.chars().nth(4) == Some('-') && s.chars().nth(7) == Some('-') {
        if let Ok(naive) = s.parse::<chrono::NaiveDate>() {
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let days = naive.signed_duration_since(epoch).num_days();
            return Some(PropertyValue::Date(mgcore::temporal::Date::from_days(days)));
        }
    }
    // ISO 8601 datetime: 2024-01-15T10:30:00 or with timezone
    if s.len() >= 19 && s.chars().nth(10) == Some('T') {
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
            let us = dt.timestamp_micros();
            let offset = dt.offset().local_minus_utc() / 60;
            return Some(PropertyValue::ZonedDateTime(mgcore::temporal::ZonedDateTime::new(
                us, offset as i16, "UTC".into()
            )));
        }
        if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
            let us = naive.and_utc().timestamp_micros();
            return Some(PropertyValue::ZonedDateTime(mgcore::temporal::ZonedDateTime::new(
                us, 0, "UTC".into()
            )));
        }
    }
    // Time only: 10:30:00 or 10:30
    if s.len() >= 5 && s.chars().nth(2) == Some(':') {
        if let Ok(naive) = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S") {
            let midnight = chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap();
            let us = naive.signed_duration_since(midnight).num_microseconds().unwrap_or(0);
            return Some(PropertyValue::LocalTime(mgcore::temporal::LocalTime::from_microseconds(us)));
        }
        if let Ok(naive) = chrono::NaiveTime::parse_from_str(s, "%H:%M") {
            let midnight = chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap();
            let us = naive.signed_duration_since(midnight).num_microseconds().unwrap_or(0);
            return Some(PropertyValue::LocalTime(mgcore::temporal::LocalTime::from_microseconds(us)));
        }
    }
    None
}

/// Try to parse a list literal string like `[1, 2, 3]` or `["a", "b"]`.
fn try_parse_list(s: &str) -> Option<Vec<PropertyValue>> {
    let inner = s.strip_prefix('[')?.strip_suffix(']')?.trim();
    if inner.is_empty() {
        return Some(vec![]);
    }
    let mut items = Vec::new();
    let mut current = String::new();
    let mut depth = 0;
    let mut in_quote = false;
    let mut quote_char = '\0';
    let mut chars = inner.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' | '\'' if !in_quote => {
                in_quote = true;
                quote_char = c;
            }
            c if in_quote && c == quote_char => {
                in_quote = false;
            }
            '[' if !in_quote => {
                depth += 1;
                current.push(c);
            }
            ']' if !in_quote => {
                depth -= 1;
                current.push(c);
            }
            ',' if !in_quote && depth == 0 => {
                let trimmed = current.trim();
                let item = if trimmed.starts_with('"') || trimmed.starts_with('\'') {
                    // Strip quotes for string items
                    let stripped = trimmed.strip_prefix(quote_char).and_then(|s| s.strip_suffix(quote_char)).unwrap_or(trimmed);
                    PropertyValue::String(stripped.to_string())
                } else {
                    infer_type(trimmed)
                };
                items.push(item);
                current.clear();
            }
            _ => current.push(c),
        }
    }
    if !current.is_empty() {
        let trimmed = current.trim();
        let item = if trimmed.starts_with('"') || trimmed.starts_with('\'') {
            let stripped = trimmed.strip_prefix(quote_char).and_then(|s| s.strip_suffix(quote_char)).unwrap_or(trimmed);
            PropertyValue::String(stripped.to_string())
        } else {
            infer_type(trimmed)
        };
        items.push(item);
    }
    Some(items)
}

// ─── CSV Writer ───────────────────────────────────────────────────────────

/// Write query results or graph data to CSV.
pub struct CsvWriter<W: Write> {
    writer: csv::Writer<W>,
}

impl<W: Write> CsvWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer: csv::Writer::from_writer(writer),
        }
    }

    /// Write a header row.
    pub fn write_header(&mut self, columns: &[&str]) -> Result<(), String> {
        self.writer.write_record(columns).map_err(|e| e.to_string())
    }

    /// Write a single data row from a HashMap.
    pub fn write_row(&mut self, columns: &[&str], row: &HashMap<String, PropertyValue>) -> Result<(), String> {
        let record: Vec<String> = columns
            .iter()
            .map(|col| {
                row.get(*col)
                    .map(property_value_to_string)
                    .unwrap_or_default()
            })
            .collect();
        self.writer.write_record(&record).map_err(|e| e.to_string())
    }

    /// Write a row from a Vec of PropertyValues.
    pub fn write_values(&mut self, values: &[PropertyValue]) -> Result<(), String> {
        let record: Vec<String> = values.iter().map(property_value_to_string).collect();
        self.writer.write_record(&record).map_err(|e| e.to_string())
    }

    /// Flush the underlying writer.
    pub fn flush(&mut self) -> Result<(), String> {
        self.writer.flush().map_err(|e| e.to_string())
    }
}

fn property_value_to_string(v: &PropertyValue) -> String {
    match v {
        PropertyValue::Null => "null".to_string(),
        PropertyValue::Bool(b) => b.to_string(),
        PropertyValue::Int(i) => i.to_string(),
        PropertyValue::Double(f) => f.to_string(),
        PropertyValue::String(s) => s.clone(),
        PropertyValue::List(list) => {
            let items: Vec<String> = list.iter().map(property_value_to_string).collect();
            format!("[{}]", items.join(", "))
        }
        PropertyValue::Map(map) => {
            let items: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{}: {}", k, property_value_to_string(v)))
                .collect();
            format!("{{{}}}", items.join(", "))
        }
        _ => format!("{:?}", v),
    }
}

// ─── Convenience functions ────────────────────────────────────────────────

/// Parse CSV content from a string.
pub fn parse_csv_string(
    content: &str,
    has_headers: bool,
) -> Result<(Option<CsvHeader>, Vec<HashMap<String, PropertyValue>>), String> {
    let mut reader = CsvReader::new(content.as_bytes(), has_headers);
    let header = if has_headers {
        Some(reader.read_header()?)
    } else {
        None
    };
    let rows = reader.read_all()?;
    Ok((header, rows))
}

/// Parse CSV from a file path.
pub fn parse_csv_file(
    path: &str,
    has_headers: bool,
) -> Result<(Option<CsvHeader>, Vec<HashMap<String, PropertyValue>>), String> {
    let file = std::fs::File::open(path).map_err(|e| format!("open {}: {}", path, e))?;
    let mut reader = CsvReader::new(file, has_headers);
    let header = if has_headers {
        Some(reader.read_header()?)
    } else {
        None
    };
    let rows = reader.read_all()?;
    Ok((header, rows))
}

/// Write rows to a CSV string.
pub fn write_csv_string(
    columns: &[&str],
    rows: &[HashMap<String, PropertyValue>],
) -> Result<String, String> {
    let mut buf = Vec::new();
    {
        let mut writer = CsvWriter::new(&mut buf);
        writer.write_header(columns)?;
        for row in rows {
            writer.write_row(columns, row)?;
        }
        writer.flush()?;
    }
    String::from_utf8(buf).map_err(|e| format!("invalid UTF-8: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_csv_with_headers() {
        let csv = "name,age,city\nAlice,30,London\nBob,25,Paris\n";
        let (header, rows) = parse_csv_string(csv, true).unwrap();

        let h = header.unwrap();
        assert_eq!(h.columns, vec!["name", "age", "city"]);
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].get("name").unwrap(),
            &PropertyValue::String("Alice".into())
        );
        assert_eq!(
            rows[1].get("city").unwrap(),
            &PropertyValue::String("Paris".into())
        );
    }

    #[test]
    fn test_parse_csv_no_headers() {
        let csv = "Alice,30,London\nBob,25,Paris\n";
        let (header, rows) = parse_csv_string(csv, false).unwrap();
        assert!(header.is_none());
        assert_eq!(rows.len(), 2);
        assert!(rows[0].contains_key("column_0"));
    }

    #[test]
    fn test_type_inference() {
        assert_eq!(infer_type("42"), PropertyValue::Int(42));
        assert_eq!(infer_type("3.14"), PropertyValue::Double(3.14));
        assert_eq!(infer_type("true"), PropertyValue::Bool(true));
        assert_eq!(infer_type("false"), PropertyValue::Bool(false));
        assert_eq!(infer_type("null"), PropertyValue::Null);
        assert_eq!(infer_type("hello"), PropertyValue::String("hello".into()));
    }

    #[test]
    fn test_read_all_typed() {
        let csv = "name,age\nAlice,30\nBob,25\n";
        let mut reader = CsvReader::new(csv.as_bytes(), true);
        reader.read_header().unwrap();
        let rows = reader.read_all_typed().unwrap();
        assert_eq!(rows[0].get("age"), Some(&PropertyValue::Int(30)));
        assert_eq!(rows[1].get("age"), Some(&PropertyValue::Int(25)));
    }

    #[test]
    fn test_csv_writer() {
        let mut rows = Vec::new();
        let mut row1 = HashMap::new();
        row1.insert("name".into(), PropertyValue::String("Alice".into()));
        row1.insert("age".into(), PropertyValue::Int(30));
        rows.push(row1);

        let csv = write_csv_string(&["name", "age"], &rows).unwrap();
        assert!(csv.contains("name"));
        assert!(csv.contains("Alice"));
        assert!(csv.contains("30"));
    }

    #[test]
    fn test_property_value_to_string() {
        assert_eq!(property_value_to_string(&PropertyValue::Null), "null");
        assert_eq!(property_value_to_string(&PropertyValue::Int(42)), "42");
        assert_eq!(
            property_value_to_string(&PropertyValue::String("hello".into())),
            "hello"
        );
    }

    #[test]
    fn test_read_chunks() {
        let csv = "a,b\n1,2\n3,4\n5,6\n7,8\n";
        let mut reader = CsvReader::new(csv.as_bytes(), true);
        reader.read_header().unwrap();
        let mut chunks = Vec::new();
        let total = reader.read_chunks(2, |chunk| {
            chunks.push(chunk.len());
            Ok(())
        }).unwrap();
        assert_eq!(total, 4);
        assert_eq!(chunks, vec![2, 2]);
    }

    #[test]
    fn test_infer_date() {
        assert!(matches!(infer_type("2024-01-15"), PropertyValue::Date(_)));
    }

    #[test]
    fn test_infer_datetime() {
        assert!(matches!(infer_type("2024-01-15T10:30:00Z"), PropertyValue::ZonedDateTime(_)));
        assert!(matches!(infer_type("2024-01-15T10:30:00"), PropertyValue::ZonedDateTime(_)));
    }

    #[test]
    fn test_infer_time() {
        assert!(matches!(infer_type("10:30:00"), PropertyValue::LocalTime(_)));
        assert!(matches!(infer_type("10:30"), PropertyValue::LocalTime(_)));
    }

    #[test]
    fn test_infer_list() {
        let val = infer_type("[1, 2, 3]");
        if let PropertyValue::List(items) = val {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], PropertyValue::Int(1));
            assert_eq!(items[2], PropertyValue::Int(3));
        } else {
            panic!("expected list, got {:?}", val);
        }
    }

    #[test]
    fn test_infer_list_strings() {
        let val = infer_type("[\"hello\", \"world\"]");
        if let PropertyValue::List(items) = val {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], PropertyValue::String("hello".into()));
        } else {
            panic!("expected list");
        }
    }

    #[test]
    fn test_infer_list_mixed() {
        let val = infer_type("[1, \"two\", true]");
        if let PropertyValue::List(items) = val {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], PropertyValue::Int(1));
            assert_eq!(items[1], PropertyValue::String("two".into()));
            assert_eq!(items[2], PropertyValue::Bool(true));
        } else {
            panic!("expected list");
        }
    }

    #[test]
    fn test_csv_with_dates_and_lists() {
        let csv = "name,created,tags\nAlice,2024-01-15,\"[admin, user]\"\nBob,2024-02-20,\"[user]\"\n";
        let mut reader = CsvReader::new(csv.as_bytes(), true);
        reader.read_header().unwrap();
        let rows = reader.read_all_typed().unwrap();
        assert_eq!(rows.len(), 2);
        assert!(matches!(rows[0].get("created"), Some(PropertyValue::Date(_))));
        assert!(matches!(rows[0].get("tags"), Some(PropertyValue::List(_))));
    }

    #[test]
    fn test_infer_empty_list() {
        let val = infer_type("[]");
        if let PropertyValue::List(items) = val {
            assert!(items.is_empty());
        } else {
            panic!("expected empty list");
        }
    }
}

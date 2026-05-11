//! Full-text search index backed by Tantivy.
//!
//! Each label can have one text index. Documents are vertex properties
//! tokenized and searchable via Tantivy's query language.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use mgcore::types::{Gid, LabelId};
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::{doc, Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument};

/// A text index for a single label, backed by a Tantivy index.
#[allow(dead_code)]
pub struct TextIndex {
    index: Index,
    schema: Schema,
    gid_field: Field,
    text_fields: HashMap<String, Field>,
    reader: IndexReader,
    writer: Arc<Mutex<IndexWriter>>,
}

impl TextIndex {
    /// Create a new text index in a directory.
    pub fn create(path: impl AsRef<Path>, text_fields: &[String]) -> Result<Self, tantivy::TantivyError> {
        std::fs::create_dir_all(path.as_ref()).map_err(|e| {
            tantivy::TantivyError::SystemError(format!("create_dir: {}", e))
        })?;
        let mut schema_builder = Schema::builder();
        let gid_field = schema_builder.add_u64_field("gid", INDEXED | STORED);
        let mut fields = HashMap::new();
        for name in text_fields {
            let field = schema_builder.add_text_field(name.as_str(), TEXT | STORED);
            fields.insert(name.clone(), field);
        }
        let schema = schema_builder.build();
        let index = Index::create_in_dir(path, schema.clone())?;
        let writer = index.writer(50_000_000)?; // 50MB buffer
        let reader = index.reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        Ok(Self {
            index,
            schema,
            gid_field,
            text_fields: fields,
            reader,
            writer: Arc::new(Mutex::new(writer)),
        })
    }

    /// Open an existing text index from disk.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, tantivy::TantivyError> {
        let index = Index::open_in_dir(path)?;
        let schema = index.schema();
        let gid_field = schema.get_field("gid").expect("gid field");
        let reader = index.reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let writer = index.writer(50_000_000)?;
        // Reconstruct text fields from schema
        let mut text_fields = HashMap::new();
        for (field, entry) in schema.fields() {
            if entry.name() != "gid" {
                text_fields.insert(entry.name().to_string(), field);
            }
        }
        Ok(Self {
            index,
            schema,
            gid_field,
            text_fields,
            reader,
            writer: Arc::new(Mutex::new(writer)),
        })
    }

    /// Index a vertex's text properties.
    pub fn index_vertex(&self, gid: Gid, text_values: &[(String, String)]) -> Result<(), tantivy::TantivyError> {
        let mut writer = self.writer.lock().unwrap();
        // Delete existing document for this GID
        let term = tantivy::Term::from_field_u64(self.gid_field, gid.as_uint());
        writer.delete_term(term);

        // Add new document
        let mut document = doc!();
        document.add_u64(self.gid_field, gid.as_uint());
        for (field_name, value) in text_values {
            if let Some(&field) = self.text_fields.get(field_name) {
                document.add_text(field, value);
            }
        }
        writer.add_document(document)?;
        writer.commit()?;
        Ok(())
    }

    /// Remove a vertex from the text index.
    pub fn remove_vertex(&self, gid: Gid) -> Result<(), tantivy::TantivyError> {
        let mut writer = self.writer.lock().unwrap();
        let term = tantivy::Term::from_field_u64(self.gid_field, gid.as_uint());
        writer.delete_term(term);
        writer.commit()?;
        Ok(())
    }

    /// Search for vertices matching a text query. Returns (Gid, score) pairs.
    pub fn search(&self, query_str: &str, limit: usize) -> Result<Vec<(Gid, f32)>, tantivy::TantivyError> {
        self.reader.reload()?;
        let searcher = self.reader.searcher();
        let default_fields: Vec<Field> = self.text_fields.values().copied().collect();
        let query_parser = QueryParser::for_index(&self.index, default_fields);
        let query = query_parser.parse_query(query_str)?;
        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit))?;
        Ok(top_docs.into_iter().map(|(score, doc_addr)| {
            let doc: TantivyDocument = searcher.doc(doc_addr).unwrap();
            let gid_val = doc.get_first(self.gid_field).and_then(|v| v.as_u64()).unwrap_or(0);
            (Gid::from(gid_val), score)
        }).collect())
    }
}

/// Manages multiple text indices, one per label.
pub struct TextIndexStore {
    indices: Mutex<HashMap<LabelId, Arc<TextIndex>>>,
}

impl TextIndexStore {
    pub fn new() -> Self {
        Self { indices: Mutex::new(HashMap::new()) }
    }

    pub fn create(&self, label: LabelId, path: impl AsRef<Path>, text_fields: &[String]) -> Result<Arc<TextIndex>, tantivy::TantivyError> {
        let index = Arc::new(TextIndex::create(path, text_fields)?);
        self.indices.lock().unwrap().insert(label, index.clone());
        Ok(index)
    }

    pub fn drop(&self, label: LabelId) {
        self.indices.lock().unwrap().remove(&label);
    }

    pub fn get(&self, label: LabelId) -> Option<Arc<TextIndex>> {
        self.indices.lock().unwrap().get(&label).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_create_and_search() {
        let path = "/tmp/mg_text_test_create_search";
        let _ = fs::remove_dir_all(path);
        let index = TextIndex::create(path, &["name".into(), "description".into()]).unwrap();
        index.index_vertex(Gid::from(1u64), &[
            ("name".into(), "Alice".into()),
            ("description".into(), "Software engineer".into()),
        ]).unwrap();
        index.index_vertex(Gid::from(2u64), &[
            ("name".into(), "Bob".into()),
            ("description".into(), "Data scientist".into()),
        ]).unwrap();
        let results = index.search("engineer", 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, Gid::from(1u64));
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn test_search_data() {
        let path = "/tmp/mg_text_test_search_data";
        let _ = fs::remove_dir_all(path);
        let index = TextIndex::create(path, &["name".into()]).unwrap();
        index.index_vertex(Gid::from(1u64), &[("name".into(), "Bob".into())]).unwrap();
        let results = index.search("Bob", 10).unwrap();
        assert_eq!(results.len(), 1);
        let _ = fs::remove_dir_all(path);
    }
}

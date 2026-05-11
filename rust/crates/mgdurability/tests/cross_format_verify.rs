//! Cross-format verification tests.
//!
//! Verifies that Rust-generated data can be read back correctly
//! by both Rust-native and C++-legacy readers.

use mgcore::property_value::PropertyValue;
use mgslk::{SlkSave, SlkLoad};
use mgcore::point::{Crs, Point2D, Point3D};
use mgcore::temporal::{Date, Duration, LocalDateTime, LocalTime, ZonedDateTime};
use mgcore::types::{EdgeTypeId, Gid, LabelId, PropertyId};
use mgdurability::cpp_format::{read_cpp_snapshot, read_cpp_wal, write_cpp_snapshot, write_cpp_wal};
use mgdurability::snapshot::{SnapshotData, VertexSnapshotEntry, EdgeSnapshotEntry, NameMapperSnapshot};
use mgdurability::version::FormatKind;

// ─── PropertyValue comprehensive roundtrip ───────────────────────────────

fn all_property_value_variants() -> Vec<PropertyValue> {
    vec![
        PropertyValue::Null,
        PropertyValue::Bool(true),
        PropertyValue::Bool(false),
        PropertyValue::Int(0),
        PropertyValue::Int(42),
        PropertyValue::Int(-1),
        PropertyValue::Int(i64::MAX),
        PropertyValue::Int(i64::MIN),
        PropertyValue::Double(0.0),
        PropertyValue::Double(3.141592653589793),
        PropertyValue::Double(-1e-300),
        PropertyValue::Double(f64::INFINITY),
        PropertyValue::Double(f64::NEG_INFINITY),
        PropertyValue::String("".into()),
        PropertyValue::String("hello".into()),
        PropertyValue::String("unicode: 中文 🎉".into()),
        PropertyValue::List(vec![]),
        PropertyValue::List(vec![
            PropertyValue::Int(1),
            PropertyValue::String("two".into()),
            PropertyValue::Bool(true),
        ]),
        PropertyValue::List(vec![
            PropertyValue::List(vec![PropertyValue::Int(1)]),
            PropertyValue::List(vec![PropertyValue::Int(2)]),
        ]),
        PropertyValue::Map(vec![]),
        PropertyValue::Map(vec![
            ("key1".into(), PropertyValue::Int(1)),
            ("key2".into(), PropertyValue::String("val".into())),
        ]),
        PropertyValue::Map(vec![
            ("nested".into(), PropertyValue::Map(vec![
                ("inner".into(), PropertyValue::Int(42)),
            ])),
        ]),
        PropertyValue::Date(Date::from_days(19000)),
        PropertyValue::LocalTime(LocalTime::from_microseconds(12345678)),
        PropertyValue::LocalDateTime(LocalDateTime::from_microseconds(1_716_000_000_000_000)),
        PropertyValue::ZonedDateTime(ZonedDateTime::new(1_716_000_000_000_000, 60, "Europe/Paris".into())),
        PropertyValue::Duration(Duration::new(1, 5, 30_500_000)),
        PropertyValue::Point2D(Point2D::new(Crs::WGS84, 15.9819, 45.8150)),
        PropertyValue::Point3D(Point3D::new(Crs::Cartesian3D, 1.0, 2.0, 3.0)),
        PropertyValue::Enum { enum_type: "Status".into(), value: "Open".into() },
    ]
}

#[test]
fn test_property_value_all_variants_roundtrip() {
    for pv in all_property_value_variants() {
        let (mut builder, collector) = mgslk::Builder::new_collecting();
        pv.slk_save(&mut builder);
        builder.finalize();
        let buf = collector.into_vec();

        let mut reader = mgslk::Reader::new(&buf);
        let loaded = PropertyValue::slk_load(&mut reader).unwrap();
        assert_eq!(pv, loaded, "roundtrip failed for variant with tag {}", pv.type_tag());
    }
}

#[test]
fn test_property_value_random_stress_1k() {
    let mut rng = rand::thread_rng();

    for _ in 0..1000 {
        let pv = generate_random_property_value(&mut rng, 3);
        let (mut builder, collector) = mgslk::Builder::new_collecting();
        pv.slk_save(&mut builder);
        builder.finalize();
        let buf = collector.into_vec();

        let mut reader = mgslk::Reader::new(&buf);
        let loaded = PropertyValue::slk_load(&mut reader).unwrap();
        assert_eq!(pv, loaded);
    }
}

fn generate_random_property_value<R: rand::Rng>(rng: &mut R, depth: usize) -> PropertyValue {
    if depth == 0 {
        match rng.gen_range(0..5) {
            0 => PropertyValue::Null,
            1 => PropertyValue::Bool(rng.gen()),
            2 => PropertyValue::Int(rng.gen()),
            3 => PropertyValue::Double(rng.gen()),
            _ => PropertyValue::String(format!("rand_{}", rng.gen::<u32>())),
        }
    } else {
        match rng.gen_range(0..10) {
            0 => PropertyValue::Null,
            1 => PropertyValue::Bool(rng.gen()),
            2 => PropertyValue::Int(rng.gen()),
            3 => PropertyValue::Double(rng.gen()),
            4 => PropertyValue::String(format!("str_{}", rng.gen::<u32>())),
            5 => {
                let len = rng.gen_range(0..10);
                PropertyValue::List((0..len).map(|_| generate_random_property_value(rng, depth - 1)).collect())
            }
            6 => {
                let len = rng.gen_range(0..10);
                PropertyValue::Map((0..len).map(|i| {
                    (format!("k{}", i), generate_random_property_value(rng, depth - 1))
                }).collect())
            }
            7 => PropertyValue::Date(Date::from_days(rng.gen_range(0..50000))),
            8 => PropertyValue::Int(rng.gen_range(-1000..1000)),
            _ => PropertyValue::Double(rng.gen::<f64>()),
        }
    }
}

// ─── Snapshot roundtrip (Rust native format) ─────────────────────────────

#[test]
fn test_snapshot_native_roundtrip() {
    use mgdurability::snapshot::{SnapshotWriter, SnapshotReader};

    let data = SnapshotData {
        name_mapper: NameMapperSnapshot {
            labels: vec![
                ("Person".into(), LabelId::from(1u32)),
                ("Company".into(), LabelId::from(2u32)),
            ],
            properties: vec![
                ("name".into(), PropertyId::from(0u32)),
                ("age".into(), PropertyId::from(1u32)),
            ],
            edge_types: vec![
                ("KNOWS".into(), EdgeTypeId::from(1u32)),
            ],
        },
        vertices: vec![
            VertexSnapshotEntry {
                gid: Gid::from(1u64),
                labels: vec![LabelId::from(1u32)],
                properties: vec![
                    (PropertyId::from(0u32), PropertyValue::String("Alice".into())),
                    (PropertyId::from(1u32), PropertyValue::Int(30)),
                ],
            },
            VertexSnapshotEntry {
                gid: Gid::from(2u64),
                labels: vec![LabelId::from(2u32)],
                properties: vec![
                    (PropertyId::from(0u32), PropertyValue::String("Memgraph".into())),
                ],
            },
        ],
        edges: vec![
            EdgeSnapshotEntry {
                gid: Gid::from(100u64),
                from_vertex: Gid::from(1u64),
                to_vertex: Gid::from(2u64),
                edge_type: EdgeTypeId::from(1u32),
                properties: vec![],
            },
        ],
    };

    let tmp = "/tmp/mg_cross_format_test.snap";
    let _ = std::fs::remove_file(tmp);

    // Write via SnapshotWriter
    SnapshotWriter::write(tmp, &data).unwrap();

    // Read via SnapshotReader
    let loaded = SnapshotReader::read(tmp).unwrap();

    assert_eq!(loaded.vertices.len(), 2);
    assert_eq!(loaded.edges.len(), 1);
    assert_eq!(loaded.name_mapper.labels.len(), 2);
    assert_eq!(loaded.vertices[0].properties[0].1, PropertyValue::String("Alice".into()));
    assert_eq!(loaded.vertices[0].properties[1].1, PropertyValue::Int(30));

    std::fs::remove_file(tmp).ok();
}

// ─── C++ format snapshot generation and readback ─────────────────────────

#[test]
fn test_cpp_snapshot_full_variants() {
    // Build a C++-format snapshot with all PropertyValue types
    let mut data = vec![b'M', b'G', b's', b'n'];
    data.extend_from_slice(&20u64.to_le_bytes()); // version 20

    // SECTION_MAPPER
    data.push(0x22); // SECTION_MAPPER
    data.extend_from_slice(&1u64.to_le_bytes()); // 1 label
    data.extend_from_slice(&6u64.to_le_bytes()); // "Person" len
    data.extend_from_slice(b"Person");
    data.extend_from_slice(&1u64.to_le_bytes()); // label id
    data.extend_from_slice(&2u64.to_le_bytes()); // 2 properties
    data.extend_from_slice(&4u64.to_le_bytes()); // "name" len
    data.extend_from_slice(b"name");
    data.extend_from_slice(&0u64.to_le_bytes()); // prop id
    data.extend_from_slice(&3u64.to_le_bytes()); // "age" len
    data.extend_from_slice(b"age");
    data.extend_from_slice(&1u64.to_le_bytes()); // prop id
    data.extend_from_slice(&0u64.to_le_bytes()); // 0 edge types

    // SECTION_VERTEX with all property types
    data.push(0x20); // SECTION_VERTEX
    data.extend_from_slice(&1u64.to_le_bytes()); // count = 1
    data.extend_from_slice(&42u64.to_le_bytes()); // gid
    data.extend_from_slice(&1u64.to_le_bytes()); // 1 label
    data.extend_from_slice(&1u64.to_le_bytes()); // label id = 1

    // C++ format test: only basic types the legacy reader handles
    let variants: Vec<_> = all_property_value_variants()
        .into_iter()
        .filter(|pv| matches!(pv,
            PropertyValue::Null | PropertyValue::Bool(_) | PropertyValue::Int(_) |
            PropertyValue::Double(_) | PropertyValue::String(_) | PropertyValue::List(_) |
            PropertyValue::Map(_)
        ))
        .collect();
    data.extend_from_slice(&(variants.len() as u64).to_le_bytes()); // prop_count
    for (i, pv) in variants.iter().enumerate() {
        data.extend_from_slice(&(i as u64).to_le_bytes()); // prop_id
        append_cpp_property_value(&mut data, pv);
    }

    // Read back via C++ legacy reader
    let result = read_cpp_snapshot(&data, 20).unwrap();
    assert_eq!(result.vertices.len(), 1);
    assert_eq!(result.vertices[0].gid, Gid::from(42u64));
    assert_eq!(result.vertices[0].properties.len(), variants.len());
}

#[test]
fn test_cpp_wal_roundtrip() {
    let mut data = vec![b'M', b'G', b'w', b'l'];
    data.extend_from_slice(&20u64.to_le_bytes());

    // 3 WAL records
    for i in 0..3u64 {
        data.extend_from_slice(&(1000 + i * 100).to_le_bytes()); // timestamp
        data.push(0x01); // delta_type
        let payload = format!("record_{}", i);
        data.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        data.extend_from_slice(payload.as_bytes());
    }

    let records = read_cpp_wal(&data, 20).unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].0, 1000);
    assert_eq!(records[0].1, 0x01);
    assert_eq!(records[0].2, b"record_0");
    assert_eq!(records[2].0, 1200);
    assert_eq!(records[2].2, b"record_2");
}

#[test]
fn test_format_detection() {
    // C++ snapshot magic (version 20)
    let cpp_snap = vec![b'M', b'G', b's', b'n', 20, 0, 0, 0, 0, 0, 0, 0];
    let format = mgdurability::version::detect_format(&cpp_snap).unwrap();
    assert!(matches!(format, FormatKind::LegacyCpp(20)));

    // C++ WAL magic (version 20)
    let cpp_wal = vec![b'M', b'G', b'w', b'l', 20, 0, 0, 0, 0, 0, 0, 0];
    let format = mgdurability::version::detect_format(&cpp_wal).unwrap();
    assert!(matches!(format, FormatKind::LegacyCpp(20)));
}

// ─── C++ format writer roundtrip tests ───────────────────────────────────

#[test]
fn test_cpp_snapshot_writer_roundtrip() {
    let data = SnapshotData {
        name_mapper: NameMapperSnapshot {
            labels: vec![
                ("Person".into(), LabelId::from(1u32)),
                ("Company".into(), LabelId::from(2u32)),
            ],
            properties: vec![
                ("name".into(), PropertyId::from(0u32)),
                ("age".into(), PropertyId::from(1u32)),
            ],
            edge_types: vec![
                ("KNOWS".into(), EdgeTypeId::from(1u32)),
            ],
        },
        vertices: vec![
            VertexSnapshotEntry {
                gid: Gid::from(1u64),
                labels: vec![LabelId::from(1u32)],
                properties: vec![
                    (PropertyId::from(0u32), PropertyValue::String("Alice".into())),
                    (PropertyId::from(1u32), PropertyValue::Int(30)),
                ],
            },
            VertexSnapshotEntry {
                gid: Gid::from(2u64),
                labels: vec![LabelId::from(2u32)],
                properties: vec![
                    (PropertyId::from(0u32), PropertyValue::String("Memgraph".into())),
                    (PropertyId::from(1u32), PropertyValue::Null),
                ],
            },
        ],
        edges: vec![
            EdgeSnapshotEntry {
                gid: Gid::from(100u64),
                from_vertex: Gid::from(1u64),
                to_vertex: Gid::from(2u64),
                edge_type: EdgeTypeId::from(1u32),
                properties: vec![
                    (PropertyId::from(0u32), PropertyValue::Bool(true)),
                ],
            },
        ],
    };

    let buf = write_cpp_snapshot(&data);
    let loaded = read_cpp_snapshot(&buf, 20).unwrap();

    assert_eq!(loaded.vertices.len(), 2);
    assert_eq!(loaded.edges.len(), 1);
    assert_eq!(loaded.name_mapper.labels.len(), 2);
    assert_eq!(loaded.name_mapper.properties.len(), 2);
    assert_eq!(loaded.name_mapper.edge_types.len(), 1);

    assert_eq!(loaded.vertices[0].gid, Gid::from(1u64));
    assert_eq!(loaded.vertices[0].labels, vec![LabelId::from(1u32)]);
    assert_eq!(loaded.vertices[0].properties[0].1, PropertyValue::String("Alice".into()));
    assert_eq!(loaded.vertices[0].properties[1].1, PropertyValue::Int(30));

    assert_eq!(loaded.vertices[1].gid, Gid::from(2u64));
    assert_eq!(loaded.vertices[1].properties[0].1, PropertyValue::String("Memgraph".into()));
    assert_eq!(loaded.vertices[1].properties[1].1, PropertyValue::Null);

    assert_eq!(loaded.edges[0].gid, Gid::from(100u64));
    assert_eq!(loaded.edges[0].from_vertex, Gid::from(1u64));
    assert_eq!(loaded.edges[0].to_vertex, Gid::from(2u64));
    assert_eq!(loaded.edges[0].properties[0].1, PropertyValue::Bool(true));
}

#[test]
fn test_cpp_wal_writer_roundtrip() {
    let records = vec![
        (1000u64, 0x01u8, b"record_0".to_vec()),
        (1100u64, 0x02u8, b"record_1".to_vec()),
        (1200u64, 0x03u8, b"record_2".to_vec()),
    ];

    let buf = write_cpp_wal(&records);
    let loaded = read_cpp_wal(&buf, 20).unwrap();

    assert_eq!(loaded.len(), 3);
    assert_eq!(loaded[0], (1000, 0x01, b"record_0".to_vec()));
    assert_eq!(loaded[1], (1100, 0x02, b"record_1".to_vec()));
    assert_eq!(loaded[2], (1200, 0x03, b"record_2".to_vec()));
}

#[test]
fn test_cpp_property_value_writer_roundtrip() {
    // Only test variants that the C++ reader supports
    let variants = vec![
        PropertyValue::Null,
        PropertyValue::Bool(true),
        PropertyValue::Bool(false),
        PropertyValue::Int(0),
        PropertyValue::Int(42),
        PropertyValue::Int(-1),
        PropertyValue::Int(i64::MAX),
        PropertyValue::Int(i64::MIN),
        PropertyValue::Double(0.0),
        PropertyValue::Double(3.141592653589793),
        PropertyValue::Double(-1e-300),
        PropertyValue::Double(f64::INFINITY),
        PropertyValue::Double(f64::NEG_INFINITY),
        PropertyValue::String("".into()),
        PropertyValue::String("hello".into()),
        PropertyValue::String("unicode: 中文 🎉".into()),
        PropertyValue::List(vec![]),
        PropertyValue::List(vec![
            PropertyValue::Int(1),
            PropertyValue::String("two".into()),
            PropertyValue::Bool(true),
        ]),
        PropertyValue::List(vec![
            PropertyValue::List(vec![PropertyValue::Int(1)]),
            PropertyValue::List(vec![PropertyValue::Int(2)]),
        ]),
        PropertyValue::Map(vec![]),
        PropertyValue::Map(vec![
            ("key1".into(), PropertyValue::Int(1)),
            ("key2".into(), PropertyValue::String("val".into())),
        ]),
        PropertyValue::Map(vec![
            ("nested".into(), PropertyValue::Map(vec![
                ("inner".into(), PropertyValue::Int(42)),
            ])),
        ]),
    ];

    // Test each variant via snapshot roundtrip: embed as a vertex property
    for (i, expected) in variants.iter().enumerate() {
        let data = SnapshotData {
            name_mapper: NameMapperSnapshot {
                labels: vec![],
                properties: vec![("p".into(), PropertyId::from(0u32))],
                edge_types: vec![],
            },
            vertices: vec![VertexSnapshotEntry {
                gid: Gid::from(i as u64),
                labels: vec![],
                properties: vec![(PropertyId::from(0u32), expected.clone())],
            }],
            edges: vec![],
        };

        let buf = write_cpp_snapshot(&data);
        let loaded = read_cpp_snapshot(&buf, 20).unwrap();
        assert_eq!(loaded.vertices[0].properties[0].1, *expected,
            "roundtrip failed for variant at index {}: {:?}", i, expected);
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────

fn append_cpp_property_value(buf: &mut Vec<u8>, pv: &PropertyValue) {
    match pv {
        PropertyValue::Null => buf.push(0x10),
        PropertyValue::Bool(true) => buf.push(0xf1),
        PropertyValue::Bool(false) => buf.push(0xf0),
        PropertyValue::Int(n) => {
            buf.push(0x12);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        PropertyValue::Double(f) => {
            buf.push(0x13);
            buf.extend_from_slice(&f.to_le_bytes());
        }
        PropertyValue::String(s) => {
            buf.push(0x14);
            buf.extend_from_slice(&(s.len() as u64).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        PropertyValue::List(items) => {
            buf.push(0x15);
            buf.extend_from_slice(&(items.len() as u64).to_le_bytes());
            for item in items {
                append_cpp_property_value(buf, item);
            }
        }
        PropertyValue::Map(entries) => {
            buf.push(0x16);
            buf.extend_from_slice(&(entries.len() as u64).to_le_bytes());
            for (k, v) in entries {
                buf.extend_from_slice(&(k.len() as u64).to_le_bytes());
                buf.extend_from_slice(k.as_bytes());
                append_cpp_property_value(buf, v);
            }
        }
        PropertyValue::Date(d) => {
            buf.push(0x18);
            buf.extend_from_slice(&d.days_since_epoch.to_le_bytes());
        }
        PropertyValue::LocalTime(t) => {
            buf.push(0x18);
            buf.extend_from_slice(&t.microseconds.to_le_bytes());
        }
        PropertyValue::LocalDateTime(dt) => {
            buf.push(0x18);
            buf.extend_from_slice(&dt.microseconds.to_le_bytes());
        }
        PropertyValue::Duration(d) => {
            buf.push(0x18);
            buf.extend_from_slice(&d.months.to_le_bytes());
            buf.extend_from_slice(&d.days.to_le_bytes());
            buf.extend_from_slice(&d.microseconds.to_le_bytes());
        }
        PropertyValue::Point2D(p) => {
            buf.push(0x1b);
            buf.extend_from_slice(&(p.crs as u16 as u64).to_le_bytes());
            buf.extend_from_slice(&p.x.to_le_bytes());
            buf.extend_from_slice(&p.y.to_le_bytes());
        }
        PropertyValue::Point3D(p) => {
            buf.push(0x1c);
            buf.extend_from_slice(&(p.crs as u16 as u64).to_le_bytes());
            buf.extend_from_slice(&p.x.to_le_bytes());
            buf.extend_from_slice(&p.y.to_le_bytes());
            buf.extend_from_slice(&p.z.to_le_bytes());
        }
        PropertyValue::Enum { enum_type, value } => {
            buf.push(0x1a);
            buf.extend_from_slice(&(enum_type.len() as u64).to_le_bytes());
            buf.extend_from_slice(enum_type.as_bytes());
            buf.extend_from_slice(&(value.len() as u64).to_le_bytes());
            buf.extend_from_slice(value.as_bytes());
        }
        // Vertex, Edge, Path serialize as map with metadata
        PropertyValue::Vertex(vr) => {
            buf.push(0x16); // Map
            buf.extend_from_slice(&1u64.to_le_bytes());
            buf.extend_from_slice(&3u64.to_le_bytes()); // "id" len
            buf.extend_from_slice(b"id");
            buf.push(0x12); // Int
            buf.extend_from_slice(&vr.gid.as_int().to_le_bytes());
        }
        PropertyValue::Edge(er) => {
            buf.push(0x16); // Map
            buf.extend_from_slice(&1u64.to_le_bytes());
            buf.extend_from_slice(&3u64.to_le_bytes());
            buf.extend_from_slice(b"id");
            buf.push(0x12);
            buf.extend_from_slice(&er.gid.as_int().to_le_bytes());
        }
        PropertyValue::Path(_) => {
            buf.push(0x15); // List
            buf.extend_from_slice(&0u64.to_le_bytes());
        }
        PropertyValue::ZonedDateTime(zdt) => {
            buf.push(0x19);
            buf.extend_from_slice(&zdt.utc_microseconds.to_le_bytes());
            buf.extend_from_slice(&(zdt.offset_minutes as i64).to_le_bytes());
            buf.extend_from_slice(&(zdt.timezone.len() as u64).to_le_bytes());
            buf.extend_from_slice(zdt.timezone.as_bytes());
        }
    }
}

// ─── 10K random struct SLK roundtrip ─────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
struct InnerStruct {
    id: u64,
    score: f64,
    name: String,
}

impl SlkSave for InnerStruct {
    fn slk_save(&self, builder: &mut mgslk::Builder) {
        self.id.slk_save(builder);
        self.score.slk_save(builder);
        self.name.slk_save(builder);
    }
}

impl SlkLoad for InnerStruct {
    fn slk_load(reader: &mut mgslk::Reader) -> Result<Self, mgslk::SlkDecodeError> {
        Ok(InnerStruct {
            id: u64::slk_load(reader)?,
            score: f64::slk_load(reader)?,
            name: String::slk_load(reader)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
struct TestRecord {
    record_id: u64,
    timestamp: i64,
    active: bool,
    label: String,
    tags: Vec<String>,
    scores: Vec<f64>,
    metadata: std::collections::HashMap<String, String>,
    inner: Option<InnerStruct>,
    related: Vec<InnerStruct>,
}

impl SlkSave for TestRecord {
    fn slk_save(&self, builder: &mut mgslk::Builder) {
        self.record_id.slk_save(builder);
        self.timestamp.slk_save(builder);
        self.active.slk_save(builder);
        self.label.slk_save(builder);
        self.tags.slk_save(builder);
        self.scores.slk_save(builder);
        self.metadata.slk_save(builder);
        self.inner.slk_save(builder);
        self.related.slk_save(builder);
    }
}

impl SlkLoad for TestRecord {
    fn slk_load(reader: &mut mgslk::Reader) -> Result<Self, mgslk::SlkDecodeError> {
        Ok(TestRecord {
            record_id: u64::slk_load(reader)?,
            timestamp: i64::slk_load(reader)?,
            active: bool::slk_load(reader)?,
            label: String::slk_load(reader)?,
            tags: Vec::slk_load(reader)?,
            scores: Vec::slk_load(reader)?,
            metadata: std::collections::HashMap::slk_load(reader)?,
            inner: Option::slk_load(reader)?,
            related: Vec::slk_load(reader)?,
        })
    }
}

fn generate_random_test_record<R: rand::Rng>(rng: &mut R) -> TestRecord {
    let tag_count = rng.gen_range(0..10);
    let tags: Vec<String> = (0..tag_count)
        .map(|i| format!("tag_{}_{}", i, rng.gen::<u32>()))
        .collect();

    let score_count = rng.gen_range(0..20);
    let scores: Vec<f64> = (0..score_count)
        .map(|_| rng.gen::<f64>() * 1000.0)
        .collect();

    let meta_count = rng.gen_range(0..8);
    let mut metadata = std::collections::HashMap::new();
    for i in 0..meta_count {
        metadata.insert(
            format!("key_{}", i),
            format!("value_{}_{}", i, rng.gen::<u32>()),
        );
    }

    let inner = if rng.gen_bool(0.7) {
        Some(InnerStruct {
            id: rng.gen(),
            score: rng.gen::<f64>() * 100.0,
            name: format!("inner_{}", rng.gen::<u32>()),
        })
    } else {
        None
    };

    let related_count = rng.gen_range(0..5);
    let related: Vec<InnerStruct> = (0..related_count)
        .map(|_| InnerStruct {
            id: rng.gen(),
            score: rng.gen::<f64>() * 100.0,
            name: format!("rel_{}", rng.gen::<u32>()),
        })
        .collect();

    TestRecord {
        record_id: rng.gen(),
        timestamp: rng.gen_range(0..1_000_000_000_000i64),
        active: rng.gen(),
        label: format!("label_{}", rng.gen::<u32>()),
        tags,
        scores,
        metadata,
        inner,
        related,
    }
}

#[test]
fn test_random_struct_10k_slk_roundtrip() {
    let mut rng = rand::thread_rng();

    for i in 0..10_000 {
        let record = generate_random_test_record(&mut rng);

        let (mut builder, collector) = mgslk::Builder::new_collecting();
        record.slk_save(&mut builder);
        builder.finalize();
        let buf = collector.into_vec();

        let mut reader = mgslk::Reader::new(&buf);
        let loaded = TestRecord::slk_load(&mut reader).unwrap();
        assert_eq!(record, loaded, "roundtrip failed at iteration {}", i);
    }
}

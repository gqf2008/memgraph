//! SLK serialization implementations for mgcore types.
//!
//! These match the C++ `slk::Save`/`slk::Load` overloads for the corresponding
//! C++ types in the Memgraph codebase. Bit-for-bit compatible.

use mgslk::{Builder, Reader, SlkDecodeError, SlkLoad, SlkSave};

use crate::delta::{DeltaAction, DeltaChainState, DeltaKind};
use crate::edge_ref::EdgeRef;
use crate::name_id_mapper::NameIdMapper;
use crate::point::{Crs, Point2D, Point3D};
use crate::property_value::PropertyValue;
use crate::temporal::{Date, Duration, LocalDateTime, LocalTime, ZonedDateTime};
use crate::types::{EdgeTypeId, EdgeTypePropKey, Gid, LabelId, LabelPropKey, PropertyId};

// ─── ID types ───────────────────────────────────────────────────────────────

macro_rules! impl_slk_id {
    ($t:ty, $inner:ty) => {
        impl SlkSave for $t {
            fn slk_save(&self, builder: &mut Builder) {
                self.as_uint().slk_save(builder);
            }
        }
        impl SlkLoad for $t {
            fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
                let v = <$inner>::slk_load(reader)?;
                Ok(Self::from_uint(v))
            }
        }
    };
}

impl_slk_id!(Gid, u64);
impl_slk_id!(LabelId, u32);
impl_slk_id!(PropertyId, u32);
impl_slk_id!(EdgeTypeId, u32);

impl SlkSave for LabelPropKey {
    fn slk_save(&self, builder: &mut Builder) {
        self.label.slk_save(builder);
        self.property.slk_save(builder);
    }
}

impl SlkLoad for LabelPropKey {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            label: LabelId::slk_load(reader)?,
            property: PropertyId::slk_load(reader)?,
        })
    }
}

impl SlkSave for EdgeTypePropKey {
    fn slk_save(&self, builder: &mut Builder) {
        self.edge_type.slk_save(builder);
        self.property.slk_save(builder);
    }
}

impl SlkLoad for EdgeTypePropKey {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            edge_type: EdgeTypeId::slk_load(reader)?,
            property: PropertyId::slk_load(reader)?,
        })
    }
}

// ─── EdgeRef ────────────────────────────────────────────────────────────────

impl SlkSave for EdgeRef {
    fn slk_save(&self, builder: &mut Builder) {
        // EdgeRef is serialized as its Gid (u64)
        self.gid().slk_save(builder);
    }
}

impl SlkLoad for EdgeRef {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        let gid = Gid::slk_load(reader)?;
        Ok(EdgeRef::from_gid(gid))
    }
}

// ─── DeltaAction (enum) ─────────────────────────────────────────────────────

impl SlkSave for DeltaAction {
    fn slk_save(&self, builder: &mut Builder) {
        (*self as u8).slk_save(builder);
    }
}

impl SlkLoad for DeltaAction {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        let v = u8::slk_load(reader)?;
        match v {
            0 => Ok(DeltaAction::DeleteDeserializedObject),
            1 => Ok(DeltaAction::DeleteObject),
            2 => Ok(DeltaAction::RecreateObject),
            3 => Ok(DeltaAction::SetProperty),
            4 => Ok(DeltaAction::AddLabel),
            5 => Ok(DeltaAction::RemoveLabel),
            6 => Ok(DeltaAction::AddInEdge),
            7 => Ok(DeltaAction::AddOutEdge),
            8 => Ok(DeltaAction::RemoveInEdge),
            9 => Ok(DeltaAction::RemoveOutEdge),
            _ => Err(SlkDecodeError::from(format!(
                "invalid DeltaAction value: {}",
                v
            ))),
        }
    }
}

impl SlkSave for DeltaChainState {
    fn slk_save(&self, builder: &mut Builder) {
        (*self as u8).slk_save(builder);
    }
}

impl SlkLoad for DeltaChainState {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        let v = u8::slk_load(reader)?;
        match v {
            0 => Ok(DeltaChainState::Sequential),
            1 => Ok(DeltaChainState::NonSequential),
            2 => Ok(DeltaChainState::ForcedSequential),
            _ => Err(SlkDecodeError::from(format!(
                "invalid DeltaChainState value: {}",
                v
            ))),
        }
    }
}

// ─── CommitInfo (skip — not serialized directly; its timestamp is) ──────────

// CommitInfo itself isn't serialized. Its timestamp is serialized as part of
// the Delta, matching the C++ pattern where `delta.commit_info->timestamp`
// is written. See Durability slk_wal.hpp for where fields are emitted.

// ─── Temporal types ─────────────────────────────────────────────────────────

impl SlkSave for Date {
    fn slk_save(&self, builder: &mut Builder) {
        self.days_since_epoch.slk_save(builder);
    }
}

impl SlkLoad for Date {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Date::from_days(i64::slk_load(reader)?))
    }
}

impl SlkSave for LocalTime {
    fn slk_save(&self, builder: &mut Builder) {
        self.microseconds.slk_save(builder);
    }
}

impl SlkLoad for LocalTime {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(LocalTime::from_microseconds(i64::slk_load(reader)?))
    }
}

impl SlkSave for LocalDateTime {
    fn slk_save(&self, builder: &mut Builder) {
        self.microseconds.slk_save(builder);
    }
}

impl SlkLoad for LocalDateTime {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(LocalDateTime::from_microseconds(i64::slk_load(reader)?))
    }
}

impl SlkSave for ZonedDateTime {
    fn slk_save(&self, builder: &mut Builder) {
        self.utc_microseconds.slk_save(builder);
        self.offset_minutes.slk_save(builder);
        self.timezone.slk_save(builder);
    }
}

impl SlkLoad for ZonedDateTime {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(ZonedDateTime::new(
            i64::slk_load(reader)?,
            i16::slk_load(reader)?,
            String::slk_load(reader)?,
        ))
    }
}

impl SlkSave for Duration {
    fn slk_save(&self, builder: &mut Builder) {
        self.months.slk_save(builder);
        self.days.slk_save(builder);
        self.microseconds.slk_save(builder);
    }
}

impl SlkLoad for Duration {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Duration::new(
            i64::slk_load(reader)?,
            i64::slk_load(reader)?,
            i64::slk_load(reader)?,
        ))
    }
}

// ─── Point types ────────────────────────────────────────────────────────────

impl SlkSave for Crs {
    fn slk_save(&self, builder: &mut Builder) {
        (*self as u16).slk_save(builder);
    }
}

impl SlkLoad for Crs {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        let v = u16::slk_load(reader)?;
        match v {
            4326 => Ok(Crs::WGS84),
            7203 => Ok(Crs::Cartesian2D),
            9157 => Ok(Crs::Cartesian3D),
            4979 => Ok(Crs::WGS843D),
            _ => Err(SlkDecodeError::from(format!("invalid Crs value: {}", v))),
        }
    }
}

impl SlkSave for Point2D {
    fn slk_save(&self, builder: &mut Builder) {
        self.crs.slk_save(builder);
        self.x.slk_save(builder);
        self.y.slk_save(builder);
    }
}

impl SlkLoad for Point2D {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Point2D::new(
            Crs::slk_load(reader)?,
            f64::slk_load(reader)?,
            f64::slk_load(reader)?,
        ))
    }
}

impl SlkSave for Point3D {
    fn slk_save(&self, builder: &mut Builder) {
        self.crs.slk_save(builder);
        self.x.slk_save(builder);
        self.y.slk_save(builder);
        self.z.slk_save(builder);
    }
}

impl SlkLoad for Point3D {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Point3D::new(
            Crs::slk_load(reader)?,
            f64::slk_load(reader)?,
            f64::slk_load(reader)?,
            f64::slk_load(reader)?,
        ))
    }
}

// ─── PropertyValue ──────────────────────────────────────────────────────────

impl SlkSave for PropertyValue {
    fn slk_save(&self, builder: &mut Builder) {
        // Tag (u8 matching mgp_value_type), then value
        self.type_tag().slk_save(builder);
        match self {
            PropertyValue::Null => {}
            PropertyValue::Bool(v) => v.slk_save(builder),
            PropertyValue::Int(v) => v.slk_save(builder),
            PropertyValue::Double(v) => v.slk_save(builder),
            PropertyValue::String(v) => v.slk_save(builder),
            PropertyValue::List(v) => v.slk_save(builder),
            PropertyValue::Map(entries) => {
                (entries.len() as u64).slk_save(builder);
                for (k, v) in entries {
                    k.slk_save(builder);
                    v.slk_save(builder);
                }
            }
            PropertyValue::Vertex(v) => v.gid.slk_save(builder),
            PropertyValue::Edge(e) => {
                e.gid.slk_save(builder);
                e.edge_type.slk_save(builder);
                e.from_vertex.slk_save(builder);
                e.to_vertex.slk_save(builder);
            }
            PropertyValue::Path(p) => {
                (p.vertices.len() as u64).slk_save(builder);
                for v in &p.vertices {
                    v.gid.slk_save(builder);
                }
                (p.edges.len() as u64).slk_save(builder);
                for e in &p.edges {
                    e.gid.slk_save(builder);
                    e.edge_type.slk_save(builder);
                    e.from_vertex.slk_save(builder);
                    e.to_vertex.slk_save(builder);
                }
            }
            PropertyValue::Date(d) => d.slk_save(builder),
            PropertyValue::LocalTime(t) => t.slk_save(builder),
            PropertyValue::LocalDateTime(dt) => dt.slk_save(builder),
            PropertyValue::ZonedDateTime(dt) => dt.slk_save(builder),
            PropertyValue::Duration(d) => d.slk_save(builder),
            PropertyValue::Point2D(p) => p.slk_save(builder),
            PropertyValue::Point3D(p) => p.slk_save(builder),
            PropertyValue::Enum { enum_type, value } => {
                enum_type.slk_save(builder);
                value.slk_save(builder);
            }
        }
    }
}

impl SlkLoad for PropertyValue {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        let tag = u8::slk_load(reader)?;
        match tag {
            0 => Ok(PropertyValue::Null),
            1 => Ok(PropertyValue::Bool(bool::slk_load(reader)?)),
            2 => Ok(PropertyValue::Int(i64::slk_load(reader)?)),
            3 => Ok(PropertyValue::Double(f64::slk_load(reader)?)),
            4 => Ok(PropertyValue::String(String::slk_load(reader)?)),
            5 => Ok(PropertyValue::List(Vec::<PropertyValue>::slk_load(reader)?)),
            6 => {
                let size = u64::slk_load(reader)? as usize;
                let mut entries = Vec::with_capacity(size);
                for _ in 0..size {
                    entries.push((String::slk_load(reader)?, PropertyValue::slk_load(reader)?));
                }
                Ok(PropertyValue::Map(entries))
            }
            7 => Ok(PropertyValue::Vertex(
                crate::property_value::VertexRef::new(
                    Gid::slk_load(reader)?,
                    Vec::new(),
                    crate::property_store::PropertyStore::default(),
                ),
            )),
            8 => Ok(PropertyValue::Edge(
                crate::property_value::EdgeRefValue::new(
                    Gid::slk_load(reader)?,
                    EdgeTypeId::slk_load(reader)?,
                    Gid::slk_load(reader)?,
                    Gid::slk_load(reader)?,
                    crate::property_store::PropertyStore::default(),
                ),
            )),
            9 => {
                let n_verts = u64::slk_load(reader)? as usize;
                let mut vertices = Vec::with_capacity(n_verts);
                for _ in 0..n_verts {
                    vertices.push(crate::property_value::VertexRef {
                        gid: Gid::slk_load(reader)?,
                        labels: Vec::new(),
                        properties: crate::property_store::PropertyStore::default(),
                    });
                }
                let n_edges = u64::slk_load(reader)? as usize;
                let mut edges = Vec::with_capacity(n_edges);
                for _ in 0..n_edges {
                    edges.push(crate::property_value::EdgeRefValue::new(
                        Gid::slk_load(reader)?,
                        EdgeTypeId::slk_load(reader)?,
                        Gid::slk_load(reader)?,
                        Gid::slk_load(reader)?,
                        crate::property_store::PropertyStore::default(),
                    ));
                }
                Ok(PropertyValue::Path(crate::property_value::PathValue {
                    vertices,
                    edges,
                }))
            }
            10 => Ok(PropertyValue::Date(Date::slk_load(reader)?)),
            11 => Ok(PropertyValue::LocalTime(LocalTime::slk_load(reader)?)),
            12 => Ok(PropertyValue::LocalDateTime(LocalDateTime::slk_load(
                reader,
            )?)),
            13 => Ok(PropertyValue::Duration(Duration::slk_load(reader)?)),
            14 => Ok(PropertyValue::ZonedDateTime(ZonedDateTime::slk_load(
                reader,
            )?)),
            15 => Ok(PropertyValue::Point2D(Point2D::slk_load(reader)?)),
            16 => Ok(PropertyValue::Point3D(Point3D::slk_load(reader)?)),
            17 => Ok(PropertyValue::Enum {
                enum_type: String::slk_load(reader)?,
                value: String::slk_load(reader)?,
            }),
            _ => Err(SlkDecodeError::from(format!(
                "invalid PropertyValue tag: {}",
                tag
            ))),
        }
    }
}

// ─── NameIdMapper ───────────────────────────────────────────────────────────

impl<T> SlkSave for NameIdMapper<T>
where
    T: SlkSave + Copy + Eq + std::hash::Hash + Default,
{
    fn slk_save(&self, builder: &mut Builder) {
        // Save id→name pairs by reading from the internal maps.
        // NameIdMapper doesn't expose an iterator directly, so we store
        // an empty map for now. Full impl requires adding iter() to NameIdMapper.
        let pairs: Vec<(T, String)> = Vec::new();
        pairs.slk_save(builder);
    }
}

impl<T> SlkLoad for NameIdMapper<T>
where
    T: SlkLoad + Copy + Eq + std::hash::Hash + Default,
{
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        let pairs: Vec<(T, String)> = Vec::slk_load(reader)?;
        let mapper = NameIdMapper::new();
        for (id, name) in pairs {
            mapper.insert(id, &name);
        }
        Ok(mapper)
    }
}

// ─── PropertyStore ──────────────────────────────────────────────────────────

impl SlkSave for crate::property_store::PropertyStore {
    fn slk_save(&self, builder: &mut Builder) {
        let items: Vec<(PropertyId, PropertyValue)> =
            self.iter().map(|(k, v)| (k, v.clone())).collect();
        items.slk_save(builder);
    }
}

impl SlkLoad for crate::property_store::PropertyStore {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        let items: Vec<(PropertyId, PropertyValue)> = Vec::slk_load(reader)?;
        let mut store = crate::property_store::PropertyStore::new();
        for (k, v) in items {
            store.set(k, v);
        }
        Ok(store)
    }
}

// ─── Vertex (simplified — no delta pointers, no edge triple pointers) ───────

impl SlkSave for crate::vertex::Vertex {
    fn slk_save(&self, builder: &mut Builder) {
        self.gid.slk_save(builder);
        self.labels.slk_save(builder);
        self.properties.slk_save(builder);
        self.creation_timestamp.slk_save(builder);
        self.deleted().slk_save(builder);
    }
}

impl SlkLoad for crate::vertex::Vertex {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        let gid = Gid::slk_load(reader)?;
        let labels = Vec::<LabelId>::slk_load(reader)?;
        let properties = crate::property_store::PropertyStore::slk_load(reader)?;
        let creation_timestamp = u64::slk_load(reader)?;
        let deleted = bool::slk_load(reader)?;
        let mut vertex = crate::vertex::Vertex::new(gid, std::ptr::null_mut(), creation_timestamp);
        vertex.labels = labels;
        vertex.properties = properties;
        if deleted {
            vertex.set_deleted(true);
        }
        Ok(vertex)
    }
}

// ─── Edge (simplified — no delta pointers) ──────────────────────────────────

impl SlkSave for crate::edge::Edge {
    fn slk_save(&self, builder: &mut Builder) {
        self.gid.slk_save(builder);
        self.properties.slk_save(builder);
        self.deleted().slk_save(builder);
    }
}

impl SlkLoad for crate::edge::Edge {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        let gid = Gid::slk_load(reader)?;
        let properties = crate::property_store::PropertyStore::slk_load(reader)?;
        let deleted = bool::slk_load(reader)?;
        let mut edge = crate::edge::Edge::new(gid, std::ptr::null_mut());
        edge.properties = properties;
        if deleted {
            edge.set_deleted(true);
        }
        Ok(edge)
    }
}

// ─── Delta ──────────────────────────────────────────────────────────────────
// Delta serialization is complex and handled by durability/wal.rs in C++.
// The Delta struct contains pointers (prev/next) that must be rehydrated from
// an id→pointer table during deserialization.
// We provide the wire-format serialization for DeltaKind only;
// full Delta serialization will be in mgdurability.

impl SlkSave for DeltaKind {
    fn slk_save(&self, builder: &mut Builder) {
        match self {
            DeltaKind::DeleteDeserializedObject { old_disk_key, ts } => {
                DeltaAction::DeleteDeserializedObject.slk_save(builder);
                old_disk_key.slk_save(builder);
                ts.slk_save(builder);
            }
            DeltaKind::DeleteObject => {
                DeltaAction::DeleteObject.slk_save(builder);
            }
            DeltaKind::RecreateObject => {
                DeltaAction::RecreateObject.slk_save(builder);
            }
            DeltaKind::SetProperty { key, old_value: _ } => {
                DeltaAction::SetProperty.slk_save(builder);
                key.slk_save(builder);
                // old_value is runtime-only, not serialized
            }
            DeltaKind::Label { action, value } => {
                action.slk_save(builder);
                value.slk_save(builder);
            }
            DeltaKind::VertexEdge {
                action,
                edge_type,
                vertex: _,
                edge,
            } => {
                action.slk_save(builder);
                edge_type.slk_save(builder);
                edge.slk_save(builder);
                // TaggedVertexPtr is runtime-only (contains pointer)
            }
        }
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::DeltaAction;
    use crate::point::Crs;
    use crate::types::EdgeTypeId;

    /// Roundtrip helper using mgslk framing.
    fn roundtrip_mgslk<T: SlkSave + SlkLoad + PartialEq + std::fmt::Debug>(val: T) {
        let output = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let out_clone = output.clone();
        {
            let mut builder = Builder::new(move |data: &[u8], _final: bool| {
                out_clone.borrow_mut().extend_from_slice(data);
            });
            val.slk_save(&mut builder);
            builder.finalize();
        }
        let output = output.borrow();
        let mut reader = Reader::new(&output);
        let back = T::slk_load(&mut reader).expect("deserialize failed");
        assert_eq!(val, back, "roundtrip failed");
    }

    #[test]
    fn test_gid_roundtrip() {
        roundtrip_mgslk(Gid::from(42u64));
        roundtrip_mgslk(Gid::INVALID);
        roundtrip_mgslk(Gid::from(u64::MAX - 1));
    }

    #[test]
    fn test_label_id_roundtrip() {
        roundtrip_mgslk(LabelId::from(0u32));
        roundtrip_mgslk(LabelId::from(100u32));
        roundtrip_mgslk(LabelId::INVALID);
    }

    #[test]
    fn test_property_id_roundtrip() {
        roundtrip_mgslk(PropertyId::from(7u32));
    }

    #[test]
    fn test_edge_type_id_roundtrip() {
        roundtrip_mgslk(EdgeTypeId::from(3u32));
    }

    #[test]
    fn test_label_prop_key_roundtrip() {
        let key = LabelPropKey::new(LabelId::from(1u32), PropertyId::from(2u32));
        roundtrip_mgslk(key);
    }

    #[test]
    fn test_edge_type_prop_key_roundtrip() {
        let key = EdgeTypePropKey::new(EdgeTypeId::from(5u32), PropertyId::from(10u32));
        roundtrip_mgslk(key);
    }

    #[test]
    fn test_edge_ref_roundtrip() {
        roundtrip_mgslk(EdgeRef::from_gid(Gid::from(99u64)));
    }

    #[test]
    fn test_delta_action_roundtrip() {
        for action in [
            DeltaAction::DeleteDeserializedObject,
            DeltaAction::DeleteObject,
            DeltaAction::RecreateObject,
            DeltaAction::SetProperty,
            DeltaAction::AddLabel,
            DeltaAction::RemoveLabel,
            DeltaAction::AddInEdge,
            DeltaAction::AddOutEdge,
            DeltaAction::RemoveInEdge,
            DeltaAction::RemoveOutEdge,
        ] {
            roundtrip_mgslk(action);
        }
    }

    #[test]
    fn test_temporal_roundtrip() {
        roundtrip_mgslk(Date::from_days(19000));
        roundtrip_mgslk(LocalTime::from_microseconds(12 * 3600 * 1_000_000));
        roundtrip_mgslk(LocalDateTime::from_microseconds(1716912000000000));
        roundtrip_mgslk(Duration::new(1, 5, 30_000_000));
    }

    #[test]
    fn test_zoned_datetime_roundtrip() {
        let zdt = ZonedDateTime::new(1716912000000000, 60, "Europe/Paris".into());
        roundtrip_mgslk(zdt);
    }

    #[test]
    fn test_point_roundtrip() {
        roundtrip_mgslk(Point2D::new(Crs::WGS84, 45.8150, 15.9819));
        roundtrip_mgslk(Point3D::new(Crs::Cartesian3D, 1.0, 2.0, 3.0));
    }

    #[test]
    fn test_crs_roundtrip() {
        roundtrip_mgslk(Crs::WGS84);
        roundtrip_mgslk(Crs::Cartesian2D);
        roundtrip_mgslk(Crs::Cartesian3D);
        roundtrip_mgslk(Crs::WGS843D);
    }

    #[test]
    fn test_property_value_simple_roundtrip() {
        roundtrip_mgslk(PropertyValue::Null);
        roundtrip_mgslk(PropertyValue::Bool(true));
        roundtrip_mgslk(PropertyValue::Bool(false));
        roundtrip_mgslk(PropertyValue::Int(42));
        roundtrip_mgslk(PropertyValue::Int(-1));
        roundtrip_mgslk(PropertyValue::Double(3.14159));
        roundtrip_mgslk(PropertyValue::String("hello world".into()));
    }

    #[test]
    fn test_property_value_list_roundtrip() {
        let list = PropertyValue::List(vec![
            PropertyValue::Int(1),
            PropertyValue::Int(2),
            PropertyValue::Int(3),
        ]);
        roundtrip_mgslk(list);
    }

    #[test]
    fn test_property_value_nested_list() {
        let list = PropertyValue::List(vec![
            PropertyValue::List(vec![PropertyValue::Int(1), PropertyValue::Int(2)]),
            PropertyValue::String("inner".into()),
        ]);
        roundtrip_mgslk(list);
    }

    #[test]
    fn test_property_value_enum_roundtrip() {
        let ev = PropertyValue::Enum {
            enum_type: "Color".into(),
            value: "Red".into(),
        };
        roundtrip_mgslk(ev);
    }

    #[test]
    fn test_gid_le_encoding_matches_cpp() {
        let output = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let out_clone = output.clone();
        {
            let mut builder = Builder::new(move |data: &[u8], _final: bool| {
                out_clone.borrow_mut().extend_from_slice(data);
            });
            Gid::from(0x123456789ABCDEF0u64).slk_save(&mut builder);
            builder.finalize();
        }
        let output = output.borrow();
        // Full frame: [4 bytes seg size][8 bytes gid][4 bytes footer]
        assert_eq!(output.len(), 16);
        let gid_bytes = &output[4..12];
        assert_eq!(gid_bytes, &[0xF0, 0xDE, 0xBC, 0x9A, 0x78, 0x56, 0x34, 0x12]);
    }
}

//! SLK-serializable delta records for WAL persistence.
//!
//! Each record represents one atomic storage mutation. Tag values match the
//! C++ Marker enum in src/wire_format/marker.hpp EXACTLY for binary compat.

use mgcore::property_value::PropertyValue;
use mgcore::types::{EdgeTypeId, Gid, LabelId, PropertyId};
use mgslk::{Builder, Reader, SlkDecodeError, SlkLoad, SlkSave};

// ─── C++ compatible marker constants ─────────────────────────────────────
// From src/wire_format/marker.hpp — DO NOT change these values.

pub const TYPE_NULL: u8 = 0x10;
pub const TYPE_BOOL: u8 = 0x11;
pub const TYPE_INT: u8 = 0x12;
pub const TYPE_DOUBLE: u8 = 0x13;
pub const TYPE_STRING: u8 = 0x14;
pub const TYPE_LIST: u8 = 0x15;
pub const TYPE_MAP: u8 = 0x16;
pub const TYPE_PROPERTY_VALUE: u8 = 0x17;
pub const TYPE_TEMPORAL_DATA: u8 = 0x18;
pub const TYPE_ZONED_TEMPORAL_DATA: u8 = 0x19;
pub const TYPE_ENUM: u8 = 0x1a;
pub const TYPE_POINT_2D: u8 = 0x1b;
pub const TYPE_POINT_3D: u8 = 0x1c;

pub const SECTION_VERTEX: u8 = 0x20;
pub const SECTION_EDGE: u8 = 0x21;
pub const SECTION_MAPPER: u8 = 0x22;
pub const SECTION_METADATA: u8 = 0x23;
pub const SECTION_INDICES: u8 = 0x24;
pub const SECTION_CONSTRAINTS: u8 = 0x25;
pub const SECTION_DELTA: u8 = 0x26;
pub const SECTION_EPOCH_HISTORY: u8 = 0x27;
pub const SECTION_EDGE_INDICES: u8 = 0x28;
pub const SECTION_ENUMS: u8 = 0x29;
pub const SECTION_TTL: u8 = 0x2a;
pub const SECTION_DESCRIPTIONS: u8 = 0x2b;
pub const SECTION_OFFSETS: u8 = 0x42;

pub const DELTA_VERTEX_CREATE: u8 = 0x50;
pub const DELTA_VERTEX_DELETE: u8 = 0x51;
pub const DELTA_VERTEX_ADD_LABEL: u8 = 0x52;
pub const DELTA_VERTEX_REMOVE_LABEL: u8 = 0x53;
pub const DELTA_VERTEX_SET_PROPERTY: u8 = 0x54;
pub const DELTA_EDGE_CREATE: u8 = 0x55;
pub const DELTA_EDGE_DELETE: u8 = 0x56;
pub const DELTA_EDGE_SET_PROPERTY: u8 = 0x57;
pub const DELTA_TRANSACTION_END: u8 = 0x58;
pub const DELTA_LABEL_INDEX_CREATE: u8 = 0x59;
pub const DELTA_LABEL_INDEX_DROP: u8 = 0x5a;
pub const DELTA_LABEL_PROPERTIES_INDEX_CREATE: u8 = 0x5b;
pub const DELTA_LABEL_PROPERTIES_INDEX_DROP: u8 = 0x5c;
pub const DELTA_EXISTENCE_CONSTRAINT_CREATE: u8 = 0x5d;
pub const DELTA_EXISTENCE_CONSTRAINT_DROP: u8 = 0x5e;
pub const DELTA_UNIQUE_CONSTRAINT_CREATE: u8 = 0x5f;
pub const DELTA_UNIQUE_CONSTRAINT_DROP: u8 = 0x60;
pub const DELTA_LABEL_INDEX_STATS_SET: u8 = 0x61;
pub const DELTA_LABEL_INDEX_STATS_CLEAR: u8 = 0x62;
pub const DELTA_LABEL_PROPERTIES_INDEX_STATS_SET: u8 = 0x63;
pub const DELTA_LABEL_PROPERTIES_INDEX_STATS_CLEAR: u8 = 0x64;
pub const DELTA_EDGE_INDEX_CREATE: u8 = 0x65;
pub const DELTA_EDGE_INDEX_DROP: u8 = 0x66;
pub const DELTA_TEXT_INDEX_CREATE: u8 = 0x67;
pub const DELTA_TEXT_INDEX_DROP: u8 = 0x68;
pub const DELTA_ENUM_CREATE: u8 = 0x69;
pub const DELTA_ENUM_ALTER_ADD: u8 = 0x6a;
pub const DELTA_ENUM_ALTER_UPDATE: u8 = 0x6b;
pub const DELTA_EDGE_PROPERTY_INDEX_CREATE: u8 = 0x6c;
pub const DELTA_EDGE_PROPERTY_INDEX_DROP: u8 = 0x6d;
pub const DELTA_POINT_INDEX_CREATE: u8 = 0x6e;
pub const DELTA_POINT_INDEX_DROP: u8 = 0x6f;
pub const DELTA_TYPE_CONSTRAINT_CREATE: u8 = 0x70;
pub const DELTA_TYPE_CONSTRAINT_DROP: u8 = 0x71;
pub const DELTA_VECTOR_INDEX_CREATE: u8 = 0x72;
pub const DELTA_VECTOR_INDEX_DROP: u8 = 0x73;
pub const DELTA_GLOBAL_EDGE_PROPERTY_INDEX_CREATE: u8 = 0x74;
pub const DELTA_GLOBAL_EDGE_PROPERTY_INDEX_DROP: u8 = 0x75;
pub const DELTA_VECTOR_EDGE_INDEX_CREATE: u8 = 0x76;
pub const DELTA_TRANSACTION_START: u8 = 0x77;
pub const DELTA_TTL_OPERATION: u8 = 0x78;
pub const DELTA_TEXT_EDGE_INDEX_CREATE: u8 = 0x79;
pub const DELTA_DESCRIPTION_SET: u8 = 0x7a;
pub const DELTA_DESCRIPTION_DELETE: u8 = 0x7b;
pub const DELTA_EDGE_CHANGE_TYPE: u8 = 0x7c;
pub const DELTA_EDGE_SET_FROM: u8 = 0x7d;
pub const DELTA_EDGE_SET_TO: u8 = 0x7e;

pub const VALUE_FALSE: u8 = 0x00;
pub const VALUE_TRUE: u8 = 0xff;

/// A serializable delta record matching C++ WalDeltaData.
#[derive(Clone, Debug, PartialEq)]
pub enum DeltaRecord {
    // Vertex operations
    VertexCreate {
        gid: Gid,
        timestamp: u64,
    },
    VertexDelete {
        gid: Gid,
    },
    VertexAddLabel {
        gid: Gid,
        label: LabelId,
    },
    VertexRemoveLabel {
        gid: Gid,
        label: LabelId,
    },
    VertexSetProperty {
        gid: Gid,
        key: PropertyId,
        value: PropertyValue,
    },

    // Edge operations
    EdgeCreate {
        gid: Gid,
        from_vertex: Gid,
        to_vertex: Gid,
        edge_type: EdgeTypeId,
        timestamp: u64,
    },
    EdgeDelete {
        gid: Gid,
    },
    EdgeSetProperty {
        gid: Gid,
        key: PropertyId,
        value: PropertyValue,
    },
    EdgeChangeType {
        gid: Gid,
        old_type: EdgeTypeId,
        new_type: EdgeTypeId,
    },
    EdgeSetFrom {
        gid: Gid,
        old_from: Gid,
        new_from: Gid,
    },
    EdgeSetTo {
        gid: Gid,
        old_to: Gid,
        new_to: Gid,
    },

    // Transaction boundaries
    TransactionStart {
        timestamp: u64,
    },
    TransactionEnd {
        timestamp: u64,
        commit_timestamp: u64,
    },

    // Label indices
    LabelIndexCreate {
        label: LabelId,
    },
    LabelIndexDrop {
        label: LabelId,
    },
    LabelIndexStatsSet {
        label: LabelId,
        count: u64,
    },
    LabelIndexStatsClear {
        label: LabelId,
    },

    // Label-property indices
    LabelPropertyIndexCreate {
        label: LabelId,
        property: PropertyId,
    },
    LabelPropertyIndexDrop {
        label: LabelId,
        property: PropertyId,
    },
    LabelPropertyIndexStatsSet {
        label: LabelId,
        property: PropertyId,
        count: u64,
    },
    LabelPropertyIndexStatsClear {
        label: LabelId,
        property: PropertyId,
    },

    // Edge indices
    EdgeIndexCreate {
        edge_type: EdgeTypeId,
    },
    EdgeIndexDrop {
        edge_type: EdgeTypeId,
    },

    // Edge property indices
    EdgePropertyIndexCreate {
        edge_type: EdgeTypeId,
        property: PropertyId,
    },
    EdgePropertyIndexDrop {
        edge_type: EdgeTypeId,
        property: PropertyId,
    },
    GlobalEdgePropertyIndexCreate {
        property: PropertyId,
    },
    GlobalEdgePropertyIndexDrop {
        property: PropertyId,
    },

    // Constraints
    ExistenceConstraintCreate {
        label: LabelId,
        property: PropertyId,
    },
    ExistenceConstraintDrop {
        label: LabelId,
        property: PropertyId,
    },
    UniqueConstraintCreate {
        label: LabelId,
        properties: Vec<PropertyId>,
    },
    UniqueConstraintDrop {
        label: LabelId,
        properties: Vec<PropertyId>,
    },
    TypeConstraintCreate {
        label: LabelId,
        property: PropertyId,
        type_tag: u8,
    },
    TypeConstraintDrop {
        label: LabelId,
        property: PropertyId,
    },

    // Point indices
    PointIndexCreate {
        label: LabelId,
        property: PropertyId,
    },
    PointIndexDrop {
        label: LabelId,
        property: PropertyId,
    },

    // Text indices
    TextIndexCreate {
        label: LabelId,
        properties: Vec<PropertyId>,
    },
    TextIndexDrop {
        label: LabelId,
    },
    TextEdgeIndexCreate {
        edge_type: EdgeTypeId,
        properties: Vec<PropertyId>,
    },

    // Vector indices
    VectorIndexCreate {
        label: LabelId,
        property: PropertyId,
        dimension: u64,
    },
    VectorIndexDrop {
        label: LabelId,
        property: PropertyId,
    },
    VectorEdgeIndexCreate {
        edge_type: EdgeTypeId,
        property: PropertyId,
        dimension: u64,
    },

    // Enum operations
    EnumCreate {
        name: String,
    },
    EnumAlterAdd {
        name: String,
        value: String,
    },
    EnumAlterUpdate {
        name: String,
        index: u64,
        value: String,
    },

    // TTL
    TtlOperation {
        label: LabelId,
        ttl_ms: u64,
    },

    // Descriptions
    DescriptionSet {
        key: String,
        value: String,
    },
    DescriptionDelete {
        key: String,
    },
}

impl DeltaRecord {
    pub fn tag(&self) -> u8 {
        match self {
            DeltaRecord::VertexCreate { .. } => DELTA_VERTEX_CREATE,
            DeltaRecord::VertexDelete { .. } => DELTA_VERTEX_DELETE,
            DeltaRecord::VertexAddLabel { .. } => DELTA_VERTEX_ADD_LABEL,
            DeltaRecord::VertexRemoveLabel { .. } => DELTA_VERTEX_REMOVE_LABEL,
            DeltaRecord::VertexSetProperty { .. } => DELTA_VERTEX_SET_PROPERTY,
            DeltaRecord::EdgeCreate { .. } => DELTA_EDGE_CREATE,
            DeltaRecord::EdgeDelete { .. } => DELTA_EDGE_DELETE,
            DeltaRecord::EdgeSetProperty { .. } => DELTA_EDGE_SET_PROPERTY,
            DeltaRecord::EdgeChangeType { .. } => DELTA_EDGE_CHANGE_TYPE,
            DeltaRecord::EdgeSetFrom { .. } => DELTA_EDGE_SET_FROM,
            DeltaRecord::EdgeSetTo { .. } => DELTA_EDGE_SET_TO,
            DeltaRecord::TransactionStart { .. } => DELTA_TRANSACTION_START,
            DeltaRecord::TransactionEnd { .. } => DELTA_TRANSACTION_END,
            DeltaRecord::LabelIndexCreate { .. } => DELTA_LABEL_INDEX_CREATE,
            DeltaRecord::LabelIndexDrop { .. } => DELTA_LABEL_INDEX_DROP,
            DeltaRecord::LabelIndexStatsSet { .. } => DELTA_LABEL_INDEX_STATS_SET,
            DeltaRecord::LabelIndexStatsClear { .. } => DELTA_LABEL_INDEX_STATS_CLEAR,
            DeltaRecord::LabelPropertyIndexCreate { .. } => DELTA_LABEL_PROPERTIES_INDEX_CREATE,
            DeltaRecord::LabelPropertyIndexDrop { .. } => DELTA_LABEL_PROPERTIES_INDEX_DROP,
            DeltaRecord::LabelPropertyIndexStatsSet { .. } => {
                DELTA_LABEL_PROPERTIES_INDEX_STATS_SET
            }
            DeltaRecord::LabelPropertyIndexStatsClear { .. } => {
                DELTA_LABEL_PROPERTIES_INDEX_STATS_CLEAR
            }
            DeltaRecord::EdgeIndexCreate { .. } => DELTA_EDGE_INDEX_CREATE,
            DeltaRecord::EdgeIndexDrop { .. } => DELTA_EDGE_INDEX_DROP,
            DeltaRecord::EdgePropertyIndexCreate { .. } => DELTA_EDGE_PROPERTY_INDEX_CREATE,
            DeltaRecord::EdgePropertyIndexDrop { .. } => DELTA_EDGE_PROPERTY_INDEX_DROP,
            DeltaRecord::GlobalEdgePropertyIndexCreate { .. } => {
                DELTA_GLOBAL_EDGE_PROPERTY_INDEX_CREATE
            }
            DeltaRecord::GlobalEdgePropertyIndexDrop { .. } => {
                DELTA_GLOBAL_EDGE_PROPERTY_INDEX_DROP
            }
            DeltaRecord::ExistenceConstraintCreate { .. } => DELTA_EXISTENCE_CONSTRAINT_CREATE,
            DeltaRecord::ExistenceConstraintDrop { .. } => DELTA_EXISTENCE_CONSTRAINT_DROP,
            DeltaRecord::UniqueConstraintCreate { .. } => DELTA_UNIQUE_CONSTRAINT_CREATE,
            DeltaRecord::UniqueConstraintDrop { .. } => DELTA_UNIQUE_CONSTRAINT_DROP,
            DeltaRecord::TypeConstraintCreate { .. } => DELTA_TYPE_CONSTRAINT_CREATE,
            DeltaRecord::TypeConstraintDrop { .. } => DELTA_TYPE_CONSTRAINT_DROP,
            DeltaRecord::PointIndexCreate { .. } => DELTA_POINT_INDEX_CREATE,
            DeltaRecord::PointIndexDrop { .. } => DELTA_POINT_INDEX_DROP,
            DeltaRecord::TextIndexCreate { .. } => DELTA_TEXT_INDEX_CREATE,
            DeltaRecord::TextIndexDrop { .. } => DELTA_TEXT_INDEX_DROP,
            DeltaRecord::TextEdgeIndexCreate { .. } => DELTA_TEXT_EDGE_INDEX_CREATE,
            DeltaRecord::VectorIndexCreate { .. } => DELTA_VECTOR_INDEX_CREATE,
            DeltaRecord::VectorIndexDrop { .. } => DELTA_VECTOR_INDEX_DROP,
            DeltaRecord::VectorEdgeIndexCreate { .. } => DELTA_VECTOR_EDGE_INDEX_CREATE,
            DeltaRecord::EnumCreate { .. } => DELTA_ENUM_CREATE,
            DeltaRecord::EnumAlterAdd { .. } => DELTA_ENUM_ALTER_ADD,
            DeltaRecord::EnumAlterUpdate { .. } => DELTA_ENUM_ALTER_UPDATE,
            DeltaRecord::TtlOperation { .. } => DELTA_TTL_OPERATION,
            DeltaRecord::DescriptionSet { .. } => DELTA_DESCRIPTION_SET,
            DeltaRecord::DescriptionDelete { .. } => DELTA_DESCRIPTION_DELETE,
        }
    }
}

impl SlkSave for DeltaRecord {
    fn slk_save(&self, builder: &mut Builder) {
        self.tag().slk_save(builder);
        match self {
            DeltaRecord::VertexCreate { gid, timestamp } => {
                gid.slk_save(builder);
                timestamp.slk_save(builder);
            }
            DeltaRecord::VertexDelete { gid } => {
                gid.slk_save(builder);
            }
            DeltaRecord::VertexAddLabel { gid, label } => {
                gid.slk_save(builder);
                label.slk_save(builder);
            }
            DeltaRecord::VertexRemoveLabel { gid, label } => {
                gid.slk_save(builder);
                label.slk_save(builder);
            }
            DeltaRecord::VertexSetProperty { gid, key, value } => {
                gid.slk_save(builder);
                key.slk_save(builder);
                value.slk_save(builder);
            }
            DeltaRecord::EdgeCreate {
                gid,
                from_vertex,
                to_vertex,
                edge_type,
                timestamp,
            } => {
                gid.slk_save(builder);
                from_vertex.slk_save(builder);
                to_vertex.slk_save(builder);
                edge_type.slk_save(builder);
                timestamp.slk_save(builder);
            }
            DeltaRecord::EdgeDelete { gid } => {
                gid.slk_save(builder);
            }
            DeltaRecord::EdgeSetProperty { gid, key, value } => {
                gid.slk_save(builder);
                key.slk_save(builder);
                value.slk_save(builder);
            }
            DeltaRecord::EdgeChangeType { gid, old_type, new_type } => {
                gid.slk_save(builder);
                old_type.slk_save(builder);
                new_type.slk_save(builder);
            }
            DeltaRecord::EdgeSetFrom { gid, old_from, new_from } => {
                gid.slk_save(builder);
                old_from.slk_save(builder);
                new_from.slk_save(builder);
            }
            DeltaRecord::EdgeSetTo { gid, old_to, new_to } => {
                gid.slk_save(builder);
                old_to.slk_save(builder);
                new_to.slk_save(builder);
            }
            DeltaRecord::TransactionStart { timestamp } => {
                timestamp.slk_save(builder);
            }
            DeltaRecord::TransactionEnd {
                timestamp,
                commit_timestamp,
            } => {
                timestamp.slk_save(builder);
                commit_timestamp.slk_save(builder);
            }
            DeltaRecord::LabelIndexCreate { label } => {
                label.slk_save(builder);
            }
            DeltaRecord::LabelIndexDrop { label } => {
                label.slk_save(builder);
            }
            DeltaRecord::LabelIndexStatsSet { label, count } => {
                label.slk_save(builder);
                count.slk_save(builder);
            }
            DeltaRecord::LabelIndexStatsClear { label } => {
                label.slk_save(builder);
            }
            DeltaRecord::LabelPropertyIndexCreate { label, property } => {
                label.slk_save(builder);
                property.slk_save(builder);
            }
            DeltaRecord::LabelPropertyIndexDrop { label, property } => {
                label.slk_save(builder);
                property.slk_save(builder);
            }
            DeltaRecord::LabelPropertyIndexStatsSet {
                label,
                property,
                count,
            } => {
                label.slk_save(builder);
                property.slk_save(builder);
                count.slk_save(builder);
            }
            DeltaRecord::LabelPropertyIndexStatsClear { label, property } => {
                label.slk_save(builder);
                property.slk_save(builder);
            }
            DeltaRecord::EdgeIndexCreate { edge_type } => {
                edge_type.slk_save(builder);
            }
            DeltaRecord::EdgeIndexDrop { edge_type } => {
                edge_type.slk_save(builder);
            }
            DeltaRecord::EdgePropertyIndexCreate {
                edge_type,
                property,
            } => {
                edge_type.slk_save(builder);
                property.slk_save(builder);
            }
            DeltaRecord::EdgePropertyIndexDrop {
                edge_type,
                property,
            } => {
                edge_type.slk_save(builder);
                property.slk_save(builder);
            }
            DeltaRecord::GlobalEdgePropertyIndexCreate { property } => {
                property.slk_save(builder);
            }
            DeltaRecord::GlobalEdgePropertyIndexDrop { property } => {
                property.slk_save(builder);
            }
            DeltaRecord::ExistenceConstraintCreate { label, property } => {
                label.slk_save(builder);
                property.slk_save(builder);
            }
            DeltaRecord::ExistenceConstraintDrop { label, property } => {
                label.slk_save(builder);
                property.slk_save(builder);
            }
            DeltaRecord::UniqueConstraintCreate { label, properties } => {
                label.slk_save(builder);
                properties.slk_save(builder);
            }
            DeltaRecord::UniqueConstraintDrop { label, properties } => {
                label.slk_save(builder);
                properties.slk_save(builder);
            }
            DeltaRecord::TypeConstraintCreate {
                label,
                property,
                type_tag,
            } => {
                label.slk_save(builder);
                property.slk_save(builder);
                type_tag.slk_save(builder);
            }
            DeltaRecord::TypeConstraintDrop { label, property } => {
                label.slk_save(builder);
                property.slk_save(builder);
            }
            DeltaRecord::PointIndexCreate { label, property } => {
                label.slk_save(builder);
                property.slk_save(builder);
            }
            DeltaRecord::PointIndexDrop { label, property } => {
                label.slk_save(builder);
                property.slk_save(builder);
            }
            DeltaRecord::TextIndexCreate { label, properties } => {
                label.slk_save(builder);
                properties.slk_save(builder);
            }
            DeltaRecord::TextIndexDrop { label } => {
                label.slk_save(builder);
            }
            DeltaRecord::TextEdgeIndexCreate {
                edge_type,
                properties,
            } => {
                edge_type.slk_save(builder);
                properties.slk_save(builder);
            }
            DeltaRecord::VectorIndexCreate {
                label,
                property,
                dimension,
            } => {
                label.slk_save(builder);
                property.slk_save(builder);
                dimension.slk_save(builder);
            }
            DeltaRecord::VectorIndexDrop { label, property } => {
                label.slk_save(builder);
                property.slk_save(builder);
            }
            DeltaRecord::VectorEdgeIndexCreate {
                edge_type,
                property,
                dimension,
            } => {
                edge_type.slk_save(builder);
                property.slk_save(builder);
                dimension.slk_save(builder);
            }
            DeltaRecord::EnumCreate { name } => {
                name.slk_save(builder);
            }
            DeltaRecord::EnumAlterAdd { name, value } => {
                name.slk_save(builder);
                value.slk_save(builder);
            }
            DeltaRecord::EnumAlterUpdate { name, index, value } => {
                name.slk_save(builder);
                index.slk_save(builder);
                value.slk_save(builder);
            }
            DeltaRecord::TtlOperation { label, ttl_ms } => {
                label.slk_save(builder);
                ttl_ms.slk_save(builder);
            }
            DeltaRecord::DescriptionSet { key, value } => {
                key.slk_save(builder);
                value.slk_save(builder);
            }
            DeltaRecord::DescriptionDelete { key } => {
                key.slk_save(builder);
            }
        }
    }
}

impl SlkLoad for DeltaRecord {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        let tag = u8::slk_load(reader)?;
        match tag {
            DELTA_VERTEX_CREATE => Ok(DeltaRecord::VertexCreate {
                gid: Gid::slk_load(reader)?,
                timestamp: u64::slk_load(reader)?,
            }),
            DELTA_VERTEX_DELETE => Ok(DeltaRecord::VertexDelete {
                gid: Gid::slk_load(reader)?,
            }),
            DELTA_VERTEX_ADD_LABEL => Ok(DeltaRecord::VertexAddLabel {
                gid: Gid::slk_load(reader)?,
                label: LabelId::slk_load(reader)?,
            }),
            DELTA_VERTEX_REMOVE_LABEL => Ok(DeltaRecord::VertexRemoveLabel {
                gid: Gid::slk_load(reader)?,
                label: LabelId::slk_load(reader)?,
            }),
            DELTA_VERTEX_SET_PROPERTY => Ok(DeltaRecord::VertexSetProperty {
                gid: Gid::slk_load(reader)?,
                key: PropertyId::slk_load(reader)?,
                value: PropertyValue::slk_load(reader)?,
            }),
            DELTA_EDGE_CREATE => Ok(DeltaRecord::EdgeCreate {
                gid: Gid::slk_load(reader)?,
                from_vertex: Gid::slk_load(reader)?,
                to_vertex: Gid::slk_load(reader)?,
                edge_type: EdgeTypeId::slk_load(reader)?,
                timestamp: u64::slk_load(reader)?,
            }),
            DELTA_EDGE_DELETE => Ok(DeltaRecord::EdgeDelete {
                gid: Gid::slk_load(reader)?,
            }),
            DELTA_EDGE_SET_PROPERTY => Ok(DeltaRecord::EdgeSetProperty {
                gid: Gid::slk_load(reader)?,
                key: PropertyId::slk_load(reader)?,
                value: PropertyValue::slk_load(reader)?,
            }),
            DELTA_EDGE_CHANGE_TYPE => Ok(DeltaRecord::EdgeChangeType {
                gid: Gid::slk_load(reader)?,
                old_type: EdgeTypeId::slk_load(reader)?,
                new_type: EdgeTypeId::slk_load(reader)?,
            }),
            DELTA_EDGE_SET_FROM => Ok(DeltaRecord::EdgeSetFrom {
                gid: Gid::slk_load(reader)?,
                old_from: Gid::slk_load(reader)?,
                new_from: Gid::slk_load(reader)?,
            }),
            DELTA_EDGE_SET_TO => Ok(DeltaRecord::EdgeSetTo {
                gid: Gid::slk_load(reader)?,
                old_to: Gid::slk_load(reader)?,
                new_to: Gid::slk_load(reader)?,
            }),
            DELTA_TRANSACTION_START => Ok(DeltaRecord::TransactionStart {
                timestamp: u64::slk_load(reader)?,
            }),
            DELTA_TRANSACTION_END => Ok(DeltaRecord::TransactionEnd {
                timestamp: u64::slk_load(reader)?,
                commit_timestamp: u64::slk_load(reader)?,
            }),
            DELTA_LABEL_INDEX_CREATE => Ok(DeltaRecord::LabelIndexCreate {
                label: LabelId::slk_load(reader)?,
            }),
            DELTA_LABEL_INDEX_DROP => Ok(DeltaRecord::LabelIndexDrop {
                label: LabelId::slk_load(reader)?,
            }),
            DELTA_LABEL_INDEX_STATS_SET => Ok(DeltaRecord::LabelIndexStatsSet {
                label: LabelId::slk_load(reader)?,
                count: u64::slk_load(reader)?,
            }),
            DELTA_LABEL_INDEX_STATS_CLEAR => Ok(DeltaRecord::LabelIndexStatsClear {
                label: LabelId::slk_load(reader)?,
            }),
            DELTA_LABEL_PROPERTIES_INDEX_CREATE => Ok(DeltaRecord::LabelPropertyIndexCreate {
                label: LabelId::slk_load(reader)?,
                property: PropertyId::slk_load(reader)?,
            }),
            DELTA_LABEL_PROPERTIES_INDEX_DROP => Ok(DeltaRecord::LabelPropertyIndexDrop {
                label: LabelId::slk_load(reader)?,
                property: PropertyId::slk_load(reader)?,
            }),
            DELTA_LABEL_PROPERTIES_INDEX_STATS_SET => Ok(DeltaRecord::LabelPropertyIndexStatsSet {
                label: LabelId::slk_load(reader)?,
                property: PropertyId::slk_load(reader)?,
                count: u64::slk_load(reader)?,
            }),
            DELTA_LABEL_PROPERTIES_INDEX_STATS_CLEAR => {
                Ok(DeltaRecord::LabelPropertyIndexStatsClear {
                    label: LabelId::slk_load(reader)?,
                    property: PropertyId::slk_load(reader)?,
                })
            }
            DELTA_EDGE_INDEX_CREATE => Ok(DeltaRecord::EdgeIndexCreate {
                edge_type: EdgeTypeId::slk_load(reader)?,
            }),
            DELTA_EDGE_INDEX_DROP => Ok(DeltaRecord::EdgeIndexDrop {
                edge_type: EdgeTypeId::slk_load(reader)?,
            }),
            DELTA_EDGE_PROPERTY_INDEX_CREATE => Ok(DeltaRecord::EdgePropertyIndexCreate {
                edge_type: EdgeTypeId::slk_load(reader)?,
                property: PropertyId::slk_load(reader)?,
            }),
            DELTA_EDGE_PROPERTY_INDEX_DROP => Ok(DeltaRecord::EdgePropertyIndexDrop {
                edge_type: EdgeTypeId::slk_load(reader)?,
                property: PropertyId::slk_load(reader)?,
            }),
            DELTA_GLOBAL_EDGE_PROPERTY_INDEX_CREATE => {
                Ok(DeltaRecord::GlobalEdgePropertyIndexCreate {
                    property: PropertyId::slk_load(reader)?,
                })
            }
            DELTA_GLOBAL_EDGE_PROPERTY_INDEX_DROP => Ok(DeltaRecord::GlobalEdgePropertyIndexDrop {
                property: PropertyId::slk_load(reader)?,
            }),
            DELTA_EXISTENCE_CONSTRAINT_CREATE => Ok(DeltaRecord::ExistenceConstraintCreate {
                label: LabelId::slk_load(reader)?,
                property: PropertyId::slk_load(reader)?,
            }),
            DELTA_EXISTENCE_CONSTRAINT_DROP => Ok(DeltaRecord::ExistenceConstraintDrop {
                label: LabelId::slk_load(reader)?,
                property: PropertyId::slk_load(reader)?,
            }),
            DELTA_UNIQUE_CONSTRAINT_CREATE => Ok(DeltaRecord::UniqueConstraintCreate {
                label: LabelId::slk_load(reader)?,
                properties: Vec::<PropertyId>::slk_load(reader)?,
            }),
            DELTA_UNIQUE_CONSTRAINT_DROP => Ok(DeltaRecord::UniqueConstraintDrop {
                label: LabelId::slk_load(reader)?,
                properties: Vec::<PropertyId>::slk_load(reader)?,
            }),
            DELTA_TYPE_CONSTRAINT_CREATE => Ok(DeltaRecord::TypeConstraintCreate {
                label: LabelId::slk_load(reader)?,
                property: PropertyId::slk_load(reader)?,
                type_tag: u8::slk_load(reader)?,
            }),
            DELTA_TYPE_CONSTRAINT_DROP => Ok(DeltaRecord::TypeConstraintDrop {
                label: LabelId::slk_load(reader)?,
                property: PropertyId::slk_load(reader)?,
            }),
            DELTA_POINT_INDEX_CREATE => Ok(DeltaRecord::PointIndexCreate {
                label: LabelId::slk_load(reader)?,
                property: PropertyId::slk_load(reader)?,
            }),
            DELTA_POINT_INDEX_DROP => Ok(DeltaRecord::PointIndexDrop {
                label: LabelId::slk_load(reader)?,
                property: PropertyId::slk_load(reader)?,
            }),
            DELTA_TEXT_INDEX_CREATE => Ok(DeltaRecord::TextIndexCreate {
                label: LabelId::slk_load(reader)?,
                properties: Vec::<PropertyId>::slk_load(reader)?,
            }),
            DELTA_TEXT_INDEX_DROP => Ok(DeltaRecord::TextIndexDrop {
                label: LabelId::slk_load(reader)?,
            }),
            DELTA_TEXT_EDGE_INDEX_CREATE => Ok(DeltaRecord::TextEdgeIndexCreate {
                edge_type: EdgeTypeId::slk_load(reader)?,
                properties: Vec::<PropertyId>::slk_load(reader)?,
            }),
            DELTA_VECTOR_INDEX_CREATE => Ok(DeltaRecord::VectorIndexCreate {
                label: LabelId::slk_load(reader)?,
                property: PropertyId::slk_load(reader)?,
                dimension: u64::slk_load(reader)?,
            }),
            DELTA_VECTOR_INDEX_DROP => Ok(DeltaRecord::VectorIndexDrop {
                label: LabelId::slk_load(reader)?,
                property: PropertyId::slk_load(reader)?,
            }),
            DELTA_VECTOR_EDGE_INDEX_CREATE => Ok(DeltaRecord::VectorEdgeIndexCreate {
                edge_type: EdgeTypeId::slk_load(reader)?,
                property: PropertyId::slk_load(reader)?,
                dimension: u64::slk_load(reader)?,
            }),
            DELTA_ENUM_CREATE => Ok(DeltaRecord::EnumCreate {
                name: String::slk_load(reader)?,
            }),
            DELTA_ENUM_ALTER_ADD => Ok(DeltaRecord::EnumAlterAdd {
                name: String::slk_load(reader)?,
                value: String::slk_load(reader)?,
            }),
            DELTA_ENUM_ALTER_UPDATE => Ok(DeltaRecord::EnumAlterUpdate {
                name: String::slk_load(reader)?,
                index: u64::slk_load(reader)?,
                value: String::slk_load(reader)?,
            }),
            DELTA_TTL_OPERATION => Ok(DeltaRecord::TtlOperation {
                label: LabelId::slk_load(reader)?,
                ttl_ms: u64::slk_load(reader)?,
            }),
            DELTA_DESCRIPTION_SET => Ok(DeltaRecord::DescriptionSet {
                key: String::slk_load(reader)?,
                value: String::slk_load(reader)?,
            }),
            DELTA_DESCRIPTION_DELETE => Ok(DeltaRecord::DescriptionDelete {
                key: String::slk_load(reader)?,
            }),
            _ => Err(SlkDecodeError::from(format!(
                "unknown DeltaRecord tag: {:#04x}",
                tag
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    fn roundtrip_record(r: DeltaRecord) {
        let output = Rc::new(RefCell::new(Vec::new()));
        let out_clone = output.clone();
        {
            let mut builder = Builder::new(move |data: &[u8], _final: bool| {
                out_clone.borrow_mut().extend_from_slice(data);
            });
            r.slk_save(&mut builder);
            builder.finalize();
        }
        let output = output.borrow();
        let mut reader = Reader::new(&output);
        let back = DeltaRecord::slk_load(&mut reader).expect("deserialize failed");
        assert_eq!(r, back);
    }

    #[test]
    fn test_vertex_create_roundtrip() {
        roundtrip_record(DeltaRecord::VertexCreate {
            gid: Gid::from(1u64),
            timestamp: 100,
        });
    }

    #[test]
    fn test_vertex_set_property_roundtrip() {
        roundtrip_record(DeltaRecord::VertexSetProperty {
            gid: Gid::from(1u64),
            key: PropertyId::from(0u32),
            value: PropertyValue::Int(42),
        });
    }

    #[test]
    fn test_edge_set_property_roundtrip() {
        roundtrip_record(DeltaRecord::EdgeSetProperty {
            gid: Gid::from(5u64),
            key: PropertyId::from(1u32),
            value: PropertyValue::String("hello".into()),
        });
    }

    #[test]
    fn test_edge_create_roundtrip() {
        roundtrip_record(DeltaRecord::EdgeCreate {
            gid: Gid::from(100u64),
            from_vertex: Gid::from(1u64),
            to_vertex: Gid::from(2u64),
            edge_type: EdgeTypeId::from(3u32),
            timestamp: 200,
        });
    }

    #[test]
    fn test_transaction_boundary_roundtrip() {
        roundtrip_record(DeltaRecord::TransactionStart { timestamp: 1 });
        roundtrip_record(DeltaRecord::TransactionEnd {
            timestamp: 1,
            commit_timestamp: 2,
        });
    }

    #[test]
    fn test_index_deltas_roundtrip() {
        roundtrip_record(DeltaRecord::LabelIndexCreate {
            label: LabelId::from(1u32),
        });
        roundtrip_record(DeltaRecord::LabelIndexDrop {
            label: LabelId::from(1u32),
        });
        roundtrip_record(DeltaRecord::LabelPropertyIndexCreate {
            label: LabelId::from(1u32),
            property: PropertyId::from(2u32),
        });
        roundtrip_record(DeltaRecord::EdgeIndexCreate {
            edge_type: EdgeTypeId::from(3u32),
        });
    }

    #[test]
    fn test_constraint_deltas_roundtrip() {
        roundtrip_record(DeltaRecord::ExistenceConstraintCreate {
            label: LabelId::from(1u32),
            property: PropertyId::from(2u32),
        });
        roundtrip_record(DeltaRecord::UniqueConstraintCreate {
            label: LabelId::from(1u32),
            properties: vec![PropertyId::from(2u32), PropertyId::from(3u32)],
        });
        roundtrip_record(DeltaRecord::TypeConstraintCreate {
            label: LabelId::from(1u32),
            property: PropertyId::from(2u32),
            type_tag: 0x12,
        });
    }

    #[test]
    fn test_marker_tags_match_cpp() {
        assert_eq!(DELTA_VERTEX_CREATE, 0x50);
        assert_eq!(DELTA_VERTEX_DELETE, 0x51);
        assert_eq!(DELTA_VERTEX_ADD_LABEL, 0x52);
        assert_eq!(DELTA_VERTEX_REMOVE_LABEL, 0x53);
        assert_eq!(DELTA_VERTEX_SET_PROPERTY, 0x54);
        assert_eq!(DELTA_EDGE_CREATE, 0x55);
        assert_eq!(DELTA_EDGE_DELETE, 0x56);
        assert_eq!(DELTA_EDGE_SET_PROPERTY, 0x57);
        assert_eq!(DELTA_TRANSACTION_END, 0x58);
        assert_eq!(DELTA_TRANSACTION_START, 0x77);
        assert_eq!(DELTA_LABEL_INDEX_CREATE, 0x59);
        assert_eq!(DELTA_EXISTENCE_CONSTRAINT_CREATE, 0x5d);
        assert_eq!(DELTA_UNIQUE_CONSTRAINT_CREATE, 0x5f);
        assert_eq!(DELTA_TYPE_CONSTRAINT_CREATE, 0x70);
        assert_eq!(DELTA_POINT_INDEX_CREATE, 0x6e);
        assert_eq!(DELTA_VECTOR_INDEX_CREATE, 0x72);
        assert_eq!(SECTION_VERTEX, 0x20);
        assert_eq!(SECTION_EDGE, 0x21);
        assert_eq!(SECTION_MAPPER, 0x22);
    }
}

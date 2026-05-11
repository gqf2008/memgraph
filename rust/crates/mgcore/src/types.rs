use std::fmt;
use std::hash::Hash;
use std::num::ParseIntError;
use std::str::FromStr;

macro_rules! define_id_type {
    ($name:ident, $store:ty, $conv:ty) => {
        #[derive(
            Clone,
            Copy,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            Default,
            serde::Serialize,
            serde::Deserialize,
        )]
        #[repr(transparent)]
        pub struct $name($store);

        impl $name {
            pub const fn from_uint(id: $store) -> Self {
                Self(id)
            }

            pub const fn from_int(id: $conv) -> Self {
                Self(id as $store)
            }

            pub const fn as_uint(self) -> $store {
                self.0
            }

            pub const fn as_int(self) -> $conv {
                self.0 as $conv
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self.0)
            }
        }

        impl FromStr for $name {
            type Err = ParseIntError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                s.parse::<$store>().map(Self)
            }
        }

        impl From<$store> for $name {
            fn from(id: $store) -> Self {
                Self(id)
            }
        }

        impl From<$name> for $store {
            fn from(id: $name) -> Self {
                id.0
            }
        }
    };
}

define_id_type!(Gid, u64, i64);
define_id_type!(LabelId, u32, i32);
define_id_type!(PropertyId, u32, i32);
define_id_type!(EdgeTypeId, u32, i32);

impl Gid {
    pub const INVALID: Self = Self(u64::MAX);
}

impl LabelId {
    pub const INVALID: Self = Self(u32::MAX);
}

impl PropertyId {
    pub const INVALID: Self = Self(u32::MAX);
}

impl EdgeTypeId {
    pub const INVALID: Self = Self(u32::MAX);
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct LabelPropKey {
    pub label: LabelId,
    pub property: PropertyId,
}

impl LabelPropKey {
    pub const fn new(label: LabelId, property: PropertyId) -> Self {
        Self { label, property }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct EdgeTypePropKey {
    pub edge_type: EdgeTypeId,
    pub property: PropertyId,
}

impl EdgeTypePropKey {
    pub const fn new(edge_type: EdgeTypeId, property: PropertyId) -> Self {
        Self {
            edge_type,
            property,
        }
    }
}

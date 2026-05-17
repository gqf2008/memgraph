//! Point index for 2D/3D spatial queries. Uses `rstar` R-trees for
//! O(log n) bounding-box and nearest-neighbor lookups instead of linear scan.

use std::collections::HashMap;
use std::sync::RwLock;

use rstar::{PointDistance, RTree, RTreeObject, AABB};

use mgcore::point::{Point2D, Point3D};
use mgcore::types::{Gid, LabelId, PropertyId};

/// R-tree entry: a 2D point with associated GID.
#[derive(Clone, Debug, PartialEq)]
struct Point2dEntry {
    geom: [f64; 2],
    gid: Gid,
}

impl RTreeObject for Point2dEntry {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_point(self.geom)
    }
}

impl PointDistance for Point2dEntry {
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        let dx = self.geom[0] - point[0];
        let dy = self.geom[1] - point[1];
        dx * dx + dy * dy
    }
}

/// R-tree entry: a 3D point with associated GID.
#[derive(Clone, Debug, PartialEq)]
struct Point3dEntry {
    geom: [f64; 3],
    gid: Gid,
}

impl RTreeObject for Point3dEntry {
    type Envelope = AABB<[f64; 3]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_point(self.geom)
    }
}

impl PointDistance for Point3dEntry {
    fn distance_2(&self, point: &[f64; 3]) -> f64 {
        let dx = self.geom[0] - point[0];
        let dy = self.geom[1] - point[1];
        let dz = self.geom[2] - point[2];
        dx * dx + dy * dy + dz * dz
    }
}

/// Per-index spatial tree: 2D or 3D.
enum SpatialTree {
    Tree2D(RTree<Point2dEntry>),
    Tree3D(RTree<Point3dEntry>),
}

/// Spatial index for 2D and 3D points. Uses R-trees for O(log n)
/// bounding-box queries and nearest-neighbor search.
pub struct PointIndex {
    entries: RwLock<HashMap<(LabelId, PropertyId), SpatialTree>>,
}

impl PointIndex {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Index a 2D point for a vertex on a given label+property.
    pub fn insert_2d(&self, label: LabelId, prop: PropertyId, gid: Gid, point: Point2D) {
        let key = (label, prop);
        let mut map = self.entries.write().unwrap();
        let entry = Point2dEntry {
            geom: [point.x, point.y],
            gid,
        };
        match map.get_mut(&key) {
            Some(SpatialTree::Tree2D(tree)) => {
                // Remove old entry for this GID, then insert
                let entries: Vec<_> = tree.iter().filter(|e| e.gid != gid).cloned().collect();
                *tree = RTree::bulk_load(entries);
                tree.insert(entry);
            }
            Some(_) => {
                // Mixed 2D/3D — replace with 2D tree
            }
            None => {
                let mut tree = RTree::new();
                tree.insert(entry);
                map.insert(key, SpatialTree::Tree2D(tree));
            }
        }
    }

    /// Index a 3D point for a vertex on a given label+property.
    pub fn insert_3d(&self, label: LabelId, prop: PropertyId, gid: Gid, point: Point3D) {
        let key = (label, prop);
        let mut map = self.entries.write().unwrap();
        let entry = Point3dEntry {
            geom: [point.x, point.y, point.z],
            gid,
        };
        match map.get_mut(&key) {
            Some(SpatialTree::Tree3D(tree)) => {
                let entries: Vec<_> = tree.iter().filter(|e| e.gid != gid).cloned().collect();
                *tree = RTree::bulk_load(entries);
                tree.insert(entry);
            }
            Some(_) => {}
            None => {
                let mut tree = RTree::new();
                tree.insert(entry);
                map.insert(key, SpatialTree::Tree3D(tree));
            }
        }
    }

    /// Remove a vertex's point from the index.
    pub fn remove(&self, label: LabelId, prop: PropertyId, gid: Gid) {
        let key = (label, prop);
        if let Ok(mut map) = self.entries.write() {
            if let Some(tree) = map.get_mut(&key) {
                match tree {
                    SpatialTree::Tree2D(t) => {
                        let entries: Vec<_> = t.iter().filter(|e| e.gid != gid).cloned().collect();
                        *t = RTree::bulk_load(entries);
                    }
                    SpatialTree::Tree3D(t) => {
                        let entries: Vec<_> = t.iter().filter(|e| e.gid != gid).cloned().collect();
                        *t = RTree::bulk_load(entries);
                    }
                }
            }
        }
    }

    /// Check if points are within a 2D bounding box.
    pub fn within_bbox_2d(
        &self,
        label: LabelId,
        prop: PropertyId,
        lower_left: Point2D,
        upper_right: Point2D,
    ) -> Vec<Gid> {
        let key = (label, prop);
        let map = self.entries.read().unwrap();
        map.get(&key)
            .map(|tree| match tree {
                SpatialTree::Tree2D(t) => {
                    let envelope = AABB::from_corners(
                        [lower_left.x, lower_left.y],
                        [upper_right.x, upper_right.y],
                    );
                    t.locate_in_envelope(&envelope)
                        .map(|e| e.gid)
                        .collect()
                }
                _ => vec![],
            })
            .unwrap_or_default()
    }

    /// Check if points are within a 3D bounding box.
    pub fn within_bbox_3d(
        &self,
        label: LabelId,
        prop: PropertyId,
        lower_left: Point3D,
        upper_right: Point3D,
    ) -> Vec<Gid> {
        let key = (label, prop);
        let map = self.entries.read().unwrap();
        map.get(&key)
            .map(|tree| match tree {
                SpatialTree::Tree3D(t) => {
                    let envelope = AABB::from_corners(
                        [lower_left.x, lower_left.y, lower_left.z],
                        [upper_right.x, upper_right.y, upper_right.z],
                    );
                    t.locate_in_envelope(&envelope)
                        .map(|e| e.gid)
                        .collect()
                }
                _ => vec![],
            })
            .unwrap_or_default()
    }

    /// Find nearest 2D points to a query point.
    pub fn nearest_2d(
        &self,
        label: LabelId,
        prop: PropertyId,
        query: Point2D,
        k: usize,
    ) -> Vec<(Gid, f64)> {
        let key = (label, prop);
        let map = self.entries.read().unwrap();
        map.get(&key)
            .map(|tree| match tree {
                SpatialTree::Tree2D(t) => {
                    let q = [query.x, query.y];
                    t.nearest_neighbor_iter(&q)
                        .take(k)
                        .map(|e| (e.gid, e.distance_2(&q)))
                        .collect()
                }
                _ => vec![],
            })
            .unwrap_or_default()
    }

    /// Find nearest 3D points to a query point.
    pub fn nearest_3d(
        &self,
        label: LabelId,
        prop: PropertyId,
        query: Point3D,
        k: usize,
    ) -> Vec<(Gid, f64)> {
        let key = (label, prop);
        let map = self.entries.read().unwrap();
        map.get(&key)
            .map(|tree| match tree {
                SpatialTree::Tree3D(t) => {
                    let q = [query.x, query.y, query.z];
                    t.nearest_neighbor_iter(&q)
                        .take(k)
                        .map(|e| (e.gid, e.distance_2(&q)))
                        .collect()
                }
                _ => vec![],
            })
            .unwrap_or_default()
    }

    /// Number of indexed points for a given label+property.
    pub fn count(&self, label: LabelId, prop: PropertyId) -> usize {
        let key = (label, prop);
        let map = self.entries.read().unwrap();
        map.get(&key)
            .map(|tree| match tree {
                SpatialTree::Tree2D(t) => t.size(),
                SpatialTree::Tree3D(t) => t.size(),
            })
            .unwrap_or(0)
    }

    /// Clear all entries.
    pub fn clear(&self) {
        self.entries.write().unwrap().clear();
    }
}

impl Default for PointIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mgcore::point::Crs;

    #[test]
    fn test_insert_and_search_2d() {
        let idx = PointIndex::new();
        let label = LabelId::from(1u32);
        let prop = PropertyId::from(0u32);
        let p1 = Point2D::new(Crs::WGS84, 10.0, 20.0);
        let p2 = Point2D::new(Crs::WGS84, 15.0, 25.0);
        idx.insert_2d(label, prop, Gid::from(1u64), p1);
        idx.insert_2d(label, prop, Gid::from(2u64), p2);

        let in_box = idx.within_bbox_2d(
            label,
            prop,
            Point2D::new(Crs::WGS84, 5.0, 15.0),
            Point2D::new(Crs::WGS84, 20.0, 30.0),
        );
        assert_eq!(in_box.len(), 2);

        let nearest = idx.nearest_2d(label, prop, Point2D::new(Crs::WGS84, 10.0, 20.0), 1);
        assert_eq!(nearest[0].0, Gid::from(1u64));
    }

    #[test]
    fn test_remove() {
        let idx = PointIndex::new();
        let label = LabelId::from(1u32);
        let prop = PropertyId::from(0u32);
        idx.insert_2d(
            label,
            prop,
            Gid::from(1u64),
            Point2D::new(Crs::WGS84, 10.0, 20.0),
        );
        assert_eq!(idx.count(label, prop), 1);
        idx.remove(label, prop, Gid::from(1u64));
        assert_eq!(idx.count(label, prop), 0);
    }

    #[test]
    fn test_nearest_3d() {
        let idx = PointIndex::new();
        let label = LabelId::from(1u32);
        let prop = PropertyId::from(0u32);
        idx.insert_3d(
            label,
            prop,
            Gid::from(1u64),
            Point3D::new(Crs::Cartesian3D, 0.0, 0.0, 0.0),
        );
        idx.insert_3d(
            label,
            prop,
            Gid::from(2u64),
            Point3D::new(Crs::Cartesian3D, 10.0, 10.0, 10.0),
        );

        let nearest = idx.nearest_3d(
            label,
            prop,
            Point3D::new(Crs::Cartesian3D, 1.0, 1.0, 1.0),
            1,
        );
        assert_eq!(nearest[0].0, Gid::from(1u64));

        let in_box = idx.within_bbox_3d(
            label,
            prop,
            Point3D::new(Crs::Cartesian3D, -5.0, -5.0, -5.0),
            Point3D::new(Crs::Cartesian3D, 5.0, 5.0, 5.0),
        );
        assert_eq!(in_box.len(), 1);
    }

    #[test]
    fn test_bbox_empty() {
        let idx = PointIndex::new();
        let in_box = idx.within_bbox_2d(
            LabelId::from(1u32),
            PropertyId::from(0u32),
            Point2D::new(Crs::WGS84, 0.0, 0.0),
            Point2D::new(Crs::WGS84, 10.0, 10.0),
        );
        assert!(in_box.is_empty());
    }
}

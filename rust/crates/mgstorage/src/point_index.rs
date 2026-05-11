//! Point index for 2D/3D spatial queries. Matches C++ `indices/point_index.cpp`.

use std::collections::HashMap;
use std::sync::RwLock;

use mgcore::point::{Point2D, Point3D};
use mgcore::types::{Gid, LabelId, PropertyId};

/// Index entry for a spatial point.
#[derive(Clone, Debug)]
#[allow(dead_code)]
struct PointEntry {
    gid: Gid,
    point_2d: Option<Point2D>,
    point_3d: Option<Point3D>,
}

/// Spatial index for 2D and 3D points. Supports bounding-box queries
/// and nearest-neighbor search via linear scan with pruning.
pub struct PointIndex {
    entries: RwLock<HashMap<(LabelId, PropertyId), Vec<PointEntry>>>,
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
        let entries = map.entry(key).or_default();
        // Remove existing entry for this GID
        entries.retain(|e| e.gid != gid);
        entries.push(PointEntry {
            gid,
            point_2d: Some(point),
            point_3d: None,
        });
    }

    /// Index a 3D point for a vertex on a given label+property.
    pub fn insert_3d(&self, label: LabelId, prop: PropertyId, gid: Gid, point: Point3D) {
        let key = (label, prop);
        let mut map = self.entries.write().unwrap();
        let entries = map.entry(key).or_default();
        entries.retain(|e| e.gid != gid);
        entries.push(PointEntry {
            gid,
            point_2d: None,
            point_3d: Some(point),
        });
    }

    /// Remove a vertex's point from the index.
    pub fn remove(&self, label: LabelId, prop: PropertyId, gid: Gid) {
        let key = (label, prop);
        if let Ok(mut map) = self.entries.write() {
            if let Some(entries) = map.get_mut(&key) {
                entries.retain(|e| e.gid != gid);
            }
        }
    }

    /// Check if a point is within a 2D bounding box.
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
            .map(|entries| {
                entries
                    .iter()
                    .filter(|e| {
                        if let Some(p) = &e.point_2d {
                            p.x >= lower_left.x
                                && p.x <= upper_right.x
                                && p.y >= lower_left.y
                                && p.y <= upper_right.y
                        } else {
                            false
                        }
                    })
                    .map(|e| e.gid)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Find nearest 2D points to a query point. Linear scan with pruning.
    pub fn nearest_2d(
        &self,
        label: LabelId,
        prop: PropertyId,
        query: Point2D,
        k: usize,
    ) -> Vec<(Gid, f64)> {
        let key = (label, prop);
        let map = self.entries.read().unwrap();
        let results: Vec<(Gid, f64)> = map
            .get(&key)
            .map(|entries| {
                let mut dists: Vec<(Gid, f64)> = entries
                    .iter()
                    .filter_map(|e| {
                        e.point_2d.as_ref().map(|p| {
                            let dx = p.x - query.x;
                            let dy = p.y - query.y;
                            (e.gid, (dx * dx + dy * dy))
                        })
                    })
                    .collect();
                dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
                dists.truncate(k);
                dists
            })
            .unwrap_or_default();
        results
    }

    /// Number of indexed points for a given label+property.
    pub fn count(&self, label: LabelId, prop: PropertyId) -> usize {
        let key = (label, prop);
        let map = self.entries.read().unwrap();
        map.get(&key).map(|e| e.len()).unwrap_or(0)
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
}

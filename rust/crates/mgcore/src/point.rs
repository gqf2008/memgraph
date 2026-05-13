// ─── Point types — 2D/3D spatial ────────────────────────────────────────────

use std::fmt;

/// Coordinate Reference System identifier.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[repr(u16)]
pub enum Crs {
    WGS84 = 4326,
    Cartesian2D = 7203,
    Cartesian3D = 9157,
    WGS843D = 4979,
}

/// 2D point with CRS, matching C++ `Point2D`.
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct Point2D {
    pub crs: Crs,
    pub x: f64,
    pub y: f64,
}

impl Point2D {
    pub fn new(crs: Crs, x: f64, y: f64) -> Self {
        Self { crs, x, y }
    }
}

impl fmt::Display for Point2D {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Point2D(crs={:?}, x={}, y={})", self.crs, self.x, self.y)
    }
}

/// 3D point with CRS, matching C++ `Point3D`.
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct Point3D {
    pub crs: Crs,
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

impl Point3D {
    pub fn new(crs: Crs, x: f64, y: f64, z: f64) -> Self {
        Self { crs, x, y, z }
    }
}

impl fmt::Display for Point3D {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Point3D(crs={:?}, x={}, y={}, z={})",
            self.crs, self.x, self.y, self.z
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point2d_wgs84() {
        let p = Point2D::new(Crs::WGS84, 45.8150, 15.9819); // Zagreb
        assert_eq!(p.crs, Crs::WGS84);
        assert!((p.x - 45.8150).abs() < 0.001);
    }

    #[test]
    fn test_point3d_cartesian() {
        let p = Point3D::new(Crs::Cartesian3D, 1.0, 2.0, 3.0);
        assert_eq!(p.z, 3.0);
    }
}

// ─── Temporal types — microsecond-precision date/time ───────────────────────

use std::fmt;

/// Date: days since epoch (1970-01-01), matching C++ `Date`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Date {
    /// Days since Unix epoch (Jan 1, 1970).
    pub days_since_epoch: i64,
}

impl Date {
    pub const fn from_days(days: i64) -> Self {
        Self {
            days_since_epoch: days,
        }
    }

    pub const fn days_since_epoch(&self) -> i64 {
        self.days_since_epoch
    }

    pub const fn days(&self) -> i64 {
        self.days_since_epoch
    }

    /// Convert to ISO 8601 date string (YYYY-MM-DD).
    pub fn to_iso_string(&self) -> String {
        // Algorithm: convert days since epoch to YYYY-MM-DD.
        // Note: days_since_epoch uses offset 1 for 1970-01-01 (chrono convention).
        let mut days = self.days_since_epoch - 1;
        let mut year = 1970i64;
        // Adjust for negative days
        while days < 0 {
            year -= 1;
            days += if is_leap_year(year) { 366 } else { 365 };
        }
        while days >= if is_leap_year(year) { 366 } else { 365 } {
            days -= if is_leap_year(year) { 366 } else { 365 };
            year += 1;
        }
        let month_days = if is_leap_year(year) {
            [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        } else {
            [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        };
        let mut month = 1;
        for (i, md) in month_days.iter().enumerate() {
            if days < *md {
                month = i as i64 + 1;
                break;
            }
            days -= *md;
            month = i as i64 + 2;
        }
        let day = days + 1;
        format!("{:04}-{:02}-{:02}", year, month, day)
    }
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Date({})", self.days_since_epoch)
    }
}

impl fmt::Debug for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Date({})", self.days_since_epoch)
    }
}

/// LocalTime: microseconds since midnight, matching C++ `LocalTime`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LocalTime {
    /// Microseconds since midnight (0..86400000000).
    pub microseconds: i64,
}

impl LocalTime {
    pub const fn from_microseconds(us: i64) -> Self {
        Self { microseconds: us }
    }

    pub const fn microseconds(&self) -> i64 {
        self.microseconds
    }

    /// Convert to ISO 8601 time string (HH:MM:SS or HH:MM:SS.ssssss).
    pub fn to_iso_string(&self) -> String {
        let total_secs = self.microseconds / 1_000_000;
        let hrs = total_secs / 3600;
        let mins = (total_secs % 3600) / 60;
        let secs = total_secs % 60;
        let us = self.microseconds % 1_000_000;
        if us == 0 {
            format!("{:02}:{:02}:{:02}", hrs, mins, secs)
        } else {
            format!("{:02}:{:02}:{:02}.{:06}", hrs, mins, secs, us)
        }
    }
}

impl fmt::Display for LocalTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let total_secs = self.microseconds / 1_000_000;
        let hrs = total_secs / 3600;
        let mins = (total_secs % 3600) / 60;
        let secs = total_secs % 60;
        let us = self.microseconds % 1_000_000;
        write!(f, "{:02}:{:02}:{:02}.{:06}", hrs, mins, secs, us)
    }
}

impl fmt::Debug for LocalTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LocalTime({})", self)
    }
}

/// LocalDateTime: microseconds since epoch, matching C++ `LocalDateTime`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LocalDateTime {
    /// Microseconds since Unix epoch.
    pub microseconds: i64,
}

impl LocalDateTime {
    pub const fn from_microseconds(us: i64) -> Self {
        Self { microseconds: us }
    }

    pub const fn microseconds(&self) -> i64 {
        self.microseconds
    }

    /// Convert to ISO 8601 datetime string (YYYY-MM-DDTHH:MM:SS or with microseconds).
    pub fn to_iso_string(&self) -> String {
        let days = self.microseconds / 86_400_000_000;
        let day_remainder_us = self.microseconds % 86_400_000_000;
        let date = Date::from_days(days);
        let time = LocalTime::from_microseconds(day_remainder_us);
        if time.microseconds % 1_000_000 == 0 {
            format!("{}T{}", date.to_iso_string(), time.to_iso_string())
        } else {
            format!("{}T{}", date.to_iso_string(), time.to_iso_string())
        }
    }
}

impl fmt::Display for LocalDateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LocalDateTime({}us)", self.microseconds)
    }
}

impl fmt::Debug for LocalDateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LocalDateTime({}us)", self.microseconds)
    }
}

/// ZonedDateTime: datetime + timezone offset + name, matching C++ `ZonedDateTime`.
#[derive(Clone, PartialEq)]
pub struct ZonedDateTime {
    /// UTC timestamp in microseconds since epoch.
    pub utc_microseconds: i64,
    /// Offset from UTC in minutes.
    pub offset_minutes: i16,
    /// Timezone name (e.g. "Europe/London").
    pub timezone: String,
}

impl ZonedDateTime {
    pub fn new(utc_microseconds: i64, offset_minutes: i16, timezone: String) -> Self {
        Self {
            utc_microseconds,
            offset_minutes,
            timezone,
        }
    }

    pub const fn microseconds(&self) -> i64 {
        self.utc_microseconds
    }

    pub const fn offset_minutes(&self) -> i16 {
        self.offset_minutes
    }

    pub fn timezone(&self) -> &str {
        &self.timezone
    }
}

impl fmt::Display for ZonedDateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ZonedDateTime({}us, offset={}min, tz={})",
            self.utc_microseconds, self.offset_minutes, self.timezone
        )
    }
}

impl fmt::Debug for ZonedDateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ZonedDateTime({})", self)
    }
}

/// Duration: months + days + microseconds, matching C++ `Duration`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Duration {
    pub months: i64,
    pub days: i64,
    pub microseconds: i64,
}

impl Duration {
    pub const fn new(months: i64, days: i64, microseconds: i64) -> Self {
        Self {
            months,
            days,
            microseconds,
        }
    }

    /// Convert to ISO 8601 duration string (P[n]Y[n]M[n]DT[n]H[n]M[n]S).
    pub fn to_iso_string(&self) -> String {
        let mut s = String::from("P");
        if self.months != 0 {
            s.push_str(&format!("{}M", self.months));
        }
        if self.days != 0 {
            s.push_str(&format!("{}D", self.days));
        }
        if self.microseconds != 0 {
            s.push('T');
            let total_secs = self.microseconds / 1_000_000;
            let hrs = total_secs / 3600;
            let mins = (total_secs % 3600) / 60;
            let secs = total_secs % 60;
            let us = self.microseconds % 1_000_000;
            if hrs != 0 {
                s.push_str(&format!("{}H", hrs));
            }
            if mins != 0 {
                s.push_str(&format!("{}M", mins));
            }
            if secs != 0 || us != 0 {
                if us == 0 {
                    s.push_str(&format!("{}S", secs));
                } else {
                    s.push_str(&format!("{}.{:06}S", secs, us));
                }
            }
        }
        if s == "P" {
            s.push_str("0D");
        }
        s
    }
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "P{}M{}DT{:.6}S",
            self.months,
            self.days,
            self.microseconds as f64 / 1_000_000.0
        )
    }
}

impl fmt::Debug for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Duration({})", self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_localtime_display() {
        // 12:34:56.789012
        let t = LocalTime::from_microseconds(12 * 3600 * 1_000_000
            + 34 * 60 * 1_000_000
            + 56 * 1_000_000
            + 789012);
        assert_eq!(format!("{}", t), "12:34:56.789012");
    }

    #[test]
    fn test_duration_display() {
        let d = Duration::new(1, 5, 30_000_000); // 1 month, 5 days, 30 sec
        assert_eq!(format!("{}", d), "P1M5DT30.000000S");
    }

    #[test]
    fn test_date_equality() {
        let d1 = Date::from_days(19000);
        let d2 = Date::from_days(19000);
        assert_eq!(d1, d2);
    }

    #[test]
    fn test_zoned_datetime() {
        let zdt = ZonedDateTime::new(1716912000000000, 60, "Europe/Paris".into());
        assert_eq!(zdt.offset_minutes, 60);
        assert_eq!(zdt.timezone, "Europe/Paris");
    }
}

use chrono::NaiveTime;
fn test() {
    let t = NaiveTime::from_hms_micro_opt(12, 0, 0, 0).unwrap();
    let _ = t.hour();
    let _ = t.minute();
    let _ = t.second();
    let _ = t.nanosecond();
}

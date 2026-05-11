//! Bolt protocol message types.

use std::collections::HashMap;

use crate::value::Value;

/// Bolt protocol message.
#[derive(Clone, Debug, PartialEq)]
pub enum Message {
    // Client messages
    Hello {
        user_agent: String,
        extra: HashMap<String, Value>,
    },
    Run {
        query: String,
        parameters: HashMap<String, Value>,
        extra: HashMap<String, Value>,
    },
    Pull {
        n: i64,
        qid: i64,
    },
    Discard {
        n: i64,
        qid: i64,
    },
    Begin {
        extra: HashMap<String, Value>,
    },
    Commit,
    Rollback,
    Reset,
    Goodbye,
    // Bolt v5+ messages
    Route {
        routing: HashMap<String, Value>,
        bookmarks: Vec<String>,
        extra: HashMap<String, Value>,
    },
    Telemetry {
        api: i64,
    },
    Logon {
        extra: HashMap<String, Value>,
    },
    Logoff,

    // Server messages
    Success {
        metadata: HashMap<String, Value>,
    },
    Failure {
        code: String,
        message: String,
    },
    Ignored,
    Record {
        fields: Vec<Value>,
    },
}

impl Message {
    /// Signature byte for each message type.
    pub fn signature(&self) -> u8 {
        match self {
            Message::Hello { .. } => 0x01,
            Message::Run { .. } => 0x10,
            Message::Pull { .. } => 0x3F,
            Message::Discard { .. } => 0x2F,
            Message::Begin { .. } => 0x11,
            Message::Commit => 0x12,
            Message::Rollback => 0x13,
            Message::Reset => 0x0F,
            Message::Goodbye => 0x02,
            Message::Route { .. } => 0x66,
            Message::Telemetry { .. } => 0x54,
            Message::Logon { .. } => 0x6A,
            Message::Logoff => 0x6B,
            Message::Success { .. } => 0x70,
            Message::Failure { .. } => 0x7F,
            Message::Ignored => 0x7E,
            Message::Record { .. } => 0x71,
        }
    }

    /// Decode a message from a Bolt value (must be a Struct).
    pub fn from_value(value: &Value) -> Result<Self, String> {
        match value {
            Value::Struct(tag, fields) => Self::from_struct(*tag, fields),
            _ => Err("expected Struct".into()),
        }
    }

    fn from_struct(tag: u8, fields: &[Value]) -> Result<Self, String> {
        match tag {
            0x01 => {
                // Hello { user_agent, extra }
                let extra = fields.get(0).and_then(Value::as_map).cloned().unwrap_or_default();
                let user_agent = extra
                    .get("user_agent")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                Ok(Message::Hello { user_agent, extra })
            }
            0x10 => {
                // Run { query, parameters, extra }
                let query = fields
                    .get(0)
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let parameters = fields.get(1).and_then(Value::as_map).cloned().unwrap_or_default();
                let extra = fields.get(2).and_then(Value::as_map).cloned().unwrap_or_default();
                Ok(Message::Run {
                    query,
                    parameters,
                    extra,
                })
            }
            0x3F => {
                // Pull { n, qid }
                let map = fields.get(0).and_then(Value::as_map).cloned().unwrap_or_default();
                let n = map.get("n").and_then(Value::as_i64).unwrap_or(-1);
                let qid = map.get("qid").and_then(Value::as_i64).unwrap_or(-1);
                Ok(Message::Pull { n, qid })
            }
            0x2F => {
                // Discard { n, qid }
                let map = fields.get(0).and_then(Value::as_map).cloned().unwrap_or_default();
                let n = map.get("n").and_then(Value::as_i64).unwrap_or(-1);
                let qid = map.get("qid").and_then(Value::as_i64).unwrap_or(-1);
                Ok(Message::Discard { n, qid })
            }
            0x11 => {
                let extra = fields.get(0).and_then(Value::as_map).cloned().unwrap_or_default();
                Ok(Message::Begin { extra })
            }
            0x12 => Ok(Message::Commit),
            0x13 => Ok(Message::Rollback),
            0x0F => Ok(Message::Reset),
            0x02 => Ok(Message::Goodbye),
            0x66 => {
                let meta = fields.get(0).and_then(Value::as_map).cloned().unwrap_or_default();
                let routing = meta
                    .get("routing")
                    .and_then(Value::as_map)
                    .cloned()
                    .unwrap_or_default();
                let bookmarks = meta
                    .get("bookmarks")
                    .and_then(Value::as_list)
                    .map(|v| {
                        v.iter()
                            .filter_map(Value::as_str)
                            .map(|s| s.to_string())
                            .collect()
                    })
                    .unwrap_or_default();
                let mut extra = meta.clone();
                extra.remove("routing");
                extra.remove("bookmarks");
                Ok(Message::Route {
                    routing,
                    bookmarks,
                    extra,
                })
            }
            0x54 => {
                let meta = fields.get(0).and_then(Value::as_map).cloned().unwrap_or_default();
                let api = meta.get("api").and_then(Value::as_i64).unwrap_or(0);
                Ok(Message::Telemetry { api })
            }
            0x6A => {
                let extra = fields.get(0).and_then(Value::as_map).cloned().unwrap_or_default();
                Ok(Message::Logon { extra })
            }
            0x6B => Ok(Message::Logoff),
            0x70 => {
                let metadata = fields.get(0).and_then(Value::as_map).cloned().unwrap_or_default();
                Ok(Message::Success { metadata })
            }
            0x7F => {
                let meta = fields.get(0).and_then(Value::as_map).cloned().unwrap_or_default();
                let code = meta
                    .get("code")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let message = meta
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                Ok(Message::Failure { code, message })
            }
            0x7E => Ok(Message::Ignored),
            0x71 => {
                let fields_vec = fields
                    .get(0)
                    .and_then(Value::as_list)
                    .cloned()
                    .unwrap_or_default();
                Ok(Message::Record { fields: fields_vec })
            }
            other => Err(format!("unknown message signature: 0x{:02X}", other)),
        }
    }

    /// Encode this message as a Bolt value.
    pub fn to_value(&self) -> Value {
        match self {
            Message::Hello { user_agent, extra } => {
                let mut meta = HashMap::new();
                meta.insert("user_agent".into(), Value::String(user_agent.clone()));
                for (k, v) in extra {
                    meta.insert(k.clone(), v.clone());
                }
                Value::Struct(self.signature(), vec![Value::Map(meta)])
            }
            Message::Run { query, parameters, .. } => {
                Value::Struct(
                    self.signature(),
                    vec![
                        Value::String(query.clone()),
                        Value::Map(parameters.clone()),
                        Value::Map(HashMap::new()), // extra/bookmarks
                    ],
                )
            }
            Message::Pull { n, qid } => {
                Value::Struct(self.signature(), vec![
                    Value::Map({
                        let mut m = HashMap::new();
                        m.insert("n".into(), Value::Int(*n));
                        m
                    }),
                    Value::Map({
                        let mut m = HashMap::new();
                        m.insert("qid".into(), Value::Int(*qid));
                        m
                    }),
                ])
            }
            Message::Discard { n, qid } => {
                Value::Struct(self.signature(), vec![
                    Value::Map({
                        let mut m = HashMap::new();
                        m.insert("n".into(), Value::Int(*n));
                        m
                    }),
                    Value::Map({
                        let mut m = HashMap::new();
                        m.insert("qid".into(), Value::Int(*qid));
                        m
                    }),
                ])
            }
            Message::Begin { extra } => {
                Value::Struct(self.signature(), vec![Value::Map(extra.clone())])
            }
            Message::Commit => Value::Struct(self.signature(), vec![]),
            Message::Rollback => Value::Struct(self.signature(), vec![]),
            Message::Reset => Value::Struct(self.signature(), vec![]),
            Message::Goodbye => Value::Struct(self.signature(), vec![]),
            Message::Route { routing, bookmarks, extra } => {
                let mut route_map = HashMap::new();
                for (k, v) in routing {
                    route_map.insert(k.clone(), v.clone());
                }
                let mut meta = HashMap::new();
                meta.insert("routing".into(), Value::Map(route_map));
                meta.insert("bookmarks".into(), Value::List(bookmarks.iter().map(|b| Value::String(b.clone())).collect()));
                for (k, v) in extra {
                    meta.insert(k.clone(), v.clone());
                }
                Value::Struct(self.signature(), vec![Value::Map(meta)])
            }
            Message::Telemetry { api } => {
                let mut meta = HashMap::new();
                meta.insert("api".into(), Value::Int(*api));
                Value::Struct(self.signature(), vec![Value::Map(meta)])
            }
            Message::Logon { extra } => Value::Struct(self.signature(), vec![Value::Map(extra.clone())]),
            Message::Logoff => Value::Struct(self.signature(), vec![]),
            Message::Success { metadata } => {
                Value::Struct(self.signature(), vec![Value::Map(metadata.clone())])
            }
            Message::Failure { code, message } => {
                let mut meta = HashMap::new();
                meta.insert("code".into(), Value::String(code.clone()));
                meta.insert("message".into(), Value::String(message.clone()));
                Value::Struct(self.signature(), vec![Value::Map(meta)])
            }
            Message::Ignored => Value::Struct(self.signature(), vec![]),
            Message::Record { fields } => {
                Value::Struct(self.signature(), vec![Value::List(fields.clone())])
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hello_to_value() {
        let msg = Message::Hello {
            user_agent: "mgbolt/0.1".into(),
            extra: HashMap::new(),
        };
        let value = msg.to_value();
        if let Value::Struct(tag, _) = value {
            assert_eq!(tag, 0x01);
        } else {
            panic!("expected struct");
        }
    }

    #[test]
    fn test_run_to_value() {
        let msg = Message::Run {
            query: "RETURN 1".into(),
            parameters: HashMap::new(),
            extra: HashMap::new(),
        };
        let value = msg.to_value();
        if let Value::Struct(tag, _) = value {
            assert_eq!(tag, 0x10);
        } else {
            panic!("expected struct");
        }
    }

    #[test]
    fn test_success_to_value() {
        let mut meta = HashMap::new();
        meta.insert("server".into(), Value::String("mgbolt".into()));
        let msg = Message::Success { metadata: meta };
        let value = msg.to_value();
        if let Value::Struct(tag, _) = value {
            assert_eq!(tag, 0x70);
        } else {
            panic!("expected struct");
        }
    }

    #[test]
    fn test_route_to_value() {
        let mut routing = HashMap::new();
        routing.insert("address".into(), Value::String("bolt://localhost:7687".into()));
        let msg = Message::Route {
            routing,
            bookmarks: vec!["bm1".into()],
            extra: HashMap::new(),
        };
        let value = msg.to_value();
        if let Value::Struct(tag, _) = value {
            assert_eq!(tag, 0x66);
        } else {
            panic!("expected struct");
        }
    }

    #[test]
    fn test_telemetry_to_value() {
        let msg = Message::Telemetry { api: 1 };
        let value = msg.to_value();
        if let Value::Struct(tag, _) = value {
            assert_eq!(tag, 0x54);
        } else {
            panic!("expected struct");
        }
    }

    #[test]
    fn test_logoff_to_value() {
        let msg = Message::Logoff;
        let value = msg.to_value();
        if let Value::Struct(tag, _) = value {
            assert_eq!(tag, 0x6B);
        } else {
            panic!("expected struct");
        }
    }
}

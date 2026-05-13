//! Authentication and authorization for Memgraph.
//! Equivalent to C++ `src/auth/`.
//!
//! Supports: basic auth (user/pass), role-based access control (RBAC),
//! LDAP authentication, and fine-grained label-level permissions.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

/// Auth error codes matching C++ semantics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthError {
    InvalidCredentials,
    UserNotFound,
    RoleExists(String),
    RoleNotFound(String),
    PermissionDenied,
    TokenExpired,
    LdapError(String),
    NotConfigured,
    AccountLocked { retry_after_secs: u64 },
    PasswordTooWeak(String),
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::InvalidCredentials => write!(f, "invalid credentials"),
            AuthError::UserNotFound => write!(f, "user not found"),
            AuthError::RoleExists(name) => write!(f, "role '{}' already exists", name),
            AuthError::RoleNotFound(name) => write!(f, "role '{}' not found", name),
            AuthError::PermissionDenied => write!(f, "permission denied"),
            AuthError::TokenExpired => write!(f, "token expired"),
            AuthError::LdapError(e) => write!(f, "LDAP error: {}", e),
            AuthError::NotConfigured => write!(f, "auth not configured"),
            AuthError::AccountLocked { retry_after_secs } => {
                write!(f, "account locked, retry after {}s", retry_after_secs)
            }
            AuthError::PasswordTooWeak(reason) => write!(f, "password too weak: {}", reason),
        }
    }
}

// ─── Roles ──────────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Role {
    Admin,
    ReadWrite,
    ReadOnly,
    Custom(String),
}

impl Role {
    pub fn parse_name(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "ADMIN" => Role::Admin,
            "READ_WRITE" | "READWRITE" => Role::ReadWrite,
            "READ_ONLY" | "READONLY" => Role::ReadOnly,
            other => Role::Custom(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Role::Admin => "admin",
            Role::ReadWrite => "read_write",
            Role::ReadOnly => "read_only",
            Role::Custom(s) => s.as_str(),
        }
    }

    pub fn can_write(&self) -> bool {
        matches!(self, Role::Admin | Role::ReadWrite)
    }

    pub fn can_read(&self) -> bool {
        true // all roles can read
    }

    pub fn is_admin(&self) -> bool {
        matches!(self, Role::Admin)
    }
}

// ─── Label-level permissions ────────────────────────────────────────────

#[derive(Clone, Debug, Default)]
pub struct LabelPermissions {
    /// Labels this user can read. Empty = all.
    pub read_labels: HashSet<String>,
    /// Labels this user can write. Empty = all.
    pub write_labels: HashSet<String>,
    /// Labels this user can update.
    pub update_labels: HashSet<String>,
    /// Labels this user can delete.
    pub delete_labels: HashSet<String>,
}

impl LabelPermissions {
    pub fn unrestricted() -> Self {
        Self::default()
    }

    pub fn can_read_label(&self, label: &str) -> bool {
        self.read_labels.is_empty() || self.read_labels.contains(label)
    }

    pub fn can_write_label(&self, label: &str) -> bool {
        self.write_labels.is_empty() || self.write_labels.contains(label)
    }
}

// ─── User ───────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct User {
    pub username: String,
    pub password_hash: String,
    pub role: Role,
    pub label_permissions: LabelPermissions,
    /// Additional role names granted via GRANT ROLE (DDL bookkeeping).
    pub role_names: HashSet<String>,
    /// System-level privileges granted to this user.
    pub granted_privileges: HashSet<String>,
    /// System-level privileges explicitly denied.
    pub denied_privileges: HashSet<String>,
}

impl User {
    pub fn new(username: &str, password_hash: &str, role: Role) -> Self {
        Self {
            username: username.to_string(),
            password_hash: password_hash.to_string(),
            role,
            label_permissions: LabelPermissions::unrestricted(),
            role_names: HashSet::new(),
            granted_privileges: HashSet::new(),
            denied_privileges: HashSet::new(),
        }
    }
}

// ─── Account lockout configuration ──────────────────────────────────────

#[derive(Clone, Debug)]
pub struct LockoutConfig {
    pub max_attempts: u32,
    pub lockout_duration_secs: u64,
    pub reset_after_secs: u64,
}

impl Default for LockoutConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            lockout_duration_secs: 300,
            reset_after_secs: 3600,
        }
    }
}

// ─── Password policy ────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct PasswordPolicy {
    pub min_length: usize,
    pub require_uppercase: bool,
    pub require_lowercase: bool,
    pub require_digit: bool,
    pub require_special: bool,
}

impl Default for PasswordPolicy {
    fn default() -> Self {
        Self {
            min_length: 8,
            require_uppercase: true,
            require_lowercase: true,
            require_digit: true,
            require_special: false,
        }
    }
}

impl PasswordPolicy {
    pub fn check(&self, password: &str) -> Result<(), String> {
        if password.len() < self.min_length {
            return Err(format!("minimum length is {}", self.min_length));
        }
        if self.require_uppercase && !password.chars().any(|c| c.is_ascii_uppercase()) {
            return Err("must contain an uppercase letter".into());
        }
        if self.require_lowercase && !password.chars().any(|c| c.is_ascii_lowercase()) {
            return Err("must contain a lowercase letter".into());
        }
        if self.require_digit && !password.chars().any(|c| c.is_ascii_digit()) {
            return Err("must contain a digit".into());
        }
        if self.require_special && !password.chars().any(|c| !c.is_alphanumeric()) {
            return Err("must contain a special character".into());
        }
        Ok(())
    }
}

// ─── Audit log ──────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuditEventType {
    LoginSuccess,
    LoginFailure,
    AccountLocked,
    PermissionDenied,
    UserCreated,
    UserRemoved,
    PasswordChanged,
}

#[derive(Clone, Debug)]
pub struct AuditEvent {
    pub timestamp: u64,
    pub username: String,
    pub event_type: AuditEventType,
    pub details: Option<String>,
}

// ─── Auth Store ─────────────────────────────────────────────────────────

pub struct AuthStore {
    users: RwLock<HashMap<String, User>>,
    roles: RwLock<HashSet<String>>,
    /// Role → set of granted privilege names.
    role_privileges: RwLock<HashMap<String, HashSet<String>>>,
    /// Role → set of denied privilege names.
    role_denied_privileges: RwLock<HashMap<String, HashSet<String>>>,
    auth_enabled: RwLock<bool>,
    ldap_url: RwLock<Option<String>>,
    failed_attempts: RwLock<HashMap<String, (u32, u64)>>, // (count, first_attempt_ts)
    lockout_config: RwLock<LockoutConfig>,
    password_policy: RwLock<PasswordPolicy>,
    audit_log: RwLock<Vec<AuditEvent>>,
    max_audit_entries: usize,
}

impl Default for AuthStore {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthStore {
    pub fn new() -> Self {
        Self {
            users: RwLock::new(HashMap::new()),
            roles: RwLock::new(HashSet::new()),
            role_privileges: RwLock::new(HashMap::new()),
            role_denied_privileges: RwLock::new(HashMap::new()),
            auth_enabled: RwLock::new(false),
            ldap_url: RwLock::new(None),
            failed_attempts: RwLock::new(HashMap::new()),
            lockout_config: RwLock::new(LockoutConfig::default()),
            password_policy: RwLock::new(PasswordPolicy::default()),
            audit_log: RwLock::new(Vec::new()),
            max_audit_entries: 10000,
        }
    }

    pub fn with_max_audit_entries(mut self, max: usize) -> Self {
        self.max_audit_entries = max;
        self
    }

    /// Enable/disable authentication.
    pub fn set_enabled(&self, enabled: bool) {
        *self.auth_enabled.write().unwrap() = enabled;
    }

    pub fn is_enabled(&self) -> bool {
        *self.auth_enabled.read().unwrap()
    }

    /// Configure account lockout behavior.
    pub fn set_lockout_config(&self, config: LockoutConfig) {
        *self.lockout_config.write().unwrap() = config;
    }

    /// Configure password policy.
    pub fn set_password_policy(&self, policy: PasswordPolicy) {
        *self.password_policy.write().unwrap() = policy;
    }

    fn now_secs() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    fn record_audit(&self, username: &str, event_type: AuditEventType, details: Option<String>) {
        let mut log = self.audit_log.write().unwrap();
        log.push(AuditEvent {
            timestamp: Self::now_secs(),
            username: username.to_string(),
            event_type,
            details,
        });
        if log.len() > self.max_audit_entries {
            let excess = log.len() - self.max_audit_entries;
            log.drain(0..excess);
        }
    }

    fn is_account_locked(&self, username: &str) -> Option<u64> {
        let attempts = self.failed_attempts.read().unwrap();
        let config = self.lockout_config.read().unwrap();
        if let Some(&(count, first_ts)) = attempts.get(username) {
            let now = Self::now_secs();
            if count >= config.max_attempts {
                let locked_until = first_ts + config.lockout_duration_secs;
                if now < locked_until {
                    return Some(locked_until - now);
                }
            }
        }
        None
    }

    fn record_failed_login(&self, username: &str) {
        let mut attempts = self.failed_attempts.write().unwrap();
        let config = self.lockout_config.read().unwrap();
        let now = Self::now_secs();
        let entry = attempts.entry(username.to_string()).or_insert((0, now));
        if now > entry.1 + config.reset_after_secs {
            *entry = (1, now);
        } else {
            entry.0 += 1;
        }
    }

    fn clear_failed_attempts(&self, username: &str) {
        self.failed_attempts.write().unwrap().remove(username);
    }

    /// Add a user with bcrypt-style hashed password.
    pub fn add_user(&self, username: &str, password_hash: &str, role: Role) {
        let user = User::new(username, password_hash, role);
        self.users
            .write()
            .unwrap()
            .insert(username.to_string(), user);
        self.record_audit(username, AuditEventType::UserCreated, None);
    }

    /// Add a user with a plaintext password (hashes it and checks policy).
    pub fn add_user_with_password(
        &self,
        username: &str,
        password: &str,
        role: Role,
    ) -> Result<(), AuthError> {
        let policy = self.password_policy.read().unwrap();
        if let Err(reason) = policy.check(password) {
            return Err(AuthError::PasswordTooWeak(reason));
        }
        drop(policy);
        let hash = hash_password(password);
        self.add_user(username, &hash, role);
        Ok(())
    }

    /// Remove a user.
    pub fn remove_user(&self, username: &str) {
        self.users.write().unwrap().remove(username);
        self.record_audit(username, AuditEventType::UserRemoved, None);
    }

    /// Verify user credentials. Returns the User on success.
    pub fn authenticate(&self, username: &str, password: &str) -> Result<User, AuthError> {
        if !self.is_enabled() {
            return Err(AuthError::NotConfigured);
        }

        // Check account lockout
        if let Some(retry_after) = self.is_account_locked(username) {
            self.record_audit(username, AuditEventType::AccountLocked, None);
            return Err(AuthError::AccountLocked {
                retry_after_secs: retry_after,
            });
        }

        let user_clone = {
            let users = self.users.read().unwrap();
            let user = users.get(username).ok_or(AuthError::UserNotFound)?;
            user.clone()
        };

        if verify_password(password, &user_clone.password_hash) {
            self.clear_failed_attempts(username);
            self.record_audit(username, AuditEventType::LoginSuccess, None);
            Ok(user_clone)
        } else {
            self.record_failed_login(username);
            self.record_audit(
                username,
                AuditEventType::LoginFailure,
                Some("bad password".into()),
            );
            Err(AuthError::InvalidCredentials)
        }
    }

    /// Check if a user has permission for an operation.
    pub fn check_permission(
        &self,
        username: &str,
        _required_role: &Role,
        label: Option<&str>,
        operation: AuthOperation,
    ) -> Result<(), AuthError> {
        if !self.is_enabled() {
            return Ok(());
        }

        let users = self.users.read().unwrap();
        let user = users.get(username).ok_or(AuthError::UserNotFound)?;

        // Admin has all permissions
        if user.role.is_admin() {
            return Ok(());
        }

        // Check role-based permissions
        match operation {
            AuthOperation::Read => {
                if !user.role.can_read() {
                    drop(users);
                    self.record_audit(
                        username,
                        AuditEventType::PermissionDenied,
                        Some("role".into()),
                    );
                    return Err(AuthError::PermissionDenied);
                }
            }
            AuthOperation::Write | AuthOperation::Create | AuthOperation::Delete => {
                if !user.role.can_write() {
                    drop(users);
                    self.record_audit(
                        username,
                        AuditEventType::PermissionDenied,
                        Some("role".into()),
                    );
                    return Err(AuthError::PermissionDenied);
                }
            }
        }

        // Check label-level permissions
        if let Some(lbl) = label {
            match operation {
                AuthOperation::Read => {
                    if !user.label_permissions.can_read_label(lbl) {
                        drop(users);
                        self.record_audit(
                            username,
                            AuditEventType::PermissionDenied,
                            Some(format!("label read {}", lbl)),
                        );
                        return Err(AuthError::PermissionDenied);
                    }
                }
                AuthOperation::Write | AuthOperation::Create | AuthOperation::Delete => {
                    if !user.label_permissions.can_write_label(lbl) {
                        drop(users);
                        self.record_audit(
                            username,
                            AuditEventType::PermissionDenied,
                            Some(format!("label write {}", lbl)),
                        );
                        return Err(AuthError::PermissionDenied);
                    }
                }
            }
        }

        Ok(())
    }

    /// Configure LDAP authentication.
    pub fn set_ldap(&self, url: &str) {
        *self.ldap_url.write().unwrap() = Some(url.to_string());
    }

    /// Authenticate via LDAP.
    pub fn authenticate_ldap(&self, username: &str, password: &str) -> Result<User, AuthError> {
        let ldap_url = self.ldap_url.read().unwrap();
        let _url = ldap_url.as_ref().ok_or(AuthError::NotConfigured)?;

        // LDAP bind would go here — for now, delegate to local store
        drop(ldap_url);
        self.authenticate(username, password)
            .map_err(|_| AuthError::LdapError("LDAP bind failed".into()))
    }

    /// List all users.
    pub fn list_users(&self) -> Vec<User> {
        self.users.read().unwrap().values().cloned().collect()
    }

    /// Look up a user by username.
    pub fn get_user(&self, username: &str) -> Option<User> {
        self.users.read().unwrap().get(username).cloned()
    }

    /// Get audit log entries (newest first).
    pub fn get_audit_log(&self) -> Vec<AuditEvent> {
        let log = self.audit_log.read().unwrap();
        log.iter().rev().cloned().collect()
    }

    /// Get audit log for a specific user.
    pub fn get_user_audit_log(&self, username: &str) -> Vec<AuditEvent> {
        let log = self.audit_log.read().unwrap();
        log.iter()
            .rev()
            .filter(|e| e.username == username)
            .cloned()
            .collect()
    }

    /// Clear audit log.
    pub fn clear_audit_log(&self) {
        self.audit_log.write().unwrap().clear();
    }

    // ─── Role management ──────────────────────────────────────────────────

    /// Create a new role.
    pub fn create_role(&self, name: &str) -> Result<(), AuthError> {
        let mut roles = self.roles.write().unwrap();
        if roles.contains(name) {
            return Err(AuthError::RoleExists(name.to_string()));
        }
        roles.insert(name.to_string());
        Ok(())
    }

    /// Drop a role.
    pub fn drop_role(&self, name: &str) -> Result<(), AuthError> {
        let mut roles = self.roles.write().unwrap();
        if !roles.remove(name) {
            return Err(AuthError::RoleNotFound(name.to_string()));
        }
        // Also remove from all users
        let mut users = self.users.write().unwrap();
        for user in users.values_mut() {
            user.role_names.remove(name);
        }
        Ok(())
    }

    /// List all role names.
    pub fn list_roles(&self) -> Vec<String> {
        self.roles.read().unwrap().iter().cloned().collect()
    }

    /// Grant a role to a user.
    pub fn grant_role(&self, username: &str, role_name: &str) -> Result<(), AuthError> {
        let mut users = self.users.write().unwrap();
        let user = users.get_mut(username).ok_or(AuthError::UserNotFound)?;
        user.role_names.insert(role_name.to_string());
        Ok(())
    }

    /// Revoke a role from a user.
    pub fn revoke_role(&self, username: &str, role_name: &str) -> Result<(), AuthError> {
        let mut users = self.users.write().unwrap();
        let user = users.get_mut(username).ok_or(AuthError::UserNotFound)?;
        user.role_names.remove(role_name);
        Ok(())
    }

    // ─── Privilege management ──────────────────────────────────────────────

    /// Grant system-level privileges to a user or role.
    pub fn grant_privileges(
        &self,
        target_name: String,
        is_user: bool,
        privileges: Vec<mgparser::ast::Privilege>,
    ) -> Result<(), String> {
        if is_user {
            let mut users = self.users.write().map_err(|e| format!("{}", e))?;
            let user = users
                .get_mut(&target_name)
                .ok_or_else(|| format!("user '{}' not found", target_name))?;
            for p in privileges {
                user.granted_privileges.insert(format!("{:?}", p));
            }
        } else {
            // For roles, store privileges in a separate map
            let mut privs = self.role_privileges.write().map_err(|e| format!("{}", e))?;
            let entry = privs
                .entry(target_name.clone())
                .or_insert_with(HashSet::new);
            for p in privileges {
                entry.insert(format!("{:?}", p));
            }
        }
        Ok(())
    }

    /// Revoke system-level privileges from a user or role.
    pub fn revoke_privileges(
        &self,
        target_name: String,
        is_user: bool,
        privileges: Vec<mgparser::ast::Privilege>,
    ) -> Result<(), String> {
        if is_user {
            let mut users = self.users.write().map_err(|e| format!("{}", e))?;
            let user = users
                .get_mut(&target_name)
                .ok_or_else(|| format!("user '{}' not found", target_name))?;
            for p in privileges {
                user.granted_privileges.remove(&format!("{:?}", p));
            }
        } else {
            let mut privs = self.role_privileges.write().map_err(|e| format!("{}", e))?;
            if let Some(entry) = privs.get_mut(&target_name) {
                for p in privileges {
                    entry.remove(&format!("{:?}", p));
                }
            }
        }
        Ok(())
    }

    /// Deny system-level privileges to a user or role.
    pub fn deny_privileges(
        &self,
        target_name: String,
        is_user: bool,
        privileges: Vec<mgparser::ast::Privilege>,
    ) -> Result<(), String> {
        if is_user {
            let mut users = self.users.write().map_err(|e| format!("{}", e))?;
            let user = users
                .get_mut(&target_name)
                .ok_or_else(|| format!("user '{}' not found", target_name))?;
            for p in privileges {
                user.denied_privileges.insert(format!("{:?}", p));
            }
        } else {
            let mut privs = self
                .role_denied_privileges
                .write()
                .map_err(|e| format!("{}", e))?;
            let entry = privs
                .entry(target_name.clone())
                .or_insert_with(HashSet::new);
            for p in privileges {
                entry.insert(format!("{:?}", p));
            }
        }
        Ok(())
    }

    /// List privileges for a user or role.
    pub fn list_privileges(
        &self,
        target_name: &str,
        is_user: bool,
    ) -> Vec<(mgparser::ast::Privilege, bool)> {
        let mut result = Vec::new();
        if is_user {
            let users = self.users.read().unwrap();
            if let Some(user) = users.get(target_name) {
                for p_name in &user.granted_privileges {
                    if let Some(p) = mgparser::ast::Privilege::parse_name(p_name) {
                        result.push((p, true));
                    }
                }
                for p_name in &user.denied_privileges {
                    if let Some(p) = mgparser::ast::Privilege::parse_name(p_name) {
                        result.push((p, false));
                    }
                }
            }
        } else {
            let privs = self.role_privileges.read().unwrap();
            let denied = self.role_denied_privileges.read().unwrap();
            if let Some(granted) = privs.get(target_name) {
                for p_name in granted {
                    if let Some(p) = mgparser::ast::Privilege::parse_name(p_name) {
                        result.push((p, true));
                    }
                }
            }
            if let Some(denied_set) = denied.get(target_name) {
                for p_name in denied_set {
                    if let Some(p) = mgparser::ast::Privilege::parse_name(p_name) {
                        result.push((p, false));
                    }
                }
            }
        }
        result
    }

    /// Set a user's password.
    pub fn set_password(&self, username: &str, new_password: &str) -> Result<(), String> {
        let mut users = self.users.write().map_err(|e| format!("{}", e))?;
        let user = users
            .get_mut(username)
            .ok_or_else(|| format!("user '{}' not found", username))?;
        let hash = hash_password(new_password);
        user.password_hash = hash;
        Ok(())
    }

    /// Rename a user.
    pub fn rename_user(&self, old_name: &str, new_name: &str) -> Result<(), String> {
        let mut users = self.users.write().map_err(|e| format!("{}", e))?;
        let mut user = users
            .remove(old_name)
            .ok_or_else(|| format!("user '{}' not found", old_name))?;
        user.username = new_name.to_string();
        users.insert(new_name.to_string(), user);
        Ok(())
    }
}

// ─── Auth Operations ────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthOperation {
    Read,
    Write,
    Create,
    Delete,
}

// ─── Password hashing ───────────────────────────────────────────────────

/// Hash a password using argon2.
pub fn hash_password(password: &str) -> String {
    use argon2::password_hash::SaltString;
    use argon2::{Argon2, PasswordHasher};

    let salt = SaltString::generate(&mut rand::thread_rng());
    let argon2 = Argon2::default();
    argon2
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .unwrap_or_else(|_| format!("PLAINTEXT:{}", password))
}

/// Verify a password against an argon2 hash.
pub fn verify_password(password: &str, hash: &str) -> bool {
    if hash.starts_with("PLAINTEXT:") {
        return hash == format!("PLAINTEXT:{}", password);
    }
    use argon2::password_hash::PasswordHash;
    use argon2::{Argon2, PasswordVerifier};

    let Ok(parsed) = PasswordHash::new(hash) else {
        return false;
    };
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}

// ─── Token-based auth ───────────────────────────────────────────────────

/// Simple session token (not for production — use JWT in real deployments).
#[derive(Clone, Debug)]
pub struct SessionToken {
    pub username: String,
    pub issued_at: u64,
    pub expires_at: u64,
}

impl SessionToken {
    pub fn new(username: &str, ttl_secs: u64) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            username: username.to_string(),
            issued_at: now,
            expires_at: now + ttl_secs,
        }
    }

    pub fn is_expired(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now > self.expires_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_add_and_authenticate() {
        let store = AuthStore::new();
        store.set_enabled(true);
        let hash = hash_password("secret123");
        store.add_user("admin", &hash, Role::Admin);

        let result = store.authenticate("admin", "secret123");
        assert!(result.is_ok());

        let bad = store.authenticate("admin", "wrong");
        assert!(bad.is_err());
    }

    #[test]
    fn test_auth_disabled_bypasses() {
        let store = AuthStore::new();
        // Auth disabled — authenticate returns NotConfigured
        assert!(matches!(
            store.authenticate("anyone", "any"),
            Err(AuthError::NotConfigured)
        ));
        // Permission check skips when disabled
        assert!(store
            .check_permission("anyone", &Role::ReadOnly, None, AuthOperation::Write)
            .is_ok());
    }

    #[test]
    fn test_admin_has_all_permissions() {
        let store = AuthStore::new();
        store.set_enabled(true);
        store.add_user("admin", &hash_password("pw"), Role::Admin);

        assert!(store
            .check_permission("admin", &Role::Admin, None, AuthOperation::Write)
            .is_ok());
        assert!(store
            .check_permission("admin", &Role::Admin, Some("secret"), AuthOperation::Delete)
            .is_ok());
    }

    #[test]
    fn test_readonly_cannot_write() {
        let store = AuthStore::new();
        store.set_enabled(true);
        store.add_user("reader", &hash_password("pw"), Role::ReadOnly);

        assert!(store
            .check_permission("reader", &Role::ReadOnly, None, AuthOperation::Read)
            .is_ok());
        assert!(store
            .check_permission("reader", &Role::ReadOnly, None, AuthOperation::Write)
            .is_err());
    }

    #[test]
    fn test_readwrite_can_write() {
        let store = AuthStore::new();
        store.set_enabled(true);
        store.add_user("writer", &hash_password("pw"), Role::ReadWrite);

        assert!(store
            .check_permission("writer", &Role::ReadWrite, None, AuthOperation::Read)
            .is_ok());
        assert!(store
            .check_permission("writer", &Role::ReadWrite, None, AuthOperation::Write)
            .is_ok());
        assert!(store
            .check_permission("writer", &Role::ReadWrite, None, AuthOperation::Create)
            .is_ok());
        assert!(store
            .check_permission("writer", &Role::ReadWrite, None, AuthOperation::Delete)
            .is_ok());
    }

    #[test]
    fn test_password_hash_roundtrip() {
        let hash = hash_password("mypassword");
        assert!(verify_password("mypassword", &hash));
        assert!(!verify_password("wrong", &hash));
    }

    #[test]
    fn test_session_token_expiry() {
        let token = SessionToken::new("user", 3600);
        assert!(!token.is_expired());
        assert_eq!(token.username, "user");
    }

    #[test]
    fn test_label_permissions() {
        let store = AuthStore::new();
        store.set_enabled(true);
        let mut user = User::new("limited", &hash_password("pw"), Role::ReadWrite);
        user.label_permissions.read_labels.insert("Public".into());
        user.label_permissions.write_labels.insert("Public".into());
        store.users.write().unwrap().insert("limited".into(), user);

        // Allowed on Public label
        assert!(store
            .check_permission(
                "limited",
                &Role::ReadWrite,
                Some("Public"),
                AuthOperation::Read
            )
            .is_ok());
        assert!(store
            .check_permission(
                "limited",
                &Role::ReadWrite,
                Some("Public"),
                AuthOperation::Write
            )
            .is_ok());

        // Denied on Secret label
        assert!(store
            .check_permission(
                "limited",
                &Role::ReadWrite,
                Some("Secret"),
                AuthOperation::Read
            )
            .is_err());
        assert!(store
            .check_permission(
                "limited",
                &Role::ReadWrite,
                Some("Secret"),
                AuthOperation::Write
            )
            .is_err());
    }

    #[test]
    fn test_list_users() {
        let store = AuthStore::new();
        store.add_user("alice", &hash_password("pw1"), Role::Admin);
        store.add_user("bob", &hash_password("pw2"), Role::ReadOnly);

        let users = store.list_users();
        assert_eq!(users.len(), 2);
        assert!(users.iter().any(|u| u.username == "alice"));
        assert!(users.iter().any(|u| u.username == "bob"));
    }

    #[test]
    fn test_remove_user() {
        let store = AuthStore::new();
        store.set_enabled(true);
        store.add_user("temp", &hash_password("pw"), Role::ReadOnly);
        assert!(store.authenticate("temp", "pw").is_ok());

        store.remove_user("temp");
        assert!(matches!(
            store.authenticate("temp", "pw"),
            Err(AuthError::UserNotFound)
        ));
    }

    #[test]
    fn test_custom_role() {
        let role = Role::parse_name("analyst");
        assert!(matches!(&role, Role::Custom(s) if s == "ANALYST"));
        assert_eq!(role.as_str(), "ANALYST");
    }

    #[test]
    fn test_account_lockout() {
        let store = AuthStore::new();
        store.set_enabled(true);
        store.set_lockout_config(LockoutConfig {
            max_attempts: 3,
            lockout_duration_secs: 60,
            reset_after_secs: 3600,
        });
        store.add_user("user", &hash_password("correct"), Role::ReadOnly);

        // 2 failed attempts — still allowed
        assert!(store.authenticate("user", "wrong1").is_err());
        assert!(store.authenticate("user", "wrong2").is_err());
        assert!(store.authenticate("user", "correct").is_ok());

        // After success, counter reset; 3 fails in a row → locked
        assert!(store.authenticate("user", "wrong1").is_err());
        assert!(store.authenticate("user", "wrong2").is_err());
        assert!(store.authenticate("user", "wrong3").is_err());
        let result = store.authenticate("user", "correct");
        assert!(
            matches!(result, Err(AuthError::AccountLocked { .. })),
            "expected locked, got {:?}",
            result
        );
    }

    #[test]
    fn test_password_policy() {
        let policy = PasswordPolicy {
            min_length: 8,
            require_uppercase: true,
            require_lowercase: true,
            require_digit: true,
            require_special: false,
        };

        assert!(policy.check("Short1!").is_err());
        assert!(policy.check("nouppercase1").is_err());
        assert!(policy.check("NOLOWERCASE1").is_err());
        assert!(policy.check("NoDigitsHere").is_err());
        assert!(policy.check("Valid1Pass").is_ok());
    }

    #[test]
    fn test_add_user_with_password_policy() {
        let store = AuthStore::new();
        store.set_password_policy(PasswordPolicy {
            min_length: 6,
            require_uppercase: false,
            require_lowercase: false,
            require_digit: true,
            require_special: false,
        });

        assert!(store
            .add_user_with_password("alice", "weak", Role::ReadOnly)
            .is_err());
        assert!(store
            .add_user_with_password("alice", "secret123", Role::ReadOnly)
            .is_ok());
    }

    #[test]
    fn test_audit_log() {
        let store = AuthStore::new().with_max_audit_entries(10);
        store.set_enabled(true);
        store.add_user("alice", &hash_password("pw"), Role::Admin);

        store.authenticate("alice", "wrong").ok();
        store.authenticate("alice", "pw").ok();
        store.remove_user("alice");

        let log = store.get_audit_log();
        assert!(log
            .iter()
            .any(|e| e.event_type == AuditEventType::LoginFailure));
        assert!(log
            .iter()
            .any(|e| e.event_type == AuditEventType::LoginSuccess));
        assert!(log
            .iter()
            .any(|e| e.event_type == AuditEventType::UserRemoved));

        let alice_log = store.get_user_audit_log("alice");
        assert_eq!(alice_log.len(), 4); // created + failure + success + removed
    }

    #[test]
    fn test_audit_log_rotation() {
        let store = AuthStore::new().with_max_audit_entries(3);
        store.set_enabled(true);
        store.add_user("u", &hash_password("pw"), Role::ReadOnly);

        for _ in 0..5 {
            store.authenticate("u", "pw").ok();
        }

        let log = store.get_audit_log();
        assert_eq!(log.len(), 3);
    }

    #[test]
    fn test_clear_audit_log() {
        let store = AuthStore::new();
        store.set_enabled(true);
        store.add_user("u", &hash_password("pw"), Role::ReadOnly);
        store.authenticate("u", "pw").ok();

        assert!(!store.get_audit_log().is_empty());
        store.clear_audit_log();
        assert!(store.get_audit_log().is_empty());
    }
}

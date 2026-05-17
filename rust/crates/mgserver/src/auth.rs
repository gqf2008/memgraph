//! Authentication and authorization with role-based access control.
//!
//! Delegates password verification to [`mgauth::AuthStore`] (argon2 hashing,
//! account lockout, audit logging) while maintaining the simple API that
//! the Bolt server consumes.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

/// A user role with label-level permissions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Role {
    pub name: String,
    /// Labels this role can read.
    pub read_labels: HashSet<String>,
    /// Labels this role can write.
    pub write_labels: HashSet<String>,
    /// Edge types this role can read.
    pub read_edge_types: HashSet<String>,
    /// Edge types this role can write.
    pub write_edge_types: HashSet<String>,
    pub admin: bool,
}

impl Role {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            read_labels: HashSet::new(),
            write_labels: HashSet::new(),
            read_edge_types: HashSet::new(),
            write_edge_types: HashSet::new(),
            admin: false,
        }
    }

    pub fn admin_role() -> Self {
        let mut r = Self::new("admin");
        r.admin = true;
        r
    }
}

/// Authentication and authorization configuration.
///
/// Wraps [`mgauth::AuthStore`] for proper password hashing (argon2),
/// account lockout, and audit logging, while providing the simpler API
/// that the Bolt server state machine expects.
#[derive(Clone, Debug)]
pub struct AuthConfig {
    auth_store: Arc<mgauth::AuthStore>,
    /// Label-level role definitions (used by can_read_label / can_write_label).
    roles: Arc<RwLock<HashMap<String, Role>>>,
    require_auth: bool,
}

impl AuthConfig {
    /// Create an auth config that allows all connections (no auth).
    pub fn none() -> Self {
        let store = mgauth::AuthStore::new();
        store.set_enabled(false);
        Self {
            auth_store: Arc::new(store),
            roles: Arc::new(RwLock::new(HashMap::new())),
            require_auth: false,
        }
    }

    /// Create an auth config with a single admin user.
    pub fn basic(user: impl Into<String>, pass: impl Into<String>) -> Self {
        let store = mgauth::AuthStore::new();
        store.set_enabled(true);
        let username = user.into();
        let _ = store.add_user_with_password(&username, &pass.into(), mgauth::Role::Admin);
        Self {
            auth_store: Arc::new(store),
            roles: Arc::new(RwLock::new(HashMap::from([(
                "admin".into(),
                Role::admin_role(),
            )]))),
            require_auth: true,
        }
    }

    /// Check if authentication is required.
    pub fn is_required(&self) -> bool {
        self.require_auth
    }

    /// Authenticate a user via argon2 password verification.
    /// Returns the username on success, `None` on failure.
    pub fn authenticate(&self, user: &str, pass: &str) -> Option<String> {
        if !self.require_auth {
            return Some("anonymous".into());
        }
        match self.auth_store.authenticate(user, pass) {
            Ok(u) => Some(u.username),
            Err(_) => None,
        }
    }

    /// Create a new user with argon2-hashed password.
    pub fn create_user(
        &self,
        username: String,
        password: String,
        roles: Vec<String>,
    ) -> Result<(), AuthError> {
        // Determine the mgauth role from the first role name
        let mgauth_role = if roles.iter().any(|r| r == "admin") {
            mgauth::Role::Admin
        } else if roles.iter().any(|r| r == "reader" || r == "read_only") {
            mgauth::Role::ReadOnly
        } else {
            mgauth::Role::ReadWrite
        };
        self.auth_store
            .add_user_with_password(&username, &password, mgauth_role)
            .map_err(|e| match e {
                mgauth::AuthError::PasswordTooWeak(reason) => AuthError::PasswordTooWeak(reason),
                mgauth::AuthError::UserExists(name) => AuthError::UserExists(name),
                _ => AuthError::UserExists(username.clone()),
            })?;

        // Grant any extra role names
        for role_name in &roles {
            self.auth_store.grant_role(&username, role_name).ok();
        }

        Ok(())
    }

    /// Drop a user.
    pub fn drop_user(&self, username: &str) -> Result<(), AuthError> {
        if self.auth_store.get_user(username).is_none() {
            return Err(AuthError::UserNotFound(username.into()));
        }
        self.auth_store.remove_user(username);
        Ok(())
    }

    /// List all usernames.
    pub fn list_users(&self) -> Vec<String> {
        self.auth_store.list_usernames()
    }

    /// Lock a user account.
    pub fn lock_user(&self, username: &str) -> Result<(), AuthError> {
        if self.auth_store.get_user(username).is_none() {
            return Err(AuthError::UserNotFound(username.into()));
        }
        self.auth_store.lock_account(username)
            .map_err(|_| AuthError::UserNotFound(username.into()))
    }

    /// Unlock a user account.
    pub fn unlock_user(&self, username: &str) -> Result<(), AuthError> {
        if self.auth_store.get_user(username).is_none() {
            return Err(AuthError::UserNotFound(username.into()));
        }
        self.auth_store.clear_failed_attempts(username);
        Ok(())
    }

    /// Create a role.
    pub fn create_role(&self, name: String) -> Result<(), AuthError> {
        let mut roles = self.roles.write().expect("lock poisoned");
        if roles.contains_key(&name) {
            return Err(AuthError::RoleExists(name));
        }
        roles.insert(name.clone(), Role::new(&name));
        self.auth_store.create_role(&name).ok();
        Ok(())
    }

    /// Drop a role.
    pub fn drop_role(&self, name: &str) -> Result<(), AuthError> {
        let mut roles = self.roles.write().expect("lock poisoned");
        if roles.remove(name).is_none() {
            return Err(AuthError::RoleNotFound(name.into()));
        }
        self.auth_store.drop_role(name).ok();
        Ok(())
    }

    /// List all role names.
    pub fn list_roles(&self) -> Vec<String> {
        let mut seen: HashSet<String> = self.roles.read().expect("lock poisoned").keys().cloned().collect();
        for r in self.auth_store.list_roles() {
            seen.insert(r);
        }
        seen.into_iter().collect()
    }

    /// Check if a user has admin privileges.
    pub fn is_admin(&self, username: &str) -> bool {
        if let Some(user) = self.auth_store.get_user(username) {
            return user.role.is_admin();
        }
        false
    }

    /// Check if a user can read a label.
    pub fn can_read_label(&self, username: &str, label: &str) -> bool {
        let user = self.auth_store.get_user(username);
        if let Some(ref user) = user {
            if user.role.is_admin() {
                return true;
            }
            if user.label_permissions.can_read_label(label) {
                return true;
            }
        }
        let roles = self.roles.read().expect("lock poisoned");
        if let Some(ref user) = user {
            for role_name in &user.role_names {
                if let Some(role) = roles.get(role_name) {
                    if role.admin || role.read_labels.is_empty() || role.read_labels.contains(label) {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Check if a user can write a label.
    pub fn can_write_label(&self, username: &str, label: &str) -> bool {
        let user = self.auth_store.get_user(username);
        if let Some(ref user) = user {
            if user.role.is_admin() {
                return true;
            }
            if !user.label_permissions.write_labels.is_empty() {
                return user.label_permissions.can_write_label(label);
            }
        }
        let roles = self.roles.read().expect("lock poisoned");
        if let Some(ref user) = user {
            for role_name in &user.role_names {
                if let Some(role) = roles.get(role_name) {
                    if role.admin {
                        return true;
                    }
                    if !role.write_labels.is_empty() && role.write_labels.contains(label) {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Get the underlying [`mgauth::AuthStore`] for privilege management.
    pub fn auth_store(&self) -> &Arc<mgauth::AuthStore> {
        &self.auth_store
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    UserExists(String),
    UserNotFound(String),
    RoleExists(String),
    RoleNotFound(String),
    Unauthorized,
    PasswordTooWeak(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_auth() {
        let auth = AuthConfig::none();
        assert!(!auth.is_required());
        assert_eq!(auth.authenticate("any", "any"), Some("anonymous".into()));
    }

    #[test]
    fn test_basic_auth_success() {
        let auth = AuthConfig::basic("admin", "Secret123");
        assert!(auth.is_required());
        assert_eq!(auth.authenticate("admin", "Secret123"), Some("admin".into()));
    }

    #[test]
    fn test_basic_auth_failure() {
        let auth = AuthConfig::basic("admin", "Secret123");
        assert_eq!(auth.authenticate("admin", "wrong"), None);
        assert_eq!(auth.authenticate("nobody", "Secret123"), None);
    }

    #[test]
    fn test_create_user() {
        let auth = AuthConfig::basic("admin", "Secret123");
        auth.create_user("alice".into(), "Password1".into(), vec!["reader".into()])
            .unwrap();
        assert!(auth.list_users().contains(&"alice".into()));
        // Duplicate user with valid password
        assert!(matches!(
            auth.create_user("alice".into(), "Password2".into(), vec![]),
            Err(AuthError::UserExists(_))
        ));
    }

    #[test]
    fn test_drop_user() {
        let auth = AuthConfig::basic("admin", "Secret123");
        auth.create_user("bob".into(), "Password1".into(), vec![])
            .unwrap();
        auth.drop_user("bob").unwrap();
        assert!(!auth.list_users().contains(&"bob".into()));
    }

    #[test]
    fn test_is_admin() {
        let auth = AuthConfig::basic("admin", "Secret123");
        assert!(auth.is_admin("admin"));
        auth.create_user("user".into(), "Password1".into(), vec!["reader".into()])
            .unwrap();
        assert!(!auth.is_admin("user"));
    }

    #[test]
    fn test_label_permissions() {
        let auth = AuthConfig::basic("admin", "Secret123");
        auth.create_role("reader".into()).unwrap();
        {
            let mut roles = auth.roles.write().unwrap();
            roles
                .get_mut("reader")
                .unwrap()
                .read_labels
                .insert("Person".into());
        }
        auth.create_user("dave".into(), "Password1".into(), vec!["reader".into()])
            .unwrap();
        auth.auth_store.grant_role("dave", "reader").unwrap();
        assert!(auth.can_read_label("dave", "Person"));
        assert!(!auth.can_write_label("dave", "Person"));
    }
}
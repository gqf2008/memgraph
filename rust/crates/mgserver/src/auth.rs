//! Authentication and authorization with role-based access control.

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

/// A user with credentials and assigned roles.
#[derive(Clone, Debug)]
pub struct User {
    pub username: String,
    pub password_hash: String,
    pub roles: Vec<String>,
    pub locked: bool,
}

/// Authentication and authorization configuration.
#[derive(Clone, Debug)]
pub struct AuthConfig {
    users: Arc<RwLock<HashMap<String, User>>>,
    roles: Arc<RwLock<HashMap<String, Role>>>,
    require_auth: bool,
}

impl AuthConfig {
    /// Create an auth config that allows all connections (no auth).
    pub fn none() -> Self {
        Self {
            users: Arc::new(RwLock::new(HashMap::new())),
            roles: Arc::new(RwLock::new(HashMap::new())),
            require_auth: false,
        }
    }

    /// Create an auth config with a single admin user.
    pub fn basic(user: impl Into<String>, pass: impl Into<String>) -> Self {
        let mut users = HashMap::new();
        let mut roles = HashMap::new();
        let username = user.into();
        let password = pass.into();
        users.insert(
            username.clone(),
            User {
                username: username.clone(),
                password_hash: password.clone(),
                roles: vec!["admin".into()],
                locked: false,
            },
        );
        roles.insert("admin".into(), Role::admin_role());
        Self {
            users: Arc::new(RwLock::new(users)),
            roles: Arc::new(RwLock::new(roles)),
            require_auth: true,
        }
    }

    /// Check if authentication is required.
    pub fn is_required(&self) -> bool {
        self.require_auth
    }

    /// Authenticate a user. Returns the username on success.
    pub fn authenticate(&self, user: &str, pass: &str) -> Option<String> {
        if !self.require_auth {
            return Some("anonymous".into());
        }
        let users = self.users.read().expect("lock poisoned");
        if let Some(u) = users.get(user) {
            if u.locked {
                return None;
            }
            if u.password_hash == pass {
                return Some(u.username.clone());
            }
        }
        None
    }

    /// Create a new user.
    pub fn create_user(&self, username: String, password: String, roles: Vec<String>) -> Result<(), AuthError> {
        let mut users = self.users.write().expect("lock poisoned");
        if users.contains_key(&username) {
            return Err(AuthError::UserExists(username));
        }
        users.insert(
            username.clone(),
            User {
                username,
                password_hash: password,
                roles,
                locked: false,
            },
        );
        Ok(())
    }

    /// Drop a user.
    pub fn drop_user(&self, username: &str) -> Result<(), AuthError> {
        let mut users = self.users.write().expect("lock poisoned");
        if users.remove(username).is_none() {
            return Err(AuthError::UserNotFound(username.into()));
        }
        Ok(())
    }

    /// List all usernames.
    pub fn list_users(&self) -> Vec<String> {
        self.users.read().expect("lock poisoned").keys().cloned().collect()
    }

    /// Lock a user account.
    pub fn lock_user(&self, username: &str) -> Result<(), AuthError> {
        let mut users = self.users.write().expect("lock poisoned");
        let user = users.get_mut(username).ok_or_else(|| AuthError::UserNotFound(username.into()))?;
        user.locked = true;
        Ok(())
    }

    /// Unlock a user account.
    pub fn unlock_user(&self, username: &str) -> Result<(), AuthError> {
        let mut users = self.users.write().expect("lock poisoned");
        let user = users.get_mut(username).ok_or_else(|| AuthError::UserNotFound(username.into()))?;
        user.locked = false;
        Ok(())
    }

    /// Create a role.
    pub fn create_role(&self, name: String) -> Result<(), AuthError> {
        let mut roles = self.roles.write().expect("lock poisoned");
        if roles.contains_key(&name) {
            return Err(AuthError::RoleExists(name));
        }
        roles.insert(name.clone(), Role::new(name));
        Ok(())
    }

    /// Drop a role.
    pub fn drop_role(&self, name: &str) -> Result<(), AuthError> {
        let mut roles = self.roles.write().expect("lock poisoned");
        if roles.remove(name).is_none() {
            return Err(AuthError::RoleNotFound(name.into()));
        }
        Ok(())
    }

    /// List all role names.
    pub fn list_roles(&self) -> Vec<String> {
        self.roles.read().expect("lock poisoned").keys().cloned().collect()
    }

    /// Check if a user has admin privileges.
    pub fn is_admin(&self, username: &str) -> bool {
        let users = self.users.read().expect("lock poisoned");
        let roles = self.roles.read().expect("lock poisoned");
        if let Some(user) = users.get(username) {
            for role_name in &user.roles {
                if let Some(role) = roles.get(role_name) {
                    if role.admin {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Check if a user can read a label.
    pub fn can_read_label(&self, username: &str, label: &str) -> bool {
        let users = self.users.read().expect("lock poisoned");
        let roles = self.roles.read().expect("lock poisoned");
        if let Some(user) = users.get(username) {
            for role_name in &user.roles {
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
        let users = self.users.read().expect("lock poisoned");
        let roles = self.roles.read().expect("lock poisoned");
        if let Some(user) = users.get(username) {
            for role_name in &user.roles {
                if let Some(role) = roles.get(role_name) {
                    if role.admin || role.write_labels.contains(label) {
                        return true;
                    }
                }
            }
        }
        false
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    UserExists(String),
    UserNotFound(String),
    RoleExists(String),
    RoleNotFound(String),
    Unauthorized,
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
        let auth = AuthConfig::basic("admin", "secret");
        assert!(auth.is_required());
        assert_eq!(auth.authenticate("admin", "secret"), Some("admin".into()));
    }

    #[test]
    fn test_basic_auth_failure() {
        let auth = AuthConfig::basic("admin", "secret");
        assert_eq!(auth.authenticate("admin", "wrong"), None);
        assert_eq!(auth.authenticate("nobody", "secret"), None);
    }

    #[test]
    fn test_create_user() {
        let auth = AuthConfig::basic("admin", "secret");
        auth.create_user("alice".into(), "password".into(), vec!["reader".into()]).unwrap();
        assert!(auth.list_users().contains(&"alice".into()));
        assert!(matches!(auth.create_user("alice".into(), "x".into(), vec![]), Err(AuthError::UserExists(_))));
    }

    #[test]
    fn test_drop_user() {
        let auth = AuthConfig::basic("admin", "secret");
        auth.create_user("bob".into(), "pass".into(), vec![]).unwrap();
        auth.drop_user("bob").unwrap();
        assert!(!auth.list_users().contains(&"bob".into()));
    }

    #[test]
    fn test_lock_unlock() {
        let auth = AuthConfig::basic("admin", "secret");
        auth.create_user("carol".into(), "pass".into(), vec![]).unwrap();
        auth.lock_user("carol").unwrap();
        assert_eq!(auth.authenticate("carol", "pass"), None);
        auth.unlock_user("carol").unwrap();
        assert_eq!(auth.authenticate("carol", "pass"), Some("carol".into()));
    }

    #[test]
    fn test_create_role() {
        let auth = AuthConfig::basic("admin", "secret");
        auth.create_role("reader".into()).unwrap();
        assert!(auth.list_roles().contains(&"reader".into()));
    }

    #[test]
    fn test_is_admin() {
        let auth = AuthConfig::basic("admin", "secret");
        assert!(auth.is_admin("admin"));
        auth.create_user("user".into(), "pass".into(), vec!["reader".into()]).unwrap();
        assert!(!auth.is_admin("user"));
    }

    #[test]
    fn test_label_permissions() {
        let auth = AuthConfig::basic("admin", "secret");
        auth.create_role("reader".into()).unwrap();
        {
            let mut roles = auth.roles.write().unwrap();
            roles.get_mut("reader").unwrap().read_labels.insert("Person".into());
        }
        auth.create_user("dave".into(), "pass".into(), vec!["reader".into()]).unwrap();
        assert!(auth.can_read_label("dave", "Person"));
        assert!(!auth.can_write_label("dave", "Person"));
    }
}

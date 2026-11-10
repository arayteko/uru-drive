use argon2::{
    password_hash::{PasswordHasher, PasswordVerifier},
    Argon2, PasswordHash,
};
use axum::{
    extract::{FromRef, FromRequestParts},
    http::{header::AUTHORIZATION, request::Parts, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use base64::prelude::*;
use rand::distr::Alphanumeric;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tracing::info;
use uuid::Uuid;

const API_KEY_SECRET: &[u8; 32] = b"uru_secret_salt_key_32_bytes_cas";

#[derive(Error, Debug)]
pub enum AuthError {
    #[error("User '{0}' already exists")]
    UserAlreadyExists(String),
    #[error("User '{0}' not found")]
    UserNotFound(String),
    #[error("Token not found: {0}")]
    TokenNotFound(Uuid),
    #[error("Cannot remove or demote the last remaining admin")]
    LastAdminCannotBeModified,
    #[error("Forbidden: insufficient permissions")]
    Forbidden,
    #[error("Database error: {0}")]
    Database(String),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("Password hashing error: {0}")]
    PasswordHash(String),
}

impl From<rocksdb::Error> for AuthError {
    fn from(err: rocksdb::Error) -> Self {
        AuthError::Database(err.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UserRole {
    Admin,
    User,
}

impl std::fmt::Display for UserRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UserRole::Admin => write!(f, "admin"),
            UserRole::User => write!(f, "user"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub user_id: String,
    pub password_hash: String,
    pub role: UserRole,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenRecord {
    pub token_id: Uuid,
    pub user_id: String,
    pub name: String,
    pub token_hash: [u8; 32],
    pub created_at_ms: u64,
}

#[derive(Debug, Clone)]
pub struct InitialAdminCredentials {
    pub username: String,
    pub password: String,
    pub token: String,
}

pub struct UserStore {
    db: Arc<rocksdb::DB>,
    _db_path: PathBuf,
}

impl UserStore {
    /// Open the RocksDB database for users and tokens.
    /// If no users exist, automatically generates an admin user and prints credentials to console.
    pub fn open(
        auth_dir: impl Into<PathBuf>,
    ) -> Result<(Arc<Self>, Option<InitialAdminCredentials>), AuthError> {
        let auth_dir = auth_dir.into();
        std::fs::create_dir_all(&auth_dir)
            .map_err(|e| AuthError::Database(format!("Failed to create auth dir: {e}")))?;

        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        let db = Arc::new(rocksdb::DB::open(&opts, &auth_dir)?);

        let store = Arc::new(Self {
            db: db.clone(),
            _db_path: auth_dir,
        });

        // Check if any users exist in DB (scan for 'u:' prefix)
        let mut has_users = false;
        let mut iter = db.raw_iterator();
        iter.seek(b"u:");
        if iter.valid() {
            if let Some(key) = iter.key() {
                if key.starts_with(b"u:") {
                    has_users = true;
                }
            }
        }

        let mut initial_creds = None;
        if !has_users {
            // Generate initial admin user and token
            let username = "admin".to_string();
            let password: String = rand::rng()
                .sample_iter(&Alphanumeric)
                .take(16)
                .map(char::from)
                .collect();

            let token_bytes: [u8; 16] = rand::rng().random();
            let token = format!("uru_{}", hex::encode(token_bytes));

            let argon2 = Argon2::default();
            let password_hash = argon2
                .hash_password(password.as_bytes())
                .map_err(|e| AuthError::PasswordHash(e.to_string()))?
                .to_string();

            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            let user = User {
                user_id: username.clone(),
                password_hash,
                role: UserRole::Admin,
                created_at_ms: now_ms,
            };

            let user_val = serde_json::to_vec(&user)?;
            db.put(format!("u:{}", username).as_bytes(), user_val)?;

            // Store initial token
            let token_id = Uuid::now_v7();
            let token_hash = Self::hash_api_key(&token);
            let token_rec = TokenRecord {
                token_id,
                user_id: username.clone(),
                name: "Initial Admin Token".to_string(),
                token_hash,
                created_at_ms: now_ms,
            };

            let token_val = serde_json::to_vec(&token_rec)?;
            let token_hex = hex::encode(token_hash);
            db.put(format!("t:{}", token_hex).as_bytes(), token_val)?;
            db.put(
                format!("ut:{}:{}", username, token_id).as_bytes(),
                token_hex.as_bytes(),
            )?;

            // Print admin credentials clearly to console
            println!("\n================================================================================");
            println!("                     URUDRIVE: INITIAL ADMIN CREDENTIALS");
            println!("================================================================================");
            println!("  An initial administrator has been automatically generated on first boot:\n");
            println!("    Username:  {}", username);
            println!("    Password:  {}", password);
            println!("    Role:      admin");
            println!("    API Token: {}", token);
            println!("\n  Store these credentials securely. You can manage users and tokens using:");
            println!("    uru-cli user ... --api-key {}", token);
            println!("    uru-cli token ... --api-key {}", token);
            println!("================================================================================\n");

            info!(
                username = %username,
                "Initial admin user and token created and printed to console"
            );

            initial_creds = Some(InitialAdminCredentials {
                username,
                password,
                token,
            });
        }

        Ok((store, initial_creds))
    }

    pub fn hash_api_key(api_key: &str) -> [u8; 32] {
        *blake3::keyed_hash(API_KEY_SECRET, api_key.as_bytes()).as_bytes()
    }

    pub fn authenticate_api_key(&self, token: &str) -> Option<User> {
        let target_hash = Self::hash_api_key(token);
        let key = format!("t:{}", hex::encode(target_hash));
        let val = self.db.get(key.as_bytes()).ok()??;
        let token_rec = serde_json::from_slice::<TokenRecord>(&val).ok()?;
        self.get_user(&token_rec.user_id)
    }

    pub fn authenticate_password(&self, user_id: &str, password: &str) -> Option<User> {
        let user = self.get_user(user_id)?;
        let parsed_hash = PasswordHash::new(&user.password_hash).ok()?;
        if Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok()
        {
            Some(user)
        } else {
            None
        }
    }

    pub fn authenticate_basic(&self, header_val: &str) -> Option<User> {
        let decoded = BASE64_STANDARD.decode(header_val.trim()).ok()?;
        let cred_str = String::from_utf8(decoded).ok()?;
        let mut parts = cred_str.splitn(2, ':');
        let user_id = parts.next()?;
        let secret = parts.next()?;

        // If secret starts with "uru_", treat as API key
        if secret.starts_with("uru_") {
            if let Some(user) = self.authenticate_api_key(secret) {
                if user.user_id == user_id {
                    return Some(user);
                }
            }
        }

        // Otherwise verify Argon2id password
        self.authenticate_password(user_id, secret)
    }

    pub fn get_user(&self, user_id: &str) -> Option<User> {
        let key = format!("u:{}", user_id);
        let val = self.db.get(key.as_bytes()).ok()??;
        serde_json::from_slice(&val).ok()
    }

    pub fn list_users(&self) -> Result<Vec<User>, AuthError> {
        let mut users = Vec::new();
        let mut iter = self.db.raw_iterator();
        iter.seek(b"u:");

        while iter.valid() {
            if let (Some(k), Some(v)) = (iter.key(), iter.value()) {
                if !k.starts_with(b"u:") {
                    break;
                }
                if let Ok(u) = serde_json::from_slice::<User>(v) {
                    users.push(u);
                }
            }
            iter.next();
        }

        users.sort_by(|a, b| a.user_id.cmp(&b.user_id));
        Ok(users)
    }

    pub fn create_user(
        &self,
        user_id: &str,
        password: &str,
        role: UserRole,
    ) -> Result<User, AuthError> {
        if self.get_user(user_id).is_some() {
            return Err(AuthError::UserAlreadyExists(user_id.to_string()));
        }

        let argon2 = Argon2::default();
        let password_hash = argon2
            .hash_password(password.as_bytes())
            .map_err(|e| AuthError::PasswordHash(e.to_string()))?
            .to_string();

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let user = User {
            user_id: user_id.to_string(),
            password_hash,
            role,
            created_at_ms: now_ms,
        };

        let val = serde_json::to_vec(&user)?;
        self.db.put(format!("u:{}", user_id).as_bytes(), val)?;

        Ok(user)
    }

    pub fn update_password(&self, user_id: &str, new_password: &str) -> Result<(), AuthError> {
        let mut user = self
            .get_user(user_id)
            .ok_or_else(|| AuthError::UserNotFound(user_id.to_string()))?;

        let argon2 = Argon2::default();
        user.password_hash = argon2
            .hash_password(new_password.as_bytes())
            .map_err(|e| AuthError::PasswordHash(e.to_string()))?
            .to_string();

        let val = serde_json::to_vec(&user)?;
        self.db.put(format!("u:{}", user_id).as_bytes(), val)?;

        Ok(())
    }

    pub fn update_role(&self, user_id: &str, new_role: UserRole) -> Result<(), AuthError> {
        let mut user = self
            .get_user(user_id)
            .ok_or_else(|| AuthError::UserNotFound(user_id.to_string()))?;

        if user.role == UserRole::Admin && new_role != UserRole::Admin {
            let admin_count = self
                .list_users()?
                .into_iter()
                .filter(|u| u.role == UserRole::Admin)
                .count();
            if admin_count <= 1 {
                return Err(AuthError::LastAdminCannotBeModified);
            }
        }

        user.role = new_role;
        let val = serde_json::to_vec(&user)?;
        self.db.put(format!("u:{}", user_id).as_bytes(), val)?;

        Ok(())
    }

    pub fn delete_user(&self, user_id: &str) -> Result<(), AuthError> {
        let user = self
            .get_user(user_id)
            .ok_or_else(|| AuthError::UserNotFound(user_id.to_string()))?;

        if user.role == UserRole::Admin {
            let admin_count = self
                .list_users()?
                .into_iter()
                .filter(|u| u.role == UserRole::Admin)
                .count();
            if admin_count <= 1 {
                return Err(AuthError::LastAdminCannotBeModified);
            }
        }

        // Revoke all tokens for this user
        let prefix = format!("ut:{}:", user_id);
        let mut iter = self.db.raw_iterator();
        iter.seek(prefix.as_bytes());

        let mut keys_to_delete = Vec::new();
        while iter.valid() {
            if let (Some(k), Some(v)) = (iter.key(), iter.value()) {
                if !k.starts_with(prefix.as_bytes()) {
                    break;
                }
                keys_to_delete.push(k.to_vec());
                if let Ok(token_hex) = std::str::from_utf8(v) {
                    keys_to_delete.push(format!("t:{}", token_hex).into_bytes());
                }
            }
            iter.next();
        }

        for k in keys_to_delete {
            let _ = self.db.delete(&k);
        }

        // Delete user record
        self.db.delete(format!("u:{}", user_id).as_bytes())?;

        Ok(())
    }

    pub fn create_token(
        &self,
        user_id: &str,
        name: &str,
    ) -> Result<(TokenRecord, String), AuthError> {
        if self.get_user(user_id).is_none() {
            return Err(AuthError::UserNotFound(user_id.to_string()));
        }

        let token_bytes: [u8; 16] = rand::rng().random();
        let plaintext_token = format!("uru_{}", hex::encode(token_bytes));
        let token_hash = Self::hash_api_key(&plaintext_token);
        let token_id = Uuid::now_v7();

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let record = TokenRecord {
            token_id,
            user_id: user_id.to_string(),
            name: name.to_string(),
            token_hash,
            created_at_ms: now_ms,
        };

        let val = serde_json::to_vec(&record)?;
        let token_hex = hex::encode(token_hash);

        self.db.put(format!("t:{}", token_hex).as_bytes(), val)?;
        self.db.put(
            format!("ut:{}:{}", user_id, token_id).as_bytes(),
            token_hex.as_bytes(),
        )?;

        Ok((record, plaintext_token))
    }

    pub fn list_tokens_for_user(&self, user_id: &str) -> Result<Vec<TokenRecord>, AuthError> {
        let prefix = format!("ut:{}:", user_id);
        let mut tokens = Vec::new();
        let mut iter = self.db.raw_iterator();
        iter.seek(prefix.as_bytes());

        while iter.valid() {
            if let (Some(k), Some(v)) = (iter.key(), iter.value()) {
                if !k.starts_with(prefix.as_bytes()) {
                    break;
                }
                if let Ok(token_hex) = std::str::from_utf8(v) {
                    let key = format!("t:{}", token_hex);
                    if let Ok(Some(rec_bytes)) = self.db.get(key.as_bytes()) {
                        if let Ok(rec) = serde_json::from_slice::<TokenRecord>(&rec_bytes) {
                            tokens.push(rec);
                        }
                    }
                }
            }
            iter.next();
        }

        tokens.sort_by(|a, b| a.created_at_ms.cmp(&b.created_at_ms));
        Ok(tokens)
    }

    pub fn list_all_tokens(&self) -> Result<Vec<TokenRecord>, AuthError> {
        let mut tokens = Vec::new();
        let mut iter = self.db.raw_iterator();
        iter.seek(b"t:");

        while iter.valid() {
            if let (Some(k), Some(v)) = (iter.key(), iter.value()) {
                if !k.starts_with(b"t:") {
                    break;
                }
                if let Ok(rec) = serde_json::from_slice::<TokenRecord>(v) {
                    tokens.push(rec);
                }
            }
            iter.next();
        }

        tokens.sort_by(|a, b| a.created_at_ms.cmp(&b.created_at_ms));
        Ok(tokens)
    }

    pub fn get_token(&self, token_id: Uuid) -> Result<Option<TokenRecord>, AuthError> {
        let all = self.list_all_tokens()?;
        Ok(all.into_iter().find(|t| t.token_id == token_id))
    }

    pub fn revoke_token(
        &self,
        token_id: Uuid,
        requesting_user: Option<&User>,
    ) -> Result<(), AuthError> {
        let token = self
            .get_token(token_id)?
            .ok_or(AuthError::TokenNotFound(token_id))?;

        if let Some(user) = requesting_user {
            if user.role != UserRole::Admin && user.user_id != token.user_id {
                return Err(AuthError::Forbidden);
            }
        }

        let token_hex = hex::encode(token.token_hash);
        self.db.delete(format!("t:{}", token_hex).as_bytes())?;
        self.db.delete(
            format!("ut:{}:{}", token.user_id, token.token_id).as_bytes(),
        )?;

        Ok(())
    }
}

/// Axum extractor for authenticated user (any role)
#[derive(Debug, Clone)]
pub struct AuthenticatedUser(pub User);

impl<S> FromRequestParts<S> for AuthenticatedUser
where
    S: Send + Sync,
    Arc<UserStore>: FromRef<S>,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let user_store = Arc::<UserStore>::from_ref(state);

        let auth_header = parts
            .headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok());

        if let Some(header) = auth_header {
            if let Some(token) = header.strip_prefix("Bearer ") {
                if let Some(user) = user_store.authenticate_api_key(token.trim()) {
                    return Ok(AuthenticatedUser(user));
                }
            } else if let Some(basic_b64) = header.strip_prefix("Basic ") {
                if let Some(user) = user_store.authenticate_basic(basic_b64.trim()) {
                    return Ok(AuthenticatedUser(user));
                }
            }
        }

        // Check query param ?api_key=uru_... for browser convenience
        if let Some(query) = parts.uri.query() {
            for pair in query.split('&') {
                if let Some(key) = pair.strip_prefix("api_key=") {
                    if let Some(user) = user_store.authenticate_api_key(key) {
                        return Ok(AuthenticatedUser(user));
                    }
                }
            }
        }

        let resp = (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({
                "error": "Unauthorized",
                "message": "Valid Authorization: Bearer uru_... or Basic auth required"
            })),
        )
            .into_response();

        Err(resp)
    }
}

/// Axum extractor for Admin user only
#[derive(Debug, Clone)]
pub struct AdminUser(pub User);

impl<S> FromRequestParts<S> for AdminUser
where
    S: Send + Sync,
    Arc<UserStore>: FromRef<S>,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let auth_user = AuthenticatedUser::from_request_parts(parts, state).await?;
        if auth_user.0.role != UserRole::Admin {
            let resp = (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({
                    "error": "Forbidden",
                    "message": "Admin privileges required"
                })),
            )
                .into_response();
            return Err(resp);
        }
        Ok(AdminUser(auth_user.0))
    }
}

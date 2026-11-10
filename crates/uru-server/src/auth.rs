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
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use subtle::ConstantTimeEq;

const API_KEY_SECRET: &[u8; 32] = b"uru_secret_salt_key_32_bytes_cas";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub user_id: String,
    pub password_hash: String,
    pub api_key_hash: [u8; 32],
}

pub struct UserStore {
    users_by_id: DashMap<String, User>,
}

impl Default for UserStore {
    fn default() -> Self {
        Self::new()
    }
}

impl UserStore {
    pub fn new() -> Self {
        let store = Self {
            users_by_id: DashMap::new(),
        };

        // Create default demo user for testing and evaluation
        store.register_user(
            "default_user",
            "password123",
            "uru_demo_key_1234567890abcdef",
        );
        store.register_user("alice", "alicepass", "uru_alice_api_key_1234567890");
        store.register_user("bob", "bobpass", "uru_bob_api_key_1234567890");

        store
    }

    pub fn hash_api_key(api_key: &str) -> [u8; 32] {
        *blake3::keyed_hash(API_KEY_SECRET, api_key.as_bytes()).as_bytes()
    }

    pub fn register_user(&self, user_id: &str, password: &str, api_key: &str) {
        let argon2 = Argon2::default();
        let password_hash = argon2
            .hash_password(password.as_bytes())
            .unwrap()
            .to_string();

        let api_key_hash = Self::hash_api_key(api_key);

        self.users_by_id.insert(
            user_id.to_string(),
            User {
                user_id: user_id.to_string(),
                password_hash,
                api_key_hash,
            },
        );
    }

    pub fn authenticate_api_key(&self, token: &str) -> Option<User> {
        let target_hash = Self::hash_api_key(token);
        for entry in self.users_by_id.iter() {
            if entry.value().api_key_hash.ct_eq(&target_hash).into() {
                return Some(entry.value().clone());
            }
        }
        None
    }

    pub fn authenticate_password(&self, user_id: &str, password: &str) -> Option<User> {
        let user = self.users_by_id.get(user_id)?;
        let parsed_hash = PasswordHash::new(&user.password_hash).ok()?;
        if Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok()
        {
            Some(user.clone())
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
        self.users_by_id.get(user_id).map(|u| u.clone())
    }
}

/// Axum extractor for authenticated user
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

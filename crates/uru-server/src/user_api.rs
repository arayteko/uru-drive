use crate::auth::{AdminUser, AuthenticatedUser, User, UserRole};
use crate::state::AppState;
use axum::{
    extract::{Path as AxumPath, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct UserInfo {
    pub user_id: String,
    pub role: UserRole,
    pub created_at_ms: u64,
}

impl From<User> for UserInfo {
    fn from(u: User) -> Self {
        Self {
            user_id: u.user_id,
            role: u.role,
            created_at_ms: u.created_at_ms,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateUserRequest {
    pub user_id: String,
    pub password: String,
    #[serde(default = "default_user_role")]
    pub role: UserRole,
}

fn default_user_role() -> UserRole {
    UserRole::User
}

#[derive(Debug, Deserialize)]
pub struct UpdatePasswordRequest {
    pub new_password: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateRoleRequest {
    pub role: UserRole,
}

#[derive(Debug, Deserialize)]
pub struct CreateTokenRequest {
    pub name: String,
    pub user_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CreateTokenResponse {
    pub token_id: Uuid,
    pub user_id: String,
    pub name: String,
    pub token: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct TokenInfo {
    pub token_id: Uuid,
    pub user_id: String,
    pub name: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Deserialize)]
pub struct ListTokensQuery {
    pub user_id: Option<String>,
}

pub fn user_management_router() -> Router<AppState> {
    Router::new()
        // Users
        .route("/users", get(list_users).post(create_user))
        .route("/users/{user_id}", get(get_user).delete(delete_user))
        .route("/users/{user_id}/password", put(update_user_password))
        .route("/users/{user_id}/role", put(update_user_role))
        // Tokens
        .route("/tokens", get(list_tokens).post(create_token))
        .route("/tokens/{token_id}", delete(revoke_token))
}

/// GET /api/v1/users (Admin only)
async fn list_users(
    _admin: AdminUser,
    State(state): State<AppState>,
) -> Result<Json<Vec<UserInfo>>, Response> {
    let users = state.users.list_users().map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response()
    })?;

    Ok(Json(users.into_iter().map(UserInfo::from).collect()))
}

/// POST /api/v1/users (Admin only)
async fn create_user(
    _admin: AdminUser,
    State(state): State<AppState>,
    Json(payload): Json<CreateUserRequest>,
) -> Result<(StatusCode, Json<UserInfo>), Response> {
    if payload.user_id.trim().is_empty() || payload.password.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "Username and password cannot be empty" })),
        )
            .into_response());
    }

    let user = state
        .users
        .create_user(&payload.user_id, &payload.password, payload.role)
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response()
        })?;

    Ok((StatusCode::CREATED, Json(UserInfo::from(user))))
}

/// GET /api/v1/users/:user_id (Self or Admin)
async fn get_user(
    caller: AuthenticatedUser,
    State(state): State<AppState>,
    AxumPath(target_id): AxumPath<String>,
) -> Result<Json<UserInfo>, Response> {
    let effective_id = if target_id == "me" {
        caller.0.user_id.as_str()
    } else {
        target_id.as_str()
    };

    if caller.0.role != UserRole::Admin && caller.0.user_id != effective_id {
        return Err((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "Cannot view another user's profile" })),
        )
            .into_response());
    }

    let user = state
        .users
        .get_user(effective_id)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": format!("User '{effective_id}' not found") })),
            )
                .into_response()
        })?;

    Ok(Json(UserInfo::from(user)))
}

/// PUT /api/v1/users/:user_id/password (Self or Admin)
async fn update_user_password(
    caller: AuthenticatedUser,
    State(state): State<AppState>,
    AxumPath(target_id): AxumPath<String>,
    Json(payload): Json<UpdatePasswordRequest>,
) -> Result<StatusCode, Response> {
    let effective_id = if target_id == "me" {
        caller.0.user_id.as_str()
    } else {
        target_id.as_str()
    };

    if caller.0.role != UserRole::Admin && caller.0.user_id != effective_id {
        return Err((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "Cannot change another user's password" })),
        )
            .into_response());
    }

    if payload.new_password.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "New password cannot be empty" })),
        )
            .into_response());
    }

    state
        .users
        .update_password(effective_id, &payload.new_password)
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response()
        })?;

    Ok(StatusCode::NO_CONTENT)
}

/// PUT /api/v1/users/:user_id/role (Admin only)
async fn update_user_role(
    _admin: AdminUser,
    State(state): State<AppState>,
    AxumPath(target_id): AxumPath<String>,
    Json(payload): Json<UpdateRoleRequest>,
) -> Result<StatusCode, Response> {
    state
        .users
        .update_role(&target_id, payload.role)
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response()
        })?;

    Ok(StatusCode::NO_CONTENT)
}

/// DELETE /api/v1/users/:user_id (Admin only)
async fn delete_user(
    _admin: AdminUser,
    State(state): State<AppState>,
    AxumPath(target_id): AxumPath<String>,
) -> Result<StatusCode, Response> {
    state.users.delete_user(&target_id).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response()
    })?;

    Ok(StatusCode::NO_CONTENT)
}

/// GET /api/v1/tokens (Self or Admin)
async fn list_tokens(
    caller: AuthenticatedUser,
    State(state): State<AppState>,
    Query(query): Query<ListTokensQuery>,
) -> Result<Json<Vec<TokenInfo>>, Response> {
    let tokens = if caller.0.role == UserRole::Admin {
        if let Some(target) = query.user_id {
            if target == "all" {
                state.users.list_all_tokens()
            } else {
                state.users.list_tokens_for_user(&target)
            }
        } else {
            state.users.list_all_tokens()
        }
    } else {
        state.users.list_tokens_for_user(&caller.0.user_id)
    }
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response()
    })?;

    let infos = tokens
        .into_iter()
        .map(|t| TokenInfo {
            token_id: t.token_id,
            user_id: t.user_id,
            name: t.name,
            created_at_ms: t.created_at_ms,
        })
        .collect();

    Ok(Json(infos))
}

/// POST /api/v1/tokens (Self or Admin)
async fn create_token(
    caller: AuthenticatedUser,
    State(state): State<AppState>,
    Json(payload): Json<CreateTokenRequest>,
) -> Result<(StatusCode, Json<CreateTokenResponse>), Response> {
    let target_user = match payload.user_id {
        Some(uid) => {
            if caller.0.role != UserRole::Admin && caller.0.user_id != uid {
                return Err((
                    StatusCode::FORBIDDEN,
                    Json(serde_json::json!({ "error": "Cannot create tokens for another user" })),
                )
                    .into_response());
            }
            uid
        }
        None => caller.0.user_id.clone(),
    };

    let name = if payload.name.trim().is_empty() {
        "CLI Token".to_string()
    } else {
        payload.name
    };

    let (rec, token) = state
        .users
        .create_token(&target_user, &name)
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response()
        })?;

    Ok((
        StatusCode::CREATED,
        Json(CreateTokenResponse {
            token_id: rec.token_id,
            user_id: rec.user_id,
            name: rec.name,
            token,
            created_at_ms: rec.created_at_ms,
        }),
    ))
}

/// DELETE /api/v1/tokens/:token_id (Self or Admin)
async fn revoke_token(
    caller: AuthenticatedUser,
    State(state): State<AppState>,
    AxumPath(token_id): AxumPath<Uuid>,
) -> Result<StatusCode, Response> {
    state
        .users
        .revoke_token(token_id, Some(&caller.0))
        .map_err(|e| {
            let status = match e {
                crate::auth::AuthError::TokenNotFound(_) => StatusCode::NOT_FOUND,
                crate::auth::AuthError::Forbidden => StatusCode::FORBIDDEN,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, Json(serde_json::json!({ "error": e.to_string() }))).into_response()
        })?;

    Ok(StatusCode::NO_CONTENT)
}

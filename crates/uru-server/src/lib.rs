pub mod auth;
pub mod custom_api;
pub mod dav_adapter;
pub mod metrics_service;
pub mod s3_adapter;
pub mod state;
pub mod user_api;

use axum::{
    body::Body,
    extract::{Request, State},
    http::{header, Method, StatusCode},
    middleware,
    response::Response,
    routing::{any, get},
    Router,
};
use dav_adapter::UruDavFileSystem;
use dav_server::DavHandler;
use s3_adapter::UruS3Adapter;
use s3s::service::S3ServiceBuilder;
use state::AppState;
use tower_http::cors::{Any, CorsLayer};

pub fn build_app(state: AppState) -> Router {
    let prometheus_handle = state.prometheus_handle.clone();

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::OPTIONS,
            Method::HEAD,
            Method::PATCH,
        ])
        .allow_headers(Any)
        .expose_headers([
            header::CONTENT_LENGTH,
            header::CONTENT_RANGE,
            header::CONTENT_TYPE,
            header::ETAG,
            header::HeaderName::from_static("x-amz-version-id"),
        ]);

    let custom_api = custom_api::custom_api_router();
    let user_mgmt = user_api::user_management_router();

    Router::new()
        .nest("/api/v1", custom_api.merge(user_mgmt))
        .route(
            "/metrics",
            get(move || {
                let h = prometheus_handle.clone();
                async move { metrics_service::metrics_handler(h).await }
            }),
        )
        .route("/dav", any(webdav_handler))
        .route("/dav/", any(webdav_handler))
        .route("/dav/{*path}", any(webdav_handler))
        .route("/s3", any(s3_handler))
        .route("/s3/", any(s3_handler))
        .route("/s3/{*path}", any(s3_handler))
        .layer(middleware::from_fn(metrics_service::track_metrics_middleware))
        .layer(axum::extract::DefaultBodyLimit::max(64 * 1024 * 1024))
        .layer(cors)
        .with_state(state)
}

async fn webdav_handler(
    State(state): State<AppState>,
    req: Request,
) -> Result<Response, StatusCode> {
    // Determine authenticated user for WebDAV (supporting Basic auth or Bearer token)
    let auth_header = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok());

    let user_id = if let Some(header_val) = auth_header {
        if let Some(basic) = header_val.strip_prefix("Basic ") {
            state.users.authenticate_basic(basic).map(|u| u.user_id)
        } else if let Some(bearer) = header_val.strip_prefix("Bearer ") {
            state.users.authenticate_api_key(bearer).map(|u| u.user_id)
        } else {
            None
        }
    } else {
        None
    };

    let user_id = match user_id {
        Some(uid) => uid,
        None => {
            return Ok(Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .header(header::WWW_AUTHENTICATE, "Basic realm=\"UruDrive WebDAV\"")
                .body(Body::from("Unauthorized"))
                .unwrap());
        }
    };

    let fs = UruDavFileSystem::new(user_id, state.storage.clone());
    let dav_handler = DavHandler::builder()
        .strip_prefix("/dav")
        .filesystem(Box::new(fs))
        .autoindex(true)
        .build_handler();

    let dav_resp = dav_handler.handle(req).await;
    let (parts, body) = dav_resp.into_parts();
    let axum_body = Body::new(body);
    Ok(Response::from_parts(parts, axum_body))
}

async fn s3_handler(
    State(state): State<AppState>,
    req: Request,
) -> Result<Response, StatusCode> {
    let adapter = UruS3Adapter::new(state.storage.clone(), state.users.clone());
    let s3_service = S3ServiceBuilder::new(adapter).build();

    let (parts, body) = req.into_parts();
    let s3_req = http::Request::from_parts(parts, s3s::Body::http_body_unsync(body));

    let res = s3_service.call(s3_req).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let (res_parts, res_body) = res.into_parts();
    let axum_body = Body::new(res_body);
    Ok(Response::from_parts(res_parts, axum_body))
}

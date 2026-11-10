use axum::{
    extract::Request,
    middleware::Next,
    response::{IntoResponse, Response},
};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::time::Instant;
use tracing::info;

pub fn setup_prometheus() -> PrometheusHandle {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    // Set global recorder
    metrics::set_global_recorder(recorder).ok();
    handle
}

pub fn init_opentelemetry_if_configured() {
    if let Ok(endpoint) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
        info!(endpoint = %endpoint, "Initializing OpenTelemetry OTLP metrics exporter");
        // OpenTelemetry initialization can be wired here
    }
}

pub async fn track_metrics_middleware(req: Request, next: Next) -> Response {
    let start = Instant::now();
    let method = req.method().to_string();
    let path = req.uri().path().to_string();

    let protocol = if path.starts_with("/s3") {
        "s3"
    } else if path.starts_with("/dav") {
        "dav"
    } else {
        "custom"
    };

    let response = next.run(req).await;
    let duration = start.elapsed().as_secs_f64();
    let status = response.status().as_u16().to_string();

    metrics::counter!(
        "uru_cas_http_requests_total",
        "protocol" => protocol.to_string(),
        "method" => method,
        "status" => status
    )
    .increment(1);

    metrics::histogram!(
        "uru_cas_http_request_duration_seconds",
        "protocol" => protocol.to_string(),
        "endpoint" => path
    )
    .record(duration);

    response
}

pub async fn metrics_handler(handle: Option<PrometheusHandle>) -> impl IntoResponse {
    match handle {
        Some(h) => h.render(),
        None => "# Metrics recorder not initialized".to_string(),
    }
}

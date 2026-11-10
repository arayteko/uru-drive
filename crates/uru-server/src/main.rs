use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing::info;
use uru_server::{
    auth::UserStore,
    build_app,
    metrics_service::{init_opentelemetry_if_configured, setup_prometheus},
    state::AppState,
};
use uru_storage::StorageEngine;

#[derive(Parser, Debug)]
#[command(name = "uru-server", author, version, about = "UruDrive CAS Server")]
struct Args {
    #[arg(long, default_value = "127.0.0.1", env = "URU_HOST")]
    host: String,

    #[arg(short, long, default_value_t = 8080, env = "URU_PORT")]
    port: u16,

    #[arg(short, long, default_value = "./data", env = "URU_DATA_DIR")]
    data_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    info!(?args, "Starting UruDrive Multi-Protocol CAS Server");

    init_opentelemetry_if_configured();
    let prometheus_handle = setup_prometheus();

    let storage = StorageEngine::open(&args.data_dir)?;
    let auth_db_dir = args.data_dir.join("auth_db");
    let (users, _) = UserStore::open(&auth_db_dir)?;

    let state = AppState::new(storage, users).with_prometheus(prometheus_handle);
    let app = build_app(state);

    let addr: SocketAddr = format!("{}:{}", args.host, args.port).parse()?;
    info!("Server listening on http://{}", addr);
    info!("Endpoints mounted:");
    info!("  Custom API: http://{}/api/v1/...", addr);
    info!("  S3 API:     http://{}/s3/...", addr);
    info!("  WebDAV:     http://{}/dav/...", addr);
    info!("  Metrics:    http://{}/metrics", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

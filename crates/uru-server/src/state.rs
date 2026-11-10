use crate::auth::UserStore;
use axum::extract::FromRef;
use metrics_exporter_prometheus::PrometheusHandle;
use std::sync::Arc;
use uru_core::version::RetentionPolicy;
use uru_storage::StorageEngine;

#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<StorageEngine>,
    pub users: Arc<UserStore>,
    pub prometheus_handle: Option<PrometheusHandle>,
    pub retention_policy: RetentionPolicy,
}

impl AppState {
    pub fn new(storage: Arc<StorageEngine>, users: Arc<UserStore>) -> Self {
        Self {
            storage,
            users,
            prometheus_handle: None,
            retention_policy: RetentionPolicy::default(),
        }
    }

    pub fn with_prometheus(mut self, handle: PrometheusHandle) -> Self {
        self.prometheus_handle = Some(handle);
        self
    }
}

impl FromRef<AppState> for Arc<StorageEngine> {
    fn from_ref(state: &AppState) -> Self {
        state.storage.clone()
    }
}

impl FromRef<AppState> for Arc<UserStore> {
    fn from_ref(state: &AppState) -> Self {
        state.users.clone()
    }
}

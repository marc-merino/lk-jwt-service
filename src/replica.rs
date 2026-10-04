// Copyright 2026 Marc Merino
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

//! Active/standby replicas: the replica holding a Redis lease runs the
//! handler and the others forward requests to it.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::response::{IntoResponse, Response};
use http::StatusCode;
use tokio::time::{Instant, sleep, timeout_at};
use tower::ServiceExt;
use tracing::{error, info, warn};

use crate::handler::Handler;

const LEASE_KEY: &str = "lk-jwt:leader";
const LEASE_TTL: Duration = Duration::from_secs(5);
const FORWARDED: &str = "x-lk-jwt-forwarded";
const MAX_BODY: usize = 8 * 1024 * 1024;
const RENEW_SCRIPT: &str = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) end return 0";
const RELEASE_SCRIPT: &str =
    "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0";

pub struct Replica {
    conn: redis::aio::ConnectionManager,
    lease: String,
    local: OnceLock<Router>,
    client: reqwest::Client,
}

impl Replica {
    pub async fn new(redis_url: &str, replica_url: &str) -> Result<Arc<Self>, String> {
        let conn = redis::Client::open(redis_url)
            .map_err(|e| format!("invalid Redis URL: {e}"))?
            .get_connection_manager()
            .await
            .map_err(|e| format!("Redis connection failed: {e}"))?;
        Ok(Arc::new(Self {
            conn,
            lease: format!("{replica_url} {:016x}", rand::random::<u64>()),
            local: OnceLock::new(),
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(30))
                .build()
                .map_err(|e| e.to_string())?,
        }))
    }

    pub fn router(self: &Arc<Self>) -> Router {
        Router::new().fallback(route).with_state(self.clone())
    }

    /// Waits for the lease and keeps renewing it. Losing it exits the process.
    pub async fn wait_for_leadership(self: &Arc<Self>) {
        let mut deadline = loop {
            let started = Instant::now();
            match self.acquire().await {
                Ok(true) => break started + LEASE_TTL,
                Ok(false) => {}
                Err(err) => warn!(%err, "Replica: lease acquisition failed"),
            }
            sleep(Duration::from_secs(1)).await;
        };
        info!("Replica: acquired leadership");
        let replica = self.clone();
        tokio::spawn(async move {
            loop {
                sleep(LEASE_TTL / 3).await;
                let started = Instant::now();
                match timeout_at(deadline, replica.compare(RENEW_SCRIPT)).await {
                    Ok(Ok(true)) => deadline = started + LEASE_TTL,
                    Ok(Ok(false)) => {
                        error!("Replica: lost leadership");
                        std::process::exit(1);
                    }
                    _ if Instant::now() >= deadline => {
                        error!("Replica: could not renew leadership in time");
                        std::process::exit(1);
                    }
                    _ => warn!("Replica: lease renewal failed"),
                }
            }
        });
    }

    pub fn serve_locally(&self, handler: &Arc<Handler>) {
        let _ = self.local.set(handler.prepare_router());
    }

    pub async fn release(&self) {
        if let Err(err) = self.compare(RELEASE_SCRIPT).await {
            warn!(%err, "Replica: lease release failed");
        }
    }

    async fn acquire(&self) -> redis::RedisResult<bool> {
        let set: Option<String> = redis::cmd("SET")
            .arg(LEASE_KEY)
            .arg(&self.lease)
            .arg("NX")
            .arg("PX")
            .arg(LEASE_TTL.as_millis() as u64)
            .query_async(&mut self.conn.clone())
            .await?;
        Ok(set.is_some())
    }

    async fn compare(&self, script: &str) -> redis::RedisResult<bool> {
        let changed: i64 = redis::cmd("EVAL")
            .arg(script)
            .arg(1)
            .arg(LEASE_KEY)
            .arg(&self.lease)
            .arg(LEASE_TTL.as_millis() as u64)
            .query_async(&mut self.conn.clone())
            .await?;
        Ok(changed != 0)
    }

    async fn forward(&self, request: Request) -> Result<Response, String> {
        let lease: Option<String> = redis::cmd("GET")
            .arg(LEASE_KEY)
            .query_async(&mut self.conn.clone())
            .await
            .map_err(|e| e.to_string())?;
        let leader = lease
            .as_deref()
            .and_then(|lease| lease.split(' ').next())
            .ok_or("no leader")?;
        let (parts, body) = request.into_parts();
        let path = parts.uri.path_and_query().map_or("/", |p| p.as_str());
        let target = url::Url::parse(&format!("{leader}{path}")).map_err(|e| e.to_string())?;
        if target.path() != parts.uri.path() {
            return Ok(StatusCode::BAD_REQUEST.into_response());
        }
        let body = to_bytes(body, MAX_BODY).await.map_err(|e| e.to_string())?;
        let upstream = self
            .client
            .request(parts.method, target)
            .headers(parts.headers)
            .header(FORWARDED, "1")
            .body(body)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let mut response = Response::builder().status(upstream.status());
        for (name, value) in upstream.headers() {
            response = response.header(name, value);
        }
        let body = upstream.bytes().await.map_err(|e| e.to_string())?;
        response.body(Body::from(body)).map_err(|e| e.to_string())
    }
}

async fn route(State(replica): State<Arc<Replica>>, request: Request) -> Response {
    if let Some(local) = replica.local.get() {
        return local.clone().oneshot(request).await.unwrap();
    }
    if request.uri().path() == "/healthz" {
        return StatusCode::OK.into_response();
    }
    if request.headers().contains_key(FORWARDED) {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    replica.forward(request).await.unwrap_or_else(|err| {
        warn!(%err, "Replica: forwarding failed");
        StatusCode::SERVICE_UNAVAILABLE.into_response()
    })
}

pub async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = ctrl_c.await;
}

#[cfg(test)]
mod tests {
    use axum::routing::any;

    use super::*;
    use crate::store::test_support::spawn_mini_redis;

    async fn serve(router: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        url
    }

    async fn send(replica: &Arc<Replica>, request: Request) -> (StatusCode, String) {
        let response = replica.router().oneshot(request).await.unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), MAX_BODY).await.unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn only_one_replica_holds_the_lease() {
        let redis = spawn_mini_redis().await;
        let url = format!("redis://{}", redis.addr);
        let first = Replica::new(&url, "http://first").await.unwrap();
        let second = Replica::new(&url, "http://second").await.unwrap();
        assert!(first.acquire().await.unwrap());
        assert!(!second.acquire().await.unwrap());
        assert!(!second.compare(RENEW_SCRIPT).await.unwrap());
        assert!(first.compare(RENEW_SCRIPT).await.unwrap());
        first.release().await;
        assert!(second.acquire().await.unwrap());
    }

    #[tokio::test]
    async fn follower_forwards_requests_to_the_leader() {
        let redis = spawn_mini_redis().await;
        let url = format!("redis://{}", redis.addr);
        let leader_url = serve(Router::new().fallback(any(|request: Request| async move {
            assert_eq!(request.headers()[FORWARDED], "1");
            let body = to_bytes(request.into_body(), MAX_BODY).await.unwrap();
            (
                StatusCode::CREATED,
                format!("leader got {}", String::from_utf8_lossy(&body)),
            )
        })))
        .await;
        let leader = Replica::new(&url, &leader_url).await.unwrap();
        let follower = Replica::new(&url, "http://follower").await.unwrap();

        let request = || {
            Request::post("/get_token?x=1")
                .body(Body::from("token"))
                .unwrap()
        };
        assert_eq!(
            send(&follower, request()).await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert!(leader.acquire().await.unwrap());
        assert_eq!(
            send(&follower, request()).await,
            (StatusCode::CREATED, "leader got token".into())
        );

        let healthz = Request::get("/healthz").body(Body::empty()).unwrap();
        assert_eq!(send(&follower, healthz).await.0, StatusCode::OK);
        let looped = Request::get("/get_token")
            .header(FORWARDED, "1")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            send(&follower, looped).await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        let traversal = Request::get("/a/../healthz").body(Body::empty()).unwrap();
        assert_eq!(send(&follower, traversal).await.0, StatusCode::BAD_REQUEST);
    }
}

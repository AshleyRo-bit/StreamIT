use axum::{
    extract::State,
    http::header,
    response::IntoResponse,
    routing::get,
    Router,
};
use futures::{stream, StreamExt};
use reqwest::{Client, StatusCode};
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::time::{sleep, Duration};

const SOURCE_URL: &str =
    "https://iptv-org.github.io/iptv/countries/in.m3u";

const LISTEN_ADDR: &str = "0.0.0.0:8095";

// Maximum number of streams checked simultaneously.
const MAX_CONCURRENT_CHECKS: usize = 10;

// Don't let one bad CDN hold up playlist generation.
const STREAM_TIMEOUT_SECS: u64 = 8;

// Maximum amount of the response we inspect.
const MAX_PROBE_BYTES: usize = 64 * 1024;

#[derive(Clone)]
struct AppState {
    playlist: Arc<Mutex<String>>,
}

#[derive(Debug, Clone)]
struct M3uEntry {
    info: String,
    url: String,
}

#[derive(Debug)]
enum StreamStatus {
    Working,
    Forbidden,
    NotFound,
    ServerError(u16),
    RedirectError,
    InvalidPlaylist,
    Failed(String),
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let state = AppState {
        playlist: Arc::new(Mutex::new(
            "#EXTM3U\n".to_string()
        )),
    };

    /*
     * Background playlist worker.
     */
    let worker_state = state.clone();

    tokio::spawn(async move {
        let client = Client::builder()
            .user_agent(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
                 AppleWebKit/537.36 (KHTML, like Gecko) \
                 Chrome/122.0.0.0 Safari/537.36",
            )
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(STREAM_TIMEOUT_SECS))
            .redirect(reqwest::redirect::Policy::limited(5))
            .build()
            .expect("Failed to create HTTP client");

        loop {
            println!(
                "[Rust Engine] Fetching Indian IPTV playlist..."
            );

            match fetch_playlist(&client).await {
                Ok(raw_playlist) => {
                    println!(
                        "[Rust Engine] Downloaded {} bytes",
                        raw_playlist.len()
                    );

                    let entries = parse_m3u(&raw_playlist);

                    println!(
                        "[Rust Engine] Found {} channels",
                        entries.len()
                    );

                    let valid_entries =
                        validate_entries(&client, entries).await;

                    println!(
                        "[Rust Engine] {} playable channels retained",
                        valid_entries.len()
                    );

                    if !valid_entries.is_empty() {
                        let playlist = build_m3u(valid_entries);

                        let mut lock =
                            worker_state.playlist.lock().unwrap();

                        *lock = playlist;

                        println!(
                            "[Rust Engine] Playlist updated successfully."
                        );
                    } else {
                        println!(
                            "[Rust Engine] WARNING: No playable channels \
                             found. Keeping previous playlist."
                        );
                    }
                }

                Err(error) => {
                    eprintln!(
                        "[Rust Engine] Failed to fetch source playlist: {}",
                        error
                    );

                    println!(
                        "[Rust Engine] Keeping previous playlist."
                    );
                }
            }

            println!(
                "[Rust Engine] Sleeping for 6 hours..."
            );

            sleep(Duration::from_secs(21600)).await;
        }
    });

    /*
     * Jellyfin-facing HTTP server.
     */
    let app = Router::new()
        .route("/indian_tv.m3u", get(get_playlist))
        .with_state(state);

    let addr: SocketAddr = LISTEN_ADDR
        .parse()
        .expect("Invalid listen address");

    println!(
        "[Rust Engine] Playlist server listening on \
         http://{}/indian_tv.m3u",
        LISTEN_ADDR
    );

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("Failed to bind HTTP server");

    axum::serve(listener, app)
        .await
        .expect("Axum server failed");
}


/*
 * Download the source M3U.
 */
async fn fetch_playlist(
    client: &Client,
) -> Result<String, String> {
    let response = client
        .get(SOURCE_URL)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if !response.status().is_success() {
        return Err(format!(
            "Source returned HTTP {}",
            response.status()
        ));
    }

    response
        .text()
        .await
        .map_err(|e| e.to_string())
}


/*
 * Parse standard M3U:
 *
 * #EXTINF:-1,...metadata...
 * https://stream.example/live.m3u8
 */
fn parse_m3u(input: &str) -> Vec<M3uEntry> {
    let mut entries = Vec::new();
    let mut current_info: Option<String> = None;

    for raw_line in input.lines() {
        let line = raw_line.trim();

        if line.is_empty() {
            continue;
        }

        if line.starts_with("#EXTINF:") {
            current_info = Some(line.to_string());
            continue;
        }

        if line.starts_with('#') {
            continue;
        }

        if let Some(info) = current_info.take() {
            if line.starts_with("http://")
                || line.starts_with("https://")
            {
                entries.push(M3uEntry {
                    info,
                    url: line.to_string(),
                });
            }
        }
    }

    entries
}


/*
 * Validate all streams concurrently.
 */
async fn validate_entries(
    client: &Client,
    entries: Vec<M3uEntry>,
) -> Vec<M3uEntry> {
    let results = stream::iter(entries.into_iter().map(|entry| {
        let client = client.clone();

        async move {
            let name = channel_name(&entry.info);

            let status =
                validate_stream(&client, &entry.url).await;

            match &status {
                StreamStatus::Working => {
                    println!(
                        "[Rust Engine] ✓ {}",
                        name
                    );

                    Some(entry)
                }

                StreamStatus::Forbidden => {
                    println!(
                        "[Rust Engine] ✗ 403 Forbidden: {}",
                        name
                    );

                    None
                }

                StreamStatus::NotFound => {
                    println!(
                        "[Rust Engine] ✗ 404 Not Found: {}",
                        name
                    );

                    None
                }

                StreamStatus::ServerError(code) => {
                    println!(
                        "[Rust Engine] ✗ HTTP {}: {}",
                        code,
                        name
                    );

                    None
                }

                StreamStatus::RedirectError => {
                    println!(
                        "[Rust Engine] ✗ Too many redirects: {}",
                        name
                    );

                    None
                }

                StreamStatus::InvalidPlaylist => {
                    println!(
                        "[Rust Engine] ✗ Invalid/empty HLS playlist: {}",
                        name
                    );

                    None
                }

                StreamStatus::Failed(error) => {
                    println!(
                        "[Rust Engine] ✗ Failed: {} ({})",
                        name,
                        error
                    );

                    None
                }
            }
        }
    }))
    .buffer_unordered(MAX_CONCURRENT_CHECKS)
    .filter_map(|result| async move { result })
    .collect::<Vec<M3uEntry>>()
    .await;

    results
}


/*
 * Check a single stream.
 *
 * For HLS (.m3u8), we don't attempt to consume the live stream.
 * We inspect only the beginning of the response.
 */
async fn validate_stream(
    client: &Client,
    url: &str,
) -> StreamStatus {
    let response = match client
        .get(url)
        .header(
            header::ACCEPT,
            "application/vnd.apple.mpegurl, \
             application/x-mpegURL, \
             application/octet-stream, \
             */*",
        )
        .header(header::CACHE_CONTROL, "no-cache")
        .send()
        .await
    {
        Ok(response) => response,

        Err(error) => {
            return StreamStatus::Failed(
                error.to_string()
            );
        }
    };

    let status = response.status();

    match status {
        StatusCode::FORBIDDEN => {
            return StreamStatus::Forbidden;
        }

        StatusCode::NOT_FOUND => {
            return StreamStatus::NotFound;
        }

        status if status.is_server_error() => {
            return StreamStatus::ServerError(
                status.as_u16()
            );
        }

        status if !status.is_success() => {
            return StreamStatus::Failed(
                format!("HTTP {}", status.as_u16())
            );
        }

        _ => {}
    }

    /*
     * Read only a limited amount of the response.
     *
     * We intentionally do NOT call response.text() or
     * response.bytes(), because a live stream may never end.
     */
    let mut response = response;
    let mut collected = Vec::new();

    while collected.len() < MAX_PROBE_BYTES {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                let remaining =
                    MAX_PROBE_BYTES - collected.len();

                let take =
                    chunk.len().min(remaining);

                collected.extend_from_slice(
                    &chunk[..take]
                );

                /*
                 * We have enough data to identify an HLS
                 * playlist.
                 */
                if collected
                    .windows(7)
                    .any(|window| window == b"#EXTM3U")
                {
                    return StreamStatus::Working;
                }

                /*
                 * MPEG-TS streams may not contain #EXTM3U.
                 * Receiving actual data from a successful
                 * response is still a useful signal.
                 */
                if collected.len() >= 4096 {
                    return StreamStatus::Working;
                }
            }

            Ok(None) => break,

            Err(error) => {
                /*
                 * If we already received useful data, consider
                 * the stream usable. Live streams may close or
                 * reset connections after delivering data.
                 */
                if collected.len() >= 1024 {
                    return StreamStatus::Working;
                }

                return StreamStatus::Failed(
                    error.to_string()
                );
            }
        }
    }

    if collected
        .windows(7)
        .any(|window| window == b"#EXTM3U")
    {
        StreamStatus::Working
    } else if collected.len() >= 1024 {
        StreamStatus::Working
    } else {
        StreamStatus::InvalidPlaylist
    }
}


/*
 * Rebuild the cleaned M3U while preserving the original
 * EXTINF metadata.
 */
fn build_m3u(entries: Vec<M3uEntry>) -> String {
    let mut output =
        String::from("#EXTM3U\n");

    for entry in entries {
        output.push_str(&entry.info);
        output.push('\n');

        output.push_str(&entry.url);
        output.push('\n');
    }

    output
}


/*
 * Extract the human-readable channel name from EXTINF.
 *
 * Example:
 *
 * #EXTINF:-1 tvg-name="9XM",9XM
 *
 * returns:
 *
 * 9XM
 */
fn channel_name(info: &str) -> String {
    info.split_once(',')
        .map(|(_, name)| name.trim())
        .filter(|name| !name.is_empty())
        .unwrap_or("Unknown")
        .to_string()
}


/*
 * Jellyfin requests this endpoint.
 */
async fn get_playlist(
    State(state): State<AppState>,
) -> impl IntoResponse {
    let playlist = state
        .playlist
        .lock()
        .unwrap()
        .clone();

    (
        [(
            header::CONTENT_TYPE,
            "application/x-mpegurl",
        )],
        playlist,
    )
}

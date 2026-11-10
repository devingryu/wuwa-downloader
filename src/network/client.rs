use colored::Colorize;
use indicatif::ProgressBar;
use reqwest::{Client, StatusCode};
use serde_json::{Value, from_str};
use std::{
    io::{self, Write},
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use tokio::io::AsyncWriteExt;
use tokio::time::sleep;

use crate::config::cfg::{Config, PackSource};
use crate::config::status::Status;
use crate::download::progress::DownloadProgress;
use crate::io::file::{file_size, get_filename};
use crate::io::logging::{SharedLogFile, log_error};
use crate::io::util::{clear_screen, get_version, read_line};

const INDEX_URL: &str = "https://gist.githubusercontent.com/yuhkix/b8796681ac2cd3bab11b7e8cdc022254/raw/4435fd290c07f7f766a6d2ab09ed3096d83b02e3/wuwa.json";
const LIVE_OS_INDEX_URL: &str = "https://prod-alicdn-gamestarter.kurogame.com/launcher/game/50004_P7xcUZnEr1AXIGON25E6KjpOgTlVrg6e/G153/official/index.json";
const MAX_RETRIES: usize = 3;
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(10_000);

enum DownloadAttemptResult {
    Completed,
    Retryable(String),
    RangeNotSatisfiable,
    RangeUnsupported,
    HttpError(String),
    Interrupted,
}

enum CdnDownloadResult {
    Success,
    RetryWithoutResume,
    Failed(String),
    Interrupted,
}

pub fn build_download_url(base_url: &str, dest: &str) -> String {
    format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        dest.trim_start_matches('/')
    )
}

async fn decompress_if_gzipped(response: reqwest::Response) -> Result<String, String> {
    response
        .text()
        .await
        .map_err(|e| format!("Error reading response text: {}", e))
}

pub async fn fetch_index(
    client: &Client,
    index_url: &str,
    log_file: &SharedLogFile,
) -> Result<Value, String> {
    println!("{} Fetching index file...", Status::info());

    let response = match client
        .get(index_url)
        .timeout(Duration::from_secs(30))
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(e) => {
            let msg = format!("Error fetching index file: {}", e);
            log_error(log_file, &msg);
            return Err(msg);
        }
    };

    if !response.status().is_success() {
        let msg = format!("Error fetching index file: HTTP {}", response.status());
        log_error(log_file, &msg);
        return Err(msg);
    }

    let text = match decompress_if_gzipped(response).await {
        Ok(t) => t,
        Err(e) => {
            let msg = format!("Error processing index file: {}", e);
            log_error(log_file, &msg);
            return Err(msg);
        }
    };

    println!("{} Index file downloaded successfully", Status::success());

    match from_str(&text) {
        Ok(v) => Ok(v),
        Err(e) => {
            let msg = format!("Error parsing index file JSON: {}", e);
            log_error(log_file, &msg);
            Err(msg)
        }
    }
}

async fn remove_partial_file(path: &Path) {
    if tokio::fs::try_exists(path).await.unwrap_or(false) {
        let _ = tokio::fs::remove_file(path).await;
    }
}

async fn rollback_counted_bytes(
    progress: &DownloadProgress,
    total_pb: &ProgressBar,
    counted_bytes_for_file: &mut u64,
) {
    let amount = *counted_bytes_for_file;
    if amount == 0 {
        return;
    }

    progress.rollback_downloaded_bytes(total_pb, amount).await;
    *counted_bytes_for_file = 0;
}

async fn wait_for_stop(should_stop: &AtomicBool) {
    while !should_stop.load(Ordering::SeqCst) {
        sleep(Duration::from_millis(100)).await;
    }
}

async fn count_total_progress(
    progress: &DownloadProgress,
    total_pb: &ProgressBar,
    counted_bytes_for_file: &mut u64,
    amount: u64,
    track_total: bool,
) {
    if amount == 0 || !track_total {
        return;
    }

    progress.add_downloaded_bytes(total_pb, amount).await;
    *counted_bytes_for_file += amount;
}

#[allow(clippy::too_many_arguments)]
async fn download_single_file(
    client: &Client,
    url: &str,
    path: &Path,
    should_stop: &std::sync::atomic::AtomicBool,
    progress: &DownloadProgress,
    total_pb: &ProgressBar,
    task_pb: &ProgressBar,
    allow_resume: bool,
    counted_bytes_for_file: &mut u64,
    track_total: bool,
) -> DownloadAttemptResult {
    let local_size = file_size(path).await;
    let use_range = allow_resume && local_size > 0;

    let request = client
        .get(url)
        .timeout(DOWNLOAD_TIMEOUT)
        .header("Connection", "keep-alive");

    let request = if use_range {
        request.header("Range", format!("bytes={}-", local_size))
    } else {
        request
    };

    let mut response = match tokio::select! {
        _ = wait_for_stop(should_stop) => return DownloadAttemptResult::Interrupted,
        resp = request.send() => resp,
    } {
        Ok(resp) => resp,
        Err(e) => return DownloadAttemptResult::Retryable(format!("Network error: {}", e)),
    };

    if response.status() == StatusCode::RANGE_NOT_SATISFIABLE {
        return DownloadAttemptResult::RangeNotSatisfiable;
    }

    if use_range && response.status() == StatusCode::OK {
        // Range request was ignored (common when server does not support byte ranges).
        let _accept_ranges = response
            .headers()
            .get("accept-ranges")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        return DownloadAttemptResult::RangeUnsupported;
    }

    if !response.status().is_success() && response.status() != StatusCode::PARTIAL_CONTENT {
        return DownloadAttemptResult::HttpError(format!("HTTP error: {}", response.status()));
    }

    let append_mode = use_range && response.status() == StatusCode::PARTIAL_CONTENT;
    let mut options = tokio::fs::OpenOptions::new();
    options.create(true);

    if append_mode {
        options.append(true);
        task_pb.set_position(local_size);
        if *counted_bytes_for_file == 0 {
            count_total_progress(
                progress,
                total_pb,
                counted_bytes_for_file,
                local_size,
                track_total,
            )
            .await;
        }
    } else {
        options.write(true).truncate(true);
        task_pb.set_position(0);
    }

    let mut file = match options.open(path).await {
        Ok(file) => file,
        Err(e) => return DownloadAttemptResult::Retryable(format!("File open error: {}", e)),
    };

    loop {
        if should_stop.load(std::sync::atomic::Ordering::SeqCst) {
            return DownloadAttemptResult::Interrupted;
        }

        let chunk = match tokio::select! {
            _ = wait_for_stop(should_stop) => return DownloadAttemptResult::Interrupted,
            chunk = response.chunk() => chunk,
        } {
            Ok(Some(chunk)) => chunk,
            Ok(None) => break,
            Err(e) => return DownloadAttemptResult::Retryable(format!("Read error: {}", e)),
        };

        if let Err(e) = file.write_all(&chunk).await {
            return DownloadAttemptResult::Retryable(format!("Write error: {}", e));
        }

        let size = chunk.len() as u64;
        task_pb.inc(size);
        count_total_progress(
            progress,
            total_pb,
            counted_bytes_for_file,
            size,
            track_total,
        )
        .await;
    }

    if let Err(e) = file.flush().await {
        return DownloadAttemptResult::Retryable(format!("File flush error: {}", e));
    }

    DownloadAttemptResult::Completed
}

#[allow(clippy::too_many_arguments)]
async fn try_download_with_cdns(
    client: &Client,
    zip_bases: &[String],
    dest: &str,
    path: &Path,
    log_file: &SharedLogFile,
    should_stop: &std::sync::atomic::AtomicBool,
    progress: &DownloadProgress,
    total_pb: &ProgressBar,
    task_pb: &ProgressBar,
    allow_resume: bool,
    counted_bytes_for_file: &mut u64,
    track_total: bool,
) -> CdnDownloadResult {
    let mut saw_range_unsupported = false;
    let mut last_error = "Unknown error".to_string();

    for (i, base_url) in zip_bases.iter().enumerate() {
        if should_stop.load(std::sync::atomic::Ordering::SeqCst) {
            return CdnDownloadResult::Interrupted;
        }

        let url = build_download_url(base_url, dest);
        let mut retries = MAX_RETRIES;

        while retries > 0 {
            let local_size = if allow_resume {
                file_size(path).await
            } else {
                0
            };
            let attempt = download_single_file(
                client,
                &url,
                path,
                should_stop,
                progress,
                total_pb,
                task_pb,
                allow_resume,
                counted_bytes_for_file,
                track_total,
            )
            .await;

            match attempt {
                DownloadAttemptResult::Completed => {
                    return CdnDownloadResult::Success;
                }
                DownloadAttemptResult::Interrupted => {
                    return CdnDownloadResult::Interrupted;
                }
                DownloadAttemptResult::Retryable(err) => {
                    last_error = err;
                    retries -= 1;
                    if !allow_resume {
                        rollback_counted_bytes(progress, total_pb, counted_bytes_for_file).await;
                        task_pb.set_position(0);
                    }
                    if retries > 0 {
                        task_pb.set_message(format!(
                            "retrying {} ({} left)",
                            get_filename(dest).yellow(),
                            retries
                        ));
                    }
                }
                DownloadAttemptResult::RangeNotSatisfiable => {
                    last_error = "Range not satisfiable, restarting file".to_string();
                    retries -= 1;
                    rollback_counted_bytes(progress, total_pb, counted_bytes_for_file).await;
                    remove_partial_file(path).await;
                    task_pb.set_position(0);
                    task_pb.set_message(format!(
                        "range invalid, restarting {} ({} left)",
                        get_filename(dest).yellow(),
                        retries
                    ));
                }
                DownloadAttemptResult::RangeUnsupported => {
                    if local_size > 0 {
                        saw_range_unsupported = true;
                        last_error = format!(
                            "CDN {} does not support resuming {}",
                            i + 1,
                            get_filename(dest)
                        );
                        log_error(log_file, &last_error);
                    }
                    break;
                }
                DownloadAttemptResult::HttpError(err) => {
                    last_error = err;
                    log_error(
                        log_file,
                        &format!(
                            "CDN {} failed for {}: {}",
                            i + 1,
                            get_filename(dest),
                            last_error
                        ),
                    );
                    break;
                }
            }
        }

        if retries == 0 {
            log_error(
                log_file,
                &format!(
                    "CDN {} retries exhausted for {}: {}",
                    i + 1,
                    get_filename(dest),
                    last_error
                ),
            );
        }
    }

    if allow_resume && saw_range_unsupported {
        CdnDownloadResult::RetryWithoutResume
    } else {
        CdnDownloadResult::Failed(last_error)
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn download_file(
    client: &Client,
    config: &Config,
    pack: usize,
    dest: &str,
    folder: &Path,
    expected_size: Option<u64>,
    log_file: &SharedLogFile,
    should_stop: &std::sync::atomic::AtomicBool,
    progress: &DownloadProgress,
    total_pb: &ProgressBar,
    task_pb: &ProgressBar,
) -> bool {
    if should_stop.load(std::sync::atomic::Ordering::SeqCst) {
        return false;
    }

    let Some(zip_bases) = config.packs.get(pack).map(|p| p.zip_bases.as_slice()) else {
        log_error(
            log_file,
            &format!("Unknown resource pack {} for {}", pack, dest),
        );
        return false;
    };

    let normalized_dest = dest.replace('\\', "/");
    let path = folder.join(&normalized_dest);
    let filename = get_filename(&normalized_dest);
    let mut counted_bytes_for_file = 0_u64;
    let track_total = expected_size.is_some();

    if let Some(total) = expected_size {
        task_pb.set_length(total);
    } else {
        task_pb.set_length(0);
    }

    if let Some(parent) = path.parent()
        && let Err(e) = tokio::fs::create_dir_all(parent).await
    {
        log_error(
            log_file,
            &format!("Directory error for {}: {}", normalized_dest, e),
        );
        task_pb.set_message(format!("directory error: {}", e));
        return false;
    }

    let first_pass = try_download_with_cdns(
        client,
        zip_bases,
        &normalized_dest,
        &path,
        log_file,
        should_stop,
        progress,
        total_pb,
        task_pb,
        true,
        &mut counted_bytes_for_file,
        track_total,
    )
    .await;

    match first_pass {
        CdnDownloadResult::Interrupted => return false,
        CdnDownloadResult::Success => {}
        CdnDownloadResult::RetryWithoutResume => {
            task_pb.set_message(format!(
                "CDN does not support resume, restarting {}",
                filename.yellow()
            ));
            rollback_counted_bytes(progress, total_pb, &mut counted_bytes_for_file).await;
            remove_partial_file(&path).await;
            task_pb.set_position(0);

            match try_download_with_cdns(
                client,
                zip_bases,
                &normalized_dest,
                &path,
                log_file,
                should_stop,
                progress,
                total_pb,
                task_pb,
                false,
                &mut counted_bytes_for_file,
                track_total,
            )
            .await
            {
                CdnDownloadResult::Success => {}
                CdnDownloadResult::Interrupted => return false,
                CdnDownloadResult::RetryWithoutResume => {
                    log_error(
                        log_file,
                        &format!("No CDN supports full redownload for {}", normalized_dest),
                    );
                    return false;
                }
                CdnDownloadResult::Failed(err) => {
                    log_error(
                        log_file,
                        &format!(
                            "Failed downloading {} after fallback: {}",
                            normalized_dest, err
                        ),
                    );
                    return false;
                }
            }
        }
        CdnDownloadResult::Failed(err) => {
            log_error(
                log_file,
                &format!("All CDNs failed for {}: {}", normalized_dest, err),
            );
            return false;
        }
    }

    true
}

pub fn ask_download_mode(_client: &Client) -> Result<String, String> {
    println!("\n{} Download Mode Selection", Status::info());
    println!(
        "{} 1. Latest game versions (from official sources)",
        Status::question()
    );
    println!(
        "{} 2. Custom version (provide resource URLs)",
        Status::question()
    );

    loop {
        print!("\n{} Choose download mode (1 or 2): ", Status::question());
        io::stdout()
            .flush()
            .map_err(|e| format!("Failed to flush stdout: {}", e))?;

        let input = read_line().map_err(|e| format!("Failed to read input: {}", e))?;

        match input.trim() {
            "1" => return Ok("latest".to_string()),
            "2" => return Ok("custom".to_string()),
            _ => println!("{} Invalid choice, please enter 1 or 2", Status::error()),
        }
    }
}

pub fn get_custom_config(_client: &Client) -> Result<Config, String> {
    println!("\n{} Custom Version Configuration", Status::info());

    print!("{} Enter resource.json URL: ", Status::question());
    io::stdout()
        .flush()
        .map_err(|e| format!("Failed to flush stdout: {}", e))?;

    let index_url = read_line().map_err(|e| format!("Failed to read input: {}", e))?;

    let index_url = index_url.trim();
    if index_url.is_empty() {
        return Err("Resource JSON URL cannot be empty".to_string());
    }

    let index_url = if index_url.starts_with("http://") || index_url.starts_with("https://") {
        index_url.to_string()
    } else {
        format!("https://{}", index_url)
    };

    print!(
        "{} Enter resource base path URL (ending with /zip): ",
        Status::question()
    );
    io::stdout()
        .flush()
        .map_err(|e| format!("Failed to flush stdout: {}", e))?;

    let base_url = read_line().map_err(|e| format!("Failed to read input: {}", e))?;

    let base_url = base_url.trim().to_string();
    if base_url.is_empty() {
        return Err("Resource base path URL cannot be empty".to_string());
    }

    let base_url = if base_url.starts_with("http://") || base_url.starts_with("https://") {
        base_url
    } else {
        format!("https://{}", base_url)
    };

    let base_url = if base_url.ends_with('/') {
        base_url
    } else {
        format!("{}/", base_url)
    };

    println!("\n{} Configuration loaded successfully", Status::success());
    Ok(Config {
        packs: vec![PackSource {
            name: "custom".to_string(),
            index_url,
            zip_bases: vec![base_url],
        }],
    })
}

fn prompt_choice(prompt: &str, max: usize) -> Result<usize, String> {
    loop {
        print!("{} {} (1-{}): ", Status::question(), prompt, max);
        io::stdout()
            .flush()
            .map_err(|e| format!("Failed to flush stdout: {}", e))?;

        let input = read_line().map_err(|e| format!("Failed to read input: {}", e))?;
        match input.trim().parse::<usize>() {
            Ok(n) if (1..=max).contains(&n) => return Ok(n - 1),
            _ => println!("{} Invalid choice, please enter 1-{}", Status::error(), max),
        }
    }
}

async fn fetch_json(client: &Client, url: &str) -> Result<Value, String> {
    let response = client
        .get(url)
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .map_err(|e| format!("Network error: {}", e))?;

    if !response.status().is_success() {
        return Err(format!("Server error: HTTP {}", response.status()));
    }

    let text = decompress_if_gzipped(response).await?;
    from_str(&text).map_err(|e| format!("Invalid JSON: {}", e))
}

fn cdn_urls_from(list: Option<&Value>) -> Vec<String> {
    list.and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(|cdn| cdn.get("url").and_then(Value::as_str))
                .map(|url| url.trim_end_matches('/').to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn ask_cdn_urls() -> Result<Vec<String>, String> {
    println!("{} Please enter CDN URLs manually.", Status::info());
    print!("{} Enter CDN URLs (comma-separated): ", Status::question());
    io::stdout()
        .flush()
        .map_err(|e| format!("Failed to flush stdout: {}", e))?;

    let input = read_line().map_err(|e| format!("Failed to read input: {}", e))?;
    let urls: Vec<String> = input
        .trim()
        .split(',')
        .map(|s| s.trim().trim_end_matches('/').to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if urls.is_empty() {
        return Err("No valid CDN URLs found".to_string());
    }
    Ok(urls)
}

fn pack_source(name: &str, pack: &Value, cdn_urls: &[String]) -> Result<PackSource, String> {
    let index_file = pack
        .get("indexFile")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("Missing or invalid indexFile in pack '{}'", name))?;
    let base_url = pack
        .get("baseUrl")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("Missing or invalid baseUrl in pack '{}'", name))?;

    Ok(PackSource {
        name: name.to_string(),
        index_url: build_download_url(&cdn_urls[0], index_file),
        zip_bases: cdn_urls
            .iter()
            .map(|cdn| build_download_url(cdn, base_url))
            .collect(),
    })
}

fn str_at(v: Option<&Value>) -> Option<String> {
    v.and_then(Value::as_str).map(str::to_string)
}

/// Version of a resource-pack section: `common`'s, else any pack's.
fn pack_section_version(section: &Value) -> Option<String> {
    let packs = section.get("resourcePacks").and_then(Value::as_object)?;
    packs
        .get("common")
        .or_else(|| packs.values().next())
        .and_then(|p| str_at(p.get("version")))
}

/// (current, predownload) versions of a launcher index.json, for either schema.
fn describe_versions(index: &Value) -> (Option<String>, Option<String>) {
    if index.get("resourcePacks").is_some() {
        let pre = index.get("predownload").and_then(pack_section_version);
        return (pack_section_version(index), pre);
    }

    let legacy = |key: &str| {
        index.get(key).and_then(|d| {
            str_at(d.get("config").and_then(|c| c.get("version")))
                .or_else(|| str_at(d.get("version")))
        })
    };
    (legacy("default"), legacy("predownload"))
}

/// Resource-pack schema: top-level `cdnList`, `resourcePacks`, `bundles`,
/// and an optional `predownload: { bundles, resourcePacks }`.
fn config_from_resource_packs(index: &Value) -> Result<Config, String> {
    let (current_version, pre_version) = describe_versions(index);
    let predownload = index
        .get("predownload")
        .filter(|p| p.get("resourcePacks").and_then(Value::as_object).is_some());

    let section = match predownload {
        None => {
            println!(
                "{} Using current version ({})",
                Status::info(),
                current_version.as_deref().unwrap_or("unknown")
            );
            index
        }
        Some(pre) => {
            println!("{} Available configs:", Status::info());
            println!(
                "1. Current ({})",
                current_version.as_deref().unwrap_or("unknown")
            );
            println!(
                "2. Predownload ({})",
                pre_version.as_deref().unwrap_or("unknown")
            );
            match prompt_choice("Choose config", 2)? {
                0 => index,
                _ => pre,
            }
        }
    };

    let packs = section
        .get("resourcePacks")
        .and_then(Value::as_object)
        .ok_or("Missing resourcePacks in response")?;

    // predownload carries its own bundles; fall back to the top-level ones.
    let bundles = section
        .get("bundles")
        .or_else(|| index.get("bundles"))
        .and_then(Value::as_object);

    let mut choices: Vec<(String, Vec<String>)> = Vec::new();
    if let Some(bundles) = bundles {
        for (name, bundle) in bundles {
            let pack_names: Vec<String> = bundle
                .get("resourcePacks")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();
            if !pack_names.is_empty() {
                choices.push((name.clone(), pack_names));
            }
        }
    }
    choices.sort_by(|a, b| a.0.cmp(&b.0));
    if choices.is_empty() {
        // No bundles declared: download every pack.
        choices.push(("all".to_string(), packs.keys().cloned().collect()));
    }

    let selected = if choices.len() == 1 {
        0
    } else {
        println!("{} Available bundles:", Status::info());
        for (i, (name, pack_names)) in choices.iter().enumerate() {
            let size: u64 = pack_names
                .iter()
                .filter_map(|p| {
                    packs
                        .get(p)
                        .and_then(|p| p.get("size"))
                        .and_then(Value::as_u64)
                })
                .sum();
            println!(
                "{}. {} [{}] ({:.1} GB)",
                i + 1,
                name,
                pack_names.join(" + "),
                size as f64 / 1e9
            );
        }
        prompt_choice("Choose bundle", choices.len())?
    };
    let (bundle_name, pack_names) = &choices[selected];

    let mut cdn_urls = cdn_urls_from(index.get("cdnList"));
    if cdn_urls.is_empty() {
        cdn_urls = ask_cdn_urls()?;
    }

    let packs = pack_names
        .iter()
        .map(|name| {
            let pack = packs.get(name).ok_or_else(|| {
                format!(
                    "Bundle '{}' references missing pack '{}'",
                    bundle_name, name
                )
            })?;
            pack_source(name, pack, &cdn_urls)
        })
        .collect::<Result<Vec<_>, _>>()?;

    println!(
        "{} Bundle {}: {}",
        Status::info(),
        bundle_name,
        pack_names.join(" + ")
    );
    Ok(Config { packs })
}

/// Legacy schema: `{ default|predownload: { cdnList, config: { baseUrl, indexFile } } }`.
fn config_from_legacy(index: &Value) -> Result<Config, String> {
    let has_default = index.get("default").is_some();
    let has_predownload = index.get("predownload").is_some();

    let selected_config = match (has_default, has_predownload) {
        (true, false) => "default",
        (false, true) => "predownload",
        (true, true) => {
            println!("{} Available configs:", Status::info());
            println!("1. default");
            println!("2. predownload");
            match prompt_choice("Choose config", 2)? {
                0 => "default",
                _ => "predownload",
            }
        }
        (false, false) => {
            return Err(
                "Unrecognized index.json: no resourcePacks, default or predownload".to_string(),
            );
        }
    };
    println!("{} Using {}.config", Status::info(), selected_config);

    let config_data = &index[selected_config];
    let base_config = config_data
        .get("config")
        .ok_or_else(|| format!("Missing config in {} response", selected_config))?;

    let mut cdn_urls = cdn_urls_from(config_data.get("cdnList"));
    if cdn_urls.is_empty() {
        let other = if selected_config == "default" {
            "predownload"
        } else {
            "default"
        };
        cdn_urls = cdn_urls_from(index.get(other).and_then(|o| o.get("cdnList")));
        if !cdn_urls.is_empty() {
            println!(
                "{} CDN list missing in '{}', using the one from '{}'.",
                Status::warning(),
                selected_config,
                other
            );
        }
    }
    if cdn_urls.is_empty() {
        cdn_urls = ask_cdn_urls()?;
    }

    Ok(Config {
        packs: vec![pack_source(selected_config, base_config, &cdn_urls)?],
    })
}

pub async fn get_config(client: &Client) -> Result<Config, String> {
    let mode = ask_download_mode(client)?;

    if mode == "custom" {
        return get_custom_config(client);
    }

    let selected_index_url = fetch_gist(client).await?;

    clear_screen();
    println!("{} Fetching download configuration...", Status::info());

    let index = fetch_json(client, &selected_index_url).await?;

    if index.get("resourcePacks").is_some() {
        config_from_resource_packs(&index)
    } else {
        config_from_legacy(&index)
    }
}

/// The upstream gist's live/os entry still serves the legacy schema, frozen at
/// 3.7.1 with no predownload; the launcher moved to `LIVE_OS_INDEX_URL`.
fn resolve_index_url(gist_data: &Value, category: &str, version: &str) -> Result<String, String> {
    match (category, version) {
        ("live", "os") => Ok(LIVE_OS_INDEX_URL.to_string()),
        _ => get_version(gist_data, category, version),
    }
}

pub async fn fetch_gist(client: &Client) -> Result<String, String> {
    let gist_data = fetch_json(client, INDEX_URL).await?;

    clear_screen();

    let entries = [
        ("live", "os", "Live - OS"),
        ("live", "cn", "Live - CN"),
        ("beta", "os", "Beta - OS"),
        ("beta", "cn", "Beta - CN"),
    ];

    println!("{} Available versions:", Status::info());

    for (i, (cat, ver, label)) in entries.iter().enumerate() {
        let index_url = resolve_index_url(&gist_data, cat, ver)?;

        let version_json = match fetch_json(client, &index_url).await {
            Ok(v) => v,
            Err(e) => {
                println!("{} Failed to fetch {}: {}", Status::warning(), index_url, e);
                continue;
            }
        };

        let (current, pre) = describe_versions(&version_json);
        let current = current.unwrap_or_else(|| "unknown".to_string());
        match pre {
            Some(pre) => println!("{}. {} ({}, predownload {})", i + 1, label, current, pre),
            None => println!("{}. {} ({})", i + 1, label, current),
        }
    }

    let selected = prompt_choice("Select version", entries.len())?;
    let (cat, ver, _) = entries[selected];
    resolve_index_url(&gist_data, cat, ver)
}

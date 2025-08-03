use clap::{Parser, Subcommand};
use futures::stream::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::time::timeout;

#[derive(Parser, Debug)]
#[command(author, version, about = "High-performance router benchmark tool", long_about = None)]
struct Args {
    /// Router URL
    #[arg(short, long, default_value = "http://0.0.0.0:8000")]
    url: String,

    /// Generate JSON report
    #[arg(long, default_value = "false")]
    json_report: bool,

    /// Output directory for reports
    #[arg(long, default_value = "./benchmark_reports")]
    report_dir: PathBuf,

    /// Subcommand to run
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run basic benchmark
    Basic {
        /// Number of total requests
        #[arg(short, long, default_value = "1000")]
        requests: usize,

        /// Concurrency level
        #[arg(short, long, default_value = "100")]
        concurrency: usize,

        /// Max new tokens
        #[arg(short, long, default_value = "50")]
        tokens: u32,

        /// Timeout per request in seconds
        #[arg(short = 'T', long, default_value = "60")]
        timeout: u64,
    },

    /// Run realistic benchmark with varied prompts and streaming
    Realistic {
        /// Number of total requests
        #[arg(short, long, default_value = "1000")]
        requests: usize,

        /// Concurrency level
        #[arg(short, long, default_value = "100")]
        concurrency: usize,

        /// Min tokens
        #[arg(long, default_value = "100")]
        min_tokens: u32,

        /// Max tokens
        #[arg(long, default_value = "500")]
        max_tokens: u32,

        /// Enable streaming
        #[arg(short, long, default_value = "true")]
        stream: bool,

        /// Timeout per request in seconds
        #[arg(short = 'T', long, default_value = "120")]
        timeout: u64,
    },

    /// Run progressive load test
    Progressive {
        /// Starting concurrency
        #[arg(long, default_value = "10")]
        start_concurrency: usize,

        /// Max concurrency
        #[arg(long, default_value = "1000")]
        max_concurrency: usize,

        /// Step size for increasing concurrency
        #[arg(long, default_value = "10")]
        step: usize,

        /// Requests per level
        #[arg(long, default_value = "100")]
        requests_per_level: usize,

        /// Timeout per request in seconds
        #[arg(short = 'T', long, default_value = "60")]
        timeout: u64,
    },

    /// Run sustained load test
    Sustained {
        /// Concurrency level
        #[arg(short, long, default_value = "100")]
        concurrency: usize,

        /// Duration in seconds
        #[arg(short, long, default_value = "60")]
        duration: u64,

        /// Max new tokens
        #[arg(short, long, default_value = "50")]
        tokens: u32,

        /// Enable streaming
        #[arg(short, long, default_value = "false")]
        stream: bool,

        /// Timeout per request in seconds
        #[arg(short = 'T', long, default_value = "60")]
        timeout: u64,
    },
}

#[derive(Serialize, Deserialize)]
struct BenchmarkReport {
    benchmark_type: String,
    start_time: String,
    end_time: String,
    duration_secs: f64,
    configuration: BenchmarkConfig,
    results: BenchmarkResults,
    system_info: SystemInfo,
}

#[derive(Serialize, Deserialize)]
struct BenchmarkConfig {
    url: String,
    total_requests: usize,
    concurrency: usize,
    timeout_secs: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    streaming: Option<bool>,
}

#[derive(Serialize, Deserialize)]
struct BenchmarkResults {
    successful_requests: u64,
    failed_requests: u64,
    success_rate: f64,
    throughput_rps: f64,
    response_times: ResponseTimeStats,
    #[serde(skip_serializing_if = "Option::is_none")]
    percentiles: Option<ResponseTimePercentiles>,
}

#[derive(Serialize, Deserialize)]
struct ResponseTimeStats {
    average_ms: f64,
    min_ms: u64,
    max_ms: u64,
}

#[derive(Serialize, Deserialize)]
struct ResponseTimePercentiles {
    p50_ms: f64,
    p75_ms: f64,
    p90_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
}

#[derive(Serialize, Deserialize)]
struct SystemInfo {
    rust_version: String,
    os: String,
    cpu_count: usize,
}

#[derive(Clone)]
struct BenchmarkStats {
    successful: Arc<AtomicU64>,
    failed: Arc<AtomicU64>,
    total_duration_ms: Arc<AtomicU64>,
    min_duration_ms: Arc<AtomicU64>,
    max_duration_ms: Arc<AtomicU64>,
    response_times: Arc<parking_lot::Mutex<Vec<u64>>>,
    start_time: Instant,
}

impl BenchmarkStats {
    fn new() -> Self {
        Self {
            successful: Arc::new(AtomicU64::new(0)),
            failed: Arc::new(AtomicU64::new(0)),
            total_duration_ms: Arc::new(AtomicU64::new(0)),
            min_duration_ms: Arc::new(AtomicU64::new(u64::MAX)),
            max_duration_ms: Arc::new(AtomicU64::new(0)),
            response_times: Arc::new(parking_lot::Mutex::new(Vec::new())),
            start_time: Instant::now(),
        }
    }

    fn record_success(&self, duration: Duration) {
        self.successful.fetch_add(1, Ordering::Relaxed);
        let duration_ms = duration.as_millis() as u64;
        self.total_duration_ms
            .fetch_add(duration_ms, Ordering::Relaxed);

        // Store response time for percentile calculation
        self.response_times.lock().push(duration_ms);

        // Update min
        let mut current_min = self.min_duration_ms.load(Ordering::Relaxed);
        while duration_ms < current_min {
            match self.min_duration_ms.compare_exchange(
                current_min,
                duration_ms,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_min = x,
            }
        }

        // Update max
        let mut current_max = self.max_duration_ms.load(Ordering::Relaxed);
        while duration_ms > current_max {
            match self.max_duration_ms.compare_exchange(
                current_max,
                duration_ms,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_max = x,
            }
        }
    }

    fn record_failure(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    fn calculate_percentiles(&self) -> Option<ResponseTimePercentiles> {
        let mut times = self.response_times.lock().clone();
        if times.is_empty() {
            return None;
        }

        times.sort_unstable();
        let len = times.len();

        Some(ResponseTimePercentiles {
            p50_ms: times[len * 50 / 100] as f64,
            p75_ms: times[len * 75 / 100] as f64,
            p90_ms: times[len * 90 / 100] as f64,
            p95_ms: times[len * 95 / 100] as f64,
            p99_ms: times.get(len * 99 / 100).copied().unwrap_or(times[len - 1]) as f64,
        })
    }

    fn generate_report(
        &self,
        benchmark_type: &str,
        config: BenchmarkConfig,
        total_requests: usize,
    ) -> BenchmarkReport {
        let elapsed = self.start_time.elapsed();
        let successful = self.successful.load(Ordering::Relaxed);
        let failed = self.failed.load(Ordering::Relaxed);
        let total_duration_ms = self.total_duration_ms.load(Ordering::Relaxed);
        let min_duration_ms = self.min_duration_ms.load(Ordering::Relaxed);
        let max_duration_ms = self.max_duration_ms.load(Ordering::Relaxed);

        let avg_duration = if successful > 0 {
            total_duration_ms as f64 / successful as f64
        } else {
            0.0
        };

        let throughput = successful as f64 / elapsed.as_secs_f64();
        let success_rate = (successful as f64 / total_requests as f64) * 100.0;

        BenchmarkReport {
            benchmark_type: benchmark_type.to_string(),
            start_time: chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
            end_time: chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
            duration_secs: elapsed.as_secs_f64(),
            configuration: config,
            results: BenchmarkResults {
                successful_requests: successful,
                failed_requests: failed,
                success_rate,
                throughput_rps: throughput,
                response_times: ResponseTimeStats {
                    average_ms: avg_duration,
                    min_ms: if min_duration_ms == u64::MAX {
                        0
                    } else {
                        min_duration_ms
                    },
                    max_ms: max_duration_ms,
                },
                percentiles: self.calculate_percentiles(),
            },
            system_info: SystemInfo {
                rust_version: "1.70+".to_string(), // or use rustc_version crate
                os: std::env::consts::OS.to_string(),
                cpu_count: num_cpus::get(),
            },
        }
    }

    fn print_summary(&self, total_requests: usize) {
        let elapsed = self.start_time.elapsed();
        let successful = self.successful.load(Ordering::Relaxed);
        let failed = self.failed.load(Ordering::Relaxed);
        let total_duration_ms = self.total_duration_ms.load(Ordering::Relaxed);
        let min_duration_ms = self.min_duration_ms.load(Ordering::Relaxed);
        let max_duration_ms = self.max_duration_ms.load(Ordering::Relaxed);

        println!("\n{}", "=".repeat(50));
        println!("Benchmark Results");
        println!("{}", "=".repeat(50));
        println!("Total Time: {:.2}s", elapsed.as_secs_f64());
        println!("Total Requests: {}", total_requests);
        println!("Successful Requests: {}", successful);
        println!("Failed Requests: {}", failed);
        println!(
            "Success Rate: {:.2}%",
            (successful as f64 / total_requests as f64) * 100.0
        );

        if successful > 0 {
            let avg_duration = total_duration_ms as f64 / successful as f64;
            println!("\nResponse Time Statistics:");
            println!("  Average: {:.2}ms", avg_duration);
            println!("  Min: {:.2}ms", min_duration_ms);
            println!("  Max: {:.2}ms", max_duration_ms);

            let throughput = successful as f64 / elapsed.as_secs_f64();
            println!("\nThroughput: {:.2} req/s", throughput);
        }
    }
}

async fn send_request(
    client: &Client,
    url: &str,
    body: serde_json::Value,
    timeout_duration: Duration,
    stats: &BenchmarkStats,
) {
    let start = Instant::now();

    match timeout(timeout_duration, client.post(url).json(&body).send()).await {
        Ok(Ok(response)) => {
            if response.status().is_success() {
                // Consume the body to ensure the request completes
                let _ = response.bytes().await;
                stats.record_success(start.elapsed());
            } else {
                stats.record_failure();
            }
        }
        Ok(Err(_)) | Err(_) => {
            stats.record_failure();
        }
    }
}

async fn send_streaming_request(
    client: &Client,
    url: &str,
    body: serde_json::Value,
    timeout_duration: Duration,
    stats: &BenchmarkStats,
) {
    let start = Instant::now();

    match timeout(timeout_duration, client.post(url).json(&body).send()).await {
        Ok(Ok(response)) => {
            if response.status().is_success() {
                // Consume the streaming body
                let mut stream = response.bytes_stream();
                while let Ok(Some(_)) = timeout(Duration::from_secs(1), stream.next()).await {
                    // Just consume the chunks
                }
                stats.record_success(start.elapsed());
            } else {
                stats.record_failure();
            }
        }
        Ok(Err(_)) | Err(_) => {
            stats.record_failure();
        }
    }
}

fn generate_basic_request(request_id: usize, max_tokens: u32) -> serde_json::Value {
    json!({
        "text": format!("Hello world, this is request #{}", request_id),
        "max_new_tokens": max_tokens,
        "stream": false
    })
}

fn generate_realistic_request(
    request_id: usize,
    min_tokens: u32,
    max_tokens: u32,
    stream: bool,
) -> serde_json::Value {
    use rand::Rng;
    let mut rng = rand::thread_rng();

    let prompts = vec![
        "Explain the concept of machine learning in simple terms.",
        "Write a short story about a robot learning to paint.",
        "What are the key differences between TCP and UDP protocols?",
        "Describe the process of photosynthesis step by step.",
        "How does blockchain technology work?",
        "Explain quantum computing to a 10-year-old.",
        "What are the main causes of climate change?",
        "Write a poem about artificial intelligence.",
        "How does the human immune system work?",
        "Explain the theory of relativity in layman's terms.",
    ];

    let prompt = prompts[request_id % prompts.len()];
    let tokens = rng.gen_range(min_tokens..=max_tokens);
    let temperature = rng.gen_range(0.5..=1.0);
    let top_p = rng.gen_range(0.9..=1.0);

    json!({
        "text": format!("{} (Request #{})", prompt, request_id),
        "max_new_tokens": tokens,
        "stream": stream,
        "temperature": temperature,
        "top_p": top_p,
        "presence_penalty": rng.gen_range(0.0..=0.2),
        "frequency_penalty": rng.gen_range(0.0..=0.2),
    })
}

fn save_report(
    report: &BenchmarkReport,
    report_dir: &PathBuf,
    json_report: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create report directory if it doesn't exist
    fs::create_dir_all(report_dir)?;

    let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
    let report_name = format!(
        "{}_{}",
        report.benchmark_type.to_lowercase().replace(" ", "_"),
        timestamp
    );

    if json_report {
        let json_path = report_dir.join(format!("{}.json", report_name));
        let json_content = serde_json::to_string_pretty(&report)?;
        fs::write(&json_path, json_content)?;
        println!("\nReport saved to: {}", json_path.display());
    }

    // Always save a human-readable summary
    let summary_path = report_dir.join(format!("{}.txt", report_name));
    let summary = format!(
        "Benchmark Report: {}\n\
        ==========================================\n\
        Start Time: {}\n\
        Duration: {:.2}s\n\
        \n\
        Configuration:\n\
        - URL: {}\n\
        - Total Requests: {}\n\
        - Concurrency: {}\n\
        - Timeout: {}s\n\
        {}\
        \n\
        Results:\n\
        - Successful: {} ({:.2}%)\n\
        - Failed: {}\n\
        - Throughput: {:.2} req/s\n\
        \n\
        Response Times:\n\
        - Average: {:.2}ms\n\
        - Min: {}ms\n\
        - Max: {}ms\n\
        {}\
        \n\
        System Info:\n\
        - OS: {}\n\
        - CPU Count: {}\n",
        report.benchmark_type,
        report.start_time,
        report.duration_secs,
        report.configuration.url,
        report.configuration.total_requests,
        report.configuration.concurrency,
        report.configuration.timeout_secs,
        if let Some(streaming) = report.configuration.streaming {
            format!("- Streaming: {}\n", streaming)
        } else {
            String::new()
        },
        report.results.successful_requests,
        report.results.success_rate,
        report.results.failed_requests,
        report.results.throughput_rps,
        report.results.response_times.average_ms,
        report.results.response_times.min_ms,
        report.results.response_times.max_ms,
        if let Some(ref p) = report.results.percentiles {
            format!(
                "\nPercentiles:\n\
                - P50: {:.2}ms\n\
                - P75: {:.2}ms\n\
                - P90: {:.2}ms\n\
                - P95: {:.2}ms\n\
                - P99: {:.2}ms",
                p.p50_ms, p.p75_ms, p.p90_ms, p.p95_ms, p.p99_ms
            )
        } else {
            String::new()
        },
        report.system_info.os,
        report.system_info.cpu_count,
    );

    fs::write(&summary_path, summary)?;
    println!("Summary saved to: {}", summary_path.display());

    Ok(())
}

async fn run_basic_benchmark(
    url: String,
    requests: usize,
    concurrency: usize,
    tokens: u32,
    timeout_secs: u64,
    report_dir: PathBuf,
    json_report: bool,
) {
    println!("Running basic benchmark:");
    println!("  URL: {}", url);
    println!("  Requests: {}", requests);
    println!("  Concurrency: {}", concurrency);
    println!("  Max tokens: {}", tokens);
    println!("  Timeout: {}s", timeout_secs);

    let client = Client::builder()
        .pool_max_idle_per_host(concurrency)
        .build()
        .expect("Failed to create HTTP client");

    let stats = BenchmarkStats::new();
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let progress = ProgressBar::new(requests as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap()
            .progress_chars("##-"),
    );

    let endpoint = format!("{}/generate", url);
    let timeout_duration = Duration::from_secs(timeout_secs);

    let tasks = (0..requests).map(|i| {
        let client = client.clone();
        let endpoint = endpoint.clone();
        let stats = stats.clone();
        let semaphore = semaphore.clone();
        let progress = progress.clone();

        tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            let body = generate_basic_request(i, tokens);
            send_request(&client, &endpoint, body, timeout_duration, &stats).await;
            progress.inc(1);
        })
    });

    futures::future::join_all(tasks).await;
    progress.finish_with_message("Complete");

    stats.print_summary(requests);

    // Generate and save report
    let config = BenchmarkConfig {
        url: url.clone(),
        total_requests: requests,
        concurrency,
        timeout_secs,
        max_tokens: Some(tokens),
        min_tokens: None,
        streaming: Some(false),
    };

    let report = stats.generate_report("Basic Benchmark", config, requests);
    if let Err(e) = save_report(&report, &report_dir, json_report) {
        eprintln!("Failed to save report: {}", e);
    }
}

async fn run_realistic_benchmark(
    url: String,
    requests: usize,
    concurrency: usize,
    min_tokens: u32,
    max_tokens: u32,
    stream: bool,
    timeout_secs: u64,
    report_dir: PathBuf,
    json_report: bool,
) {
    println!("Running realistic benchmark:");
    println!("  URL: {}", url);
    println!("  Requests: {}", requests);
    println!("  Concurrency: {}", concurrency);
    println!("  Token range: {}-{}", min_tokens, max_tokens);
    println!("  Streaming: {}", stream);
    println!("  Timeout: {}s", timeout_secs);

    let client = Client::builder()
        .pool_max_idle_per_host(concurrency)
        .build()
        .expect("Failed to create HTTP client");

    let stats = BenchmarkStats::new();
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let progress = ProgressBar::new(requests as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap()
            .progress_chars("##-"),
    );

    let endpoint = format!("{}/generate", url);
    let timeout_duration = Duration::from_secs(timeout_secs);

    let tasks = (0..requests).map(|i| {
        let client = client.clone();
        let endpoint = endpoint.clone();
        let stats = stats.clone();
        let semaphore = semaphore.clone();
        let progress = progress.clone();

        tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            let body = generate_realistic_request(i, min_tokens, max_tokens, stream);

            if stream {
                send_streaming_request(&client, &endpoint, body, timeout_duration, &stats).await;
            } else {
                send_request(&client, &endpoint, body, timeout_duration, &stats).await;
            }

            progress.inc(1);
        })
    });

    futures::future::join_all(tasks).await;
    progress.finish_with_message("Complete");

    stats.print_summary(requests);

    // Generate and save report
    let config = BenchmarkConfig {
        url: url.clone(),
        total_requests: requests,
        concurrency,
        timeout_secs,
        max_tokens: Some(max_tokens),
        min_tokens: Some(min_tokens),
        streaming: Some(stream),
    };

    let report = stats.generate_report("Realistic Benchmark", config, requests);
    if let Err(e) = save_report(&report, &report_dir, json_report) {
        eprintln!("Failed to save report: {}", e);
    }
}

async fn run_progressive_benchmark(
    url: String,
    start_concurrency: usize,
    max_concurrency: usize,
    step: usize,
    requests_per_level: usize,
    timeout_secs: u64,
    report_dir: PathBuf,
    json_report: bool,
) {
    println!("Running progressive load test:");
    println!("  URL: {}", url);
    println!(
        "  Concurrency range: {} to {} (step: {})",
        start_concurrency, max_concurrency, step
    );
    println!("  Requests per level: {}", requests_per_level);

    let mut current_concurrency = start_concurrency;

    while current_concurrency <= max_concurrency {
        println!("\n{}", "-".repeat(50));
        println!("Testing with concurrency: {}", current_concurrency);

        let client = Client::builder()
            .pool_max_idle_per_host(current_concurrency)
            .build()
            .expect("Failed to create HTTP client");

        let stats = BenchmarkStats::new();
        let semaphore = Arc::new(Semaphore::new(current_concurrency));
        let endpoint = format!("{}/generate", url);
        let timeout_duration = Duration::from_secs(timeout_secs);

        let tasks = (0..requests_per_level).map(|i| {
            let client = client.clone();
            let endpoint = endpoint.clone();
            let stats = stats.clone();
            let semaphore = semaphore.clone();

            tokio::spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();
                let body = generate_basic_request(i, 50);
                send_request(&client, &endpoint, body, timeout_duration, &stats).await;
            })
        });

        futures::future::join_all(tasks).await;

        let successful = stats.successful.load(Ordering::Relaxed);
        let failed = stats.failed.load(Ordering::Relaxed);
        let success_rate = (successful as f64 / requests_per_level as f64) * 100.0;

        println!("  Success rate: {:.2}%", success_rate);
        println!("  Failed requests: {}", failed);

        if success_rate < 95.0 {
            println!("\nStopping test - success rate dropped below 95%");
            break;
        }

        current_concurrency += step;
    }
}

async fn run_sustained_benchmark(
    url: String,
    concurrency: usize,
    duration_secs: u64,
    tokens: u32,
    stream: bool,
    timeout_secs: u64,
    report_dir: PathBuf,
    json_report: bool,
) {
    println!("Running sustained load test:");
    println!("  URL: {}", url);
    println!("  Concurrency: {}", concurrency);
    println!("  Duration: {}s", duration_secs);
    println!("  Max tokens: {}", tokens);
    println!("  Streaming: {}", stream);

    let client = Client::builder()
        .pool_max_idle_per_host(concurrency)
        .build()
        .expect("Failed to create HTTP client");

    let stats = BenchmarkStats::new();
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let endpoint = format!("{}/generate", url);
    let timeout_duration = Duration::from_secs(timeout_secs);

    let start_time = Instant::now();
    let duration = Duration::from_secs(duration_secs);

    let progress = ProgressBar::new(duration_secs);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len}s")
            .unwrap()
            .progress_chars("##-"),
    );

    // Start progress updater
    let progress_clone = progress.clone();
    let progress_handle = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            progress_clone.inc(1);
            if progress_clone.position() >= duration_secs {
                break;
            }
        }
    });

    let mut request_id = 0;
    let mut handles = vec![];

    while start_time.elapsed() < duration {
        if handles.len() >= concurrency {
            // Wait for some requests to complete
            if !handles.is_empty() {
                let handle = handles.remove(0);
                let _ = handle.await;
            }
        }

        let client = client.clone();
        let endpoint = endpoint.clone();
        let stats = stats.clone();
        let semaphore = semaphore.clone();
        let req_id = request_id;
        request_id += 1;

        let handle = tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            let body = if stream {
                generate_realistic_request(req_id, tokens / 2, tokens, true)
            } else {
                generate_basic_request(req_id, tokens)
            };

            if stream {
                send_streaming_request(&client, &endpoint, body, timeout_duration, &stats).await;
            } else {
                send_request(&client, &endpoint, body, timeout_duration, &stats).await;
            }
        });

        handles.push(handle);
    }

    // Wait for remaining requests
    futures::future::join_all(handles).await;
    progress_handle.abort();
    progress.finish();

    let total_requests =
        stats.successful.load(Ordering::Relaxed) + stats.failed.load(Ordering::Relaxed);
    stats.print_summary(total_requests as usize);

    // Generate and save report
    let config = BenchmarkConfig {
        url: url.clone(),
        total_requests: total_requests as usize,
        concurrency,
        timeout_secs,
        max_tokens: Some(tokens),
        min_tokens: if stream { Some(tokens / 2) } else { None },
        streaming: Some(stream),
    };

    let report = stats.generate_report("Sustained Load Test", config, total_requests as usize);
    if let Err(e) = save_report(&report, &report_dir, json_report) {
        eprintln!("Failed to save report: {}", e);
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    match args.command {
        Commands::Basic {
            requests,
            concurrency,
            tokens,
            timeout,
        } => {
            run_basic_benchmark(
                args.url,
                requests,
                concurrency,
                tokens,
                timeout,
                args.report_dir,
                args.json_report,
            )
            .await;
        }
        Commands::Realistic {
            requests,
            concurrency,
            min_tokens,
            max_tokens,
            stream,
            timeout,
        } => {
            run_realistic_benchmark(
                args.url,
                requests,
                concurrency,
                min_tokens,
                max_tokens,
                stream,
                timeout,
                args.report_dir,
                args.json_report,
            )
            .await;
        }
        Commands::Progressive {
            start_concurrency,
            max_concurrency,
            step,
            requests_per_level,
            timeout,
        } => {
            run_progressive_benchmark(
                args.url,
                start_concurrency,
                max_concurrency,
                step,
                requests_per_level,
                timeout,
                args.report_dir,
                args.json_report,
            )
            .await;
        }
        Commands::Sustained {
            concurrency,
            duration,
            tokens,
            stream,
            timeout,
        } => {
            run_sustained_benchmark(
                args.url,
                concurrency,
                duration,
                tokens,
                stream,
                timeout,
                args.report_dir,
                args.json_report,
            )
            .await;
        }
    }
}
